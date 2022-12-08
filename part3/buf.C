#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}

// Flushes dirty pages and deallocates the buffer pool and bufDesc table (Destructor).
BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid && tmpbuf->dirty) {   // iterate through buffer pool, and write dirty pages to disk

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i])); // write page to file
        }
    }
//    delete hashTable;
    delete [] bufTable;
    delete [] bufPool;
}

/*
 * Allocates a free frame using the clock algorithm;
 * if necessary, writing a dirty page back to disk.
 * Returns BUFFEREXCEEDED if all buffer frames are pinned, UNIXERR if the call to the I/O layer returned an error when a dirty page was being written to disk and OK otherwise.
 * This private method will get called by the readPage() and allocPage() methods described below.
 */
const Status BufMgr::allocBuf(int & frame)
{
    // check BUFFEREXCEEDED

    bool exceeded = true;
    for(int i = 0; i < numBufs; i++){
        if (bufTable[i].pinCnt == 0){ // current buffer frames are not pinned
            exceeded = false;
            break;
        }
    }
    if(exceeded){
        return BUFFEREXCEEDED;
    }

    while(true){
        advanceClock();
        // if the current frame is free to use
        if(!bufTable[clockHand].valid){
            frame = clockHand;
            return OK;
        }

        // if refbit is set, unset it and restart the loop
        if(bufTable[clockHand].refbit == true){
            bufTable[clockHand].refbit = false; // clear refbit
            continue;
        }

        // if page is pinned (pinCnt != 0), restart the loop. else we have found the frame
        if(bufTable[clockHand].pinCnt){
            continue;
        }

        // if dirty, we need to write back data first
        if(bufTable[clockHand].dirty){
            Status status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &(bufPool[clockHand])); // flush page back to disk
            if (status != OK){
                return UNIXERR;
            }
        }

        // remove old hash table entry
        hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);


        // return frame
        frame = clockHand;
        bufTable[frame].Clear();
        return OK;
    }

}


const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, PageNo, frameNo);
    if(status != OK){
        status = allocBuf(frameNo);
        if(status != OK){
            return status;
        }
        bufStats.diskreads++;
        // read the page into the bufPool
        status = file->readPage(PageNo,&bufPool[frameNo]); //FIX ME?
        if(status != OK){
            return status;
        }
        status = hashTable->insert(file, PageNo, frameNo);
        if(status != OK){
            return status;
        }
        bufTable[frameNo].Set(file, PageNo);
        // set the page corresponding to the address in bufPool
        page = &bufPool[frameNo];
    }else {
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &(bufPool[frameNo]);
    }


    return status;
}


/*
 * Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets the dirty bit.
 * Returns OK if no errors occurred, HASHNOTFOUND if the page is not in the buffer pool hash table, PAGENOTPINNED if the pin count is already 0.
 */
const Status BufMgr::unPinPage(File* file, const int PageNo,
                               const bool dirty)
{

    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if(status == OK){
        if(bufTable[frameNo].pinCnt == 0){
            return PAGENOTPINNED;
        }else{
            bufTable[frameNo].pinCnt = bufTable[frameNo].pinCnt - 1;
            if(dirty){
                bufTable[frameNo].dirty = true;
            }
        }
    }else if(status == HASHNOTFOUND){
        return status;
    }

    return status;
}


/*
 *  The first step is to allocate an empty page in the specified file by invoking the file->allocatePage() method.
 *  This method will return the page number of the newly allocated page.
 *  Then allocBuf() is called to obtain a buffer pool frame.  Next, an entry is inserted into the hash table and Set() is invoked on the frame to set it up properly.
 *  The method returns both the page number of the newly allocated page to the caller via the pageNo parameter and a pointer to the buffer frame allocated for the page via the page parameter.
 *  Returns OK if no errors occurred, UNIXERR if a Unix error occurred, BUFFEREXCEEDED if all buffer frames are pinned and HASHTBLERROR if a hash table error occurred.
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)
{
    int frameNo = 0;
    Status status = file->allocatePage(pageNo);
    if(status != OK){
        return status;
    }
    status = allocBuf(frameNo);
    if(status != OK){
        return status;
    }
    status = hashTable->insert(file, pageNo, frameNo);
    if(status != OK){
        return status;
    }
    bufTable[frameNo].Set(file, pageNo);

    page = &(bufPool[frameNo]);

    return status;

}

const Status BufMgr::disposePage(File* file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file)
{
    Status status;

    for (int i = 0; i < numBufs; i++) {
        BufDesc* tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file) {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file,tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}


void BufMgr::printSelf(void)
{
    BufDesc* tmpbuf;

    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


