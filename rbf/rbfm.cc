#include <stdio.h>
#include <stdlib.h> // for malloc
#include <string.h> // for memset
#include "rbfm.h"
#include <iostream>
using namespace std;

const RC success = 0;
const RC fail = -1;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
    pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager() {
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return pfm->createFile(fileName.c_str());
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return pfm->destroyFile(fileName.c_str());
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return pfm->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

    int len = 0;
    int l_offset = 0;
    //calculate length of data block.
    //the data block length eqauls to
    //[total data length][1st element relative index][1st element length][2nd RI][2NDEL]..[nri][nnd][data1][data2]..[dataN]
    //total length = length of data + 4 + 4n * 2
    // //printf("there are %d attributes\n", recordDescriptor.size());
    for (int i = 0; i < (int) recordDescriptor.size(); i++) {
        if (recordDescriptor[i].type == TypeVarChar) {
            int strLen;
            memcpy(&strLen, (char*) data + l_offset, sizeof (int));
            l_offset += sizeof (int) +strLen;
            len += strLen + sizeof (int);
        } else {
            l_offset += sizeof (int);
            len = len + recordDescriptor[i].length;
        }
    }
    int dataLen = len; // record the actual length of the data
    len = len + sizeof (int) + (sizeof (int) * recordDescriptor.size() * 2);
    //printf("total length is: %d\n", len);
    int pageNum = locatePageInDirectory(fileHandle, len);

    // Success: returns a non negative page number. fail: return -1.
    if (pageNum < 0) {
        printf("unable to locate a page in directory");
        return fail;
    }
    //printf("found an empty page number %d\n", pageNum);

    //INSERT LOGIC GOES HERE
    char* datPage = (char*) malloc(PAGE_SIZE);

    // read the page for inserting
    if (fileHandle.readPage(pageNum, datPage) == fail) {
        ////printf("unable to read data page %d\n", pageNum);
        return fail;
    }

    int offset = 0; // the relative pointer of the first available byte
    int numOfDataBlocks = 0;

    // if not a new page, get the page information from the last two bytes
    if ((unsigned) pageNum != fileHandle.getNumberOfPages()) {
        memcpy(&offset, datPage + PAGE_SIZE - sizeof (int), sizeof (int));
        memcpy(&numOfDataBlocks, datPage + PAGE_SIZE - 2 * sizeof (int), sizeof (int));
    }

    // number of data blocks increases by 1 after inserting
    // record the relative index of the new data block
    numOfDataBlocks++;
    memcpy(datPage + PAGE_SIZE - 2 * sizeof (int), &numOfDataBlocks, sizeof (int));
    memcpy(datPage + PAGE_SIZE - (numOfDataBlocks + 2) * sizeof (int), &offset, sizeof (int));

    // the position of the insertion
    rid.pageNum = pageNum;
    rid.slotNum = numOfDataBlocks;

    // the relative index for current element
    int relativeIndex = sizeof (int) + (sizeof (int) * recordDescriptor.size() * 2);

    // write in the actual length of the data
    memcpy(datPage + offset, &dataLen, sizeof (int));
    offset += sizeof (int);

    // write in the relative index and length of the n elements
    l_offset = 0;
    for (int i = 0; i < (int) recordDescriptor.size(); i++) {
        if (recordDescriptor[i].type == TypeVarChar) {
            // write in the relative index of the string element
            memcpy(datPage + offset, &relativeIndex, sizeof (int));
            offset += sizeof (int);

            // calculate the length of the string element
            // including the byte which stores the length of the string
            int strLen;
            memcpy(&strLen, (char*) data + l_offset, sizeof (int));
            strLen += sizeof (int);

            // write in the length of the string element
            memcpy(datPage + offset, &strLen, sizeof (int));
            offset += sizeof (int);

            // move the relative pointer to the next element
            l_offset += strLen;
            relativeIndex += strLen;
        } else {
            // write in the relative index and length of the element
            memcpy(datPage + offset, &relativeIndex, sizeof (int));
            offset += sizeof (int);
            memcpy(datPage + offset, &recordDescriptor[i].length, sizeof (int));
            offset += sizeof (int);

            // move the relative pointer to the next element
            l_offset += recordDescriptor[i].length;
            relativeIndex += recordDescriptor[i].length;
        }
    }
    

    // write in the whole data
    memcpy(datPage + offset, data, dataLen);
    offset += dataLen;

    // move the space pointer to the first available byte
    memcpy(datPage + PAGE_SIZE - sizeof (int), &offset, sizeof (int));

    // write back the data page
    fileHandle.writePage(pageNum, datPage);

    // free the memory
    free(datPage);

    return success;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

    //speical cases:
    // read a directory page should be forbidden 0. 1025 2050 etc..
    // read a slot number that not in the page should return fail, .i.e.  sid= -1, or 99999
    // the slot number should be start with 1, because the empty page slot is default to 0.
    //if I am reading pid =1, sid = 0, from an emtpy page, it will consider as exists and cause error.
    //printf("page number: %d\n", rid.pageNum);
    if (rid.pageNum % 1025 == 0 || rid.pageNum < 0) { // not a data page
        printf("not a valid data page\n");
        return fail;
    }
    char* datPage = (char*) malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, datPage) == fail) {
        //printf("unable to read data page\n");
        return fail;
    }
    int numOfDataBlocks;
    memcpy(&numOfDataBlocks, datPage + PAGE_SIZE - 2 * sizeof (int), sizeof (int));
    //printf("number of blocks is: %d\n", numOfDataBlocks);
    if (rid.slotNum <= 0 || rid.slotNum > (unsigned) numOfDataBlocks) { // not a legal slot number
        printf("not a valid slot: slot %d in page %d with %d blocks\n", rid.slotNum, rid.pageNum, numOfDataBlocks);
        return fail;
    }

    int offset; // the relative address of the first byte of the data block
    memcpy(&offset, datPage + PAGE_SIZE - (rid.slotNum + 2) * sizeof (int), sizeof (int));
    
    if (offset == -1) {    //this is a deleted record.
        free(datPage);
        // printf("this is a deleted record\n");
        return fail;
    }
    int dataLen; // the actual length of the record
    memcpy(&dataLen, datPage + offset, sizeof (int));
    
    if (dataLen == -2) { // this is a deleted record
        free(datPage);
        // printf("this is a deleted record\n");
        return fail;
    }
    if (dataLen == -1) { // this is a tombstone
        
        RID newRID;
        memcpy(&newRID.pageNum, datPage + offset + sizeof (int), sizeof (int));
        memcpy(&newRID.slotNum, datPage + offset + sizeof (int) * 2, sizeof (int));
        // printf("it is redirected to: %d %d\n", newRID.pageNum, newRID.slotNum);
        RC rc = readRecord(fileHandle, recordDescriptor, newRID, data);
        free(datPage);
        return rc;
    }
    
    int l_offset = 0;
    char* buffer = (char*)malloc(1000);
    for (unsigned i = 0; i < recordDescriptor.size(); i++) {
        if (recordDescriptor[i].length == 0) continue;
        int attrStart;
        int attrLen;
        memcpy(&attrStart, datPage + offset + sizeof(int) * (i * 2 + 1), sizeof(int));
        memcpy(&attrLen, datPage + offset + sizeof(int) * (i * 2 + 2), sizeof(int));
        
        memcpy(buffer + l_offset, datPage + offset + attrStart, attrLen);
        l_offset += attrLen;
    }
    memcpy(data, buffer, dataLen);
    free(buffer);
    
//    int startOfRecord; // the first byte of the record
//    memcpy(&startOfRecord, datPage + offset + sizeof (int), sizeof (int));
//    // read the record to data
//    memcpy(data, datPage + offset + startOfRecord, dataLen);
    // free the memory
    free(datPage);
    return success;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {

    int offset = 0;
    for (unsigned i = 0; i < recordDescriptor.size(); i++) {
        if (recordDescriptor[i].type == TypeVarChar) {
            int strLen;
            memcpy(&strLen, (char *) data + offset, sizeof (int));
            offset += sizeof (int);
            
            char* buffer = (char*)malloc(100);
            memcpy(buffer, (char*)data + offset, strLen);
            offset += strLen;
            printf("%s:%s\n", recordDescriptor[i].name.c_str(), buffer);

        } else if (recordDescriptor[i].type == TypeReal) {
            float val;
            memcpy(&val, (char *) data + offset, sizeof (int));
            offset += sizeof (int);

            printf("%s:%f\n", recordDescriptor[i].name.c_str(), val);
        } else {
            int val;
            memcpy(&val, (char *) data + offset, sizeof (int));
            offset += sizeof (int);

            printf("%s:%d\n", recordDescriptor[i].name.c_str(), val);
        }
    }
    return success;
}

int RecordBasedFileManager::locatePageInDirectory(FileHandle &fileHandle, int length) {

    int pageNum = -1;
    int freeLen = 0;
    char* dirPage = (char*) malloc(PAGE_SIZE * sizeof (char));
    memset(dirPage, 0, PAGE_SIZE);

    int totalPage = fileHandle.getNumberOfPages();
    if (totalPage == 0) {
        //the file has not been formatted yet.
        freeLen = PAGE_SIZE - length - (sizeof (int) * 3);
        memset(dirPage, -1, PAGE_SIZE);
        memcpy(dirPage, &freeLen, sizeof (int));
        fileHandle.appendPage(dirPage);

        void* page = malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        fileHandle.appendPage(page);
        ////printf("the page is appended\n");
        free(dirPage);
        free(page);
        return 1;
    }

    //loop all directory page in the file, each page directory page should be the first
    //of next follow 1024 data pages.
    //Directory page :0, [1-1024]
    //Directory page : 1025, [1026 - 2049]
    //Directory page: 2050, [2051 - 3074] etc

    for (int i = 0; i < totalPage; i += 1025) {
        
        fileHandle.readPage(i, dirPage);

        int pageSpace;
        for (int j = 0; j < (int) (PAGE_SIZE / sizeof (int)); j++) {
            memcpy(&pageSpace, dirPage + (j * sizeof (int)), sizeof (int));
            if (pageSpace == -1) {
                //fist, modify directory
                //for an empty page, length is page size - data length - two of int - one int data slot
                //first for slot pointer
                //second for the slot number.
                // usable space for a page of a 1 length record is 4096 - 4 - 12
                freeLen = PAGE_SIZE - length - (sizeof (int) * 3);
                ////printf("usable space is :%d (after current record)where directory page Number = %d\n", freeLen, i);
                pageNum = i + j + 1;

                //j is the slot number * size of int to locate place to write.
                memcpy(dirPage + (j * sizeof (int)), &freeLen, sizeof (int));
                fileHandle.writePage(i, dirPage);

                void* page = malloc(PAGE_SIZE);
                memset(page, 0, PAGE_SIZE);
                fileHandle.appendPage(page);
                ////printf("appended an empty page into data file\n");

                free(dirPage);
                free(page);
                return pageNum;

            } else if (pageSpace >= (int) (length + (sizeof (int)))) { // extra 1 slot for count space.
                //found a space in page.
                pageNum = i + j + 1;

                //calculate new free space
                freeLen = pageSpace - length - (sizeof (int)); // extra integer for the slot
                ////printf("usable space is :%d where directory page Number = %d\n", freeLen, i);

                //write new free space to the page
                memcpy(dirPage + (j * sizeof (int)), &freeLen, sizeof (int));
                fileHandle.writePage(i, dirPage);

                free(dirPage);

                return pageNum;
            }
        }
    }
    //all pages are full in directory, append a new directory page, and append a data page.
    freeLen = PAGE_SIZE - length - (sizeof (int) * 3);
    ////printf("all pages are full, append a directory page and a data page");
    memset(dirPage, -1, PAGE_SIZE);
    memcpy(dirPage, &freeLen, sizeof (int));
    fileHandle.appendPage(dirPage);

    void* page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    fileHandle.appendPage(page);

    free(page);
    free(dirPage);
    return totalPage + 1; //bug, should add 1 instead of 2. // found by pressure test.



}

int RecordBasedFileManager::getPageSpaceInDirectory(FileHandle &fileHandle, int pageNum) {

    int space = -1;
    int dir_page = pageNum / 1025;
    dir_page = dir_page * 1025;


    void* page = malloc(PAGE_SIZE);
    if (fileHandle.readPage(dir_page, page) == fail) {
        return fail;
    }

    int offset = (pageNum % 1025 - 1) * sizeof (int);

    memcpy(&space, (char*) page + offset, sizeof (int));

    return space;
}

RC RecordBasedFileManager::setPageSpaceInDirectory(FileHandle &fileHandle, int pageNum, int space) {
    // in directory, each page takes 4 bytes(one integer)
    //each directory page holds 1024 data pages space information.
    int dir_page = pageNum / 1025;
    dir_page = dir_page * 1025;

    void* page = malloc(PAGE_SIZE);
    if (fileHandle.readPage(dir_page, page) == fail) {
        ////printf("unable to read the page contain data page space");
        return fail;
    }

    int offset = (pageNum % 1025 - 1) * sizeof(int);

    memcpy((char*) page + offset, &space, sizeof (int));
    if (fileHandle.writePage(dir_page, page) == fail) {
        ////printf("unable to write back the page");
        return fail;
    }

    free(page);

    return success;
}

RC RecordBasedFileManager::deleteRecord(FileHandle& fileHandle, const vector<Attribute>& recordDescriptor, const RID& rid) {
    
    if (rid.pageNum % 1025 == 0 || rid.pageNum < 0) return fail; // not a data page

    char* datPage = (char*) malloc(PAGE_SIZE * sizeof (char));
    memset(datPage, 0, PAGE_SIZE);

    if (fileHandle.readPage(rid.pageNum, datPage) == fail) {
        free(datPage);
        return fail;
    }

    int numOfSlots;
    memcpy(&numOfSlots, datPage + (PAGE_SIZE - sizeof(int) * 2), sizeof(int));
    
    // the slot index in out of range
    if (numOfSlots <= 0 || rid.slotNum > (unsigned)numOfSlots) {
        free(datPage);
        return fail;
    }
    int offset;
    memcpy(&offset, datPage + (PAGE_SIZE - sizeof(int) * (rid.slotNum + 2)), sizeof(int));
    if (offset < 0) {    // already deleted
        free(datPage);
        return success;  // OR FAIL??
    }
    
    // set the beginning byte to -2, which means the record is deleted
    int tmp = -2;
    memcpy(datPage + offset, &tmp, sizeof(int));

    if (fileHandle.writePage(rid.pageNum, datPage) == fail) { // write back the page.)
        free(datPage);
        return fail;
    }

    free(datPage);
    return success;
}

RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle) {
   
    if (pfm->destroyFile(fileHandle.fileName) == fail 
            || pfm->createFile(fileHandle.fileName) == fail)
    {
        return fail;
    }
    return success;
    return fail;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data) {
    
    // special cases
    if (rid.pageNum % 1025 == 0 || rid.pageNum < 0) return fail; // not a data page

    char* datPage = (char*) malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, datPage) == fail) {
        printf("unable to read data page\n");
        return fail;
    }

    int numOfDataBlocks;
    memcpy(&numOfDataBlocks, datPage + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
    if (rid.slotNum <= 0 || rid.slotNum > (unsigned) numOfDataBlocks) return fail; // not a legal slot number

    int offset; // the relative address of the first byte of the data block
    memcpy(&offset, datPage + PAGE_SIZE - (rid.slotNum + 2) * sizeof(int), sizeof(int));
    
    if (offset == -1) {    //this is a deleted record.
        free(datPage);
        // printf("this is a deleted record\n");
        return fail;
    }
    
    int dataLen; // the actual length of the record
    memcpy(&dataLen, datPage + offset, sizeof(int));
    
    if (dataLen == -2) { // this is a deleted record
        free(datPage);
        // printf("this is a deleted record\n");
        return fail;
    }
    if (dataLen == -1) { // this is a tombstone
        RID newRID;
        memcpy(&newRID.pageNum, datPage + offset + sizeof(int), sizeof(int));
        memcpy(&newRID.slotNum, datPage + offset + sizeof(int) * 2, sizeof(int));
        RC result = readAttribute(fileHandle, recordDescriptor, newRID, attributeName, data);
        free(datPage);
        return result;
    }
    
    int startOfRecord; // the first byte of the record
    memcpy(&startOfRecord, datPage + offset + sizeof(int), sizeof(int));
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++) {
        if (attributeName.compare(recordDescriptor[i].name) == 0) {    // find the attribute
            // get the start index and length of the attribute
            int attrStart;
            int attrLen;
            memcpy(&attrStart, datPage + offset + sizeof(int) * (i * 2 + 1), sizeof(int));
            memcpy(&attrLen, datPage + offset + sizeof(int) * (i * 2 + 2), sizeof(int));
            
            memcpy(data, datPage + offset + attrStart, attrLen);
            
            // free the memory
            free(datPage);
            return success;
        }
    }
    
    free(datPage);
    return fail;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    if (rid.pageNum % 1025 == 0 || rid.pageNum < 0) return fail; // not a data page
    
    char* datPage = (char*) malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, datPage) == fail) {
        printf("unable to read data page\n");
        return fail;
    }
    
    int numOfDataBlocks;
    memcpy(&numOfDataBlocks, datPage + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
    if (rid.slotNum <=0 || rid.slotNum > (unsigned)numOfDataBlocks)
    {
        printf("not a legal slot number\n");
        return fail;
    }
    
    // calculate the length of the new data
    int len = 0;
    int l_offset = 0;
    for (int i = 0; i < (int) recordDescriptor.size(); i++) {
        if (recordDescriptor[i].type == TypeVarChar) {
            int strLen;
            memcpy(&strLen, (char*) data + l_offset, sizeof(int));
            l_offset += sizeof(int) +strLen;
            len += strLen + sizeof(int);
        } else {
            l_offset += sizeof(int);
            len = len + recordDescriptor[i].length;
        }
    }
    int dataLen = len;
    len = len + sizeof(int) + (sizeof(int) * recordDescriptor.size() * 2);
    // printf("total length is: %d\n", len);
    
    int space; // calculate the space after deleting the original data
    int offset; // the relative address of the first byte of the data block
    memcpy(&offset, datPage + PAGE_SIZE - (rid.slotNum + 2) * sizeof(int), sizeof(int));
    memcpy(&space, datPage + offset, sizeof(int));
    
    if (space == -1) {// this is a tombstone
        RID nextRid;
        memcpy(&nextRid.pageNum, datPage + offset + sizeof(int), sizeof(int));
        memcpy(&nextRid.slotNum, datPage + offset + 2 * sizeof(int), sizeof(int));
        updateRecord(fileHandle, recordDescriptor, data, nextRid);
        return success;
    }
    
    int startOftheData;
    memcpy(&startOftheData, datPage + offset + sizeof(int), sizeof(int));
    space += startOftheData;
    // printf("total space is: %d\n", space);
    
    if (space >= len) // space is enough for the new data
    {
        // the relative index for current element
        int relativeIndex = sizeof(int) + (sizeof(int) * recordDescriptor.size() * 2);

        // write in the actual length of the data
        memcpy(datPage + offset, &dataLen, sizeof(int));
        offset += sizeof(int);

        // write in the relative index and length of the n elements
        l_offset = 0;
        for (int i = 0; i < (int) recordDescriptor.size(); i++) {
            if (recordDescriptor[i].type == TypeVarChar) {
                // write in the relative index of the string element
                memcpy(datPage + offset, &relativeIndex, sizeof(int));
                offset += sizeof(int);

                // calculate the length of the string element
                // including the byte which stores the length of the string
                int strLen;
                memcpy(&strLen, (char*) data + l_offset, sizeof(int));
                strLen += sizeof(int);

                // write in the length of the string element
                memcpy(datPage + offset, &strLen, sizeof(int));
                offset += sizeof(int);

                // move the relative pointer to the next element
                l_offset += strLen;
                relativeIndex += strLen;
            } else {
                // write in the relative index and length of the element
                memcpy(datPage + offset, &relativeIndex, sizeof(int));
                offset += sizeof(int);
                memcpy(datPage + offset, &recordDescriptor[i].length, sizeof(int));
                offset += sizeof(int);

                // move the relative pointer to the next element
                l_offset += recordDescriptor[i].length;
                relativeIndex += recordDescriptor[i].length;
            }
        }
        // write in the whole data
        memcpy(datPage + offset, data, dataLen);
        
        offset += dataLen;
    } else { // space not enough
        // leave a tombstone
        RID newRid;
        insertRecord(fileHandle, recordDescriptor, data, newRid);
        // printf("update: it is redirected from %d to %d\n", rid.pageNum, newRid.pageNum);
        int tmp = -1;
        memcpy(datPage + offset, &tmp, sizeof(int));
        memcpy(datPage + offset + sizeof(int), &newRid.pageNum, sizeof(int));
        memcpy(datPage + offset + 2 * sizeof(int), &newRid.slotNum, sizeof(int));
    }
    
    // write back the data page
    fileHandle.writePage(rid.pageNum, datPage);
    
    // free the memory
    free(datPage);
    
    return success;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber) {
    // read the page
    
    if (pageNumber % 1025 == 0) return success;
    char* datPage = (char*) malloc(PAGE_SIZE);
    if (fileHandle.readPage(pageNumber, datPage) == fail) {
        printf("unable to read data page %d\n", pageNumber);
        free(datPage);
        return fail;
    }
    
    int numOfDataBlocks;
    memcpy(&numOfDataBlocks, datPage + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
    
    // if no records in the page, reset all the byte to 0
    if (numOfDataBlocks == 0) {
        memset(datPage, 0, PAGE_SIZE);
        fileHandle.writePage(pageNumber, datPage);
        return success;
    }
    
    int offset = 0;
    for (int i = 1; i <= numOfDataBlocks; i++) {
        int startOfBlock;
        memcpy(&startOfBlock, datPage + PAGE_SIZE - (i + 2) * sizeof(int), sizeof(int));
        if (startOfBlock == -1) continue; // it is already deleted and reorganized
        int dataLen;
        int blockLen;
        memcpy(&dataLen, datPage + offset, sizeof(int));
        if (dataLen == -1) {
            blockLen = 3;
        } else if (dataLen == -2) {
            blockLen = 0;
            // set the pointer to -1, which means deleted
            int tmp = -1;
            memcpy(datPage + PAGE_SIZE - sizeof(int) * (i + 2), &tmp, sizeof(int));
        } else {
            int startOfRecord;
            memcpy(&startOfRecord, datPage + offset + sizeof(int), sizeof(int));
            blockLen = startOfRecord + dataLen;
        }
        // copy the data block after the end of the last one
        memcpy(datPage + offset, datPage + startOfBlock, blockLen);
        // reset the pointer to the data block
        memcpy(datPage + PAGE_SIZE - (i + 2) * sizeof(int), &offset, sizeof(int));
        offset += blockLen;
    }
    
    
    // refresh the free space pointer of the page
    memcpy(datPage + PAGE_SIZE - sizeof(int), &offset, sizeof(int));
    
    // set page space in the directory
    int pageSpace = PAGE_SIZE - offset * sizeof(int) - (numOfDataBlocks + 2) * sizeof(int);
    setPageSpaceInDirectory(fileHandle, pageNumber, pageSpace);
    
    // free the memory
    free(datPage);
    return success;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
        const vector<Attribute> &recordDescriptor,
        const string &conditionAttribute,
        const CompOp compOp, // comparision type such as "<" and "="
        const void *value, // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RBFM_ScanIterator &rbfm_ScanIterator) {
    
    // scan all pages
    for (unsigned pageNum = 0; pageNum < fileHandle.getNumberOfPages(); pageNum++) {
        if (pageNum % 1025 == 0) continue;    // the directory page
        
        // read the page with the page number
        char* datPage = (char*) malloc(PAGE_SIZE);
        if (fileHandle.readPage(pageNum, datPage) == fail) {
                printf("unable to read data page %d\n", pageNum);
                free(datPage);
                return fail;
        }
        
        int numOfSlots;
        memcpy(&numOfSlots, datPage + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
        
        // scan all slots
        for (int slotNum = 1; slotNum <= numOfSlots; slotNum++) {
            
            int offset;
            memcpy(&offset, datPage + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
            if (offset == -1) continue;    // a deleted record
            int dataLen;
            memcpy(&dataLen, datPage + offset, sizeof(int));
            if (dataLen < 0) continue;    // a deleted record or a tombstone
            
            // read the value of the condition attribute
            void* valConAtt = malloc(1000);
            int indexConAtt = -1;
            int lenOfString = 0; // store the length if it is a string
            
            if (conditionAttribute != "")
            {
                int startOfRecord; // the first byte of the record
                memcpy(&startOfRecord, datPage + offset + sizeof(int), sizeof(int));
                for (unsigned i = 0; i < recordDescriptor.size(); i++) {
                    
                    if (conditionAttribute.compare(recordDescriptor[i].name) == 0) {    // find the attribute
                        // get the start index and length of the attribute
                        int attrStart;
                        int attrLen;
                        memcpy(&attrStart, datPage + offset + sizeof(int) * (i * 2 + 1), sizeof(int));
                        memcpy(&attrLen, datPage + offset + sizeof(int) * (i * 2 + 2), sizeof(int));
                        
                        if (recordDescriptor[i].type == TypeVarChar) {
                            memcpy(&attrLen, datPage + offset + attrStart, sizeof(int));
                            attrStart += sizeof(int);
                            lenOfString = attrLen;
                        }
                        memcpy(valConAtt, datPage + offset + attrStart, attrLen);
                        if (recordDescriptor[i].type == TypeVarChar) {
                            ((char*)valConAtt)[attrLen] = '\0';
                        }
                        indexConAtt = i;
                        break;
                    }
                }
                if (indexConAtt == -1) {
                        free(valConAtt);
                        free(datPage);
                        return fail;
                }
            }
            
            string recordStr;
            string valueStr;
            if (recordDescriptor[indexConAtt].type == TypeVarChar && value != NULL) {
                recordStr = string((char*)valConAtt);
                int strLen;
                memcpy(&strLen, value, sizeof(int));
                char* tmp = (char*)malloc(1000);
                memcpy(tmp, (char*)value + sizeof(int), strLen);
                tmp[strLen] = '\0';
                valueStr = string(tmp);
                free(tmp);
            }
            bool flag = false;    // check if the record satisfies the condition
            switch (compOp) {
                case EQ_OP:
                    if (recordDescriptor[indexConAtt].type == TypeInt) {
                        if (*(int*)valConAtt == *(int*)value) flag = true;
                    } else if (recordDescriptor[indexConAtt].type == TypeReal){
                        if (*(float*)valConAtt == *(float*)value) flag = true;
                    } else {
                        if (recordStr == valueStr) flag = true;
                    }
                    break;
                case LT_OP:
                    if (recordDescriptor[indexConAtt].type == TypeInt) {
                        if (*(int*)valConAtt < *(int*)value) flag = true;
                    } else if (recordDescriptor[indexConAtt].type == TypeReal){
                        if (*(float*)valConAtt < *(float*)value) flag = true;
                    } else {
                        if (recordStr < valueStr) flag = true;
                    }
                    break;
                case GT_OP:
                    if (recordDescriptor[indexConAtt].type == TypeInt) {
                        if (*(int*)valConAtt > *(int*)value) flag = true;
                    } else if (recordDescriptor[indexConAtt].type == TypeReal){
                        if (*(float*)valConAtt > *(float*)value) flag = true;
                    } else {
                        if (recordStr > valueStr) flag = true;
                    }
                    break;
                case LE_OP:
                    if (recordDescriptor[indexConAtt].type == TypeInt) {
                        if (*(int*)valConAtt <= *(int*)value) flag = true;
                    } else if (recordDescriptor[indexConAtt].type == TypeReal){
                        if (*(float*)valConAtt <= *(float*)value) flag = true;
                    } else {
                        if (recordStr <= valueStr) flag = true;
                    }
                    break;
                case GE_OP:
                    if (recordDescriptor[indexConAtt].type == TypeInt) {
                        if (*(int*)valConAtt >= *(int*)value) flag = true;
                    } else if (recordDescriptor[indexConAtt].type == TypeReal){
                        if (*(float*)valConAtt >= *(float*)value) flag = true;
                    } else {
                        if (recordStr >= valueStr) flag = true;
                    }
                    break;
                case NE_OP:
                    if (recordDescriptor[indexConAtt].type == TypeInt) {
                        if (*(int*)valConAtt != *(int*)value) flag = true;
                    } else if (recordDescriptor[indexConAtt].type == TypeReal){
                        if (*(float*)valConAtt != *(float*)value) flag = true;
                    } else {
                        if (recordStr != valueStr) flag = true;
                    }
                    break;
                case NO_OP:
                    flag = true;
                    break;
            }
            free(valConAtt);
            
            if (!flag) continue;    // if not satisfactory, continue
            
            // read the attribute projected
            char* projectedAttr = (char*)malloc(1000);
            int l_offset = 0;
            for (unsigned attrNum = 0; attrNum < attributeNames.size(); attrNum++) {
                
                // search for the attribute with the name
                for (unsigned i = 0; i < recordDescriptor.size(); i++) {
                    if (attributeNames[attrNum].compare(recordDescriptor[i].name) == 0) {    // find the attribute
                        // get the start index and length of the attribute
                        int attrStart;
                        int attrLen;
                        memcpy(&attrStart, datPage + offset + sizeof(int) * (i * 2 + 1), sizeof(int));
                        memcpy(&attrLen, datPage + offset + sizeof(int) * (i * 2 + 2), sizeof(int));
                        memcpy((char*)projectedAttr + l_offset, datPage + offset + attrStart, attrLen);
                        l_offset += attrLen;
                    }
                }
            }
            
            // insert the projected attribute into RBFM_ScanIterator
            RID newRID;
            newRID.pageNum = pageNum;
            newRID.slotNum = slotNum;
            //printf("Age is : %d\n", *(int*)projectedAttr);
            rbfm_ScanIterator.insertRecord(newRID, projectedAttr, l_offset);
            free(projectedAttr);
        }
        free(datPage);
    }
    return success;
}

RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor) {
//    for (int pageNum = 0; pageNum < fileHandle.getNumberOfPages(); pageNum++) {
//        reorganizePage(fileHandle, recordDescriptor, pageNum);
//    }
    for (unsigned pageNum = 0; pageNum < fileHandle.getNumberOfPages(); pageNum++) {
        if (pageNum % 1025 == 0) continue;
        
        char* datPage = (char*) malloc(PAGE_SIZE);
        if (fileHandle.readPage(pageNum, datPage) == fail) {
            printf("unable to read data page\n");
            return fail;
        }
        
        int numOfDataBlocks;
        memcpy(&numOfDataBlocks, datPage + PAGE_SIZE - 2 * sizeof (int), sizeof (int));
        
        RID rid;
        rid.pageNum = pageNum;
        int numOfData = 0;
        
        char* returnedData[1000];
        for (int i = 0; i < 1000; i++) {
            returnedData[i] = (char*)malloc(1000);
        }
        char* data = (char*)malloc(1000);
        for (rid.slotNum = 1; rid.slotNum <= (unsigned)numOfDataBlocks; rid.slotNum++) {
            if (readRecord(fileHandle, recordDescriptor, rid, data) == success) {
                memcpy(returnedData[numOfData++], data, 1000);
            }
        }
        free(data);
        
        free(datPage);
        
        datPage = (char*) malloc(PAGE_SIZE);
        memset(datPage, 0, PAGE_SIZE);
        fileHandle.writePage(pageNum, datPage);
        setPageSpaceInDirectory(fileHandle, pageNum, -1);
        
        for (int i = 0; i < numOfData; i++) {
            RID tmp;
            insertRecord(fileHandle, recordDescriptor, returnedData[i], tmp);
        }
        for (int i = 0; i < 1000; i++)
            free(returnedData[i]);
        
        free(datPage);
    }
    return success;
}

