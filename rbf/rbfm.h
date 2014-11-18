
#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <stdlib.h>
#include <string.h>

#include "../rbf/pfm.h"

using namespace std;


// Record ID

typedef struct {
    unsigned pageNum;
    unsigned slotNum;
} RID;

// Attribute

typedef enum {
    TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string name; // attribute name
    AttrType type; // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)

typedef enum {
    EQ_OP = 0, // =
    LT_OP, // <
    GT_OP, // >
    LE_OP, // <=
    GE_OP, // >=
    NE_OP, // !=
    NO_OP // no condition
} CompOp;



/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project 
 *****************************************************************************/

#define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

// structure stored in RBFM_ScanIterator
struct LinkedList{
    RID rid;
    void* data;
    int dataLen;
    LinkedList* next;
};

typedef LinkedList ScanedData;

class RBFM_ScanIterator {
private:
    ScanedData* head;
    ScanedData* cur;
    ScanedData* tail;
public:
    
    RBFM_ScanIterator() {
        tail = cur = head = NULL;
    };

    ~RBFM_ScanIterator() {
        while (head != NULL) {
            ScanedData* tmp = head;
            head = head->next;
            free(tmp->data);
            delete tmp;
        }
        cur = tail = NULL;
    };

    // "data" follows the same format as RecordBasedFileManager::insertRecord()
    
    RC insertRecord(const RID& rid, const void* data, int dataLen) {
        ScanedData* tmp = new ScanedData;
        tmp->rid = rid;
        tmp->data = malloc(1000);
        memcpy(tmp->data, data, dataLen);
        tmp->dataLen = dataLen;
        tmp->next = NULL;
        if (head == NULL)
            cur = tail = head = tmp;
        else 
            tail->next = tmp;
        tail = tmp;
        return 0;
    };
    
    RC getNextRecord(RID &rid, void *data) {
        if (cur == NULL) return RBFM_EOF;
        rid = cur->rid;
        memcpy(data, cur->data, cur->dataLen);
        cur = cur->next;
        return 0;
    };

    RC close() {
        while (head != NULL) {
            ScanedData* tmp = head;
            head = head->next;
            delete tmp;
        }
        tail = cur = head;
        return 0;
    };
};

class RecordBasedFileManager {
public:
    static RecordBasedFileManager* instance();

    PagedFileManager* pfm;

    RC createFile(const string &fileName);

    RC destroyFile(const string &fileName);

    RC openFile(const string &fileName, FileHandle &fileHandle);

    RC closeFile(FileHandle &fileHandle);

    //  Format of the data passed into the function is the following:
    //  1) data is a concatenation of values of the attributes
    //  2) For int and real: use 4 bytes to store the value;
    //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
    RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

    RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

    // This method will be mainly used for debugging/testing
    RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

    // insertTuple calls this method to locate a page that has enough space to insert the data.
    int locatePageInDirectory(FileHandle &fileHandle, int length);
    //given a page number, return the available space size in the directory.
    //will be used by reorganize page and update
    int getPageSpaceInDirectory(FileHandle &fileHandle, int pageNum);
    // set a size of page into directory., will be used by reorganize page and update
    RC setPageSpaceInDirectory(FileHandle &fileHandle, int pageNum, int space);


    /**************************************************************************************************************************************************************
     ***************************************************************************************************************************************************************
    IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
     ***************************************************************************************************************************************************************
     ***************************************************************************************************************************************************************/
    RC deleteRecords(FileHandle &fileHandle);

    RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

    RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data);
    
    // Assume the rid does not change after update
    RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

    RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber);

    // scan returns an iterator to allow the caller to go through the results one by one. 
    RC scan(FileHandle &fileHandle,
            const vector<Attribute> &recordDescriptor,
            const string &conditionAttribute,
            const CompOp compOp, // comparision type such as "<" and "="
            const void *value, // used in the comparison
            const vector<string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);

    // Extra credit for part 2 of the project, please ignore for part 1 of the project
public:
    RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);


protected:
    RecordBasedFileManager();
    ~RecordBasedFileManager();

private:
    static RecordBasedFileManager *_rbf_manager;
};

#endif
