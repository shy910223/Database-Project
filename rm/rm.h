
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"


using namespace std;


# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {
public:
    RBFM_ScanIterator* rbfmsi;
    RM_ScanIterator() {rbfmsi = new RBFM_ScanIterator();};
    ~RM_ScanIterator() {delete rbfmsi;};
    
    // "data" follows the same format as RelationManager::insertTuple()
    RC insertTuple(const RID& rid, const void* data, int dataLen ) {
        return rbfmsi->insertRecord(rid, data, dataLen);
    }
    RC getNextTuple(RID &rid, void *data) {
        void* returnedData = malloc(100);
        RC rc = rbfmsi->getNextRecord(rid, returnedData);
        memcpy(data, returnedData, 100);
        return rc;
    };
    RC close() {
        RC rc = rbfmsi->close();
        return rc;
    };
};

class RM_IndexScanIterator {
public:
    IX_ScanIterator* ixsi;
    RM_IndexScanIterator() {ixsi = new IX_ScanIterator();};  	// Constructor
    ~RM_IndexScanIterator() {delete ixsi;} ;	// Destructor
    
    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key) {
        return ixsi->getNextEntry(rid, key);
    };  	// Get next matching entry
    RC close() {
        return ixsi->close();
    };             			// Terminate index scan
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RecordBasedFileManager* rbfm;
  IndexManager* ix;
  
  vector<Attribute> getTableStructure();
  
  
  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuples(const string &tableName);

  RC deleteTuple(const string &tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  RC reorganizePage(const string &tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

   RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);
  
  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);
  
// Extra credit
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);

  RC reorganizeTable(const string &tableName);



protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
};

#endif
