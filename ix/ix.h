
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stdio.h>
#include <stdlib.h> // for malloc
#include <string.h> // for memset

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;

class IndexManager {
public:
  static IndexManager* instance();
  
  PagedFileManager* pfm;

  RC createFile(const string &fileName);

  RC destroyFile(const string &fileName);

  RC openFile(const string &fileName, FileHandle &fileHandle);

  RC closeFile(FileHandle &fileHandle);

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Insert new index entry
  RC deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Delete index entry

  void splitLeafNode(FileHandle &fileHandle, const Attribute &attribute, unsigned pageNum);
  void splitIndexNode(FileHandle &fileHandle, const Attribute &attribute, unsigned pageNum);
  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity

  void scanPage(FileHandle &fileHandle,
      int pageNum,
      const Attribute &attribute,
      const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);
  
  RC scan(FileHandle &fileHandle,
      const Attribute &attribute,
      const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

 protected:
  IndexManager   ();                            // Constructor
  ~IndexManager  ();                            // Destructor

 private:
  static IndexManager *_index_manager;
};

class IX_ScanIterator {
public:
    RBFM_ScanIterator* rbfmsi;
    IX_ScanIterator();
    ~IX_ScanIterator();
    RC insertDataEntry(const RID& rid, const void* key, int keyLen);
    RC getNextEntry(RID &rid, void *key);    // Get next matching entry
    RC close();    // Terminate index scan
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
