#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include "rm.h"

using namespace std;

RelationManager* RelationManager::_rm = 0;
const RC success = 0;
const RC fail = -1;

RelationManager* RelationManager::instance() {
    if (!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager() {
     rbfm = RecordBasedFileManager::instance();
     ix = IndexManager::instance();
}

RelationManager::~RelationManager() {
}

vector<Attribute> RelationManager::getTableStructure() {
    vector<Attribute> recordDescriptor;
    Attribute attr;

    attr.name = "TableName";
    attr.length = 200;
    attr.type = TypeVarChar;
    recordDescriptor.push_back(attr);

    attr.name = "ColumnName";
    attr.length = 200;
    attr.type = TypeVarChar;
    recordDescriptor.push_back(attr);

    attr.name = "ColumnType";
    attr.length = sizeof (int);
    attr.type = TypeInt;
    recordDescriptor.push_back(attr);

    attr.name = "ColumnLength";
    attr.length = sizeof (int);
    attr.type = TypeInt;
    recordDescriptor.push_back(attr);

    attr.name = "ColumnPosition";
    attr.length = sizeof (int);
    attr.type = TypeInt;
    recordDescriptor.push_back(attr);

    return recordDescriptor;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs) {

    string catalog = tableName + ".tbl";
    FileHandle fh;

    if (rbfm->openFile(catalog, fh) == success) {
        return fail;
    }

    if (rbfm->createFile(catalog) == fail) {
        return fail;
    }

    if (rbfm->openFile(catalog, fh) == fail) {
        return fail;
    }

    char* buffer = (char*) malloc(sizeof (char) * 100);
    int offset = 0;
    for (unsigned i = 0; i < attrs.size(); i++) {
        memset(buffer, 0, 100);
        offset = 0;

        int len = tableName.length();
        memcpy(buffer + offset, &len, sizeof (int));
        offset += sizeof (int);

        memcpy(buffer + offset, tableName.c_str(), len);
        offset += len;
        
               
        len = attrs[i].name.length();
        memcpy(buffer + offset, &len, sizeof (int));
        offset += sizeof (int);

        memcpy(buffer + offset, attrs[i].name.c_str(), len);
        offset += len;

        memcpy(buffer + offset, &attrs[i].type, sizeof (int));
        offset += sizeof (int);

        memcpy(buffer + offset, &attrs[i].length, sizeof (int));
        offset += sizeof (int);

        memcpy(buffer + offset, &i, sizeof (int));
        offset += sizeof (int);


        RID rid;
        vector<Attribute> recordDescriptor = getTableStructure();
        if (rbfm->insertRecord(fh, recordDescriptor, buffer, rid) == fail) {
            printf("unable to insert system catalog\n");
            return fail;
        }

    }

    free(buffer);
    rbfm->closeFile(fh);
    return success;
}

RC RelationManager::deleteTable(const string &tableName) {


    string catalog = tableName + ".tbl";
    string dataFile = tableName + ".dat";

    if (rbfm->destroyFile(catalog.c_str()) == fail || rbfm->destroyFile(dataFile.c_str()) == fail)
        return fail;
    else
        return success;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {
    string catalog = tableName + ".tbl";
    FileHandle fh;

    if (rbfm->openFile(catalog, fh) == fail) {
        return fail;
    }
    RBFM_ScanIterator rbfmsi;
    const string table = "TableName";
    
    const vector<Attribute> recordDescriptor = getTableStructure();
    vector<string> attributeNames;
    attributeNames.push_back("ColumnName");
    attributeNames.push_back("ColumnType");
    attributeNames.push_back("ColumnLength");
    rbfm->scan(fh, recordDescriptor, "", NO_OP, NULL, attributeNames, rbfmsi);
    RID rid;
    void* returnedData = malloc(100);
    while (rbfmsi.getNextRecord(rid, returnedData) != RBFM_EOF) {
        Attribute attr;
        int offset = 0;
        int len;
        memcpy(&len, (char*)returnedData + offset, sizeof (int));
        offset += sizeof (int);

        char* name = (char*)malloc(100);
        memcpy(name, (char*)returnedData + offset, len);
        name[len] = '\0';
        attr.name = string(name);
        offset += len;
        free(name);

        memcpy(&attr.type, (char*)returnedData + offset, sizeof (AttrType));
        offset += sizeof (AttrType);

        memcpy(&attr.length, (char*)returnedData + offset, sizeof (int));
        offset += sizeof (int);

        attrs.push_back(attr);
    }
    free(returnedData);
    rbfmsi.close();

    rbfm->closeFile(fh);
    return success;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {

    string dataFile = tableName + ".dat";
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) == fail) {
        printf("unable to get table descriptor\n");
        return fail;
    }

    FileHandle fh;
    if (rbfm->openFile(dataFile.c_str(), fh) == fail) {
        if (rbfm->createFile(dataFile.c_str()) == fail) {
            printf("unable to create data file\n");
            return fail;
        }

        if (rbfm->openFile(dataFile.c_str(), fh) == fail) {
            return fail;
        }
    }
    RC rc1 = rbfm->insertRecord(fh, recordDescriptor, data, rid);
    rbfm->closeFile(fh);
    
    RC rc2 = fail;
    for (vector<Attribute>::iterator it = recordDescriptor.begin(); it != recordDescriptor.end(); it++) {
        string indexFile = tableName + it->name + ".idx";
        FileHandle fh2;
        if (ix->openFile(indexFile,fh2) == fail) continue;
        else {
            rc2 = success;
            ix->insertEntry(fh2, *it, data, rid);
            ix->closeFile(fh2);
        }
    }
        
    return rc1;
}

RC RelationManager::deleteTuples(const string &tableName) {

    string dataFile = tableName + ".dat";
    rbfm->destroyFile(dataFile);
    return rbfm->createFile(dataFile);
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {

    string dataFile = tableName + ".dat";
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) == fail) {
        printf("unable to get table descriptor\n");
        return fail;
    }

    FileHandle fh;
    if (rbfm->openFile(dataFile.c_str(), fh) == fail) {
        printf("unable to open data file in deleteTuple\n");
        return fail;
    }

    RC rc = rbfm->deleteRecord(fh, recordDescriptor, rid);
    rbfm->closeFile(fh);
    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid) {
    string dataFile = tableName + ".dat";
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) == fail) {
        printf("unable to get table descriptor\n");
        return fail;
    }

    FileHandle fh;
    if (rbfm->openFile(dataFile.c_str(), fh) == fail) {
        printf("unable to open data file in updateTuple\n");
        return fail;
    }

    RC rc = rbfm->updateRecord(fh, recordDescriptor, data, rid);
    rbfm->closeFile(fh);
    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data) {

    string dataFile = tableName + ".dat";
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) == fail) {
        printf("unable to get table descriptor\n");
        return fail;
    }

    FileHandle fh;
    if (rbfm->openFile(dataFile.c_str(), fh) == fail) {
        printf("unable to open data file in readTuple\n");
        return fail;
    }
    RC rc = rbfm->readRecord(fh, recordDescriptor, rid, data);
    rbfm->closeFile(fh);
    return rc;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data) {

    string dataFile = tableName + ".dat";
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) == fail) {
        printf("unable to get table descriptor\n");
        return fail;
    }

    FileHandle fh;
    if (rbfm->openFile(dataFile.c_str(), fh) == fail) {
        printf("unable to open data file in readAttribute\n");
        return fail;
    }

    RC rc = rbfm->readAttribute(fh, recordDescriptor, rid, attributeName, data);
    rbfm->closeFile(fh);
    return rc;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber) {

    string dataFile = tableName + ".dat";
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) == fail) {
        printf("unable to get table descriptor\n");
        return fail;
    }

    FileHandle fh;
    if (rbfm->openFile(dataFile.c_str(), fh) == fail) {
        printf("unable to open data file in reorganizePage\n");
        return fail;
    }
    
    RC rc = rbfm->reorganizePage(fh, recordDescriptor, pageNumber);
    rbfm->closeFile(fh);
    return rc;
}

RC RelationManager::scan(const string &tableName,
        const string &conditionAttribute,
        const CompOp compOp,
        const void *value,
        const vector<string> &attributeNames,
        RM_ScanIterator &rm_ScanIterator) {
    
    string dataFile = tableName + ".dat";
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) == fail) {
        printf("unable to get table descriptor\n");
        return fail;
    }

    FileHandle fh;
    if (rbfm->openFile(dataFile.c_str(), fh) == fail) {
        printf("unable to open data file in scan\n");
        return fail;
    }

    RC rc = rbfm->scan(fh, recordDescriptor, conditionAttribute, compOp, value, attributeNames, *rm_ScanIterator.rbfmsi);
    rbfm->closeFile(fh);
    return rc;
}

// Extra credit

RC RelationManager::dropAttribute(const string &tableName, const string &attributeName) {
    string catalog = tableName + ".tbl";
    FileHandle fh;

    if (rbfm->openFile(catalog, fh) == fail) {
        return fail;
    }
    RBFM_ScanIterator rbfmsi;
    const string table = "TableName";

    const vector<Attribute> recordDescriptor = getTableStructure();
    vector<string> attributeNames;
    attributeNames.push_back("ColumnName");
    attributeNames.push_back("ColumnType");
    attributeNames.push_back("ColumnLength");
    
    rbfm->scan(fh, recordDescriptor, "ColumnName", EQ_OP, attributeName.c_str(), attributeNames, rbfmsi);
    char* returnedData = (char*) malloc(100);
    RID rid;

    while (rbfmsi.getNextRecord(rid, returnedData) != RBFM_EOF) {
        Attribute attr;
        int offset = 0;
        int len;
        memcpy(&len, returnedData + offset, sizeof (int));
        offset += sizeof (int);

        char* name = (char*)malloc(100);
        memcpy(name, (char*)returnedData + offset, len);
        name[len] = '\0';
        attr.name = string(name);
        offset += len;
        free(name);
        
        
        memcpy(&attr.type, returnedData + offset, sizeof (AttrType));
        offset += sizeof (AttrType);

        memcpy(&attr.length, returnedData + offset, sizeof (int));
        attr.length = 0;
        memcpy(returnedData + offset, &attr.length, sizeof(int));
        offset += sizeof (int);
        rbfm->updateRecord(fh, recordDescriptor, returnedData, rid);
    }
    free(returnedData);
    rbfmsi.close();

    rbfm->closeFile(fh);
    return success;
}

// Extra credit

RC RelationManager::addAttribute(const string &tableName, const Attribute &attr) { 
    string catalog = tableName + ".tbl";
    FileHandle fh;

    if (rbfm->openFile(catalog, fh) == fail) {
        return fail;
    }
    
    vector<Attribute> attrs;
    if (getAttributes(tableName, attrs) == fail) {
        printf("unable to get table descriptor\n");
        return fail;
    }
    int position = attrs.size() + 1;
    
    char* buffer = (char*) malloc(sizeof (char) * 100);

    int offset = 0;

    int len = tableName.length();
    memcpy(buffer + offset, &len, sizeof (int));
    offset += sizeof (int);
    
    memcpy(buffer + offset, tableName.c_str(), len);
    offset += len;
    
    len = attr.name.length();
    memcpy(buffer + offset, &len, sizeof (int));
    offset += sizeof (int);

    memcpy(buffer + offset, attr.name.c_str(), len);
    offset += len;

    memcpy(buffer + offset, &attr.type, sizeof (int));
    offset += sizeof (int);

    memcpy(buffer + offset, &attr.length, sizeof (int));
    offset += sizeof (int);
    
    memcpy(buffer + offset, &position, sizeof (int));
    offset += sizeof (int);

    RID rid;
    vector<Attribute> recordDescriptor = getTableStructure();
    if (rbfm->insertRecord(fh, recordDescriptor, buffer, rid) == fail) {
        printf("unable to insert system catalog\n");
        return fail;
    }

    free(buffer);
    rbfm->closeFile(fh);
    return success;
}

// Extra credit

RC RelationManager::reorganizeTable(const string &tableName) {
    string dataFile = tableName + ".dat";
    FileHandle fh;

    if (rbfm->openFile(dataFile, fh) == fail) {
        return fail;
    }

    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);

    rbfm->reorganizeFile(fh, recordDescriptor);
    rbfm->closeFile(fh);

    return success;
}

//new added methods for project 4
RC RelationManager::createIndex(const string& tableName, const string& attributeName){
    string indexFileName = tableName + attributeName + ".idx";
    string tableFileName = tableName + ".tbl";
    string dataFileName = tableName + ".dat";
    
    FileHandle fh;

    if (ix->openFile(indexFileName, fh) == success) {
        return fail;
    }

    if (ix->createFile(indexFileName) == fail) {
        return fail;
    }
    
    FileHandle fh2;
    if (rbfm->openFile(tableFileName, fh2) == fail) {
        return success;
    }
    FileHandle fh3;
    if (rbfm->openFile(dataFileName, fh3) == fail) {
        return success;
    }
    
    ix->openFile(indexFileName, fh);
    
    Attribute attribute;
    vector<Attribute> allAttributes;
    getAttributes(tableName, allAttributes);
    for (vector<Attribute>::iterator it = allAttributes.begin(); it != allAttributes.end(); it++) {
        if (it->name == attributeName) {
            attribute = *it;
            break;
        }
    }
    vector<string> attributeNames;
    attributeNames.push_back(attributeName);
    RM_ScanIterator* rmsi = new RM_ScanIterator();
    scan(tableName, "", NO_OP, NULL, attributeNames, *rmsi);
    
    RID rid;
    void* returnedData = malloc(1000);
    while (rmsi->getNextTuple(rid, returnedData) != RM_EOF) {
        ix->insertEntry(fh, attribute, returnedData, rid);
    }
    
    ix->closeFile(fh);
    free(returnedData);
    return success;
}

RC RelationManager::destroyIndex(const string& tableName, const string& attributeName){
    string indexFileName = tableName + attributeName + ".index";
    return ix->destroyFile(indexFileName);
}

RC RelationManager::indexScan(const string& tableName, const string& attributeName, const void* lowKey, const void* highKey, bool lowKeyInclusive, bool highKeyInclusive, RM_IndexScanIterator& rm_IndexScanIterator){
    string indexFileName = tableName + attributeName + ".idx";
    FileHandle fh;

    if (ix->openFile(indexFileName, fh) == fail) {
        return fail;
    }
    
    Attribute attribute;
    vector<Attribute> allAttributes;
    getAttributes(tableName, allAttributes);
    for (vector<Attribute>::iterator it = allAttributes.begin(); it != allAttributes.end(); it++) {
        if (it->name == attributeName) {
            attribute = *it;
            break;
        }
    }
    RC rc = ix->scan(fh, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, *rm_IndexScanIterator.ixsi);
    ix->closeFile(fh);
    
    return rc;
}
