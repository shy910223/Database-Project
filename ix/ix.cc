
#include "ix.h"
#include <iostream>
using namespace std;

const RC success = 0;
const RC fail = -1;

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    pfm = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return pfm->createFile(fileName.c_str());
}

RC IndexManager::destroyFile(const string &fileName)
{
    return pfm->destroyFile(fileName.c_str());
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    RC rc = pfm->openFile(fileName.c_str(), fileHandle);
    if (rc == fail) return fail;
    if (fileHandle.getNumberOfPages() == 0) {
        int root = -1;    // the father page will be append later
        char* firstPage = (char*)malloc(PAGE_SIZE);
        memcpy(firstPage, &root, sizeof(int));
        fileHandle.appendPage(firstPage);
        free(firstPage);
    //    printf("%d\n", fileHandle.getNumberOfPages());
    }
    return rc;
}

RC IndexManager::closeFile(FileHandle &fileHandle)
{
    return pfm->closeFile(fileHandle);
}

void IndexManager::splitLeafNode(FileHandle &fileHandle, const Attribute &attribute, unsigned pageNum)
{
    int order = (attribute.type == TypeVarChar) ? ((PAGE_SIZE - 12) / (attribute.length + 12) - 1) / 2 : ((PAGE_SIZE - 12) / (attribute.length + 8) - 1) / 2;
    int lenOfSearchKey = (attribute.type == TypeVarChar) ? (attribute.length + 4) : attribute.length;
    
    char* curLeafPage = (char*)malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, curLeafPage);
    char* newLeafPage = (char*)malloc(PAGE_SIZE);
    
    int isLeaf = 1;
    memcpy(newLeafPage + PAGE_SIZE - sizeof(int), &isLeaf, sizeof(int));
        
    for (int i = 0; i < order; i++) {
        memcpy(newLeafPage + i * (2 * sizeof(int) + lenOfSearchKey), curLeafPage + (i + order + 1) * (2 * sizeof(int) + lenOfSearchKey), (2 * sizeof(int) + lenOfSearchKey));
    }
    
    char* tmpStr = (char*)malloc(110);    // the middle value will be pushed up
    int lenOfMiddleValue = 0;
    if (attribute.type == TypeVarChar) {
        memcpy(&lenOfMiddleValue, newLeafPage, sizeof(int));
        memcpy(tmpStr, newLeafPage + sizeof(int), lenOfMiddleValue);
        tmpStr[lenOfMiddleValue] = '\0';
    }
    lenOfMiddleValue += sizeof(int);
    string middleValueString = string(tmpStr);
    free(tmpStr);
    
    void* middleValue = malloc(110);
    memcpy(middleValue, newLeafPage, lenOfMiddleValue);
    
    // write in the number of data entries on the page
    int numOfDataEntries;
    numOfDataEntries = order + 1;
    memcpy(curLeafPage + PAGE_SIZE - 2 * sizeof(int), &numOfDataEntries, sizeof(int));
    memcpy(newLeafPage + PAGE_SIZE - 2 * sizeof(int), &order, sizeof(int));
    
    int father;    // get the father page number
    memcpy(&father, curLeafPage + PAGE_SIZE - 3 * sizeof(int), sizeof(int));
    unsigned newPageNum = fileHandle.getNumberOfPages();    // get the page number of the new page
        
    if (father == -1) {
        int root = newPageNum + 1;    // the father page will be append later
        char* firstPage = (char*)malloc(PAGE_SIZE);
        memcpy(firstPage, &root, sizeof(int));
        fileHandle.writePage(0, firstPage);
        free(firstPage);
        
        memcpy(curLeafPage + PAGE_SIZE - 3 * sizeof(int), &root, sizeof(int));
        memcpy(newLeafPage + PAGE_SIZE - 3 * sizeof(int), &root, sizeof(int));
    } else {
        memcpy(newLeafPage + PAGE_SIZE - 3 * sizeof(int), &father, sizeof(int));
    }
    // write back the current page and append the new page
    fileHandle.writePage(pageNum, curLeafPage);
    fileHandle.appendPage(newLeafPage);
    
    // free the memory
    free(curLeafPage);
    free(newLeafPage);
    
    if (father == -1) {    // the splitting node is the root node
        int fathersIsLeaf = 0;
        int fathersNumOfDataEntries = 1;
        int fathersFather = -1;
        char* newRootPage = (char*)malloc(PAGE_SIZE);
        memcpy(newRootPage + PAGE_SIZE - sizeof(int), &fathersIsLeaf, sizeof(int));
        memcpy(newRootPage + PAGE_SIZE - 2 * sizeof(int), &fathersNumOfDataEntries, sizeof(int));
        memcpy(newRootPage + PAGE_SIZE - 3 * sizeof(int), &fathersFather, sizeof(int));
        memcpy(newRootPage, &pageNum, sizeof(int));
        memcpy(newRootPage + sizeof(int), middleValue, lenOfMiddleValue);
        memcpy(newRootPage + sizeof(int) + lenOfSearchKey, &newPageNum, sizeof(int));
        fileHandle.appendPage(newRootPage);
        free(middleValue);
        free(newRootPage);
        return;
    } else {
        char* fatherPage = (char*)malloc(PAGE_SIZE);
        fileHandle.readPage(father, fatherPage);
        
        int numOfIndexes;
        memcpy(&numOfIndexes, fatherPage + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
        
        int insertPos = numOfIndexes;
        if (attribute.type == TypeVarChar) {
            int len;
            char* index = (char*)malloc(110);
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&len, fatherPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                memcpy(index, fatherPage + i * (lenOfSearchKey + sizeof(int)) + 2 * sizeof(int), len);
                index[len] = '\0';
                string indexString = string(index);
                if (indexString > middleValueString) {
                    insertPos = i;
                    break;
                }
            }
            free(index);
        } else if (attribute.type == TypeInt) {
            int index;
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&index, fatherPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                if (index > *(int*)middleValue) {    // located the key value
                    insertPos = i;
                    break;
                }
            }
        } else {
            float index;
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&index, fatherPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(float));
                if (index > *(float*)middleValue) {    // located the key value
                    insertPos = i;
                    break;
                }
            }
        }
        for (int i = numOfIndexes; i > insertPos; i--) {
            // copy the indexes after the insertion position to the right
            memcpy(fatherPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), fatherPage + (i - 1) * (lenOfSearchKey + sizeof(int)) + sizeof(int), lenOfSearchKey + sizeof(int));
        }
        memcpy(fatherPage + insertPos * (lenOfSearchKey + sizeof(int)) + sizeof(int), middleValue, lenOfMiddleValue);
        memcpy(fatherPage + (insertPos + 1) * (lenOfSearchKey + sizeof(int)), &newPageNum, sizeof(int));
        numOfIndexes++;
        memcpy(fatherPage + PAGE_SIZE - 2 * sizeof(int), &numOfIndexes, sizeof(int));
        fileHandle.writePage(father, fatherPage);
        free(fatherPage);
        if (numOfIndexes == 2 * order + 1) {
            splitIndexNode(fileHandle, attribute, father);
        }
    }
    free(middleValue);
    return;
}

void IndexManager::splitIndexNode(FileHandle &fileHandle, const Attribute &attribute, unsigned pageNum)
{
    int order = (attribute.type == TypeVarChar) ? ((PAGE_SIZE - 12) / (attribute.length + 12) - 1) / 2 : ((PAGE_SIZE - 12) / (attribute.length + 8) - 1) / 2;
    int lenOfSearchKey = (attribute.type == TypeVarChar) ? (attribute.length + 4) : attribute.length;
    
    char* curIndexPage = (char*)malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, curIndexPage);
    char* newIndexPage = (char*)malloc(PAGE_SIZE);
    
    int isLeaf = 0;
    memcpy(newIndexPage + PAGE_SIZE - sizeof(int), &isLeaf, sizeof(int));
    
    memcpy(newIndexPage, curIndexPage + (order + 1) * (sizeof(int) + lenOfSearchKey), order * (sizeof(int) + lenOfSearchKey) + sizeof(int));
    
    char* tmpStr = (char*)malloc(110);    // the middle value will be pushed up
    int lenOfMiddleValue = 0;
    if (attribute.type == TypeVarChar) {
        memcpy(&lenOfMiddleValue, curIndexPage + order * (sizeof(int) + lenOfSearchKey) + sizeof(int), sizeof(int));
        memcpy(tmpStr, curIndexPage + order * (sizeof(int) + lenOfSearchKey) + 2 * sizeof(int), lenOfMiddleValue);
        tmpStr[lenOfMiddleValue] = '\0';
    }
    lenOfMiddleValue += sizeof(int);
    string middleValueString = string(tmpStr);
    free(tmpStr);
    
    void* middleValue = malloc(110);
    memcpy(middleValue, curIndexPage + order * (sizeof(int) + lenOfSearchKey) + sizeof(int), lenOfMiddleValue);
    
    // write in the number of indexes
    memcpy(curIndexPage + PAGE_SIZE - 2 * sizeof(int), &order, sizeof(int));
    memcpy(newIndexPage + PAGE_SIZE - 2 * sizeof(int), &order, sizeof(int));
    
    int father;    // get the father page number
    memcpy(&father, curIndexPage + PAGE_SIZE - 3 * sizeof(int), sizeof(int));
    unsigned newPageNum = fileHandle.getNumberOfPages();    // get the page number of the new page
    
    if (father == -1) {
        int root = newPageNum + 1;    // the father page will be append later
        char* firstPage = (char*)malloc(PAGE_SIZE);
        memcpy(firstPage, &root, sizeof(int));
        fileHandle.writePage(0, firstPage);
        free(firstPage);
        
        memcpy(curIndexPage + PAGE_SIZE - 3 * sizeof(int), &root, sizeof(int));
        memcpy(newIndexPage + PAGE_SIZE - 3 * sizeof(int), &root, sizeof(int));
    } else {
        memcpy(newIndexPage + PAGE_SIZE - 3 * sizeof(int), &father, sizeof(int));
    }
    
    // reset the children page's father pointer
    char* childPage = (char*)malloc(PAGE_SIZE);
    for (int i = 0; i <= order; i++) {
        int child;
        memcpy(&child, newIndexPage + (i * 2) * sizeof(int), sizeof(int));
        fileHandle.readPage(child, childPage);
        memcpy(childPage + PAGE_SIZE - 3 * sizeof(int), &newPageNum, sizeof(int));
        fileHandle.writePage(child, childPage);
    }
    free(childPage);
    
    // write back the current page and append the new page
    fileHandle.writePage(pageNum, curIndexPage);
    fileHandle.appendPage(newIndexPage);
    
    // free the memory
    free(curIndexPage);
    free(newIndexPage);
    
    if (father == -1) {    // the splitting node is the root node
        int fathersIsLeaf = 0;
        int fathersNumOfDataEntries = 1;
        int fathersFather = -1;
        char* newRootPage = (char*)malloc(PAGE_SIZE);
        memcpy(newRootPage + PAGE_SIZE - sizeof(int), &fathersIsLeaf, sizeof(int));
        memcpy(newRootPage + PAGE_SIZE - 2 * sizeof(int), &fathersNumOfDataEntries, sizeof(int));
        memcpy(newRootPage + PAGE_SIZE - 3 * sizeof(int), &fathersFather, sizeof(int));
        memcpy(newRootPage, &pageNum, sizeof(int));
        memcpy(newRootPage + sizeof(int), middleValue, lenOfMiddleValue);
        memcpy(newRootPage + sizeof(int) + lenOfMiddleValue, &newPageNum, sizeof(int));
        fileHandle.appendPage(newRootPage);
        free(middleValue);
        free(newRootPage);
        return;
    } else {
        char* fatherPage = (char*)malloc(PAGE_SIZE);
        fileHandle.readPage(father, fatherPage);
        
        int numOfIndexes;
        memcpy(&numOfIndexes, fatherPage + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
        
        int insertPos = numOfIndexes;
        if (attribute.type == TypeVarChar) {
            int len;
            char* index = (char*)malloc(110);
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&len, fatherPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                memcpy(index, fatherPage + i * (lenOfSearchKey + sizeof(int)) + 2 * sizeof(int), len);
                index[len] = '\0';
                string indexString = string(index);
                if (indexString > middleValueString) {
                    insertPos = i;
                    break;
                }
            }
            free(index);
        } else if (attribute.type == TypeInt) {
            int index;
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&index, fatherPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                if (index > *(int*)middleValue) {    // located the key value
                    insertPos = i;
                    break;
                }
            }
        } else {
            float index;
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&index, fatherPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(float));
                if (index > *(float*)middleValue) {    // located the key value
                    insertPos = i;
                    break;
                }
            }
        }
        for (int i = numOfIndexes; i > insertPos; i--) {
            // copy the indexes after the insertion position to the right
            memcpy(fatherPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), fatherPage + (i - 1) * (lenOfSearchKey + sizeof(int)) + sizeof(int), lenOfSearchKey + sizeof(int));
        }
        memcpy(fatherPage + insertPos * (lenOfSearchKey + sizeof(int)) + sizeof(int), middleValue, lenOfMiddleValue);
        memcpy(fatherPage + (insertPos + 1) * (lenOfSearchKey + sizeof(int)), &newPageNum, sizeof(int));
        numOfIndexes++;
        memcpy(fatherPage + PAGE_SIZE - 2 * sizeof(int), &numOfIndexes, sizeof(int));
        fileHandle.writePage(father, fatherPage);
        free(fatherPage);
        if (numOfIndexes == 2 * order + 1) {
            splitIndexNode(fileHandle, attribute, father);
        }
    }
    free(middleValue);
    return;
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    if (fileHandle.handle == NULL) return fail;
    
    char* firstPage = (char*)malloc(PAGE_SIZE);
    fileHandle.readPage(0, firstPage);
    int root;
    memcpy(&root, firstPage, sizeof(int));
    free(firstPage);
    
    int order = (attribute.type == TypeVarChar) ? ((PAGE_SIZE - 12) / (attribute.length + 12) - 1) / 2 : ((PAGE_SIZE - 12) / (attribute.length + 8) - 1) / 2;
    int lenOfSearchKey = (attribute.type == TypeVarChar) ? (attribute.length + 4) : attribute.length;
    // calculate the length of the key
    int keyLen = 0;
    char* tmpKey = (char*)malloc(110);
    if (attribute.type == TypeVarChar) {
        memcpy(&keyLen, (char*)key, sizeof(int));
        memcpy(tmpKey, (char*)key + sizeof(int), keyLen);
        tmpKey[keyLen] = '\0';
    }
    keyLen += sizeof(int);
    string keyString = string(tmpKey);
    free(tmpKey);
    
    char* indexPage = (char*)malloc(PAGE_SIZE);
    
    if (root == -1) {    // if the B+ tree is empty, append a page
        root = 1;    // the father page will be append later
        char* firstPage = (char*)malloc(PAGE_SIZE);
        memcpy(firstPage, &root, sizeof(int));
        fileHandle.writePage(0, firstPage);
        free(firstPage);
                
        int isLeaf = 1;
        int numOfDataEntries = 1;
        int father = -1;
        memcpy(indexPage + PAGE_SIZE - sizeof(int), &isLeaf, sizeof(int));
        memcpy(indexPage + PAGE_SIZE - 2 * sizeof(int), &numOfDataEntries, sizeof(int));
        memcpy(indexPage + PAGE_SIZE - 3 * sizeof(int), &father, sizeof(int));
        memcpy(indexPage, key, keyLen);
        memcpy(indexPage + lenOfSearchKey, &rid.pageNum, sizeof(int));
        memcpy(indexPage + lenOfSearchKey + sizeof(int), &rid.slotNum, sizeof(int));
        fileHandle.appendPage(indexPage);
        free(indexPage);
        return success;
    }
    
    int curPageNum = root;
    
    int count = 5;
    while (count--) {
        fileHandle.readPage(curPageNum, indexPage);    // read in the index page at the node
        
        int isLeaf;
        memcpy(&isLeaf, indexPage + PAGE_SIZE - sizeof(int), sizeof(int));
        if (isLeaf) break;    // this is a leaf page
        
        int numOfIndexes;
        memcpy(&numOfIndexes, indexPage + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
        
        bool found = false;
        
        if (attribute.type == TypeVarChar) {
            int len;
            char* index = (char*)malloc(110);
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&len, indexPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                memcpy(index, indexPage + i * (lenOfSearchKey + sizeof(int)) + 2 * sizeof(int), len);
                index[len] = '\0';
                string indexString = string(index);
                if (indexString > keyString) {
                    memcpy(&curPageNum, indexPage + i * (lenOfSearchKey + sizeof(int)), sizeof(int));
                    found = true;
                    break;
                }
            }
            free(index);
        } else if (attribute.type == TypeInt) {
            int index;
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&index, indexPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                if (index > *(int*)key) {    // located the key value
                    memcpy(&curPageNum, indexPage + i * (lenOfSearchKey + sizeof(int)), sizeof(int));
                    found = true;
                    break;
                }
            }
        } else {
            float index;
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&index, indexPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(float));
                if (index > *(float*)key) {    // located the key value
                    memcpy(&curPageNum, indexPage + i * (lenOfSearchKey + sizeof(int)), sizeof(int));
                    found = true;
                    break;
                }
            }
        }
        if (!found) {    // the key value is larger than the largest index
            memcpy(&curPageNum, indexPage + numOfIndexes * (lenOfSearchKey + sizeof(int)), sizeof(int));
        }
    }
    
    // now curPageNum is the page number of the leaf page
    int numOfDataEntries;
    memcpy(&numOfDataEntries, indexPage + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
    
    int insertPos = numOfDataEntries;
    if (attribute.type == TypeVarChar) {
        int len;
        char* index = (char*)malloc(110);
        for (int i = 0; i < numOfDataEntries; i++) {
            memcpy(&len, indexPage + i * (2 * sizeof(int) + lenOfSearchKey), sizeof(int));
            memcpy(index, indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + sizeof(int), len);
            index[len] = '\0';
            string indexString = string(index);
            if (indexString == keyString) {
                RID ridAtI;
                memcpy(&ridAtI.pageNum, indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, sizeof(int));
                memcpy(&ridAtI.slotNum, indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey + sizeof(int), sizeof(int));
                if (ridAtI.pageNum == rid.pageNum && ridAtI.slotNum == rid.slotNum) {    // inserting duplicates
                    return success;
                }
            }
            if (indexString > keyString) {
                insertPos = i;
                break;
            }
        }
        free(index);
    } else if (attribute.type == TypeInt) {
        int index;
        for (int i = 0; i < numOfDataEntries; i++) {
            memcpy(&index, indexPage + i * (2 * sizeof(int) + lenOfSearchKey), sizeof(int));
            if (index == *(int*)key) {
                RID ridAtI;
                memcpy(&ridAtI.pageNum, indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, sizeof(int));
                memcpy(&ridAtI.slotNum, indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey + sizeof(int), sizeof(int));
                if (ridAtI.pageNum == rid.pageNum && ridAtI.slotNum == rid.slotNum) {    // inserting duplicates
                    return success;
                }
            }
            if (index > *(int*)key) {
                insertPos = i;
                break;
            }
        }
    } else {
        float index;
        for (int i = 0; i < numOfDataEntries; i++) {
            memcpy(&index, indexPage + i * (2 * sizeof(int) + lenOfSearchKey), sizeof(float));
            if (index == *(float*)key) {
                RID ridAtI;
                memcpy(&ridAtI.pageNum, indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, sizeof(int));
                memcpy(&ridAtI.slotNum, indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey + sizeof(int), sizeof(int));
                if (ridAtI.pageNum == rid.pageNum && ridAtI.slotNum == rid.slotNum) {    // inserting duplicates
                    return success;
                }
            }
            if (index > *(float*)key) {
                insertPos = i;
                break;
            }
        }
    }
    for (int i = numOfDataEntries; i > insertPos; i--) {
        // copy the data entries after the insertion position to the right
        memcpy(indexPage + i * (2 * sizeof(int) + lenOfSearchKey), indexPage + (i - 1) * (2 * sizeof(int) + lenOfSearchKey), 2 * sizeof(int) + lenOfSearchKey);
    }
    
    memcpy(indexPage + insertPos * (2 * sizeof(int) + lenOfSearchKey), key, keyLen);
    memcpy(indexPage + insertPos * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, &rid.pageNum, sizeof(int));
    memcpy(indexPage + insertPos * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey + sizeof(int), &rid.slotNum, sizeof(int));
    numOfDataEntries++;
    memcpy(indexPage + PAGE_SIZE - 2 * sizeof(int), &numOfDataEntries, sizeof(int));
    
    // printf("current page num: %d and number of entries in the page: %d\n", curPageNum, numOfDataEntries);
    
    fileHandle.writePage(curPageNum, indexPage);
    
    if (numOfDataEntries == 2 * order + 1) {    // will be split
        splitLeafNode(fileHandle, attribute, curPageNum);
    }
    
    free(indexPage);
    return success;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    if (fileHandle.handle == NULL) return fail;
    
    char* firstPage = (char*)malloc(PAGE_SIZE);
    fileHandle.readPage(0, firstPage);
    int root;
    memcpy(&root, firstPage, sizeof(int));
    free(firstPage);
        
    //int order = (attribute.type == TypeVarChar) ? ((PAGE_SIZE - 12) / (attribute.length + 12) - 1) / 2 : ((PAGE_SIZE - 12) / (attribute.length + 8) - 1) / 2;
    int lenOfSearchKey = (attribute.type == TypeVarChar) ? (attribute.length + 4) : attribute.length;
    
    if (root == -1) {     // this is an empty B+ tree
        return fail;
    }
    
    // calculate the length of the key
    int keyLen = 0;
    char* tmpKey = (char*)malloc(110);
    if (attribute.type == TypeVarChar) {
        memcpy(&keyLen, (char*)key, sizeof(int));
        memcpy(tmpKey, (char*)key + sizeof(int), keyLen);
        tmpKey[keyLen] = '\0';
    }
    keyLen += sizeof(int);
    string keyString = string(tmpKey);
    free(tmpKey);
    
    char* indexPage = (char*)malloc(PAGE_SIZE);
    int curPageNum = root;
    
    while (true) {
        fileHandle.readPage(curPageNum, indexPage);    // read in the index page at the node
        
        int isLeaf;
        memcpy(&isLeaf, indexPage + PAGE_SIZE - sizeof(int), sizeof(int));
        if (isLeaf) break;    // this is a leaf page
        
        int numOfIndexes;
        memcpy(&numOfIndexes, indexPage + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
        
        bool found = false;
        
        if (attribute.type == TypeVarChar) {
            int len;
            char* index = (char*)malloc(110);
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&len, indexPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                memcpy(index, indexPage + i * (lenOfSearchKey + sizeof(int)) + 2 * sizeof(int), len);
                index[len] = '\0';
                string indexString = string(index);
                if (indexString > keyString) {
                    memcpy(&curPageNum, indexPage + i * (lenOfSearchKey + sizeof(int)), sizeof(int));
                    found = true;
                    break;
                }
            }
            free(index);
        } else if (attribute.type == TypeInt) {
            int index;
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&index, indexPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                if (index > *(int*)key) {    // located the key value
                    memcpy(&curPageNum, indexPage + i * (lenOfSearchKey + sizeof(int)), sizeof(int));
                    found = true;
                    break;
                }
            }
        } else {
            float index;
            for (int i = 0; i < numOfIndexes; i++) {
                memcpy(&index, indexPage + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(float));
                if (index > *(float*)key) {    // located the key value
                    memcpy(&curPageNum, indexPage + i * (lenOfSearchKey + sizeof(int)), sizeof(int));
                    found = true;
                    break;
                }
            }
        }
        if (!found) {    // the key value is larger than the largest index
            memcpy(&curPageNum, indexPage + numOfIndexes * (lenOfSearchKey + sizeof(int)), sizeof(int));
        }
    }
    
    // now curPageNum is the page number of the leaf page
    int numOfDataEntries;
    memcpy(&numOfDataEntries, indexPage + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
    
    if (attribute.type == TypeVarChar) {
        int len;
        char* index = (char*)malloc(110);
        for (int i = 0; i < numOfDataEntries; i++) {
            memcpy(&len, indexPage + i * (2 * sizeof(int) + lenOfSearchKey), sizeof(int));
            memcpy(index, indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + sizeof(int), len);
            index[len] = '\0';
            string indexString = string(index);
            if (indexString == keyString) {
                int tmp;
                memcpy(&tmp, indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, sizeof(int));
                if (tmp == -1) continue;
                tmp = -1;
                memcpy(indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, &tmp, sizeof(int));
                memcpy(indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey + sizeof(int), &tmp, sizeof(int));
                fileHandle.writePage(curPageNum, indexPage);
                free(indexPage);
                return success;
            }
        }
        free(index);
    } else if (attribute.type == TypeInt) {
        int index;
        for (int i = 0; i < numOfDataEntries; i++) {
            memcpy(&index, indexPage + i * (2 * sizeof(int) + lenOfSearchKey), sizeof(int));
            if (index == *(int*)key) {
                int tmp;
                memcpy(&tmp, indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, sizeof(int));
                if (tmp == -1) continue;
                tmp = -1;
                memcpy(indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, &tmp, sizeof(int));
                memcpy(indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey + sizeof(int), &tmp, sizeof(int));
                fileHandle.writePage(curPageNum, indexPage);
                free(indexPage);
                return success;
            }
        }
    } else {
        float index;
        for (int i = 0; i < numOfDataEntries; i++) {
            memcpy(&index, indexPage + i * (2 * sizeof(int) + lenOfSearchKey), sizeof(float));
            if (index == *(float*)key) {
                int tmp;
                memcpy(&tmp, indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, sizeof(int));
                if (tmp == -1) continue;
                tmp = -1;
                memcpy(indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, &tmp, sizeof(int));
                memcpy(indexPage + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey + sizeof(int), &tmp, sizeof(int));
                fileHandle.writePage(curPageNum, indexPage);
                free(indexPage);
                return success;
            }
        }
    }
    free(indexPage);
    return fail;
}

void IndexManager::scanPage(FileHandle &fileHandle,
      int pageNum,
      const Attribute &attribute,
      const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator)
{
    //int order = (attribute.type == TypeVarChar) ? ((PAGE_SIZE - 12) / (attribute.length + 12) - 1) / 2 : ((PAGE_SIZE - 12) / (attribute.length + 8) - 1) / 2;
    int lenOfSearchKey = (attribute.type == TypeVarChar) ? (attribute.length + 4) : attribute.length;
    
    char* page = (char*)malloc(PAGE_SIZE);
    
    fileHandle.readPage(pageNum, page);
    
    int isLeaf;
    memcpy(&isLeaf, page + PAGE_SIZE - sizeof(int), sizeof(int));
    
    int keyLen = 0;
    char* tmpKey = (char*)malloc(110);
    if (attribute.type == TypeVarChar) {
        memcpy(&keyLen, (char*)lowKey, sizeof(int));
        memcpy(tmpKey, (char*)lowKey + sizeof(int), keyLen);
        tmpKey[keyLen] = '\0';
    }
    string lowKeyString = string(tmpKey);
    if (attribute.type == TypeVarChar) {
        memcpy(&keyLen, (char*)highKey, sizeof(int));
        memcpy(tmpKey, (char*)highKey + sizeof(int), keyLen);
        tmpKey[keyLen] = '\0';
    }
    string highKeyString = string(tmpKey);
    keyLen += sizeof(int);
    free(tmpKey);
    
    if (isLeaf) {
        int numOfDataEntries;
        memcpy(&numOfDataEntries, page + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
        
        if (attribute.type == TypeVarChar) {
            int len;
            char* index = (char*)malloc(110);
            for (int i = 0; i < numOfDataEntries; i++) {
                memcpy(&len, page + i * (2 * sizeof(int) + lenOfSearchKey), sizeof(int));
                memcpy(index, page + i * (2 * sizeof(int) + lenOfSearchKey) + sizeof(int), len);
                index[len] = '\0';
                string indexString = string(index);
                RID rid;
                memcpy(&rid.pageNum, page + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, sizeof(int));
                memcpy(&rid.slotNum, page + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey + sizeof(int), sizeof(int));
                if ( (lowKey == NULL || (lowKeyInclusive && lowKeyString <= indexString) || ((!lowKeyInclusive) && lowKeyString < indexString)) 
                        && (highKey == NULL || (highKeyInclusive && highKeyString >= indexString) || ((!highKeyInclusive) && highKeyString > indexString)) ) {
                    if (rid.pageNum != (unsigned)-1) {
                        ix_ScanIterator.insertDataEntry(rid, index, len);
                    }
                }
            }
            free(index);
        } else if (attribute.type == TypeInt) {
            int index;
            for (int i = 0; i < numOfDataEntries; i++) {
                memcpy(&index, page + i * (2 * sizeof(int) + lenOfSearchKey), sizeof(int));
                RID rid;
                memcpy(&rid.pageNum, page + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, sizeof(int));
                memcpy(&rid.slotNum, page + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey + sizeof(int), sizeof(int));
                if (  (lowKey == NULL || (lowKeyInclusive && *(int*)lowKey <= index) || ((!lowKeyInclusive) && *(int*)lowKey < index)) 
                        && (highKey == NULL || (highKeyInclusive && *(int*)highKey >= index) || ((!highKeyInclusive) && *(int*)highKey > index)) ) {
                    if (rid.pageNum != (unsigned)-1) {
                        ix_ScanIterator.insertDataEntry(rid, &index, sizeof(int));
                    }
                }
            }
        } else {
            float index;
            for (int i = 0; i < numOfDataEntries; i++) {
                memcpy(&index, page + i * (2 * sizeof(int) + lenOfSearchKey), sizeof(float));
                RID rid;
                memcpy(&rid.pageNum, page + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey, sizeof(int));
                memcpy(&rid.slotNum, page + i * (2 * sizeof(int) + lenOfSearchKey) + lenOfSearchKey + sizeof(int), sizeof(int));
                if ( (lowKey == NULL || (lowKeyInclusive && *(float*)lowKey <= index) || ((!lowKeyInclusive) && *(float*)lowKey < index)) 
                        && (highKey == NULL || (highKeyInclusive && *(float*)highKey >= index) || ((!highKeyInclusive) && *(float*)highKey > index)) ) {
                    if (rid.pageNum != (unsigned)-1) {
                        ix_ScanIterator.insertDataEntry(rid, &index, sizeof(float));
                    }
                }
            }
        }
    } else {
        int numOfIndexes;
        memcpy(&numOfIndexes, page + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
        
        if (attribute.type == TypeVarChar) {
            int lowRange = numOfIndexes;
            int highRange = 0;
            int len;
            char* index = (char*)malloc(110);
            if (lowKey == NULL)
                lowRange = 0;
            else {
                for (int i = 0; i < numOfIndexes; i++) {
                    memcpy(&len, page + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                    memcpy(index, page + i * (lenOfSearchKey + sizeof(int)) + 2 * sizeof(int), len);
                    index[len] = '\0';
                    string indexString = string(index);
                    if (indexString >= lowKeyString) {
                        lowRange = i;
                        break;
                    }
                }
            }
            if (highKey == NULL)
                highRange = numOfIndexes;
            else {
                for (int i = numOfIndexes - 1; i >= 0; i--) {
                    memcpy(&len, page + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                    memcpy(index, page + i * (lenOfSearchKey + sizeof(int)) + 2 * sizeof(int), len);
                    index[len] = '\0';
                    string indexString = string(index);
                    if (indexString <= highKeyString) {
                        highRange = i + 1;
                        break;
                    }
                }
            }
            free(index);
            for (int i = lowRange; i <= highRange; i++) {
                int child;
                memcpy(&child, page + i * (lenOfSearchKey + sizeof(int)), sizeof(int));
                scanPage(fileHandle, child, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_ScanIterator);
            }
        } else if (attribute.type == TypeInt) {
            int lowRange = numOfIndexes;
            int highRange = 0;
            int index;
            if (lowKey == NULL)
                lowRange = 0;
            else {
                for (int i = 0; i < numOfIndexes; i++) {
                    memcpy(&index, page + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                    if (index >= *(int*)lowKey) {
                        lowRange = i;
                        break;
                    }
                }
            }
            if (highKey == NULL)
                highRange = numOfIndexes;
            else {
                for (int i = numOfIndexes - 1; i >= 0; i--) {
                    memcpy(&index, page + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(int));
                    if (index <= *(int*)highKey) {
                        highRange = i + 1;
                        break;
                    }
                }
            }
            for (int i = lowRange; i <= highRange; i++) {
                int child;
                memcpy(&child, page + i * (lenOfSearchKey + sizeof(int)), sizeof(int));
                scanPage(fileHandle, child, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_ScanIterator);
            }
        } else {
            int lowRange = numOfIndexes;
            int highRange = 0;
            float index;
            if (lowKey == NULL)
                lowRange = 0;
            else {
                for (int i = 0; i < numOfIndexes; i++) {
                    memcpy(&index, page + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(float));
                    if (index >= *(float*)lowKey) {
                        lowRange = i;
                        break;
                    }
                }
            }
            if (highKey == NULL)
                highRange = numOfIndexes;
            else {
                for (int i = numOfIndexes - 1; i >= 0; i--) {
                    memcpy(&index, page + i * (lenOfSearchKey + sizeof(int)) + sizeof(int), sizeof(float));
                    if (index <= *(float*)highKey) {
                        highRange = i + 1;
                        break;
                    }
                }
            }
            for (int i = lowRange; i <= highRange; i++) {
                int child;
                memcpy(&child, page + i * (lenOfSearchKey + sizeof(int)), sizeof(int));
                scanPage(fileHandle, child, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_ScanIterator);
            }
        }
    }
    free(page);
}

RC IndexManager::scan(FileHandle &fileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
    if (fileHandle.handle == NULL) return fail;
    char* firstPage = (char*)malloc(PAGE_SIZE);
    fileHandle.readPage(0, firstPage);
    int root;
    memcpy(&root, firstPage, sizeof(int));
    free(firstPage);
    
    if (root == -1) return success;
    scanPage(fileHandle, root, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_ScanIterator);
    return success;
}

IX_ScanIterator::IX_ScanIterator()
{
    rbfmsi = new RBFM_ScanIterator();
}

IX_ScanIterator::~IX_ScanIterator()
{
    delete rbfmsi;
}

RC IX_ScanIterator::insertDataEntry(const RID& rid, const void* key, int keyLen)
{
    return rbfmsi->insertRecord(rid, key, keyLen);
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return rbfmsi->getNextRecord(rid, key);
}

RC IX_ScanIterator::close()
{
    return rbfmsi->close();
}

void IX_PrintError (RC rc)
{
}
