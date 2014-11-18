/**
 * pfm implementation 
 * by Xi Huang & Hongyang Sun
 * version: 1/11/2014
 * To remove all debug information(errors,),
 * use replace in IDE, replace //printf to ////printf.
 * enable the debug vice versa.
 */


#include <stdio.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;
const RC success = 0;
const RC fail = -1;

PagedFileManager* PagedFileManager::instance() {
    if (!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

PagedFileManager::PagedFileManager() {
    //no need to edit.
}

PagedFileManager::~PagedFileManager() {
    //no need to edit.
}

/**
 * 
 * @param fileName file name want to create
 * @return 
 * This method creates a paged file called fileName.
 * The file should not already exist.
 */
RC PagedFileManager::createFile(const char *fileName) {
    struct stat fileInfo;
    if (stat(fileName, &fileInfo) == 0) {
        //printf("ERROR 100: createFile fail, file exists\n");
        return fail; // file exists
    }

    FILE *handle = fopen(fileName, "wb+");
    if (handle != NULL) {
        fclose(handle);
        return success;
    }
    //printf("ERROR 101: createFile fail, file cannot be created\n");
    return fail;
}

/**
 * 
 * @param fileName the final name want to remove
 * @return remove status
 * This method destroys the paged file whose name is fileName. The file should exist.
 */
RC PagedFileManager::destroyFile(const char *fileName) {
    if (remove(fileName) != -1) {
        return success; //exists, and has been removed
    }
    printf("ERROR 102: destroyFIle fail, file cannot be removed\n");
    return fail; //fail if not exists or in use.
}

/**
 * 
 * @param fileName the final file want to open
 * @param fileHandle file handle want to deliver
 * @return returns open status
 * This method opens the paged file whose name is fileName
 * The file must already exist and it must have been created using the CreateFile method
 * If the method is successful, the fileHandle object whose address is passed as a parameter becomes a "handle" for the open file. 
 * It is an error if fileHandle is already a handle for an open file when it is passed to the OpenFile method
 * It is not an error to open the same file more than once if desired  using a different fileHandle object each time
 * Each call to the OpenFile method creates a new "instance" of the open file
 * Opening a file more than once for data modification is not prevented by the PF component,
 * but doing so is likely to corrupt the file structure and may crash the PF component. Opening a file more than once for reading is no problem.
 */
RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle) {
    //check file existence
    struct stat fileInfo;
    if (stat(fileName, &fileInfo) != 0) {
        //printf("ERROR 103: openFile fail, file does not exists\n");
        return fail; // file does not exist
    }

    //open file
    FILE *handle = fopen(fileName, "rb+");
    if (handle != NULL) //file open successfully
    {
        if (fileHandle.handle == NULL) //file handle is NOT in use
        {
            fileHandle.handle = handle;
            fileHandle.fileName = fileName;
            return success;
        } else if (fileHandle.fileName == fileName) {
            fclose(fileHandle.handle); //close in used same file name handle
            fileHandle.handle = handle; // pass in a new opened handle
            return success;
        } else {
            //printf("ERROR 104: openFile fail, fileHanle is in use.\n");
            //fclose(handle); // in use file should not be closed ? could be a question.
            return fail;
        }

    }
    //printf("ERROR 105: openFile fail, file cannot be opened.\n");
    return fail;
}

/**
 * 
 * @param fileHandle
 * @return return RC status
 * This method closes the open file instance referred to by fileHandle. 
 * The file must have been opened using the OpenFile method. 
 * All of the file's pages are flushed to disk when the file is closed.
 */
RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    if (fclose(fileHandle.handle) == 0) {
        return success;
    }
    //printf("ERROR 106: closeFile fail, cannot close stream in fileHandle\n");
    return fail;
}

FileHandle::FileHandle() {
    this->handle = NULL;
}

FileHandle::~FileHandle() {
    this->handle = NULL;
}

/**
 * 
 * @param pageNum given page number
 * @param data given a block of memory
 * @return  return RC status
 * This method reads the page into the memory block pointed by data. 
 * The page should exist. Note the page number starts from 0.
 */
RC FileHandle::readPage(PageNum pageNum, void *data) {
    if (pageNum < 0 || this->getNumberOfPages() <= pageNum) {
        //printf("ERROR 107: readPage fail, pageNum is less than 0 or larger than total number\n");
        return fail; //bad pageNum to read
    }
    int offset = pageNum * PAGE_SIZE;
    fseek(handle, offset, SEEK_SET);

    int result = fread(data, 1, PAGE_SIZE, handle);
    if (result != PAGE_SIZE) {
        //printf("ERROR 108: readPage fail, total read number is not equal to PAGE_SIZE\n");
        return fail;
    }
    return success;
}

/**
 * 
 * @param pageNum given page number to write
 * @param data given data memory area
 * @return operate status
 * This method writes the data into a page specified by the pageNum.
 * The page should exist. Note the page number starts from 0.
 */
RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if (pageNum < 0 || getNumberOfPages() <= pageNum) {
        //printf("ERROR 109: writePage fail, pageNum is less than 0 or larger than total number\n");
        return fail;
    }

    int offset = pageNum * PAGE_SIZE;
    fseek(handle, offset, SEEK_SET);
    int result = fwrite(data, 1, PAGE_SIZE, handle);
    if (result == PAGE_SIZE) {
        return success;
    }
    //printf("ERROR 110: writePage fail, total write number is not equal to PAGE_SIZE\n");
    return fail;
}

/**
 * 
 * @param data given data memory area
 * @return operate status
 * This method appends a new page to the file, 
 * and writes the data into the new allocated page.
 */
RC FileHandle::appendPage(const void *data) {
    fseek(handle, 0, SEEK_END);
    int result = fwrite(data, 1, PAGE_SIZE, handle);
    if (result == PAGE_SIZE) {
        return success;
    }
    //printf("ERROR 111: appendPage fail, append file error, page cannot be added to file\n");
    return fail;
}

/**
 * 
 * @return total number of pages
 * This method returns the total number of pages in the file.
 */
unsigned FileHandle::getNumberOfPages() {
    //forbid call for a null handle object
    if (handle == NULL) {
        printf("ERROR 111: getNumberOfPages fail, fileHandle is null\n");
        return fail;
    }
    //calculate total page: all byte / each page size
    unsigned totalPage;
    fseek(handle, 0, SEEK_END);
    totalPage = ftell(handle) / PAGE_SIZE;
    return totalPage;
}


