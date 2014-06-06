//
//File:        ix_manager.cc
//Description: IX_Manager class implementation
//

#include "ix_internal.h"
#include <stdlib.h>
#include <stdio.h>

//
//IX_Manager
//
//Desc: Constructor
//
IX_Manager::IX_Manager(PF_Manager &pfm){
    // Set the associated PF_Manager object
    pPfm = &pfm;
}

// ~IX_Manager
//
//Desc: Destructor
//
IX_Manager::~IX_Manager(){
    //Clear the associated PF_Manager object
    pPfm = NULL;
}

//
// CreateFile
//
// Desc: Create a new Index file whose name is "fileName"
//       Allocate a file header page and fill out some information
// In:   fileName - name of file to create
//       indexNo - number of index
//       attrType   - INT|FLOAT|STRING
//       attrLength - 4 for INT|FLOAT, 1~MAXSTRINGLEN for STRING
// Ret:  ?? or PF return code
//
RC IX_Manager::CreateIndex(const char *fileName, int indexNo, AttrType attrType, int attrLength){
    
    RC rc;
    PF_FileHandle pfFileHandle;
    PF_PageHandle pageHandle;
    char* pData;
    IX_FileHdr *fileHdr;
    
   // You may assume that clients of this method will ensure that the indexNo parameter is unique and nonnegative for each index created on a file.
    
    // Sanity Check: attrType, attrLength
    switch (attrType) {
        case INT:
        case FLOAT:
            if (attrLength != 4)
                // Test: wrong _attrLength
                return (IX_INVALIDATTR);
            break;
            
        case STRING:
            if (attrLength < 1 || attrLength > MAXSTRINGLEN)
                // Test: wrong _attrLength
                return (IX_INVALIDATTR);
            break;
            
        default:
            // Test: wrong _attrType
            return (IX_INVALIDATTR);
    }
    
    char filename[5];//filename = fileName.indexNo as the new filename
    strcpy(filename, fileName);
    strcat(filename, ".");
    char index[5];
    sprintf(index, "%d", indexNo);
    strcat(filename, index);
    
    
    // Call PF_Manager::CreateFile()
    if (rc = pPfm->CreateFile(filename))
        // Test: existing fileName, wrong permission
        goto err_return;
    
    // Call PF_Manager::OpenFile()
    if (rc = pPfm->OpenFile(filename, pfFileHandle))
        // Should not happen
        goto err_destroy;
    
    // Allocate the header page (pageNum must be 0)
    if (rc = pfFileHandle.AllocatePage(pageHandle))
        // Should not happen
        goto err_close;
    
    // Get a pointer where header information will be written
    if (rc = pageHandle.GetData(pData))
        // Should not happen
        goto err_unpin;
    
    // Write the file header (to the buffer pool)
    fileHdr = (IX_FileHdr *)pData;
    fileHdr->indexNo=indexNo;
    fileHdr->attrType=attrType;
    fileHdr->attrLength=attrLength;
    fileHdr->numMaxEntries=(PF_PAGE_SIZE)/(attrLength+sizeof(PageNum));
    fileHdr->rootPageNum=1;
    fileHdr->numRidsPerBucket=(PF_PAGE_SIZE)/(sizeof(PageNum)+sizeof(SlotNum));

    //add by dzh : calculate numRidsPerBucket and bucketHeaderSize
    fileHdr->numRidsPerBucket = (PF_PAGE_SIZE - sizeof(IX_BucketHdr) - 1)
            / (sizeof(RID) + 1.0/8);
    fileHdr->bucketHeaderSize = sizeof(IX_BucketHdr) + (fileHdr->numRidsPerBucket + 7) / 8;

    // Mark the header page as dirty
    if (rc = pfFileHandle.MarkDirty(IX_HEADER_PAGE_NUM))
        // Should not happen
        goto err_unpin;
    
    // Unpin the header page
    if (rc = pfFileHandle.UnpinPage(IX_HEADER_PAGE_NUM))
        // Should not happen
        goto err_close;
    
    // Call PF_Manager::CloseFile()
    if (rc = pPfm->CloseFile(pfFileHandle))
        // Should not happen
        goto err_destroy;
    
    // Return ok
    return (0);
    
    // Recover from inconsistent state due to unexpected error
err_unpin:
    pfFileHandle.UnpinPage(IX_HEADER_PAGE_NUM);
err_close:
    pPfm->CloseFile(pfFileHandle);
err_destroy:
    pPfm->DestroyFile(filename);
err_return:
    // Return error
    return (rc);
    
    
}

//
//DestroyFile
//
// Desc: Delete a IX file name fileName (fileName must exist and not open)
//In: fileName - name of file to delete, indexNo - indexNo of file to delete
//Ret: PF return code
//
RC IX_Manager::DestroyIndex(const char *fileName, int indexNo){
    RC rc;
    
    char filename[5];//filename = fileName.indexNo as the new filename
    strcpy(filename, fileName);
    strcat(filename, ".");
    char index[5];
    sprintf(index, "%d", indexNo);
    strcat(filename, index);
    
    
    // Call PF_Manager::DestroyFile()
    if (rc = pPfm->DestroyFile(filename))
        // Test: non-existing fileName, wrong permission
        goto err_return;
    
    // Return ok
    return (0);
    
err_return:
    // Return error
    return (rc);
    
}

//
// OpenFile
//
// Desc: Open the paged file whose name is "fileName" and index number is "indexNo"
//       Copy the file header information into a private variable in
//       the file handle so that we can unpin the header page immediately
//       and later find out details without reading the header page
// In:   fileName - name of file to open
// In:   indexNo - index number
// Out:  indexHandle - refer to the open file
// Ret:  PF return code
//

RC IX_Manager::OpenIndex(const char *fileName, int indexNo, IX_IndexHandle &indexHandle){
    RC rc;
    PF_PageHandle pageHandle;
    char* pData;
    
    char filename[5];//filename = fileName.indexNo as the new filename
    strcpy(filename, fileName);
    strcat(filename, ".");
    char index[5];
    sprintf(index, "%d", indexNo);
    strcat(filename, index);
    
    // Call PF_Manager::OpenFile()
    if (rc = pPfm->OpenFile(filename, indexHandle.pfFileHandle))
        // Test: non-existing fileName, opened fileHandle
        goto err_return;
    
    // Get the header page
    if (rc = indexHandle.pfFileHandle.GetFirstPage(pageHandle))
        // Test: invalid file
        goto err_close;
    
    // Get a pointer where header information resides
    if (rc = pageHandle.GetData(pData))
        // Should not happen
        goto err_unpin;
    
    // Read the file header (from the buffer pool to IX_FileHandle)
    memcpy(&indexHandle.fileHdr, pData, sizeof(indexHandle.fileHdr));
    
    // Unpin the header page
    if (rc = indexHandle.pfFileHandle.UnpinPage(IX_HEADER_PAGE_NUM))
        // Should not happen
        goto err_close;
    
    // TODO: cannot guarantee the validity of file header at this time
    
    // Set file header to be not changed
    indexHandle.bHdrChanged = FALSE;
    
    // Return ok
    return (0);
    
    // Recover from inconsistent state due to unexpected error
err_unpin:
    indexHandle.pfFileHandle.UnpinPage(IX_HEADER_PAGE_NUM);
err_close:
    pPfm->CloseFile(indexHandle.pfFileHandle);
err_return:
    // Return error
    return (rc);

}

//
// CloseFile
//
// Desc: Close file associated with fileHandle
//       Write back the file header (if there was any changes)
// In:   indexHandle - handle of file to close
// Out:  fileHandle - no longer refers to an open file
// Ret:  IX return code
//
RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle){
    RC rc;
    
    // Write back the file header if any changes made to the header
    // while the file is open
    if (indexHandle.bHdrChanged) {
        PF_PageHandle pageHandle;
        char* pData;
        
        // Get the header page
        if (rc = indexHandle.pfFileHandle.GetFirstPage(pageHandle))
            // Test: unopened(closed) fileHandle, invalid file
            goto err_return;
        
        // Get a pointer where header information will be written
        if (rc = pageHandle.GetData(pData))
            // Should not happen
            goto err_unpin;
        
        // Write the file header (to the buffer pool)
        memcpy(pData, &indexHandle.fileHdr, sizeof(indexHandle.fileHdr));
        
        // Mark the header page as dirty
        if (rc = indexHandle.pfFileHandle.MarkDirty(IX_HEADER_PAGE_NUM))
            // Should not happen
            goto err_unpin;
        
        // Unpin the header page
        if (rc = indexHandle.pfFileHandle.UnpinPage(IX_HEADER_PAGE_NUM))
            // Should not happen
            goto err_return;
        
        // Set file header to be not changed
        indexHandle.bHdrChanged = FALSE;
    }
    
    // Call PF_Manager::CloseFile()
    if (rc = pPfm->CloseFile(indexHandle.pfFileHandle))
        // Test: unopened(closed) fileHandle
        goto err_return;
    
    // Reset member variables
    memset(&indexHandle.fileHdr, 0, sizeof(indexHandle.fileHdr));
//    fileHandle.fileHdr.firstFree = RM_PAGE_LIST_END;
    
    // Return ok
    return (0);
    
    // Recover from inconsistent state due to unexpected error
err_unpin:
    indexHandle.pfFileHandle.UnpinPage(IX_HEADER_PAGE_NUM);
err_return:
    // Return error
    return (rc);

}
