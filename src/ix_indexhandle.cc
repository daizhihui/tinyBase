//
//  ix_handler.cpp
//  TinyBaseP
//
//  Created by Cyril on 5/31/14.
//  Copyright (c) 2014 Cyril. All rights reserved.
//

#include "ix.h"
#include "ix_internal.h"
#include <stdio.h>
#include <string.h>
//temporary, just for algo.
//we will need these
struct IX_FileHdr filehdr;
PF_FileHandle filehandler;
PF_PageHandle pageHandler;

//TODO: make sure bucket logic is there.

//node =data + child page numbers
//t is the minimum degree of the Tree: every node other than the root must have t-1 keys at least.
int t;
struct node
{
    PageNum pageNumber;
    int numberOfKeys;
    //    int numberOfChildren; = numberOfKeys+1;
    char** keys; //read as array of bytes
    PageNum* children;
    bool leaf;
};

//compare
//returns true when k1 is greater than k2
// if k1 and k2 are strings, it will return the one with the biggest first character that does not match.
int compare(void* k1,void* k2)
{
    switch (filehdr.attrType) {
        case FLOAT:
            if(*(float*)k1==(*(float*)k2))
                return 0;
            return (*(float*)k1)>(*(float*)k2)?1:-1;
            break;
        case INT:
            if((*(int*)k1)==(*(int*)k2))
                return 0;
            return (*(int*)k1)>(*(int*)k2)?1:-1;
            break;
        case STRING:
            return strcmp((char*)k1,(char*)k2);
            break;
        default:
            break;
    }
    
}

//Will read and init a node from this page on the disk

node* readNodeFromPageNum(PageNum pn)
{
    filehandler.GetThisPage(pn, pageHandler);
    char* pData;
    pageHandler.GetData(pData);
    node* x = new node();
    x->pageNumber = pData[0];
    x->numberOfKeys = pData[sizeof(PageNum)];
    
    x->keys = new char*[x->numberOfKeys];
    x->children = new PageNum[x->numberOfKeys+1];
    for(int i =0;i<x->numberOfKeys;i++)
        memcpy(x->keys[i], &pData[sizeof(PageNum)+sizeof(int)+i*filehdr.attrLength], filehdr.attrLength);
    
    //in case we want the exact length of the strings:
    //    switch (filehdr.attrType) {
    //        case STRING:
    //        {
    //            x->keys = new char*[x->numberOfKeys];
    //            int count=0;
    //            for(int i =0;i<x->numberOfKeys;i++)
    //            {
    //                x->keys[i]=new char[MAXSTRINGLEN];
    //                int j =0;
    //                while(true)
    //                {
    //                    x->keys[i][j] = pData[sizeof(PageNum)+1+count];
    //                    if(x->keys[i][j]=='\0' || j==MAXSTRINGLEN)
    //                        break;
    //                    j++;
    //                    count++;
    //                }
    //            }
    //            break;
    //        }
    //        default:
    //        {
    //            x->keys = new char*[x->numberOfKeys];
    //            for(int i =0;i<x->numberOfKeys;i++)
    //                x->keys[i]=&pData[sizeof(PageNum)+sizeof(int)+i*filehdr.attrLength];
    //            break;
    //        }
    //    }
    
    for(int i =0;i<=x->numberOfKeys;i++)
    {
        x->children[i] = (PageNum)pData[sizeof(PageNum)+sizeof(int)+x->numberOfKeys*filehdr.attrLength+i*sizeof(PageNum)];
    }
    x->leaf = (bool)pData[sizeof(PageNum)+sizeof(int)+x->numberOfKeys*filehdr.attrLength+(x->numberOfKeys+1)*sizeof(PageNum)];
    return  x;
}

//will write a node on a new page
void writeNodeOnNewPage(node* x)
{
    filehandler.AllocatePage(pageHandler);
    char* pData;
    pageHandler.GetData(pData);
    pageHandler.GetPageNum(x->pageNumber);
    pData[0]=x->pageNumber;
    pData[0+sizeof(PageNum)]=x->numberOfKeys;
    for(int i =0;i<x->numberOfKeys;i++)
    {
        memcpy(&pData[sizeof(PageNum)+sizeof(int)+i*filehdr.attrLength], x->keys[i], filehdr.attrLength);
    }
    
    for(int i =0;i<=x->numberOfKeys;i++)
    {
        pData[sizeof(PageNum)+sizeof(int)+x->numberOfKeys*filehdr.attrLength+i*sizeof(PageNum)]= x->children[i];
    }
    x->leaf = (bool)pData[sizeof(PageNum)+sizeof(int)+x->numberOfKeys*filehdr.attrLength+(x->numberOfKeys+1)*sizeof(PageNum)];
    filehandler.MarkDirty(x->pageNumber);
    
}

void addToBucket(PageNum bucket, const RID &rid)
{
    PageNum ridPN;
    SlotNum ridSN;
    rid.GetPageNum(ridPN);
    rid.GetSlotNum(ridSN);
    
    if(bucket == -1 || filehandler.GetThisPage(bucket, pageHandler)!=0)
    {//bucket does not exist
        filehandler.AllocatePage(pageHandler);
        pageHandler.GetPageNum(bucket);
        char* data;
        pageHandler.GetData(data);
        data[0] = (int)(2*sizeof(int)+sizeof(PageNum)+sizeof(SlotNum));
        data[sizeof(int)]=(int)-1;//no next TODO: change?
        data[2*sizeof(int)]=ridPN;
        data[2*sizeof(int)+sizeof(PageNum)]=ridSN;
        filehandler.MarkDirty(bucket);
    }
    else //bucket exists
    {
        char* data;
        pageHandler.GetData(data);

        int size = (int)data[0];
        PageNum next = (PageNum)data[sizeof(int)];
        if(next!=-1)
        {//bucket full and already has next
            
            addToBucket(next,rid);
        }
        else if(size + sizeof(PageNum) + sizeof(SlotNum)>= PF_PAGE_SIZE)
        {
            //allocate new bucket
            addToBucket(next, rid);
            data[sizeof(int)]=next;

        }
        else
        {
            // goes into bucket
            data[size] = ridPN;
            data[size+sizeof(PageNum)]=ridSN;
            data[0]= size+sizeof(PageNum)+sizeof(SlotNum);
            filehandler.MarkDirty(bucket);
        }
    }
    
}
void splitChild(node * x, int i)
{
    //the new node to write
    node *z = new node();
    //read x's child from disk and fill out
    node *y = readNodeFromPageNum(x->children[i]);
    //
    z->leaf = y->leaf;
    z->numberOfKeys=t-1;
    for(int j = 1;j<=t-1;j++)
    {
        z->keys[j]=y->keys[j+1];
    }
    if(!y->leaf)
        for(int j =1;j<=t;j++)
            z->children[j]=y->children[j+1];
    y->numberOfKeys=t-1;
    for(int j = x->numberOfKeys+1;j>i+1;j--)
        x->children[j+1]=x->children[j];
    x->children[i+1]=z->pageNumber;
    for(int j = x->numberOfKeys;j>i;j--)
        x->keys[j+1]=x->keys[j];
    x->keys[i]=y->keys[t];
    x->numberOfKeys=x->numberOfKeys+1;
    //write y,z,x
    filehandler.MarkDirty(y->pageNumber);
    filehandler.MarkDirty(x->pageNumber);
    writeNodeOnNewPage(z);
    
}


//called recursively, if node x is a leaf, it adds the entry to the bucket
// if x is not a leaf, it goes to its children while splitting them if they have
// the necessary amount of keys.

void insertNonFull(node* x, void*  pData,const RID &rid)
{
    //if same key, add to bucket and increment number of rids.
    //if new key create bucket and add.
    //Leaves point only to 1 bucket.
    
    int i = x->numberOfKeys;
    int comp =0;
    if(x->leaf)
    {
        while(i>=1 && ((comp = compare(pData,x->keys[i]))==-1))
        {
                x->keys[i+1]=x->keys[i];
                i--;
        }
        if(comp == 1)//new key
        {
            x->keys[i+1]=(char*)pData;
            x->numberOfKeys++;
            PageNum next=-1;
            addToBucket(next, rid);
            x->children[i+1] = next;
            //new bucket
        }
        else
        {// existing bucket
            addToBucket(x->children[i], rid);
        }
        
        
        //x->children = new PageNum();
        filehandler.MarkDirty(x->pageNumber);
    }
    else
    {
        while(i>=1 && !compare(pData,x->keys[i]))
            i--;
        i++;
        //read x->children[i];
        node* childi = readNodeFromPageNum(x->children[i]);
        
        if(childi->numberOfKeys==2*t-1)
        {
            splitChild(x,i);
            if(compare(pData,x->keys[i]))
                i++;
        }
        insertNonFull(childi,pData,rid);
    }
    
    
}

//start of recursion for the insert, x should be the root node
void insert(node* x, void* pData, const RID &rid)
{
    //x is the root node
    if(x->numberOfKeys==2*t-1)
    {
        node* s = new node();//s becomes the root
        s->leaf = false;
        s->numberOfKeys=0;
        s->children[0]=x->pageNumber;
        splitChild(s,0);
        insertNonFull(s,pData,rid);
    }
    else
        insertNonFull(x,pData,rid);
}

IX_IndexHandle::IX_IndexHandle()
{
    // Initialize member variables
    bHdrChanged = FALSE;
    memset(&fileHdr, 0, sizeof(fileHdr));
    fileHdr.rootPageNum = IX_ROOT_NULL;
}

IX_IndexHandle::~IX_IndexHandle()
{
    
}

// Insert a new index entry
RC IX_IndexHandle::InsertEntry(void *pData, const RID &rid)
{
    
}

// Delete a new index entry
RC IX_IndexHandle::DeleteEntry(void *pData, const RID &rid)
{
    
}


// Force index files to disk
RC IX_IndexHandle::ForcePages()
{
    RC rc;

    // Write back the file header if any changes made to the header
    // while the file is open
    if (bHdrChanged) {
       PF_PageHandle pageHandle;
       char* pData;

       // Get the header page
       if (rc = pfFileHandle.GetFirstPage(pageHandle))
          // Test: unopened(closed) fileHandle, invalid file
          goto err_return;

       // Get a pointer where header information will be written
       if (rc = pageHandle.GetData(pData))
          // Should not happen
          goto err_unpin;

       // Write the file header (to the buffer pool)
       memcpy(pData, &fileHdr, sizeof(fileHdr));

       // Mark the header page as dirty
       if (rc = pfFileHandle.MarkDirty(IX_HEADER_PAGE_NUM))
          // Should not happen
          goto err_unpin;

       // Unpin the header page
       if (rc = pfFileHandle.UnpinPage(IX_HEADER_PAGE_NUM))
          // Should not happen
          goto err_return;

       if (rc = pfFileHandle.ForcePages(IX_HEADER_PAGE_NUM))
          // Should not happen
          goto err_return;

       // Set file header to be not changed
       bHdrChanged = FALSE;
    }

    // Call PF_FileHandle::ForcePages()
    if (rc = pfFileHandle.ForcePages(ALL_PAGES))
       goto err_return;

    // Return ok
    return (0);

    // Recover from inconsistent state due to unexpected error
 err_unpin:
    pfFileHandle.UnpinPage(IX_HEADER_PAGE_NUM);
 err_return:
    // Return error
    return (rc);
}
