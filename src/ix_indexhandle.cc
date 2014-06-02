//
//  ix_handler.cpp
//  TinyBaseP
//
//  Created by Cyril on 5/31/14.
//  Copyright (c) 2014 Cyril. All rights reserved.
//

#include "ix.h"
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
bool compare(void* k1,void* k2)
{
    switch (filehdr.attrType) {
        case FLOAT:
            return (*(float*)k1)>(*(float*)k2);
            break;
        case INT:
            return (*(int*)k1)>(*(int*)k2);
            break;
        case STRING:
            return strcmp((char*)k1,(char*)k2);
            break;
        default:
            break;
    }
    
}

node* readNodeFromPageNum(PageNum pn)
{
    filehandler.GetThisPage(pn, pageHandler);
    char* pData;
    pageHandler.GetData(pData);
    node* x = new node();
    x->pageNumber = pData[0];
    x->numberOfKeys = pData[sizeof(PageNum)];
    
    x->keys = new char*[x->numberOfKeys];
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

void insertNonFull(node* x, void*  pData,const RID &rid)
{
    //if same key, add to bucket and increment number of rids.
    //if new key create bucket and add.
    //Leaves point only to 1 bucket.
    
    int i = x->numberOfKeys;
    if(x->leaf)
    {
        while(i>=1 && !compare(pData,x->keys[i]))
        {
            x->keys[i+1]=x->keys[i];
            i--;
        }
        x->keys[i+1]=(char*)pData;
        x->numberOfKeys++;
        //write x;
        
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
    
}