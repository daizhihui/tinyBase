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


//TODO: make sure bucket logic is there.m

//indexNode =data + child page numbers
//fileHdr.orderOfTree is the minimum degree of the Tree: every indexNode other than the root must have fileHdr.orderOfTree-1 keys at least.




//compare
//returns true when k1 is greater than k2
// if k1 and k2 are strings, it will return the one with the biggest first character that does not match.
int IX_IndexHandle::compare(void* k1,void* k2)
{
    switch (fileHdr.attrType) {
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

//Will read and init a indexNode from this page on the disk

indexNode* IX_IndexHandle::readNodeFromPageNum(PageNum pn)
{
    printf("Reading node from page\n");
    PF_PageHandle pageHandler;
    int rc = pfFileHandle.GetThisPage(pn, pageHandler);
    char* pData;
    pageHandler.GetData(pData);
    indexNode* x = new indexNode();
    x->pageNumber=(int)pData[0];
    x->previous=(int)pData[sizeof(PageNum)];
    x->next =(int)pData[2*sizeof(PageNum)];
    x->leaf=(bool)pData[3*sizeof(PageNum)];
    x->isRoot=(bool)pData[3*sizeof(PageNum)+sizeof(bool)];
    x->numberOfKeys=(int)pData[3*sizeof(PageNum)+2*sizeof(bool)];

//    printf("Node structure: PN: %d\nPrev: %d\nNext: %d\nLeaf: %c\nRoot: %c\nNumberKeys: %d\n",(int)pData[0],(int)pData[sizeof(PageNum)],(int)pData[2*sizeof(PageNum)],(bool)pData[3*sizeof(PageNum)],(bool)pData[3*sizeof(PageNum)+sizeof(bool)],(int)pData[3*sizeof(PageNum)+2*sizeof(bool)]);
//    printf("PN : %d\n",x->previous);
    x->entries = new entry[x->numberOfKeys];
    for(int i =0;i<x->numberOfKeys;i++)
    {
        x->entries[i].child = pData[3*sizeof(PageNum)+2*sizeof(bool)+sizeof(int)+i*(fileHdr.attrLength+sizeof(PageNum))];
        x->entries[i].key = (char*)malloc(sizeof(fileHdr.attrLength));
        memcpy(x->entries[i].key,pData+4*sizeof(PageNum)+2*sizeof(bool)+sizeof(int)+i*(fileHdr.attrLength+sizeof(PageNum)),fileHdr.attrLength);

//        printf("Key %d: %d\n",i,*(int*)x->entries[i].key);
//        printf("PN %d: %d\n",i,(int)x->entries[i].child);
        
    }
    return  x;
}

//will write a indexNode on a new page
//must write part by part because of dynamic allocation on entries.
void IX_IndexHandle::writeNodeOnNewPage(indexNode* x)
{
    printf("Writing node on new page\n");
    PF_PageHandle pageHandler;
    pfFileHandle.AllocatePage(pageHandler);
    char* pData;
    pageHandler.GetData(pData);
    pageHandler.GetPageNum(x->pageNumber);
//    printf("Writing %d number of keys\n",x->numberOfKeys);
    pData[0] = x->pageNumber;
    pData[sizeof(PageNum)]=x->previous;
    pData[2*sizeof(PageNum)]=x->next;
    pData[3*sizeof(PageNum)]=x->leaf;
    pData[3*sizeof(PageNum)+sizeof(bool)]=x->isRoot;
    pData[3*sizeof(PageNum)+2*sizeof(bool)]=x->numberOfKeys;
//    printf("Wrote number of keys: %d\n",pData[3*sizeof(PageNum)+2*sizeof(bool)]);
    //copy entries
    for(int i =0;i<x->numberOfKeys;i++)
    {
        pData[3*sizeof(PageNum)+2*sizeof(bool)+sizeof(int)+i*(fileHdr.attrLength+sizeof(PageNum))]=x->entries[i].child;
        memcpy(pData+4*sizeof(PageNum)+2*sizeof(bool)+sizeof(int)+i*(fileHdr.attrLength+sizeof(PageNum)),x->entries[i].key,fileHdr.attrLength);
//        printf("Writing Key %d: %d\n",i,pData[4*sizeof(PageNum)+2*sizeof(bool)+sizeof(int)+i*(fileHdr.attrLength+sizeof(PageNum))]);
//        printf("Writing PN %d: %d\n",i,pData[3*sizeof(PageNum)+2*sizeof(bool)+sizeof(int)+i*(fileHdr.attrLength+sizeof(PageNum))]);
//        memcpy(&pData[3*sizeof(PageNum)+2*sizeof(bool)+sizeof(int)+i*(fileHdr.attrLength+sizeof(PageNum))], &(x->entries[i]), fileHdr.attrLength+sizeof(PageNum));
    }
    pfFileHandle.MarkDirty(x->pageNumber);
        printf("Done Writing node on new page\n");
}

void IX_IndexHandle::addToBucket(PageNum& bucket, const RID &rid, PageNum prev)
{
    printf("Adding to bucket\n");

    PageNum ridPN;
    SlotNum ridSN;
    rid.GetPageNum(ridPN);
    rid.GetSlotNum(ridSN);
    PF_PageHandle pageHandler;
    if(bucket == -1 || pfFileHandle.GetThisPage(bucket, pageHandler)!=0)
    {//bucket does not exist
        printf("New bucket\n");
        pfFileHandle.AllocatePage(pageHandler);
        pageHandler.GetPageNum(bucket);
        char* data;
        pageHandler.GetData(data);
//        int maxEntries = (PF_PAGE_SIZE-sizeof(IX_BucketHdr))/(sizeof(RID)+1);
        
        data[0] = prev;//prev
        data[sizeof(PageNum)] = -1;//next
        //create bitmap: set all to 0 -- if bitmap isn't a multiple of bytes, uncomment following
        int bytes =  fileHdr.bucketHeaderSize - sizeof(IX_BucketHdr);
        memset(data+sizeof(IX_BucketHdr), 0, bytes);//set to 0
        
//        int r = filehdr.numRidsPerBucket-bytes; //rest of bits
//        int ind  = 0;
//        while(r!=0)//set bit by bit
//        {
//            ClrBitmap(data+sizeof(IX_BucketHdr),filehdr.numRidsPerBucket-bytes*8+ind);
//            r--;
//            ind++;
//        }
//        //set first to 1;
        SetBitmap(data+sizeof(IX_BucketHdr),0);
        
        data[sizeof(IX_BucketHdr)+bytes]=ridPN;
        data[sizeof(IX_BucketHdr)+bytes+sizeof(PageNum)]=ridSN;

        pfFileHandle.MarkDirty(bucket);
        ForcePages();
        pfFileHandle.UnpinPage(bucket);
    }
    else //bucket exists
    {
        char* data;
        int rc = pageHandler.GetData(data);
        printf("rc: %d\n",rc);
        //find first empty rid num
        int ridnum;
        for(ridnum=0;ridnum<fileHdr.numRidsPerBucket;ridnum++)
            if(GetBitmap(data+sizeof(IX_BucketHdr),ridnum))
                break;
        if(ridnum!=fileHdr.numRidsPerBucket)
        {
            //found empty one.
            //insert
            int bytes =  fileHdr.bucketHeaderSize - sizeof(IX_BucketHdr);

            data[sizeof(IX_BucketHdr)+bytes + ridnum*(sizeof(PageNum)+sizeof(SlotNum))] = ridPN;
            data[sizeof(IX_BucketHdr)+bytes + ridnum*(sizeof(PageNum)+sizeof(SlotNum))+sizeof(PageNum)] = ridSN;
            SetBitmap(data+sizeof(IX_BucketHdr),ridnum);
            pfFileHandle.MarkDirty(bucket);
            ForcePages();
            pfFileHandle.UnpinPage(bucket);

            
        }
        else//bucket full
        {
            PageNum next = data[sizeof(PageNum)];
            if(next!=-1)
            {//bucket full and already has next
                addToBucket(next,rid,-1);
                pfFileHandle.UnpinPage(bucket);

            }
            else
            {//allocate new bucket
                addToBucket(next, rid,bucket);
                data[sizeof(PageNum)]=next;
                pfFileHandle.MarkDirty(bucket);
                ForcePages();
                pfFileHandle.UnpinPage(bucket);

            }
        }
    }
    printf("Done adding to bucket\n");

    
}

void IX_IndexHandle::reOrderDataInPage(indexNode* x)
{
    printf("Reordering data in page %d\n",x->pageNumber);
    PF_PageHandle pageHandler;
    pfFileHandle.GetThisPage(x->pageNumber, pageHandler);
    char* data;
    int rc=pageHandler.GetData(data);
    printf("Got data with rc: %d\n",rc);
    printf("Writing %d number of keys\n",x->numberOfKeys);
    data[0] = x->pageNumber;
    data[sizeof(PageNum)]=x->previous;
    data[2*sizeof(PageNum)]=x->next;
    data[3*sizeof(PageNum)]=x->leaf;
    data[3*sizeof(PageNum)+sizeof(bool)]=x->isRoot;
    data[3*sizeof(PageNum)+2*sizeof(bool)]=x->numberOfKeys;
    
    
//    memcpy(data,x,3*sizeof(PageNum)+2*sizeof(bool)+sizeof(int));
    //copy entries
//    printf("Wrote number of keys: %d\n",data[3*sizeof(PageNum)+2*sizeof(bool)]);

    for(int i =0;i<x->numberOfKeys;i++)
    {
//        printf("writing %d key+child of %d\n",i,x->numberOfKeys);
        data[3*sizeof(PageNum)+2*sizeof(bool)+sizeof(int)+i*(fileHdr.attrLength+sizeof(PageNum))]=x->entries[i].child;
        memcpy(data+4*sizeof(PageNum)+2*sizeof(bool)+sizeof(int)+i*(fileHdr.attrLength+sizeof(PageNum)),x->entries[i].key,fileHdr.attrLength);
//        printf("Writing Key %d: %d\n",i,data[4*sizeof(PageNum)+2*sizeof(bool)+sizeof(int)+i*(fileHdr.attrLength+sizeof(PageNum))]);
//        printf("Writing PN %d: %d\n",i,data[3*sizeof(PageNum)+2*sizeof(bool)+sizeof(int)+i*(fileHdr.attrLength+sizeof(PageNum))]);
        
    }
    pfFileHandle.MarkDirty(x->pageNumber);
    ForcePages();
    pfFileHandle.UnpinPage(x->pageNumber);
    
    

}

//x is father
//y is child
//i is index in x's entries, -1 if first
void IX_IndexHandle::splitChild(indexNode * x, int i,indexNode * y)
{
    printf("Splitting child\n");

    //the new indexNode to write
    indexNode *z = new indexNode();

    //
    z->leaf = y->leaf;
    z->numberOfKeys=fileHdr.orderOfTree-1;
    z->entries = new entry[fileHdr.orderOfTree-1];
    for(int j = 0;j<=fileHdr.orderOfTree-2;j++)//largest fileHdr.orderOfTree-1 keys of y
        z->entries[j].key = y->entries[j+fileHdr.orderOfTree].key;
    if(!y->leaf)
    {   z->previous=y->entries[fileHdr.orderOfTree-1].child;//1 child +
        for(int j =0;j<=fileHdr.orderOfTree-2;j++)//fileHdr.orderOfTree-1 children
            z->entries[j].child=y->entries[j+fileHdr.orderOfTree].child;
    }
    y->numberOfKeys=fileHdr.orderOfTree-1;
    if(i!=-1)
    {
        
        for(int j = x->numberOfKeys;j>i;j--)
        {
            x->entries[j].child=x->entries[j-1].child;
            x->entries[j].key=x->entries[j-1].key;
        }
        x->entries[i+1].child=z->pageNumber;
        x->entries[i].key=y->entries[fileHdr.orderOfTree-1].key;
    }
    x->numberOfKeys=x->numberOfKeys+1;
    
    if(y->leaf)//double linked list.
    {
        z->next=y->next;
        y->next=z->pageNumber;
        z->previous=y->pageNumber;
    }
    pfFileHandle.MarkDirty(y->pageNumber);
    pfFileHandle.MarkDirty(x->pageNumber);
    writeNodeOnNewPage(z);
    ForcePages();
    pfFileHandle.UnpinPage(z->pageNumber);
    for(int i =0;i<z->numberOfKeys;i++)
        free(z->entries[i].key);
    delete[] z->entries;
    delete z;
    printf("Done splitting child\n");

    
}


//called recursively, if indexNode x is a leaf, it adds the entry to the bucket
// if x is not a leaf, it goes to its children while splitting them if they have
// the necessary amount of keys.

void IX_IndexHandle::insertNonFull(indexNode* x, void*  pData,const RID &rid)
{
    printf("insert non full\n");

    //if same key, add to bucket and increment number of rids.
    //if new key create bucket and add.
    //Leaves point only to 1 bucket.
    
    int i = x->numberOfKeys;
    int comp =2;
    printf("%d keys\n",i);
    if(x->leaf)
    {
        if(i==0)
        {
            x->numberOfKeys++;
            x->entries = new entry();
            x->entries->key = (char*)malloc(fileHdr.attrLength);
            memcpy(x->entries->key,pData,fileHdr.attrLength);
            PageNum next=-1;
            addToBucket(next, rid,-1);
            x->entries[0].child = next;
            reOrderDataInPage(x);
            
//            pfFileHandle.MarkDirty(x->pageNumber);
            return;
            
        }
        i--;
        entry* tmp = x->entries;
        x->entries = new entry[x->numberOfKeys+1];
        memcpy(x->entries, tmp, x->numberOfKeys*sizeof(entry));
        delete[] tmp;
        while(i>=0 && ((comp = compare(pData,x->entries[i].key))==-1))
        {
            
                x->entries[i+1].key=x->entries[i].key;
                x->entries[i+1].child=x->entries[i].child;
            
                i--;
        }
        if(comp == 0)
        {
            printf("Adding to existing bucket\n");
            addToBucket(x->entries[i].child, rid,-1);
        }
        else
        {
            printf("Adding new key\n");
            x->entries[i+1].key=(char*)malloc(fileHdr.attrLength);
            memcpy(x->entries[i+1].key,pData,fileHdr.attrLength);
            //            x->entries[i+1].key=(char*)pData;
            x->numberOfKeys++;
            PageNum next=-1;
            addToBucket(next, rid,-1);
            x->entries[i+1].child = next;
            //new bucket
        }
        printf("$$COMP: %d\n",comp);
        //x->children = new PageNum();
        reOrderDataInPage(x);
//        pfFileHandle.MarkDirty(x->pageNumber);
    }
    else
    {
        i--;
        while(i>=0 && !compare(pData,x->entries[i].key))
        {
            i--;
        }
        i++;
        //read x->children[i];
        indexNode* childi = readNodeFromPageNum(x->entries[i].child);
        
        if(childi->numberOfKeys==2*fileHdr.orderOfTree-1)
        {
            splitChild(x,i,childi);
            if(compare(pData,x->entries[i].key))
                i++;
        }
        printf("Inserting in child \n");
        insertNonFull(childi,pData,rid);
        pfFileHandle.UnpinPage(childi->pageNumber);

        for(int i =0;i<childi->numberOfKeys;i++)
            free(childi->entries[i].key);
        delete[] childi->entries;
        delete childi;
    }
    
    printf("done insert non full\n");

}

//start of recursion for the insert, x should be the root indexNode
void IX_IndexHandle::insert(indexNode* x, void* pData, const RID &rid)
{
    printf("insert %d\n",*(int*)pData);

    //x is the root indexNode
    if(x->numberOfKeys==2*fileHdr.orderOfTree-1)
    {
        indexNode* s = new indexNode();//s becomes the root
        //allocate and write s?
        s->leaf = false;
        s->isRoot=true;
        s->numberOfKeys=0;
        s->previous = x->previous;
        x->leaf = true;
        x->isRoot=false;
        s->entries= new entry[fileHdr.orderOfTree-1];//allocate entries for new root.
        splitChild(s,-1,x);
        insertNonFull(s,pData,rid);
        fileHdr.treeLayerNums++;
        fileHdr.rootPageNum=s->pageNumber;
        bHdrChanged = 1;
        writeNodeOnNewPage(s);
        pfFileHandle.MarkDirty(x->pageNumber);
        //point T->root = s; if we put the root in memory
        ForcePages();
        pfFileHandle.UnpinPage(x->pageNumber);
        pfFileHandle.UnpinPage(s->pageNumber);

        for(int i =0;i<s->numberOfKeys;i++)
            free(s->entries[i].key);
        delete[] s->entries;
        delete s;
    
    }
    else
    {
        insertNonFull(x,pData,rid);
        pfFileHandle.UnpinPage(x->pageNumber);

    }
    printf("done insert \n");

    
}

IX_IndexHandle::IX_IndexHandle()
{
    // Initialize member variables
    bHdrChanged = FALSE;
    memset(&fileHdr, 0, sizeof(fileHdr));
    fileHdr.rootPageNum = IX_EMPTY_TREE;
  //  fileHdr.orderOfTree = fileHdr.orderOfTree;
    printf("constructor\n");
}

IX_IndexHandle::~IX_IndexHandle()
{
    
}

// Insert a new index entry
RC IX_IndexHandle::InsertEntry(void *pData, const RID &rid)
{
    
    printf("insertEntry %d\n",*(int*)pData);

    if(pData==NULL)
        return 5;
    if(fileHdr.rootPageNum==IX_EMPTY_TREE)
    {
        indexNode * root = new indexNode();
        root->leaf = true;
        root->isRoot=  true;
        fileHdr.treeLayerNums++;
        root->numberOfKeys=0;
        root->previous=-1;
        writeNodeOnNewPage(root);
        fileHdr.rootPageNum=root->pageNumber;
        bHdrChanged = 1;
        insertNonFull(root, pData, rid);
        
        pfFileHandle.MarkDirty(root->pageNumber);
        int rc =  ForcePages();
        pfFileHandle.UnpinPage(root->pageNumber);
        free(root->entries->key);
        delete root->entries;
        delete root;
        
    }
    else
    {
        indexNode* root = readNodeFromPageNum(fileHdr.rootPageNum);
        insert(root, pData, rid);
        for(int i =0;i<root->numberOfKeys;i++)
        {
            free(root->entries[i].key);
        }
        delete[] root->entries;
        delete root;
        
    }
    printf("done insert entry \n");
   
    return 0;
    
}




//desc : after delete the last entry in the root, this function collapseRoot change the root
//      if root is also the leaf, the tree becomes empty;
//      otherwise, the first child of oldRoot becomes the new root
//      treeLayerNums descrease by 1

//input : oldRoot - old root
void IX_IndexHandle::collapseRoot(indexNode * oldRoot){
    char* data;
    PF_PageHandle pageHandle;


    //tree becomes empty
    if(oldRoot->leaf) {
        fileHdr.rootPageNum = IX_EMPTY_TREE;
    }
    else{ //generate new root
        fileHdr.rootPageNum = oldRoot->previous;
    }


    //write back to buffer pool fileHeader
    pfFileHandle.GetFirstPage(pageHandle);
    pageHandle.GetData(data);
    PageNum fileHeaderPage;
    pageHandle.GetPageNum(fileHeaderPage);
    ((IX_FileHdr *)data)->rootPageNum = fileHdr.rootPageNum;
    //number of layers in the tree decrease by 1
    ((IX_FileHdr *)data)->treeLayerNums--;
    bHdrChanged = 1;
    pfFileHandle.MarkDirty(fileHeaderPage);

    //dispose of oldRoot page
    pfFileHandle.MarkDirty(oldRoot->pageNumber);
    pfFileHandle.UnpinPage(oldRoot->pageNumber);
    pfFileHandle.DisposePage(oldRoot->pageNumber);
}

//Desc: merge two nodes, copy all the entries in the right indexNode to the leftnode
//      delete the right indexNode, that means call recusively the function deleteEntryInNode(indexNode* x, int keyNum, nodeInfoInPath * path)
//      where indexNode x is replaced by the indexNode anchorNode
// 10/06/2014 deal with leaf linked list

//input : keyNum - is the position of key that separates this indexNode and neighorNode
//        depthInPath - the depth of indexNode thisNode in the path
//

void IX_IndexHandle::merge (indexNode * thisNode , indexNode *neighborNode, indexNode *anchorNode, nodeInfoInPath * path,int keyNum, int depthInPath){
    //to get right indexNode and left indexNode
    indexNode * leftN = NULL;
    indexNode * rightN = NULL;
    if(compare(thisNode->entries[0].key,neighborNode->entries[0].key) >=0) {
        leftN = neighborNode;
        rightN = thisNode;
    }
    else{
        leftN = thisNode;
        rightN = neighborNode;
    }

    //indexNode is not leaf
    if(!leftN->leaf){
        //copy keyNum key to leftN
        leftN->entries[leftN->numberOfKeys].key = anchorNode->entries[keyNum].key;
        //copy the first pointer in the right indexNode to the left indexNode
        leftN->entries[leftN->numberOfKeys].child = rightN->previous;
        leftN->numberOfKeys++;
    }

    //copy all the entries in the right indexNode to the leftnode
    for(int i=0; i<rightN->numberOfKeys; i++){
        leftN->entries[leftN->numberOfKeys] = rightN->entries[i];
        leftN->numberOfKeys++;

    }

    //deal with leaf linked list
    if(leftN->leaf){
        leftN->next = rightN->next;
        readNodeFromPageNum(rightN->next)->previous = leftN->pageNumber;
    }

    //dispose page in indexNode rightN
    pfFileHandle.UnpinPage(rightN->pageNumber);
    pfFileHandle.DisposePage(rightN->pageNumber);

    //write left indexNode to buffer pool
    writeNodeOnNewPage(leftN);

     //delete keyNum in anchorNode, recursively
    deleteEntryInNode(anchorNode,keyNum,path,depthInPath);
}


//Desc: shift over half of a neighbor’s plus keys to this indexNode
//      update anchor : the key value in the index entry pointing to the second indexNode must be changed to the the lowest search key in the second indexNode

//input : keyNum - is the position of key that separates this indexNode and neighorNode
//        isRight - true : neighbor is on the rightside; false : neighbor is on the leftside
//
void IX_IndexHandle::shift (indexNode * thisNode , indexNode *neighborNode, indexNode *anchorNode, int keyNum, bool isRight ){

    int numKeysShifted = (neighborNode->numberOfKeys - thisNode->numberOfKeys) / 2;
    // neighbor is on the rightside
    if(isRight){
        //indexNode is not leaf
        if(!thisNode->leaf){
            //copy key in position keyNum to thisNode
            thisNode->entries[fileHdr.orderOfTree-1].key = anchorNode->entries[keyNum].key;
            thisNode->entries[fileHdr.orderOfTree-1].child = neighborNode->previous;
            thisNode->numberOfKeys++;
            //shift numKeysShifted-1 (p,k) to thisNode from neighborNode
            for(int i=0; i<numKeysShifted-1;i++){
                thisNode->entries[thisNode->numberOfKeys] = neighborNode->entries[i];
                thisNode->numberOfKeys++;
            }

            //TODO to verify
            //shift neighborNode (k,v) to the left
            neighborNode->previous = neighborNode->entries[numKeysShifted-1].child;
            for(int i=0; i<neighborNode->numberOfKeys-numKeysShifted;i++){
                neighborNode->entries[i] = neighborNode->entries[i+numKeysShifted];
            }
            //copy separate value to anchorNode key
            anchorNode->entries[keyNum].key = neighborNode->entries[numKeysShifted-1].key;
        }

        //indexNode is leaf, shift numKeysShifted (k,p) to thisNode from neighborNode
        else{
            for(int i=0; i<numKeysShifted;i++){
                thisNode->entries[thisNode->numberOfKeys] = neighborNode->entries[i];
                thisNode->numberOfKeys++;
            }

            //shift neighborNode (k,v) to the left
            for(int i=0; i<neighborNode->numberOfKeys-numKeysShifted;i++){
                neighborNode->entries[i] = neighborNode->entries[i+numKeysShifted];
            }

            //copy the the lowest search key in the second indexNode to anchor's
             anchorNode->entries[keyNum].key = neighborNode->entries[numKeysShifted].key;
        }


        //update the number of keys in neighborNode
        neighborNode->numberOfKeys -= numKeysShifted;
    }
    //neighbor is on the leftside
    else {
        //shift all the entries in thisNode by numKeysShifted to the right
        for(int i=thisNode->numberOfKeys-1; i>=0;i--){
            thisNode->entries[i+numKeysShifted] = thisNode->entries[i];
        }


        //indexNode is not leaf
        if(!thisNode->leaf){
            //shift the first pointer of thisNode
            thisNode->entries[numKeysShifted-1].child = thisNode->previous;

            //copy key in position keyNum of anchorNode to thisNode
            thisNode->entries[numKeysShifted-1].key = anchorNode->entries[keyNum].key;
            thisNode->numberOfKeys++;


            //shift numKeysShifted-1 pairs of (pointer,key) to thisNode from the end of neighborNode
            for(int i=numKeysShifted-2; i>=0; i--){
                thisNode->entries[i] = neighborNode->entries[neighborNode->numberOfKeys-1];
                thisNode->numberOfKeys++;
                neighborNode->numberOfKeys--;
            }

            //copy the last pointer of neighborNode to the fisrt pointer of thisNode
            thisNode->previous = neighborNode->entries[neighborNode->numberOfKeys-1].child;


            //copy the last key value from neighborNode to the position keyNum of anchorNode
            anchorNode->entries[keyNum].key = neighborNode->entries[neighborNode->numberOfKeys-1].key;
        }

        //indexNode is leaf, shift numKeysShifted (k,p) to thisNode from neighborNode
        else{
            for(int i=numKeysShifted-1; i>=0; i--){
                thisNode->entries[i] = neighborNode->entries[neighborNode->numberOfKeys-1];
                thisNode->numberOfKeys++;
                neighborNode->numberOfKeys--;
            }

            //copy the the lowest search key in the second indexNode to anchor's
             anchorNode->entries[keyNum].key = thisNode->entries[0].key;
        }
    }

    //write these changes to buffer pool
    writeNodeOnNewPage(thisNode);
    writeNodeOnNewPage(neighborNode);
    writeNodeOnNewPage(anchorNode);
}

//desc : delete the entry in position keyNum in indexNode x
//input : depthInPath - the depth of indexNode x in the path
void IX_IndexHandle::deleteEntryInNode(indexNode* x, int keyNum, nodeInfoInPath * path, int depthInPath)
{
    //remove the entry and move its following entries forward
    // TODO make more efficient
    for(int i=keyNum; i < x->numberOfKeys ; ++i){
        x->entries[i] = x->entries[i+1];
    }
     --(x->numberOfKeys);

    //underflow
    if(x->numberOfKeys < fileHdr.orderOfTree){
        //this indexNode is root
        if(x->isRoot) {
            if(x->numberOfKeys==0) //root has no more keys after delete
                collapseRoot(x);
            else //root has still keys, do nothing
                return;
        }
        //check immediate neighbors
        nodeInfoInPath nodeInfo = path[depthInPath];
        PageNum neighborR = path[depthInPath].neighborR;
        PageNum neighborL = path[depthInPath].neighborL;

        //choose a neighor with more keys
        indexNode * nodeNeighbor = NULL;
        int entryNum = 0;
        bool isRight = false;
        if(neighborR!=-1 && neighborL!=-1){
            indexNode * nodeR = readNodeFromPageNum(neighborR);
            indexNode * nodeL = readNodeFromPageNum(neighborL);
            if(nodeR->numberOfKeys >= nodeL->numberOfKeys){
                nodeNeighbor = nodeR;
                entryNum = nodeInfo.entryNum;
                isRight = true;

            }
            else{
                nodeNeighbor = nodeL;
                entryNum = nodeInfo.entryNum - 1;
                isRight = false;
            }

        }
        else if(neighborR!= -1 && neighborL==-1) {
            nodeNeighbor = readNodeFromPageNum(neighborR);
            entryNum = nodeInfo.entryNum;
            isRight = true;
        }
        else if(neighborR == -1 && neighborL!=-1) {
            nodeNeighbor = readNodeFromPageNum(neighborL);
            entryNum = nodeInfo.entryNum -1;
            isRight = false;
        }
        else return; //TODO shouldn't happen


        indexNode *anchorNode = readNodeFromPageNum(nodeInfo.anchor);

        //both neighbors are minimum size
        if(nodeNeighbor->numberOfKeys==fileHdr.orderOfTree){
            merge(x,nodeNeighbor,anchorNode,path,entryNum, depthInPath-1);
        }
        else {
            shift(x,nodeNeighbor,anchorNode,entryNum,isRight);
        }
    }

}

// input :  bucket - the pageNum of bucket page
//          rid - the rid that need be deleted
//          path - an array of nodeNeighbor infos
//          pathDepth - the length of array path

RC IX_IndexHandle::deleteRID(PageNum &bucket, const RID &rid, nodeInfoInPath * path, int pathDepth){
    printf("begin deleteRID, entryNum in path %d\n", path[1].entryNum);

    char* data;
    PF_PageHandle pageHandle;
    pfFileHandle.GetThisPage(bucket, pageHandle);
    pageHandle.GetData(data);
    IX_BucketHdr *bucketHdr = (IX_BucketHdr*)data;

    PageNum before = bucketHdr->before;
    PageNum next = bucketHdr->next;
    printf("bucketHdr %d, next %d \n",(int)bucketHdr->before,(int)bucketHdr->next);


    int ridNum=0;
    RID *ridi;
        //iterate for all non-empty rids
    for(; ridNum< fileHdr.numRidsPerBucket && GetBitmap(data + sizeof(IX_BucketHdr), ridNum); ++ridNum){
        ridi = (RID*)(data+fileHdr.bucketHeaderSize+ridNum*sizeof(RID));
        PageNum ridPN;
        SlotNum ridSN;
        ridi->GetPageNum(ridPN);
        ridi->GetSlotNum(ridSN);
        printf(" rid: pN %d, SN %d\n", ridPN,ridSN);

        //found in this page
        if((*ridi)==rid) {
            printf("found in this bucket\n");
            // Clear bit
            ClrBitmap(data + sizeof(IX_BucketHdr), ridNum);
            // Find a non-free rid
            for ( ridNum = 0; ridNum < fileHdr.numRidsPerBucket; ridNum++)
               if (GetBitmap(data + sizeof(IX_BucketHdr), ridNum))
                  break;

            // Dispose the bucket if empty (the deleted rid was the last one)
            // This will help the total number of occupied pages to be remained
            // as small as possible
            if (ridNum == fileHdr.numRidsPerBucket) {
                printf("after deletion, the bucket is empty\n");
               //TODO change insert (rid space be free, can be reused, add freeSlot in bucket page header)
                //dispose of page bucket
                pfFileHandle.MarkDirty(bucket);
                pfFileHandle.UnpinPage(bucket);
                pfFileHandle.DisposePage(bucket);

               //the first bucket to be removed
                if(before == -1) {
                    printf("the first bucket to be removed\n");
                    PageNum leafPage = path[pathDepth].anchor;
                    int entryNum = path[pathDepth].entryNum;
                    indexNode * leaf = readNodeFromPageNum(leafPage);
                    if(bucketHdr->next == -1) {
                         printf("no nextbucket,delete the entry in leaf\n");
                        //delete the entry in leaf
                        deleteEntryInNode(leaf,entryNum,path,pathDepth-1);

                    }
                    else{
                        printf("has nextbucket,replace pageNum in leaf\n");
                        //replace pageNum in leaf, change IX_BucketHdr.before in next bucket
                        pfFileHandle.GetThisPage(bucketHdr->next,pageHandle);
                        pageHandle.GetData(data);
                        ((IX_BucketHdr *) data)->before = -1; //this bucket becomes the first bucket in the chain list
                        leaf->entries[entryNum].child = bucketHdr->next;
                        pfFileHandle.MarkDirty(leafPage);
                        pfFileHandle.MarkDirty(bucketHdr->next);

                    }

                }
                //rid not in the first bucket
                else{
                    printf("the non-first bucket to be removed\n");
                    //modify IX_BucketHdr.after in last bucket
                    pfFileHandle.GetThisPage(bucketHdr->before,pageHandle);
                    pageHandle.GetData(data);
                    ((IX_BucketHdr *) data)->next = bucketHdr->next; //
                    pfFileHandle.MarkDirty(bucketHdr->before);
                    if(bucketHdr->next!=-1){
                        //modify IX_BucketHdr.before in next bucket
                        pfFileHandle.GetThisPage(bucketHdr->next,pageHandle);
                        pageHandle.GetData(data);
                        ((IX_BucketHdr *) data)->before = bucketHdr->before; //
                        pfFileHandle.MarkDirty(bucketHdr->next);
                    }
                }

            }
            return 0;
        }
    }

    //not found in this page, find the next
    if(bucketHdr->next == -1) return IX_INDEX_NOTFOUND;
    printf("search rid in the next bucket\n");
    return deleteRID(next,rid,path,pathDepth);
}



//desc : Delete a index entry
//input : pData - the key value of index
//          rid - the rid to be deleted
RC IX_IndexHandle::DeleteEntry(void *pData, const RID &rid){
    PageNum rootPage = fileHdr.rootPageNum;
    indexNode *root = readNodeFromPageNum(rootPage);
    //an array of nodeInfo to save infos about neighors and anchors
    nodeInfoInPath path[fileHdr.treeLayerNums];
    //for debugging
    printf("treeLayer%d,order%d\n",fileHdr.treeLayerNums,fileHdr.orderOfTree);
    //for root
    path[0].nodeSelf = rootPage;
    path[0].anchor=-1; //implicit root
    path[0].neighborL=-1;
    path[0].neighborR=-1;
    //path[0].key = (void*)-1;
    path[0].entryNum = -1;

    int pathDepth = 0;
    //parcourir l'arbre et obtenir les donnees enregistrees dans path
    printf("before traversal\n");
    traversalTree(root,pData,path,pathDepth);
    printf("after traversal\n");
    //remove rid in bucket
    //get the first bucket page
    printf("pathDepth%d\n",pathDepth);
    PageNum bucket = path[pathDepth].nodeSelf;
    //delete rid in the buckets
    return deleteRID(bucket,rid,path,pathDepth);
}


//desc : recurse to a leaf indexNode from root to find deletable entry:
//       for nodes in the search path, calculate immediate neighbors and their anchors
//input : pData - the key value of index
//          x - the indexNode to
//return : path - a pointer that points to a list of nodeInfoInPath
//         pathDepth - the length of this list of nodeInfoInPath
RC IX_IndexHandle::traversalTree(indexNode *x, void *pData, nodeInfoInPath *path,int &pathDepth){
    pathDepth++;
    printf("pdata%d\nnumberofKeys%d\n",*(int*)pData,x->numberOfKeys);
    if(x->leaf){
        printf("traversal leaf\n");
        int i = 0;
        while(i<x->numberOfKeys && compare(pData,x->entries[i].key)!=0)
            i++;
        //for debug
        printf("print all the elements key in leaf\n");
        int j=0;
        while(j<x->numberOfKeys){
            printf("%d\n",*(int*)x->entries[j].key);
            j++;
        }
        // if not found in leaf
        if(compare(pData,x->entries[i].key)!=0) return IX_INDEX_NOTFOUND; //TODO define ERROR

        //found in leaf, save neighbor infos for bucket page
        path[pathDepth].nodeSelf = x->entries[i].child;
        path[pathDepth].anchor = x->pageNumber;
        path[pathDepth].neighborL = -1; //for bucket, no need to know its neighbors
        path[pathDepth].neighborR = -1;
        //path[pathDepth].key = x->entries[i].key;
        path[pathDepth].entryNum = i;
        return 0;
    }

    printf("traversal non-leaf\n");
    int i = 0;
    while(i<x->numberOfKeys && compare(pData,x->entries[i].key)!=-1)
        i++;
    indexNode* childi = NULL;
    //pData is less than the first key, so childi is pointed by the first pointer in x
    if(i==0){
        childi = readNodeFromPageNum(x->previous);
        path[pathDepth].nodeSelf = x->previous;
        //if pointer to this page is the first pointer in its parent
        path[pathDepth].neighborL = -1; //doesn't have left neighbor
        path[pathDepth].neighborR = x->entries[0].child;

    }
    else{
        childi = readNodeFromPageNum(x->entries[i-1].child);
        path[pathDepth].nodeSelf = x->entries[i-1].child;
    }
    //save immdediate neighors for childi
    path[pathDepth].anchor = x->pageNumber; //childi's anchor
    //path[pathDepth].key = x->entries[i].key;
    path[pathDepth].entryNum = i;


    if(0<i<x->numberOfKeys) {
        if(i==1)    path[pathDepth].neighborL = x->previous;
        else    path[pathDepth].neighborL = x->entries[i-2].child;
        path[pathDepth].neighborR = x->entries[i].child;
    }

    //if pointer to this page is the last pointer in its parent
    else if(i==x->numberOfKeys){
        path[pathDepth].neighborL = x->entries[x->numberOfKeys-2].child;
        path[pathDepth].neighborR = -1;
    }
    //this shouldn't happen
    else{
        path[pathDepth].neighborL = -1;
        path[pathDepth].neighborR = -1;
    }
    return traversalTree(childi,pData,path,pathDepth);
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
       if ((rc = pfFileHandle.GetFirstPage(pageHandle)))
          // Test: unopened(closed) fileHandle, invalid file
          goto err_return;

       // Get a pointer where header information will be written
       if ((rc = pageHandle.GetData(pData)))
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


//
// GetBitmap
//
// Desc: Return a bit corresponding to the given index
// In:   map - address of bitmap
//       idx - bit index
// Ret:  TRUE or FALSE
//
int IX_IndexHandle::GetBitmap(char *map, int idx) const
{
   return (map[idx / 8] & (1 << (idx % 8))) != 0;
}

//
// SetBitmap
//
// Desc: Set a bit corresponding to the given index
// In:   map - address of bitmap
//       idx - bit index
//
void IX_IndexHandle::SetBitmap(char *map, int idx) const
{
   map[idx / 8] |= (1 << (idx % 8));
}

//
// ClrBitmap
//
// Desc: Clear a bit corresponding to the given index
// In:   map - address of bitmap
//       idx - bit index
//
void IX_IndexHandle::ClrBitmap(char *map, int idx) const
{
   map[idx / 8] &= ~(1 << (idx % 8));
}

