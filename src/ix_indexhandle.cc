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

    //add by dzh
    bool isRoot;
    int layerNum; //layer number in the tree
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



struct nodeInfoInPath{
    PageNum self;
    PageNum neighborL;
    PageNum neighborR;
    PageNum anchor;
    AttrType key; //key value
    int entryNum; //position of this entry
};


void collapseRoot(node * oldRoot){
    char* data;
    PF_PageHandle pageHandle;

    //tree becomes empty
    if(oldRoot->leaf) {
        filehdr.rootPageNum = IX_EMPTY_TREE;
    }
    else{ //generate new root
        filehdr.rootPageNum = oldRoot->children[0];
    }
    //write back to buffer pool fileHeader
    filehandler.GetFirstPage(pageHandle);
    pageHandle.GetData(data);
    PageNum fileHeaderPage;
    pageHandle.GetPageNum(fileHeaderPage);
    ((IX_FileHdr *)data)->rootPageNum = filehdr.rootPageNum;
    filehandler.MarkDirty(fileHeaderPage); //

    //dispose of oldRoot page
    filehandler.MarkDirty(oldRoot->pageNumber);
    filehandler.UnpinPage(oldRoot->pageNumber);
    filehandler.DisposePage(oldRoot->pageNumber);
}

//Desc: merge two nodes, copy all the entries in the right node to the leftnode
//      delete the right node, that means call recusively the function deleteEntryInNode(node* x, int keyNum, nodeInfoInPath * path)
//      where node x is replaced by the node anchorNode

//input : keyNum - is the position of key that separates this node and neighorNode
//
//

void merge (node * thisNode , node *neighborNode, node *anchorNode, int keyNum){
    //to get right node and left node
    node * leftN = NULL;
    node * rightN = NULL;
    if(compare(thisNode->keys[0],neighborNode->keys[0]) >=0) {
        leftN = neighborNode;
        rightN = thisNode;
    }
    else{
        leftN = thisNode;
        rightN = neighborNode;
    }

    //node is not leaf
    if(!leftN->leaf){
        //copy keyNum key to leftN
        leftN->keys[leftN->numberOfKeys] = anchorNode->keys[keyNum];
        leftN->numberOfKeys++;
    }

    //copy all the entries in the right node to the leftnode
    for(int i=0; i<rightN->numberOfKeys; i++){
        leftN->children[leftN->numberOfKeys] = rightN->children[i];
        leftN->keys[leftN->numberOfKeys] = rightN->keys[i];
        leftN->numberOfKeys++;

    }

    //node is not leaf, copy the last child of rightN to leftN
    if(!leftN->leaf){
        leftN->children[leftN->numberOfKeys] = rightN->children[rightN->numberOfKeys];
    }

    //dispose page in node rightN
    filehandler.UnpinPage(rightN->pageNumber);
    filehandler.DisposePage(rightN->pageNumber);

    //write left node to buffer pool
    writeNodeOnNewPage(leftN);

     //delete keyNum in anchorNode, recursively
    deleteEntryInNode(anchorNode,keyNum,path);
}


//Desc: shift over half of a neighbor’s plus keys to this node
//      update anchor : the key value in the index entry pointing to the second node must be changed to the the lowest search key in the second node

//input : keyNum - is the position of key that separates this node and neighorNode
//        isRight - true : neighbor is on the rightside; false : neighbor is on the leftside
//
void shift (node * thisNode , node *neighborNode, node *anchorNode, int keyNum, bool isRight ){

    int numKeysShifted = (neighborNode->numberOfKeys - thisNode->numberOfKeys) / 2;
    // neighbor is on the rightside
    if(isRight){
        //node is not leaf
        if(!thisNode->leaf){
            //copy key in position keyNum to thisNode
            thisNode->keys[t-1] = anchorNode->keys[keyNum];
            thisNode->numberOfKeys++;
            //shift numKeysShifted-1 (p,k) to thisNode from neighborNode
            for(int i=0; i<numKeysShifted-1;i++){
                thisNode->children[thisNode->numberOfKeys] = neighborNode->children[i];
                thisNode->keys[thisNode->numberOfKeys] = neighborNode->keys[i];
                thisNode->numberOfKeys++;
            }
            thisNode->children[thisNode->numberOfKeys] = neighborNode->children[numKeysShifted-1];
            //copy separate value to anchorNode key
            anchorNode->keys[keyNum] = neighborNode->keys[numKeysShifted-1];
        }

        //node is leaf, shift numKeysShifted (k,p) to thisNode from neighborNode
        else{
            for(int i=0; i<numKeysShifted;i++){
                thisNode->children[thisNode->numberOfKeys] = neighborNode->children[i];
                thisNode->keys[thisNode->numberOfKeys] = neighborNode->keys[i];
                thisNode->numberOfKeys++;
            }
            //copy the the lowest search key in the second node to anchor's
             anchorNode->keys[keyNum] = neighborNode->keys[numKeysShifted];
        }

        //shift neighborNode (k,v) to the left
        for(int i=0; i<neighborNode->numberOfKeys-numKeysShifted;i++){
            neighborNode->keys[i] = neighborNode->keys[i+numKeysShifted];
            neighborNode->children[i] = neighborNode->children[i+numKeysShifted];
        }
        //update the number of keys in neighborNode
        neighborNode->numberOfKeys -= numKeysShifted;
    }
    //neighbor is on the leftside
    else {
        //shift all the entries in thisNode by numKeysShifted to the right
        for(int i=thisNode->numberOfKeys-1; i>=0;i--){
            thisNode->children[i+numKeysShifted] = thisNode->children[i];
            thisNode->keys[i+numKeysShifted] = thisNode->keys[i];
        }

        //node is not leaf
        if(!thisNode->leaf){
            //copy the last pointer of neighborNode to the numKeysShifted-1 of thisNode
            thisNode->children[numKeysShifted-1] = neighborNode->children[neighborNode->numberOfKeys];

            //shift numKeysShifted-1 pairs of (pointer,key) to thisNode from the end of neighborNode
            for(int i=numKeysShifted-2; i>=0; i--){
                thisNode->keys[i] = neighborNode->keys[neighborNode->numberOfKeys-1];
                thisNode->children[i] = neighborNode->children[neighborNode->numberOfKeys-1];
                thisNode->numberOfKeys++;
                neighborNode->numberOfKeys--;
            }

            //copy key in position keyNum of anchorNode to thisNode
            thisNode->keys[numKeysShifted-1] = anchorNode->keys[keyNum];
            thisNode->numberOfKeys++;

            //copy the last key value from neighborNode to the position keyNum of anchorNode
            anchorNode->keys[keyNum] = neighborNode->keys[neighborNode->numberOfKeys-1];
        }

        //node is leaf, shift numKeysShifted (k,p) to thisNode from neighborNode
        else{
            for(int i=numKeysShifted-1; i>=0; i--){
                thisNode->keys[i] = neighborNode->keys[neighborNode->numberOfKeys-1];
                thisNode->children[i] = neighborNode->children[neighborNode->numberOfKeys-1];
                thisNode->numberOfKeys++;
                neighborNode->numberOfKeys--;
            }

            //copy the the largest search key in the second node to anchor's
             anchorNode->keys[keyNum] = neighborNode->keys[neighborNode->numberOfKeys-1];
        }
    }

    //write these changes to buffer pool
    writeNodeOnNewPage(thisNode);
    writeNodeOnNewPage(neighborNode);
    writeNodeOnNewPage(anchorNode);
}

//delete the entry in position keyNum in node x
void deleteEntryInNode(node* x, int keyNum, nodeInfoInPath * path)
{
    //remove the entry and move its following entries forward
    // TODO make more efficient
    if(x->leaf){
        for(int i=keyNum; i < x->numberOfKeys ; ++i){
            x->keys[i] = x->keys[i+1];
            x->children[i] = x->children[i+1];
        }
    }
    else{
        for(int i=keyNum; i < x->numberOfKeys ; ++i){
            x->keys[i] = x->keys[i+1];
            x->children[i+1] = x->children[i+2];
        }
    }
    --(x->numberOfKeys);

    //underflow
    if(x->numberOfKeys < t){
        //this node is root
        if(x->isRoot) {
            collapseRoot(x);
            return;
        }

        //check immediate neighbors
        nodeInfoInPath = path[x->layerNum];
        PageNum neighborR = path[x->layerNum].neighborR;
        PageNum neighborL = path[x->layerNum].neighborL;

        //choose a neighor with more keys
        node * nodeNeighbor = NULL;
        int entryNum = 0;
        bool isRight = false;
        if(neighborR!=-1 && neighborL!=-1){
            node * nodeR = readNodeFromPageNum(neighborR);
            node * nodeL = readNodeFromPageNum(neighborL);
            if(nodeR->numberOfKeys >= nodeL->numberOfKeys){
                nodeNeighbor = nodeR;
                entryNum = nodeInfoInPath.entryNum;
                isRight = true;

            }
            else{
                nodeNeighbor = nodeL;
                entryNum = nodeInfoInPath.entryNum - 1;
                isRight = false;
            }

        }
        else if(neighborR!= -1 && neighborL==-1) {
            nodeNeighbor = readNodeFromPageNum(neighborR);
            entryNum = nodeInfoInPath.entryNum;
            isRight = true;
        }
        else if(neighborR == -1 && neighborL!=-1) {
            nodeNeighbor = readNodeFromPageNum(neighborL);
            entryNum = nodeInfoInPath.entryNum -1;
            isRight = false;
        }
        else return; //TODO shouldn't happen


        node *anchorNode = readNodeFromPageNum(nodeInfoInPath.anchor);

        //both neighbors are minimum size
        if(nodeNeighbor->numberOfKeys==t){
            merge(x,nodeNeighbor,anchorNode,entryNum);
        }
        else {
            shift(x,nodeNeighbor,anchorNode,entryNum,isRight);
        }
    }

}


RC IX_IndexHandle::deleteRID(PageNum &bucket, const RID &rid, nodeInfoInPath * path){
    char* data;
    PF_PageHandle pageHandle;
    filehandler.GetThisPage(bucket, pageHandle);
    pageHandle.GetData(data);
    IX_BucketHdr bucketHdr = (IX_BucketHdr)data[sizeof(IX_BucketHdr)];

    PageNum before = bucketHdr.before;
    PageNum next = bucketHdr.next;

    int ridNum=0;
    RID ridi;
    //iterate for all non-empty rids
    for(; ridNum< size && GetBitmap(pData + sizeof(IX_BucketHdr), ridNum); ++ridNum){
        ridi = data[filehdr.bucketHeaderSize+ridNum*sizeof(RID)];
        //found in this page
        if(ridi==rid) {
            // Clear bit
            ClrBitmap(pData + sizeof(IX_BucketHdr), ridNum);
            // Find a non-free rid
            for ( ridNum = 0; ridNum < filehdr.numRidsPerBucket; ridNum++)
               if (GetBitmap(pData + sizeof(IX_BucketHdr), ridNum))
                  break;

            // Dispose the bucket if empty (the deleted rid was the last one)
            // This will help the total number of occupied pages to be remained
            // as small as possible

            if (ridNum == filehdr.numRidsPerBucket) {
               //TODO change insert (rid space be free, can be reused, add freeSlot in bucket page header)
                //dispose of page bucket
                pfFileHandle.MarkDirty(bucket);
                pfFileHandle.UnpinPage(bucket);
                pfFileHandle.DisposePage(bucket);

               //the first bucket to be removed
                if(before == -1) {
                    PageNum leafPage = path[IX_NUM_Layers].anchor;
                    int entryNum = path[IX_NUM_Layers].entryNum;
                    node * leaf = readNodeFromPageNum(leafPage);
                    if(bucketHdr.next == -1) {
                        //delete the entry in leaf
                        deleteEntryInNode(leaf,entryNum,path);

                    }
                    else{
                        //replace pageNum in leaf, change IX_BucketHdr.before in next bucket
                        filehandler.GetThisPage(bucketHdr.next,pageHandle);
                        pageHandle.GetData(data);
                        ((IX_BucketHdr *) data)->before = -1; //this bucket becomes the first bucket in the chain list
                        leaf->children[entryNum] = bucketHdr.next;
                        pfFileHandle.MarkDirty(leafPage);
                        pfFileHandle.MarkDirty(bucketHdr.next);

                    }

                }
                //rid not in the first bucket
                else{

                    //modify IX_BucketHdr.after in last bucket
                    filehandler.GetThisPage(bucketHdr.before,pageHandle);
                    pageHandle.GetData(data);
                    ((IX_BucketHdr *) data)->next = bucketHdr.next; //
                    pfFileHandle.MarkDirty(bucketHdr.before);
                    if(bucketHdr.next!=-1){
                        //modify IX_BucketHdr.before in next bucket
                        filehandler.GetThisPage(bucketHdr.next,pageHandle);
                        pageHandle.GetData(data);
                        ((IX_BucketHdr *) data)->before = bucketHdr.before; //
                        pfFileHandle.MarkDirty(bucketHdr.next);
                    }
                }

            }
            return 0;
        }
    }

    //not found in this page, find the next
    if(bucketHdr.next == -1) return IX_INDEX_NOTFOUND;
    return deleteRID(next,rid);
}



// Delete a index entry
RC IX_IndexHandle::DeleteEntry(void *pData, const RID &rid){
    //rechercher la feuille L contenant le point d’accès.
//    node* x = new node();
//    //TODO

    PageNum rootPage = filehdr.rootPageNum;
    node *root = readNodeFromPageNum(rootPage);
    nodeInfoInPath path[IX_NUM_Layers]; //TODO define IX_NUM_Layers(not including buckets)
    //for root
    path[0].self = rootPage;
    path[0].anchor=-1; //implicit root
    path[0].neighborL=-1;
    path[0].neighborR=-1;
    path[0].key = -1;
    path[0].entryNum = -1;
    //parcourir l'arbre et obtenir les donnees enregistrees dans path
    traversalTree(root,pData,path,0);
    //remove rid in bucket
    //get the first bucket page
    PageNum bucket = path[IX_NUM_Layers].self;
    filehandler.GetThisPage(bucket, pageHandler);

    //delete rid in the buckets
    RC rc = deleteRID(bucket,rid,path);
}


//recurse to a leaf node from root to find deletable entry:
//for nodes in the search path, calculate immediate neighbors and their anchors
RC traversalTree(node *x, void *pData, nodeInfoInPath *path,int pathLayer){
    if(x->leaf){
        int i = 0;
        while(i<=x->numberOfKeys && compare(pData,x->keys[i])!=0)
            i++;
        // if not found in leaf
        if(compare(pData,x->keys[i])!=0) return IX_INDEX_NOTFOUND; //TODO define ERROR

        //found in leaf
        path[pathLayer+1].self = x->children[i]; //TODO verify
        path[pathLayer+1].anchor = x->pageNumber;
        path[pathLayer+1].neighborL = -1; //for bucket, no need to know its neighbors
        path[pathLayer+1].neighborR = -1;
        path[pathLayer+1].key = x->keys[i];
        path[pathLayer+1].entryNum = i;
        return 0;
    }

    int i = 0;
    while(i<=x->numberOfKeys && compare(pData,x->keys[i])==1)
        i++;
    node* childi = readNodeFromPageNum(x->children[i]);
    //save immdediate neighor
    path[pathLayer+1].anchor = x->pageNumber; //childi's anchor
    path[pathLayer+1].self = x->children[i];
    path[pathLayer+1].key = x->keys[i];
    path[pathLayer+1].entryNum = i;


    if(0<i<=x->numberOfKeys) {
        path[pathLayer+1].neighborL = x->children[i-1];
        path[pathLayer+1].neighborR = x->children[i+1];
    }
    else if(i==0){
        path[pathLayer+1].neighborR = x->children[1];
    }
    else{
        path[pathLayer+1].neighborR = x->children[x->numberOfKeys];
    }
    return traversalTree(childi,pData,path,pathLayer+1);
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

