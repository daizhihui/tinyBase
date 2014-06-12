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



//compare
//returns true when k1 is greater than k2
// if k1 and k2 are strings, it will return the one with the biggest first character that does not match.
int IX_IndexHandle::compare(void* k1,void* k2)
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

node* IX_IndexHandle::readNodeFromPageNum(PageNum pn)
{
    filehandler.GetThisPage(pn, pageHandler);
    char* pData;
    pageHandler.GetData(pData);
    node* x = (node*)pData;
//    x->pageNumber = pData[0];
//    x->numberOfKeys = pData[sizeof(PageNum)];
//    
//    x->keys = new char*[x->numberOfKeys];
//    x->children = new PageNum[x->numberOfKeys+1];
//    for(int i =0;i<x->numberOfKeys;i++)
//        memcpy(x->keys[i], &pData[sizeof(PageNum)+sizeof(int)+i*filehdr.attrLength], filehdr.attrLength);
    
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
    
//    for(int i =0;i<=x->numberOfKeys;i++)
//    {
//        x->children[i] = (PageNum)pData[sizeof(PageNum)+sizeof(int)+x->numberOfKeys*filehdr.attrLength+i*sizeof(PageNum)];
//    }
//    x->leaf = (bool)pData[sizeof(PageNum)+sizeof(int)+x->numberOfKeys*filehdr.attrLength+(x->numberOfKeys+1)*sizeof(PageNum)];
    return  x;
}

//will write a node on a new page
//must write part by part because of dynamic allocation on entries.
void IX_IndexHandle::writeNodeOnNewPage(node* x)
{
    filehandler.AllocatePage(pageHandler);
    char* pData;
    pageHandler.GetData(pData);
    pageHandler.GetPageNum(x->pageNumber);
    
    memcpy(pData,x,3*sizeof(PageNum)+2*sizeof(bool)+2*sizeof(int));
    
    //copy entries
    for(int i =0;i<x->numberOfKeys;i++)
    {
        memcpy(&pData[3*sizeof(PageNum)+2*sizeof(bool)+2*sizeof(int)+i*(filehdr.attrLength+sizeof(PageNum))], &(x->entries[i]), filehdr.attrLength+sizeof(PageNum));
    }
    filehandler.MarkDirty(x->pageNumber);
}

void IX_IndexHandle::addToBucket(PageNum& bucket, const RID &rid, PageNum prev)
{
    PageNum ridPN;
    SlotNum ridSN;
    rid.GetPageNum(ridPN);
    rid.GetSlotNum(ridSN);
    //use IX_BUCKETHEADER
    
    if(bucket == -1 || filehandler.GetThisPage(bucket, pageHandler)!=0)
    {//bucket does not exist
        filehandler.AllocatePage(pageHandler);
        pageHandler.GetPageNum(bucket);
        char* data;
        pageHandler.GetData(data);
//        int maxEntries = (PF_PAGE_SIZE-sizeof(IX_BucketHdr))/(sizeof(RID)+1);
        
        data[0] = prev;//prev
        data[sizeof(PageNum)] = -1;//next
        //create bitmap: set all to 0 -- if bitmap isn't a multiple of bytes, uncomment following
        int bytes =  filehdr.bucketHeaderSize - sizeof(IX_BucketHdr);
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

        filehandler.MarkDirty(bucket);
    }
    else //bucket exists
    {
        char* data;
        pageHandler.GetData(data);
        //find first empty rid num
        int ridnum;
        for(ridnum=0;ridnum<filehdr.numRidsPerBucket;ridnum++)
            if(GetBitmap(data+sizeof(IX_BucketHdr),ridnum))
                break;
        if(ridnum!=filehdr.numRidsPerBucket)
        {
            //found empty one.
            //insert
            int bytes =  filehdr.bucketHeaderSize - sizeof(IX_BucketHdr);

            data[sizeof(IX_BucketHdr)+bytes + ridnum*(sizeof(PageNum)+sizeof(SlotNum))] = ridPN;
            data[sizeof(IX_BucketHdr)+bytes + ridnum*(sizeof(PageNum)+sizeof(SlotNum))+sizeof(PageNum)] = ridSN;
            SetBitmap(data+sizeof(IX_BucketHdr),ridnum);
            
        }
        else//bucket full
        {
            PageNum next = data[sizeof(PageNum)];
            if(next!=-1)
            {//bucket full and already has next
                addToBucket(next,rid,-1);
            }
            else
            {//allocate new bucket
                addToBucket(next, rid,bucket);
                data[sizeof(PageNum)]=next;
            }
        }
    }
    
}
//x is father
//y is child
//i is index in x's entries, -1 if first
void IX_IndexHandle::splitChild(node * x, int i,node * y)
{
    
    //the new node to write
    node *z = new node();

    //
    z->leaf = y->leaf;
    z->numberOfKeys=t-1;
    z->entries = new entry[t-1];
    for(int j = 0;j<=t-2;j++)//largest t-1 keys of y
        z->entries[j].key = y->entries[j+t].key;
    if(!y->leaf)
    {   z->previous=y->entries[t-1].child;//1 child +
        for(int j =0;j<=t-2;j++)//t-1 children
            z->entries[j].child=y->entries[j+t].child;
    }
    y->numberOfKeys=t-1;
    if(i!=-1)
    {
        
        for(int j = x->numberOfKeys;j>i;j--)
        {
            x->entries[j].child=x->entries[j-1].child;
            x->entries[j].key=x->entries[j-1].key;
        }
        x->entries[i+1].child=z->pageNumber;
        x->entries[i].key=y->entries[t-1].key;
    }
    x->numberOfKeys=x->numberOfKeys+1;
    
    if(y->leaf)//double linked list.
    {
        z->next=y->next;
        y->next=z->pageNumber;
        z->previous=y->pageNumber;
    }
    filehandler.MarkDirty(y->pageNumber);
    filehandler.MarkDirty(x->pageNumber);
    writeNodeOnNewPage(z);
    filehandler.ForcePages();
    for(int i =0;i<z->numberOfKeys;i++)
        delete z->entries[i].key;
    delete z->entries;
    delete z;
    
}


//called recursively, if node x is a leaf, it adds the entry to the bucket
// if x is not a leaf, it goes to its children while splitting them if they have
// the necessary amount of keys.

void IX_IndexHandle::insertNonFull(node* x, void*  pData,const RID &rid)
{
    //if same key, add to bucket and increment number of rids.
    //if new key create bucket and add.
    //Leaves point only to 1 bucket.
    
    int i = x->numberOfKeys;
    int comp =0;
    if(x->leaf)
    {
        if(i==0)
        {
            x->numberOfKeys++;
            x->entries = new entry();
            x->entries[0].key = (char*)pData;
            PageNum next=-1;
            addToBucket(next, rid,-1);
            x->entries[0].child = next;

            
        }
        while(i>=1 && ((comp = compare(pData,x->entries[i].key))==-1))
        {
                x->entries[i+1].key=x->entries[i].key;
                i--;
        }
        if(comp == 1)//new key
        {
            x->entries[i+1].key=(char*)pData;
            x->numberOfKeys++;
            PageNum next=-1;
            addToBucket(next, rid,-1);
            x->entries[i+1].child = next;
            //new bucket
        }
        else
        {// existing bucket
            addToBucket(x->entries[i].child, rid,-1);
        }
        
        
        //x->children = new PageNum();
        filehandler.MarkDirty(x->pageNumber);
    }
    else
    {
        while(i>=1 && !compare(pData,x->entries[i].key))
            i--;
        i++;
        //read x->children[i];
        node* childi = readNodeFromPageNum(x->entries[i].child);
        
        if(childi->numberOfKeys==2*t-1)
        {
            splitChild(x,i,childi);
            if(compare(pData,x->entries[i].key))
                i++;
        }
        insertNonFull(childi,pData,rid);
        
        for(int i =0;i<childi->numberOfKeys;i++)
            delete childi->entries[i].key;
        delete childi->entries;
        delete childi;
    }
    
    
}

//start of recursion for the insert, x should be the root node
void IX_IndexHandle::insert(node* x, void* pData, const RID &rid)
{
    //x is the root node
    if(x->numberOfKeys==2*t-1)
    {

        node* s = new node();//s becomes the root
        //allocate and write s?
        s->leaf = false;
        s->isRoot=true;
        s->numberOfKeys=0;
        s->previous = x->previous;
        x->leaf = true;
        x->isRoot=false;
        s->entries= new entry[t-1];//allocate entries for new root.
        splitChild(s,-1,x);
        insertNonFull(s,pData,rid);
        filehdr.treeLayerNums++;
        filehdr.rootPageNum=s->pageNumber;
        writeNodeOnNewPage(s);
        filehandler.MarkDirty(x->pageNumber);
        //point T->root = s; if we put the root in memory
        filehandler.ForcePages();
        
        for(int i =0;i<x->numberOfKeys;i++)
            delete x->entries[i].key;
        delete x->entries;
        delete x;
    
    }
    else
        insertNonFull(x,pData,rid);
   
    
}

IX_IndexHandle::IX_IndexHandle()
{
    // Initialize member variables
    bHdrChanged = FALSE;
    memset(&fileHdr, 0, sizeof(fileHdr));
    fileHdr.rootPageNum = IX_EMPTY_TREE;
}

IX_IndexHandle::~IX_IndexHandle()
{
    
}

// Insert a new index entry
RC IX_IndexHandle::InsertEntry(void *pData, const RID &rid)
{
    if(filehdr.rootPageNum==IX_EMPTY_TREE)
    {
        node * root = new node();
        root->leaf = true;
        root->isRoot=  true;
        filehdr.treeLayerNums++;
        root->numberOfKeys=0;
        root->previous=-1;
        writeNodeOnNewPage(root);
        filehdr.rootPageNum=root->pageNumber;
        insertNonFull(root, pData, rid);
        filehandler.ForcePages();
        delete root->entries;
        delete root;
        
    }
    else
    {
        node* root = readNodeFromPageNum(filehdr.rootPageNum);
        insert(root, pData, rid);
    }
}




//desc : after delete the last entry in the root, this function collapseRoot change the root
//      if root is also the leaf, the tree becomes empty;
//      otherwise, the first child of oldRoot becomes the new root
//      treeLayerNums descrease by 1

//input : oldRoot - old root
void IX_IndexHandle::collapseRoot(node * oldRoot){
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
    //number of layers in the tree decrease by 1
    ((IX_FileHdr *)data)->treeLayerNums--;

    filehandler.MarkDirty(fileHeaderPage);

    //dispose of oldRoot page
    filehandler.MarkDirty(oldRoot->pageNumber);
    filehandler.UnpinPage(oldRoot->pageNumber);
    filehandler.DisposePage(oldRoot->pageNumber);
}

//Desc: merge two nodes, copy all the entries in the right node to the leftnode
//      delete the right node, that means call recusively the function deleteEntryInNode(node* x, int keyNum, nodeInfoInPath * path)
//      where node x is replaced by the node anchorNode
// 10/06/2014 deal with leaf linked list

//input : keyNum - is the position of key that separates this node and neighorNode
//        depthInPath - the depth of node thisNode in the path
//

void IX_IndexHandle::merge (node * thisNode , node *neighborNode, node *anchorNode, int keyNum, int depthInPath){
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
        leftN->entries[leftN->numberOfKeys].key = anchorNode->entries[keyNum].key;
        //copy the first pointer in the right node to the left node
        leftN->entries[leftN->numberOfKeys].child = rightN->previous;
        leftN->numberOfKeys++;
    }

    //copy all the entries in the right node to the leftnode
    for(int i=0; i<rightN->numberOfKeys; i++){
        leftN->entries[leftN->numberOfKeys] = rightN->entries[i];
        leftN->numberOfKeys++;

    }

    //deal with leaf linked list
    if(leftN->leaf){
        leftN->next = rightN->next;
        readNodeFromPageNum(rightN->next)->previous = leftN->pageNumber;
    }

    //dispose page in node rightN
    filehandler.UnpinPage(rightN->pageNumber);
    filehandler.DisposePage(rightN->pageNumber);

    //write left node to buffer pool
    writeNodeOnNewPage(leftN);

     //delete keyNum in anchorNode, recursively
    deleteEntryInNode(anchorNode,keyNum,path,depthInPath);
}


//Desc: shift over half of a neighbor’s plus keys to this node
//      update anchor : the key value in the index entry pointing to the second node must be changed to the the lowest search key in the second node

//input : keyNum - is the position of key that separates this node and neighorNode
//        isRight - true : neighbor is on the rightside; false : neighbor is on the leftside
//
void IX_IndexHandle::shift (node * thisNode , node *neighborNode, node *anchorNode, int keyNum, bool isRight ){

    int numKeysShifted = (neighborNode->numberOfKeys - thisNode->numberOfKeys) / 2;
    // neighbor is on the rightside
    if(isRight){
        //node is not leaf
        if(!thisNode->leaf){
            //copy key in position keyNum to thisNode
            thisNode->entries[t-1].key = anchorNode->entries[keyNum].key;
            thisNode->entries[t-1].child = neighborNode->previous;
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

        //node is leaf, shift numKeysShifted (k,p) to thisNode from neighborNode
        else{
            for(int i=0; i<numKeysShifted;i++){
                thisNode->entries[thisNode->numberOfKeys] = neighborNode->entries[i];
                thisNode->numberOfKeys++;
            }

            //shift neighborNode (k,v) to the left
            for(int i=0; i<neighborNode->numberOfKeys-numKeysShifted;i++){
                neighborNode->entries[i] = neighborNode->entries[i+numKeysShifted];
            }

            //copy the the lowest search key in the second node to anchor's
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


        //node is not leaf
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

        //node is leaf, shift numKeysShifted (k,p) to thisNode from neighborNode
        else{
            for(int i=numKeysShifted-1; i>=0; i--){
                thisNode->entries[i] = neighborNode->entries[neighborNode->numberOfKeys-1];
                thisNode->numberOfKeys++;
                neighborNode->numberOfKeys--;
            }

            //copy the the lowest search key in the second node to anchor's
             anchorNode->entries[keyNum].key = thisNode->entries[0].key;
        }
    }

    //write these changes to buffer pool
    writeNodeOnNewPage(thisNode);
    writeNodeOnNewPage(neighborNode);
    writeNodeOnNewPage(anchorNode);
}

//desc : delete the entry in position keyNum in node x
//input : depthInPath - the depth of node x in the path
void deleteEntryInNode(node* x, int keyNum, nodeInfoInPath * path, int depthInPath)
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
            if(x->numberOfKeys==0) //root has no more keys after delete
                collapseRoot(x);
            else //root has still keys, do nothing
                return;
        }
        //check immediate neighbors
        nodeInfoInPath = path[depthInPath];
        PageNum neighborR = path[depthInPath].neighborR;
        PageNum neighborL = path[depthInPath].neighborL;

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
            merge(x,nodeNeighbor,anchorNode,entryNum, depthInPath-1);
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
                    PageNum leafPage = path[pathDepth].anchor;
                    int entryNum = path[pathDepth].entryNum;
                    node * leaf = readNodeFromPageNum(leafPage);
                    if(bucketHdr.next == -1) {
                        //delete the entry in leaf
                        deleteEntryInNode(leaf,entryNum,path,pathDepth-1);

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
    return deleteRID(next,rid,path,pathDepth);
}



//desc : Delete a index entry
//input : pData - the key value of index
//          rid - the rid to be deleted
RC IX_IndexHandle::DeleteEntry(void *pData, const RID &rid){
    PageNum rootPage = filehdr.rootPageNum;
    node *root = readNodeFromPageNum(rootPage);
    //an array of nodeInfo to save infos about neighors and anchors
    nodeInfoInPath path[fileHdr.treeLayerNums];
    //for root
    path[0].self = rootPage;
    path[0].anchor=-1; //implicit root
    path[0].neighborL=-1;
    path[0].neighborR=-1;
    path[0].key = -1;
    path[0].entryNum = -1;

    int pathDepth = 0;
    //parcourir l'arbre et obtenir les donnees enregistrees dans path
    traversalTree(root,pData,path,pathDepth);
    //remove rid in bucket
    //get the first bucket page
    PageNum bucket = path[pathDepth].self;


    //delete rid in the buckets
    return deleteRID(bucket,rid,path,pathDepth);
}


//desc : recurse to a leaf node from root to find deletable entry:
//       for nodes in the search path, calculate immediate neighbors and their anchors
//input : pData - the key value of index
//          x - the node to
//return : path - a pointer that points to a list of nodeInfoInPath
//         pathDepth - the length of this list of nodeInfoInPath
RC IX_IndexHandle::traversalTree(node *x, void *pData, nodeInfoInPath *path,int &pathDepth){
    pathDepth++;
    if(x->leaf){
        int i = 0;
        while(i<x->numberOfKeys && compare(pData,x->entries[i].key)!=0)
            i++;
        // if not found in leaf
        if(compare(pData,x->entries[i].key)!=0) return IX_INDEX_NOTFOUND; //TODO define ERROR

        //found in leaf, save neighbor infos for bucket page
        path[pathDepth].self = x->entries[i].child;
        path[pathDepth].anchor = x->pageNumber;
        path[pathDepth].neighborL = -1; //for bucket, no need to know its neighbors
        path[pathDepth].neighborR = -1;
        path[pathDepth].key = x->entries[i].key;
        path[pathDepth].entryNum = i;
        return 0;
    }

    int i = 0;
    while(i<x->numberOfKeys && compare(pData,x->entries[i].key)!=-1)
        i++;
    node* childi = NULL;
    //pData is less than the first key, so childi is pointed by the first pointer in x
    if(i==0){
        childi = readNodeFromPageNum(x->previous);
        path[pathDepth].self = x->previous;
        //if pointer to this page is the first pointer in its parent
        path[pathDepth].neighborL = -1; //doesn't have left neighbor
        path[pathDepth].neighborR = x->entries[0].child;

    }
    else{
        childi = readNodeFromPageNum(x->entries[i-1].child);
        path[pathDepth].self = x->entries[i-1].child;
    }
    //save immdediate neighors for childi
    path[pathDepth].anchor = x->pageNumber; //childi's anchor
    path[pathDepth].key = x->entries[i].key;
    path[pathDepth].entryNum = i;


    if(0<i<x->numberOfKeys) {
        if(i==1)    path[pathDepth].neighborL = x->previous;
        else    path[pathDepth].neighborL = x->children[i-2];
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

