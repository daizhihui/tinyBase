//
// File:        IX_indexscan.cc
// Description: IX_IndexScan class implementation
// Author:
//
#include "ix.h"
#include "ix_internal.h"

IX_IndexScan::IX_IndexScan()
{
    // Initialize member variables

    bScanOpen=FALSE;
    endScan=FALSE;
    curSlotNum=-1;
    currentBucket=-1;
    currentLeaf= NULL;
    nextLeaf=-1;
    prevLeaf=-1;
    currentPosInLeaf=-1;
    pIndexHandle= NULL;
    compOp=-1;
    value=NULL;
    pinHint=-1;

}

//
// IX_IndexScan
//
// Desc: Destructor
//
IX_IndexScan::~IX_IndexScan()
{
    // Don't need to do anything
}


RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle,CompOp _compOp, void *_value,ClientHint _pinHint){
    // Sanity Check: 'this' should not be open yet
    if (bScanOpen)
       // Test: opened IX_IndexScan, c'est à dire paramètres deja initialisés
       return (IX_SCANOPEN);

    //verifier si indexHandle est deja ouvert, je peux faire le test sur attributeLenght return (IX_CLOSEDFILE);

    // Sanity Check: compOp
    switch (_compOp) {
    case EQ_OP:
    case LT_OP:
    case GT_OP:
    case LE_OP:
    case GE_OP:
    case NO_OP:
       break;

    default:
       return (IX_INVALIDCOMPOP);// to define
    }
    if (_compOp != NO_OP && compOp != NE_OP) {
       // Sanity Check: value must not be NULL
       if (_value == NULL) 
          return (IX_NULLPOINTER); //to define

       }
    // Copy parameters to local variable
    compOp= _compOp;
    value =  _value;
    pinHint= _pinHint;
   pIndexHandle=(IX_IndexHandle *) &indexHandle;

    // Set local state variables
    bScanOpen = TRUE;
   // curPageNum =  pIndexHandle->fileHdr.rootPageNum; // valeur de la racine
   // curSlotNum = pIndexHandle->fileHdr.numMaxEntries;

    // Return ok
    return (0);
}


 // search of leaf containing value

int searchLeaf(PageNum startPageNum,void* value, PageNum &leaf){ // Penser à gerer les cas d'erreurs
 node* currentNode;
 int numberOfKeys;
 int pos=0;
 currentNode=pIndexHandle->readNodeFromPageNum(startPageNum);
 numberOfKeys=currentNode->numberOfKeys;

 if(currentNode->leaf){
     leaf=startPageNum;
 }else{
     while( pos< numberOfKeys){

         if(pIndexHandle->compare(value,(currentNode->entries[pos])->key)<0){ // value < current Key
            // appel recursif à partir du nouveau noeud
             searchLeaf((currentNode->entries[pos])->child,value,leaf);
         }
         else{
             if(pos+1==numberOfKeys){  // je suis sur la derniere entrée du noeud
                 searchLeaf((currentNode->entries[pos])->child,value,leaf);
             }

         }
         pos++;
     }

 }
 return 0;


}

//
//testBitValue in bitmap
//

int testBitValue(unsigned char bitmap[],int bit)
{

    int numoctet = bit/8;
    int numbit = bit%8;

    int result;

    unsigned char e = bitmap[numoctet] & (1 << numbit);
    result = (int)e;

    if(result == 0)
        return 0 ;
    else
        return 1;
}


//

//
//getNextFullSlot
//

int getNextFullSlot(int start,unsigned char bitmap[],int maxrecnumber)
{
    start++;
    while(start<maxrecnumber)
    {
        if(testBitValue(bitmap,start)==1)
        {
            return start;
        }
        else
            start ++;
    }
    return -2;
}








RC IX_IndexScan::GetNextEntry(RID &rid){

    RC rc;
    PageNumber leaf;

   PageNum gBucket=-1,pBucket=-1, nBucket=-1;
   int pos_g=-1,pos_p=-1,pos_n=-1;  // Positions linked to  entries

   int nextSlot;
   int pos=0;
   char * pdata;
   IX_BucketHdr bucketHdr;
   boolean stop;

   PF_PageHandle pageHandler;

    if(bScanOpen){

        if(currentBucket == -1 && endScan !=true){ //  First access
            searchLeaf(pIndexHandle->fileHdr.rootPageNum,value,leaf);
            currentLeaf= pIndexHandle->pIndexHandle->readNodeFromPageNum(leaf);


            while(pos<currentLeaf->numberOfKeys){
                if(pIndexHandle->compare(value,(currentLeaf->entries[pos])->key)==0 && gBucket==-1){

                    gBucket= (currentLeaf->entries[pos])->child;
                    pos_g=pos;
                }
                if(pIndexHandle->compare(value,(currentleaf->entries[pos])->key)>0 ){
                    pBucket= (currentLeaf->entries[pos])->child;
                    pos_p=pos;
                }
                if(pIndexHandle->compare(value,(currentLeaf->entries[pos])->key)<0 && nBucket==-1){ //correspond au premier element inferieur à value
                    nBucket= (currentLeaf->entries[pos])->child;
                    pos_n=pos;
                }
                pos++;
            }

            if(gBucket==-1){
                if(compOp==EQ_OP){

                    endScan=true;
                }
                if(compOp==GE_OP){
                    currentBucket=nBucket;
                    currentPosInLeaf=pos_n;
                }
                if(compOp==LE_OP){
                    currentBucket=pBucket;
                    currentPosInLeaf=pos_p;
                }
            }
            else{

                if((compOp==EQ_OP)||(compOp==GE_OP)||(compOp==LE_OP)){
                    currentBucket=gBucket;
                    currentPosInLeaf=pos_g;
                 }
            }
            if(compOp==GT_OP){
                currentBucket=nBucket;
                currentPosInLeaf=pos_n;
            }
            if(compOp==LT_OP){
                currentBucket=pBucket;
                currentPosInLeaf=pos_p;
            }


            if(currentBucket==-1){
                endScan=true;
            }else{
                curSlotNum=-1;
            }
        }

        if(endScan){
            return IX_EOF;
        }else{
            stop=false;
            while(!stop){

                pIndexHandle->filehandler.GetThisPage(currentBucket,pageHandler);
                pageHandler.GetData(pdata);
                memcpy(&bucketHdr,(IX_BucketHdr*)pdata, sizeof(IX_BucketHdr));
             //find nextSlot in bitmap
                nextSlot= getNextFullSlot(curSlotNum, pdata+sizeof(IX_BucketHdr),pIndexHandle->filehdr.numRidsPerBucket);

                 if(nextSlot!=-2){
                    stop=true; // Sortie de la boucle While
                    pdata=pdata+(nextSlot*sizeof(RID));
                    memcpy(&rid,(RID*)pdata,sizeof(RID));
                    curSlotNum=nextSlot;

            //unpinpage currentBucket ??
                 }else{ //I'm at the end of currentBucket

        //unpinpage currentBucket??

                    if(bucketHdr.next!=-1){// Next bucket if it exits
                        currentBucket=bucketHdr.next;
                        curSlotNum=-1;
                    }else{
                    // End of the scan
                        if(compOp==EQ_OP){
                            endScan=true;
                            stop=true;
                        }
                        if(compOp==LE_OP || compOp==LT_OP){
                            if(currentPosInLeaf -1 >= 0){
                                currentPosInLeaf=currentPosInLeaf-1;
                                currentBucket=(currentLeaf->entries[currentPosInLeaf])->child;
                                curSlotNum=-1;
                            }else{ // I must go to another leaf
                                if(currentLeaf->previous !=-1){
                                    int tempLeaf;
                                    tempLeaf=currentLeaf->previous;
                                    currentLeaf= pIndexHandle->pIndexHandle->readNodeFromPageNum(tempLeaf);
                                    currentPosInLeaf=currentLeaf->numberOfKeys -1;
                                    currentBucket=(currentLeaf->entries[currentPosInLeaf])->child;
                                    curSlotNum=-1;

                                }else{ // end of the list of leaves
                                    endScan=true;
                                    stop=true;
                                }

                            }
                        }
                        if(compOp== GT_OP || compOp== GE_OP){
                            if(currentPosInLeaf +1 <currentLeaf->numberOfKeys){
                                currentPosInLeaf=currentPosInLeaf+1;
                                currentBucket=(currentLeaf->entries[currentPosInLeaf])->child;
                                curSlotNum=-1;

                            }else{
                                if(currentLeaf->next !=-1){
                                    int tempLeaf;
                                    tempLeaf=currentLeaf->next;
                                    currentLeaf= pIndexHandle->pIndexHandle->readNodeFromPageNum(tempLeaf);
                                    currentPosInLeaf=0;
                                    currentBucket=(currentLeaf->entries[currentPosInLeaf])->child;
                                    curSlotNum=-1;

                                }else{ // end of the list of leaves
                                    endScan=true;
                                    stop=true;
                                }

                            }

                        }

                   }

                }

            }

        }

    }else{
        return -1; // scan not defined

    }
    // the scan is not finished
    if(!endScan)
        return 0;
    else
        return IX_EOF;

}


//
// CloseScan
//
// Desc: Close an index scan with the given indexHandle and scan condition
// Ret:  IX_CLOSEDSCAN
//
RC IX_IndexScan::CloseScan()
{

    if (!bScanOpen)
        // Test: closed RM_FileScan
        return (IX_CLOSEDSCAN);

    // Reset member variables
    bScanOpen=FALSE;
    endScan=FALSE;
    curSlotNum=-1;
    currentBucket=-1;
    currentLeaf= NULL;
    nextLeaf=-1;
    prevLeaf=-1;
    currentPosInLeaf=-1;
    pIndexHandle= NULL;
    compOp=-1;
    value=NULL;
    pinHint=-1;

    // Return ok
    return (0);
}
