//
// ix.h
//
//   Index Manager Component Interface
//

#ifndef IX_H
#define IX_H

// Please do not include any other files than the ones below in this file.

#include "redbase.h"  // Please don't change these lines
#include "rm_rid.h"  // Please don't change these lines
#include "pf.h"

//
// IX_FileHdr: Header structure for files
//
struct IX_FileHdr {
    AttrType attrType; //attribute type
    int attrLength; //attribute length
    int indexNo; //indexNo
    //int numMaxEntries; // max number of entries in a node
    int orderOfTree;  //every node contains m entries, where order<=m<=2order except for the root, corresponding to t
    PageNum rootPageNum; // page number of the root in the B+ tree
    //add by dzh
    int numRidsPerBucket; //# of rids in each bucket
    int bucketHeaderSize; // size of (IX_BucketHdr + bitMap)
    int treeLayerNums; //number of layers in the b+ tree
};

//
// IX_IndexHandle: IX Index File interface
//
class IX_IndexHandle {
public:
    IX_IndexHandle();
    ~IX_IndexHandle();

    // Insert a new index entry
    RC InsertEntry(void *pData, const RID &rid);

    // Delete a new index entry
    RC DeleteEntry(void *pData, const RID &rid);

    // Force index files to disk
    RC ForcePages();
    
    PF_FileHandle pfFileHandle;
    IX_FileHdr fileHdr;                                   // file header
    int bHdrChanged;                                      // dirty flag for file

private:
    // Bitmap Manipulation
    int GetBitmap  (char *map, int idx) const;
    void SetBitmap (char *map, int idx) const;
    void ClrBitmap (char *map, int idx) const;

};

//
// IX_IndexScan: condition-based scan of index entries
//
class IX_IndexScan {
public:
    IX_IndexScan();
    ~IX_IndexScan();

    // Open index scan
    RC OpenScan(const IX_IndexHandle &indexHandle,
                CompOp compOp,
                void *value,
                ClientHint  pinHint = NO_HINT);

    // Get the next matching entry return IX_EOF if no more matching
    // entries.
    RC GetNextEntry(RID &rid);

    // Close index scan
    RC CloseScan();
    private:

    boolean bScanOpen;
    boolean endScan;
    SlotNum curSlotNum;
    PageNum currentBucket;
    node* currentLeaf;
    PageNum nextLeaf;
    PageNum prevLeaf;
    int currentPosInLeaf;
    IX_IndexHandle *pIndexHandle;
    CompOp compOp;
    void *value;
    ClientHint pinHint;
};

//
// IX_Manager: provides IX index file management
//
class IX_Manager {
public:
    IX_Manager(PF_Manager &pfm);
    ~IX_Manager();

    // Create a new Index
    RC CreateIndex(const char *fileName, int indexNo,
                   AttrType attrType, int attrLength);

    // Destroy and Index
    RC DestroyIndex(const char *fileName, int indexNo);

    // Open an Index
    RC OpenIndex(const char *fileName, int indexNo,
                 IX_IndexHandle &indexHandle);

    // Close an Index
    RC CloseIndex(IX_IndexHandle &indexHandle);

    //add zhihui
private:
    PF_Manager *pPfm;

};




//
// Print-error function
//
void IX_PrintError(RC rc);

#define  IX_INVALIDATTR (START_IX_WARN + 0) //invalid attribute parameters
#define IX_INDEX_NOTFOUND (START_IX_WARN + 1) //index not found
#endif
