//
// File:        ix_internal.h
// Description: Declarations internal to the IX component
// Author:      Hyunjung Park (hyunjung@stanford.edu)
//

#ifndef IX_INTERNAL_H
#define IX_INTERNAL_H

#include <stdlib.h>
#include <string.h>
#include "ix.h"

//
// Constants and defines
//
const int IX_HEADER_PAGE_NUM = 0;
const int IX_EMPTY_TREE = -1; //an empty tree


struct IX_BucketHdr {
    PageNum before;
    PageNum next;
};

//a structure to save infos about neighors and anchors when delete a index
struct nodeInfoInPath{
    PageNum self;
    PageNum neighborL;
    PageNum neighborR;
    PageNum anchor;
    AttrType key; //key value
    int entryNum; //position of this entry
};


#endif // IX_INTERNAL_H
