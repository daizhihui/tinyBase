//
// File:        sm_internal.h
// Description: Declarations internal to the SM component
// Author:      Hyunjung Park (hyunjung@stanford.edu)
//

#ifndef SM_INTERNAL_H
#define SM_INTERNAL_H

#include <stdlib.h>
#include <string.h>
//#include <stdio.h>
#include <unistd.h>
#include <cassert>
#include "sm.h"
#include "printer.h"

//
// Constants and defines
//
#define MAXDBNAME MAXNAME
#define MAXLINE (2048)
#define RELCAT "relcat"
#define ATTRCAT "attrcat"

#define OFFSET(type, member) ((int)&((type *) 0)->member)

//
// Query Optimization Options
//
extern bool bIxScan;
extern bool bNLIJ;
extern bool bPushProj;
extern bool bNBJ;
extern bool bRocking;
extern bool bOptimize;
extern bool bPlanOnly;

//
// SM_RelcatRec : structure for relcat record
//
struct SM_RelcatRec {
   char relName[MAXNAME];
   int tupleLength;
   int attrCount;
   int cardinality;
   int numPages;
};

#define SM_SetRelcatRec(r, _relName, _tupleLength,          \
                        _attrCount, _cardinality, _numPages)\
do {                                                        \
   memset(r.relName, '\0', sizeof(r.relName));              \
   strncpy(r.relName, _relName, sizeof(r.relName));         \
   r.tupleLength = _tupleLength;                            \
   r.attrCount = _attrCount;                                \
   r.cardinality = _cardinality;                            \
   r.numPages = _numPages;                                  \
} while (0)

//
// SM_AttrcatRec : structure for attrcat record
//
struct SM_AttrcatRec {
   char relName[MAXNAME];
   char attrName[MAXNAME];
   int offset;
   AttrType attrType;
   int attrLength;
   int indexNo;
   int distinctVals;
   int ixDepth;
   int minValue;
   int maxValue;
};

#define SM_SetAttrcatRec(r, _relName, _attrName, _offset,   \
                         _attrType, _attrLength, _indexNo,  \
                         _distinctVals, _ixDepth)           \
do {                                                        \
   memset(r.relName, '\0', sizeof(r.relName));              \
   strncpy(r.relName, _relName, MAXNAME);                   \
   memset(r.attrName, '\0', sizeof(r.attrName));            \
   strncpy(r.attrName, _attrName, MAXNAME);                 \
   r.offset = _offset;                                      \
   r.attrType = _attrType;                                  \
   r.attrLength = _attrLength;                              \
   r.indexNo = _indexNo;                                    \
   r.distinctVals = _distinctVals;                          \
   r.ixDepth = _ixDepth;                                    \
   r.minValue = 0;                                          \
   r.maxValue = 0;                                          \
} while (0)

//
// for DataAttrInfo
//
#define SetDataAttrInfo(r, _relName, _attrName, _offset,    \
                        _attrType, _attrLength, _indexNo)   \
do {                                                        \
   memset(r.relName, '\0', sizeof(r.relName));              \
   strncpy(r.relName, _relName, MAXNAME);                   \
   memset(r.attrName, '\0', sizeof(r.attrName));            \
   strncpy(r.attrName, _attrName, MAXNAME);                 \
   r.offset = _offset;                                      \
   r.attrType = _attrType;                                  \
   r.attrLength = _attrLength;                              \
   r.indexNo = _indexNo;                                    \
} while (0)

#endif
