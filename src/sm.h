//
// sm.h
//   Data Manager Component Interface
//

#ifndef SM_H
#define SM_H

// Please do not include any other files than the ones below in this file.

#include <stdlib.h>
#include <string.h>
#include "redbase.h"  // Please don't change these lines
#include "parser.h"
#include "rm.h"
#include "ix.h"

//
// SM_Manager: provides data management
//
class SM_Manager {
    friend class QL_Manager;
public:
    SM_Manager    (IX_Manager &ixm, RM_Manager &rmm);
    ~SM_Manager   ();                             // Destructor

    RC OpenDb     (const char *dbName);           // Open the database
    RC CloseDb    ();                             // close the database

    RC CreateTable(const char *relName,           // create relation relName
                   int        attrCount,          //   number of attributes
                   AttrInfo   *attributes);       //   attribute data
    RC CreateIndex(const char *relName,           // create an index for
                   const char *attrName);         //   relName.attrName
    RC DropTable  (const char *relName);          // destroy a relation

    RC DropIndex  (const char *relName,           // destroy index on
                   const char *attrName);         //   relName.attrName
    RC Load       (const char *relName,           // load relName from
                   const char *fileName);         //   fileName
    RC Help       ();                             // Print relations in db
    RC Help       (const char *relName);          // print schema of relName

    RC Print      (const char *relName);          // print relName contents

    RC Set        (const char *paramName,         // set parameter to
                   const char *value);            //   value

private:
    RM_Manager * Rmm;
    IX_Manager * Ixm;
    RM_FileHandle fileHandle_Relcat;
    RM_FileHandle fileHandle_Attrcat;
    
};

//
// Print-error function
//

//struct for Catalog relcat
struct Relation{
    const char *relName; //relation name
    int tupleLength; //tuple length in bytes
    int attrCount; //number of attributes
    int indexCount; //number of indexed attributes
};

//struct for Catalog attrcat in printer.h : DataAttrInfo

//
// Error table
//
static char *SM_WarnMsg[] = {
    (char*)"inviable relation name",
    (char*)"invalid attribute name",
    (char*)"too much attributes",
    (char*)"invalid attribute",
    (char*)"index already exists",
    (char*)"attribute index not exists",
};

void SM_PrintError(RC rc);

#define SM_INVALIDRELNAME     (START_SM_WARN + 0) // invalid relation name
#define SM_INVALIDATTRNAME     (START_SM_WARN + 1) // invalid attribute name
#define SM_EXCEEDMAXATTRS     (START_SM_WARN + 2) //too much attributes
#define SM_INVALIDATTR        (START_SM_WARN + 3) // invalid attribute
#define SM_INDEXEXIST       (START_SM_WARN + 4) //index already exists
#define SM_ATTRINDEX_NOT_EXIST (START_SM_WARN + 5) // attribute index not exists
#define SM_LASTWARN        SM_ATTRINDEX_NOT_EXIST
#endif
