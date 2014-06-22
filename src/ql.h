//
// ql.h
//   Query Language Component Interface
//

// This file only gives the stub for the QL component

#ifndef QL_H
#define QL_H

#include <stdlib.h>
#include <string.h>
#include <vector>
#include "redbase.h"
#include "parser.h"
#include "rm.h"
#include "ix.h"
#include "sm.h"
#include "printer.h"

using namespace std;
//
// QL_Manager: query language (DML)
//
class QueryNode;
class nodeIterator;

class QL_Manager {
public:
    QL_Manager (SM_Manager &smm, IX_Manager &ixm, RM_Manager &rmm);
    ~QL_Manager();                       // Destructor

    RC Select  (int nSelAttrs,           // # attrs in select clause
        const RelAttr selAttrs[],        // attrs in select clause
        int   nRelations,                // # relations in from clause
        const char * const relations[],  // relations in from clause
        int   nConditions,               // # conditions in where clause
        const Condition conditions[]);   // conditions in where clause

    RC Insert  (const char *relName,     // relation to insert into
        int   nValues,                   // # values
        const Value values[]);           // values to insert

    RC Delete  (const char *relName,     // relation to delete from
        int   nConditions,               // # conditions in where clause
        const Condition conditions[]);   // conditions in where clause

    RC Update  (const char *relName,     // relation to update
        const RelAttr &updAttr,          // attribute to update
        const int bIsValue,              // 1 if RHS is a value, 0 if attribute
        const RelAttr &rhsRelAttr,       // attr on RHS to set LHS equal to
        const Value &rhsValue,           // or value to set attr equal to
        int   nConditions,               // # conditions in where clause
        const Condition conditions[]);   // conditions in where clause

private:
    SM_Manager *_smm;
    IX_Manager *_ixm;
    RM_Manager *_rmm;
    RC printResultSelection();
    RC getDataAttributsByRelation(const char *relName, DataAttrInfo attributs[], int &attr_count);
    RC getDataAttributByRelAttr(const RelAttr &relAttr, DataAttrInfo &attribut);
    void getSelCondition(int nConditions, const Condition conditions[], const char * rel,
                         int &nConReturn, const Condition **selConds, int offset[], int  length[]);
    void getJoinConditions(const int &numRel, int nConditions, const Condition conditions[],
                      int &nConReturn, Condition **selConds, int offset[], int  length[]);
    //Status iteratorExecution(QueryTree * tree, RM_Record *&input, RM_Record *&output);
    const char* getRelName(const RelAttr &relAttr, int nRelations, const char * const relations[]);
};




//
// Print-error function
//
#define QL_INVALIDRELNAME     (START_QL_WARN + 0) // invalid relation name
#define QL_INVALID_ATTR_NUM        (START_QL_WARN + 1) //invalid attribute number
#define QL_INVALID_ATTR_TYPE  (START_QL_WARN + 2) //invalid attribute type

void QL_PrintError(RC rc);

#endif
