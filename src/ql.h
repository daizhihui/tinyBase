//
// ql.h
//   Query Language Component Interface
//

// This file only gives the stub for the QL component

#ifndef QL_H
#define QL_H

#include <stdlib.h>
#include <string.h>
#include "redbase.h"
#include "parser.h"
#include "printer.h"
#include "rm.h"
#include "ix.h"
#include "sm.h"

class QL_Operator;

//
// QL_Manager: query language (DML)
//
class QL_Manager {
public:
    QL_Manager (SM_Manager &smm, IX_Manager &ixm, RM_Manager &rmm, PF_Manager &pfm);
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
    // Copy constructor
    QL_Manager(const QL_Manager &manager);
    // Overloaded =
    QL_Manager& operator=(const QL_Manager &manager);

    RC SelectCheck(int   nSelAttrs,
                   const RelAttr selAttrs[],
                   int   nRelations,
                   const char * const relations[],
                   int   nConditions,
                   const Condition conditions[],
                   int   &nTotalAttrs);

    RC DeleteCheck(const char *relName,
                   int   nConditions,
                   const Condition conditions[],
                   int   &nAttrs);

    RC UpdateCheck(const char *relName,
                   const RelAttr &updAttr,
                   const int bIsValue,
                   const RelAttr &rhsRelAttr,
                   const Value &rhsValue,
                   int   nConditions,
                   const Condition conditions[],
                   int   &nAttrs);

    RC RelAttrCheck(const RelAttr &, AttrType &,
                    int, const char * const []);

    RC SelectPlan0(int   nSelAttrs,
                   const RelAttr selAttrs[],
                   int   nRelations,
                   const char * const relations[],
                   int   nConditions,
                   const Condition conditions[],
                   Condition &alwaysMatch,
                   RM_FileHandle **pRmfhs,
                   QL_Operator *&root);

    RC SelectPlan1(int   nSelAttrs,
                   const RelAttr selAttrs[],
                   int   nRelations,
                   const char * const relations[],
                   int   nConditions,
                   const Condition conditions[],
                   Condition &alwaysMatch,
                   RM_FileHandle **pRmfhs,
                   QL_Operator *&root);

    RC SelectPlan2(int   nSelAttrs,
                   const RelAttr selAttrs[],
                   int   nRelations,
                   const char * const relations[],
                   int   nConditions,
                   const Condition conditions[],
                   Condition &alwaysMatch,
                   RM_FileHandle **pRmfhs,
                   QL_Operator *&root);

    RC SelectPrinter(DataAttrInfo *&attributes,
                     int nSelAttrs, const RelAttr selAttrs[],
                     int nRelations, const char * const relations[],
                     QL_Operator *root, int nTotalAttrs);

    RC DeleteUpdatePlan(const char *relName,
                        const char *updAttrName,
                        int   nConditions,
                        const Condition conditions[],
                        Condition &alwaysMatch,
                        RM_FileHandle &rmfh,
                        QL_Operator *&root);

    SM_Manager *pSmm;
    IX_Manager *pIxm;
    RM_Manager *pRmm;
    PF_Manager *pPfm;
};

//
// Print-error function
//
void QL_PrintError(RC rc);

#define QL_DUPLICATEDREL   (START_QL_WARN + 0) // duplicated relation names
                                               // in the From clause
#define QL_RELNOTINFROM    (START_QL_WARN + 1) // relation name doesn't appear
                                               // in the From clause
#define QL_AMBIGUOUSATTR   (START_QL_WARN + 2) // not exactly one relation
                                               // with attribute name
#define QL_UNCOMPATTYPE    (START_QL_WARN + 3) // uncompatible type
#define QL_NUMVALUE        (START_QL_WARN + 4) // uncompatible type
#define QL_EOF             (START_QL_WARN + 5) // end of file
#define QL_LASTWARN        QL_EOF

#define QL_NOMEM           (START_QL_ERR - 0)  // no memory
#define QL_LASTERROR       QL_NOMEM
#endif
