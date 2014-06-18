//
// ql_manager_stub.cc
//

// Note that for the SM component (HW3) the QL is actually a
// simple stub that will allow everything to compile.  Without
// a QL stub, we would need two parsers.

#include <cstdio>
#include <iostream>
#include <sys/times.h>
#include <sys/types.h>
#include <cassert>
#include <unistd.h>
#include "redbase.h"
#include "ql.h"
#include "sm.h"
#include "ix.h"
#include "rm.h"

using namespace std;

//
// QL_Manager::QL_Manager(SM_Manager &smm, IX_Manager &ixm, RM_Manager &rmm)
//
// Constructor for the QL Manager
//
QL_Manager::QL_Manager(SM_Manager &smm, IX_Manager &ixm, RM_Manager &rmm)
{
    // Can't stand unused variable warnings!
    //assert (&smm && &ixm && &rmm);
    _smm = &smm;
    _ixm = &ixm;
    _rmm = &rmm;

}

//
// QL_Manager::~QL_Manager()
//
// Destructor for the QL Manager
//
QL_Manager::~QL_Manager()
{
}

//
// Handle the select clause
//
RC QL_Manager::Select(int nSelAttrs, const RelAttr selAttrs[],
                      int nRelations, const char * const relations[],
                      int nConditions, const Condition conditions[])
{
    int i;

    cout << "Select\n";

    cout << "   nSelAttrs = " << nSelAttrs << "\n";
    for (i = 0; i < nSelAttrs; i++)
        cout << "   selAttrs[" << i << "]:" << selAttrs[i] << "\n";

    cout << "   nRelations = " << nRelations << "\n";
    for (i = 0; i < nRelations; i++)
        cout << "   relations[" << i << "] " << relations[i] << "\n";

    cout << "   nCondtions = " << nConditions << "\n";
    for (i = 0; i < nConditions; i++)
        cout << "   conditions[" << i << "]:" << conditions[i] << "\n";


    return 0;
}

//
// Insert the values into relName
//
RC QL_Manager::Insert(const char *relName,
                      int nValues, const Value values[])
{
    int i;

    cout << "Insert\n";

    cout << "   relName = " << relName << "\n";
    cout << "   nValues = " << nValues << "\n";
    for (i = 0; i < nValues; i++)
        cout << "   values[" << i << "]:" << values[i] << "\n";

    return 0;
}

//
// Delete from the relName all tuples that satisfy conditions
//
RC QL_Manager::Delete(const char *relName,
                      int nConditions, const Condition conditions[])
{
    int i;

    cout << "Delete\n";

    cout << "   relName = " << relName << "\n";
    cout << "   nCondtions = " << nConditions << "\n";
    for (i = 0; i < nConditions; i++)
        cout << "   conditions[" << i << "]:" << conditions[i] << "\n";

    return 0;
}


//
// Update from the relName all tuples that satisfy conditions
//
RC QL_Manager::Update(const char *relName,
                      const RelAttr &updAttr,
                      const int bIsValue,
                      const RelAttr &rhsRelAttr,
                      const Value &rhsValue,
                      int nConditions, const Condition conditions[])
{
    int i;

    cout << "Update\n";

    cout << "   relName = " << relName << "\n";
    cout << "   updAttr:" << updAttr << "\n";
    if (bIsValue)
        cout << "   rhs is value: " << rhsValue << "\n";
    else
        cout << "   rhs is attribute: " << rhsRelAttr << "\n";

    cout << "   nCondtions = " << nConditions << "\n";
    for (i = 0; i < nConditions; i++)
        cout << "   conditions[" << i << "]:" << conditions[i] << "\n";

    return 0;
}

RC QL_Manager::createQueryTree(int nSelAttrs, const RelAttr selAttrs[],
                               int nRelations, const char * const relations[],
                               int nConditions, const Condition conditions[]){

    //if rhs is a value, it's restriction, must be done before join
    for (i = 0; i < nConditions; i++){
        if(!conditions[i].bRhsIsAttr) {
            char * relationL = getRelNameForUniqueAttr(conditions[i].lhsAttr,nRelations,relations[]);
            //verify if this relation has index on attribut
            //TODO
            bool isIndexed = true;
            //define an internal node
            NodeValue item;
            InternalNodeValue iv;
            iv.condition = &conditions[i];
            if(isIndexed){
                iv.operation = IdexScanSelection;
            }
            item.internalValue =iv;
            QueryNode *pptr = new QueryNode(item,0,0,0);
            //define a leaf node
            LeafValue leaf;
            leaf.relation = relationL;
            item.leafValue =leaf;
            QueryNode * leafNode = new QueryNode(item,0,0,pptr);
            pptr->left = leafNode;
        }
        else{

        }
    }
}

//to get relation name for relAttr even relAttr is not "rel.attr"
char* QL_Manager::getRelName(const RelAttr &relAttr, int nRelations, const char * const relations[]){
    if(relAttr.relName != NULL) return relAttr.relName;
    char* relName;
    //to search metadata about all the relations and find the attribut
        //TODO
    //_smm->
    //relAttr.attrName;
    return relName;
}

//
// void QL_PrintError(RC rc)
//
// This function will accept an Error code and output the appropriate
// error.
//
void QL_PrintError(RC rc)
{
    cout << "QL_PrintError\n   rc=" << rc << "\n";
}
