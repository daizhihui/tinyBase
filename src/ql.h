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
    QueryTree *queryTree;
    RC iteratorExecution(QueryTree * tree, const PF_PageHandle&inputPageHandle, const PF_PageHandle&outputPageHandle);
    char* getRelNameForUniqueAttr(const RelAttr &relAttr, int nRelations, const char * const relations[]);
    RC createQueryTree(int nSelAttrs, const RelAttr selAttrs[],
                       int nRelations, const char * const relations[],
                       int nConditions, const Condition conditions[]);    //create a query logic tree for the execution plan

};

enum Operation{
    IdexScanSelection,
    FileScanSelection,
    Projection,
    NestedJoin,
    HashJoin,
    SortMergeJoin
};

struct InternalNodeValue{
    Operation operation;
    //int nRelations; //1 for projection or restriction or 2 for join
    const Condition **condition; //operation condition
};

struct LeafValue{
    const char * const relation; //
};

union NodeValue{
    InternalNodeValue internalValue;
    LeafValue leafValue;
};

class QueryNode{
public:
    QueryNode() : _parent(nullptr) { }
    // Construct node with one child
    QueryNode(QueryNode* n) : _parent(nullptr) { addChild(n); }

    //constructor
    QueryNode (const NodeValue& item, QueryNode *lptr = NULL,
         QueryNode*rptr = NULL, QueryNode *pptr = NULL):
       nodeValue(item), left(lptr), right(rptr), parent(pptr){
    }

private:
    QueryNode* parent;
    QueryNode* left;
    QueryNode* right;
    NodeValue nodeValue;     //content in a node
    bool isLeaf; //the leaf node contains original relations
};


class QueryTree{
    typedef const nodeIterator const_iterator;
    typedef nodeIterator iterator;
    QueryTree();
    //iterator operation
    inline const_iterator begin() const; //return an iterator
    inline const_iterator end() const; //return an iterator

private:
    QueryNode* root;
    int numLayer;
};





class nodeIterator {

public:
    // Compare two iterators
    bool operator==(const nodeIterator& di) const { return nptr == di.nptr; }
    bool operator!=(const nodeIterator& di) const { return !(*this == di); }


    // Dereferencing
    const QueryNode& operator*() const { return *nptr;  }
    const QueryNode* operator->() const { return nptr; }
    nodeIterator& operator++();
    nodeIterator& operator--();

private:
    // nptr is the current location in the tree. we can move
    // freely about the tree using left, right, and parent.
    // tree is the address of the stree object associated
    // with this iterator. it is used only to access the
    // root pointer, which is needed for ++ and --
    // when the iterator value is end()
    const QueryNode* nptr; // pointer to node
    const QueryTree* tree; // pointer to tree
    // Construct iterator from pointer to node
    nodeIterator(const QueryNode* fp,const QueryTree* t) : nptr(fp),tree(t) { }
};
//
// Print-error function
//
void QL_PrintError(RC rc);

#endif
