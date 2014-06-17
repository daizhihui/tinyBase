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
    QueryNode *queryNode;
    RC createQueryTree(int nSelAttrs, const RelAttr selAttrs[],
                       int nRelations, const char * const relations[],
                       int nConditions, const Condition conditions[]);    //create a query logic tree for the execution plan

};


class QueryNode{
public:
    QueryNode() : _parent(nullptr) { }
    // Construct node with one child
    QueryNode(QueryNode* n) : _parent(nullptr) { addChild(n); }
    typedef std::vector<QueryNode*>::iterator arg_iterator;
    typedef std::vector<QueryNode*>::const_iterator const_arg_iterator;
    typedef std::vector<QueryNode*>::size_type arity_type;

    void addChild(QueryNode* n);
    //void delChild(QueryNode* n);
    arity_type arity() const { return _children.size(); }
    bool is_leaf() const { return _children.empty(); }
    // Set/Get parent
    QueryNode*& parent() { return _parent; }
    const QueryNode* parent() const { return _parent; }
    // Iterator access to arguments
    arg_iterator arg_begin() { return _children.begin(); }
    const_arg_iterator arg_begin() const { return _children.begin(); }
    arg_iterator arg_end() { return _children.end(); }

    const_arg_iterator arg_end() const { return _children.end(); }
    // Erase argument (use const_arg_iterator if your compiler properly supports C++11)
    arg_iterator  arg_erase(arg_iterator i) { return _children.erase(i); }
    // Get argument
    QueryNode* arg_first() { return _children.front(); }
    QueryNode* arg_last() { return _children.back(); }
    QueryNode* arg(arity_type i) { return _children[i]; }
private:
    QueryNode* _parent;
    std::vector<QueryNode*> _children;
    void remove_parent_ptr();

};

class nodeIterator {
        const QueryNode* nptr; // pointer to node
        const QueryNode* root; // pointer to subtree root
public:
 typedef std::bidirectional_iterator_tag iterator_category;
 typedef QueryNode value_type;
 typedef QueryNode* pointer;
 typedef QueryNode& reference;
 typedef ptrdiff_t difference_type;

        // Construct iterator from pointer to node
 nodeIterator(const QueryNode* fp,const QueryNode* r) : nptr(fp),root(r) { }


        // Compare two iterators
 bool operator==(const nodeIterator& di) const { return nptr == di.nptr; }
 bool operator!=(const nodeIterator& di) const { return !(*this == di); }


        // Dereferencing

 const QueryNode& operator*() const { return *nptr;  }
 const QueryNode* operator->() const { return nptr; }
 nodeIterator& operator++();
 nodeIterator& operator--();
};
//
// Print-error function
//
void QL_PrintError(RC rc);

#endif
