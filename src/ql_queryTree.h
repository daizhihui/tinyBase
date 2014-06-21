#ifndef QL_QUERYTREE_H
#define QL_QUERYTREE_H
#include "redbase.h"
#include "ql_iterator.h"
enum RelSpec {outer, inner};

struct FldSpec {
    RelSpec relation;
    int offset;
};


enum Operation{
    IndexScanSelection,
    FileScanSelection,
    Projection,
    Join
    //HashJoin,
    //SortMergeJoin
};

struct InternalNodeValue{
    Iterator * iterator;
//    Operation operation;
//    //int nRelations; //1 for projection or restriction or 2 for join
//    const Condition **condition; //operation condition
};

struct LeafValue{
    const char * const relation; //
};

union NodeValue{
    Iterator * iterator;
    const char * const relation;
};


class QueryNode{
    friend class QL_Manager;
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
//    bool isLeaf; //the leaf node contains original relations
//    bool isRoot;
};


class QueryTree{
    friend class QL_Manager;
    QueryTree(QueryTree *r);
    RC createQueryTree(int nSelAttrs, const RelAttr selAttrs[],
                       int nRelations, const char * const relations[],
                       int nConditions, const Condition conditions[]);    //create a query logic tree for the execution plan
private:
    QueryNode* root;

    //int numLayer;
};
#endif // QL_QUERYTREE_H
