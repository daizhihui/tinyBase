#ifndef QL_QUERYTREE_H
#define QL_QUERYTREE_H
#include "redbase.h"
#include "ql.h"
#include "ql_iterator.h"
#include "ql_conditionconvertion.h"
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
    const char * relation;
};


class QueryNode{
    friend class QL_Manager;
    friend class QueryTree;
public:
    QueryNode() : parent(nullptr) { }

    //constructor
    QueryNode (const NodeValue& item, QueryNode *lptr = NULL,
         QueryNode*rptr = NULL, QueryNode *pptr = NULL):
       nodeValue(item), left(lptr), right(rptr), parent(pptr){
    }

    //constructor for inner node
    QueryNode (Iterator* ite, QueryNode *lptr = NULL,
         QueryNode*rptr = NULL, QueryNode *pptr = NULL):
        left(lptr), right(rptr), parent(pptr){
        this->nodeValue.iterator = ite;
        //cout << "construtorIterator" << endl;

    }

    //constructor for relation node
    QueryNode (const char * rel, QueryNode *lptr = NULL,
         QueryNode*rptr = NULL, QueryNode *pptr = NULL):
        left(lptr), right(rptr), parent(pptr){

        this->nodeValue.relation = rel;
        //cout << "construtor" << endl;
    }

private:
    QueryNode* parent;
    QueryNode* left;
    QueryNode* right;
    NodeValue nodeValue;     //content in a node
};


class QueryTree{
    friend class QL_Manager;
    QueryTree(QueryNode *r, ql_conditionConvertion * pcc);
    RC createQueryTree(int nSelAttrs, const RelAttr selAttrs[],
                       int nRelations, const char * const relations[],
                       int nConditions, const Condition conditions[]);    //create a query logic tree for the execution plan
private:
    QueryNode* root;
    ql_conditionConvertion *pcc; //indicate the ptr of ql_manager


    //int numLayer;
};
#endif // QL_QUERYTREE_H
