#include <cstdio>
#include <iostream>
#include <sys/times.h>
#include <sys/types.h>
#include <cassert>
#include <unistd.h>
#include "redbase.h"
#include "ql_queryTree.h"

QueryTree::QueryTree(QueryNode * r,ql_conditionConvertion *pconverter):root(r), pcc(pconverter){}

RC QueryTree::createQueryTree(int nSelAttrs, const RelAttr selAttrs[],
                              int nRelations, const char * const relations[],
                              int nConditions, const Condition conditions[]){
    //
    cout << "create tree" << endl;
    QueryNode * nodes[nRelations];
    for(int i = 0; i<nRelations; i++){
        const char * rel = relations[i];
        //get all the selection conditions about rel
        int nS;
        const Condition *selCons[MAXATTRS];
        int offset[MAXATTRS];
        int length[MAXATTRS];
        pcc->getSelCondition(nConditions,conditions,rel,nS,selCons,offset,length);

        //create a node with the relation
        nodes[i] = new QueryNode(rel,0,0,0);
        if(nS != 0){
            //create a subtree with the relation and its selection conditions
            Iterator * it = new filterIterator(rel,nS,selCons,length,offset);
            QueryNode *nNode = new QueryNode(it,nodes[i],0,0);
            nodes[i]->parent = nNode;
            nodes[i] = nNode;
        }
        root = nodes[0];
//        RM_Record *r = NULL;
//        root->nodeValue.iterator->get_next(r);


        /*
     //reorder nRelations nodes to a tree
    for(int i = 1; i<nRelations; i++){
        //get all the join conditions between relation i and relation 0 -> (i-1)
        int nConJoin;
        Condition* condsJoin; //condsJoin is a list of pointers to the conditions
        pcc->getJoinConditions(i,nConditions,conditions,nConJoin,&condsJoin,&offset,&length);

        //create a new root node with old root and node[i] as children
        this->root = new QueryNode();
        this->root->left = queryTree->root;
        this->root->right = node[i];
        //define nodeValue for the root
        NodeValue item2;
        item2.iterator = new nested_loops_join(
                    );
        this->root->nodeValue = item2;
    }*/


    }
}
