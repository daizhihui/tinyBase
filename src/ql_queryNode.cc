#include <cstdio>
#include <iostream>
#include <sys/times.h>
#include <sys/types.h>
#include <cassert>
#include <unistd.h>
#include "redbase.h"
#include "ql.h"

QueryTree::QueryTree(QueryNode * r):root(r){}

RC QueryTree::createQueryTree(int nSelAttrs, const RelAttr selAttrs[],
                               int nRelations, const char * const relations[],
                               int nConditions, const Condition conditions[]){
    //
    QueryNode ** nodes;
    for(int i = 0; i<nRelations; i++){
        const char * rel = relations[i];
        //get all the selection conditions about rel
        int nS;
        Condition *selCons;
        int *offset;
        int *length;
        getSelCondition(nConditions,conditions,rel,nS,&selCons,&offset,&length);

        //create a node with the relation
        NodeValue item;
        item.relation = rel;
        nodes[i] = new QueryNode(item,0,0,0) ;

        if(nS != 0){
            //create a subtree with the relation and its selection conditions
            NodeValue item1;
            item1.iterator = new filterIterator(rel,nS,&selCons,&offset,&length);
            nodes[i] = new QueryNode(item1,node[i],0,0);
        }
    }

    //reorder nRelations nodes to a tree
    queryTree->root = nodes[0];
    for(int i = 1; i<nRelations; i++){
        //get all the join conditions between relation i and relation 0 -> (i-1)
        int nConJoin;
        Condition* condsJoin; //condsJoin is a list of pointers to the conditions
        getJoinConditions(i,nConditions,conditions,nConJoin,&condsJoin,&offset,&length);

        //create a new root node with old root and node[i] as children
        queryTree->root = new QueryNode();
        queryTree->root->left = queryTree->root;
        queryTree->root->right = node[i];
        //define nodeValue for the root
        NodeValue item2;
        item2.iterator = new nested_loops_join(
                    );
        queryTree->root->nodeValue = item2;
    }


}
