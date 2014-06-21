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
#include "printer.h"

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
        RC rc;
    // define de RM_FileScan
    RM_FileScan filescan;
    RM_Record rec;
    if((rc=filescan.OpenScan(_smm->fileHandle_Relcat, INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    // check if exists the same attribute name in relcat
    char * data;
    int attr_count=0;
    int tuplelength=0;
    bool flag_rel_exist=false;
    //scan all the records in relcat
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF) return (rc);
        if(rc!=RM_EOF){
            //copy record to relation
            if((rc=rec.GetData(data))) return (rc);
            
            //compare two strings: strcmp (equal return 0)
            if(!strcmp(((Relation*)data)->relName,relName)) {
                flag_rel_exist=true;
                attr_count=((Relation*)data)->attrCount;
                tuplelength=((Relation*)data)->tupleLength;
                break;
            }
        }
    }

    if((rc=filescan.CloseScan())) return (rc);
    
    if(attr_count!=nValues) return(QL_INVALID_ATTR_NUM);
    DataAttrInfo attributes[MAXATTRS];
    int k=0;
    
    if((rc=filescan.OpenScan(_smm->fileHandle_Attrcat, INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    // check if exists the same attribute name in relcat

    //scan all the records in relcat
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF) return (rc);
        if(rc!=RM_EOF){
            //copy record to relation
            if((rc=rec.GetData(data))) return (rc);
            
            //compare two strings: strcmp (equal return 0)
            if(!strcmp(((DataAttrInfo*)data)->relName,relName)) {
                memcpy(&attributes[k],data,sizeof(DataAttrInfo));
                k++;
            }
        }
    }
    
    if((rc=filescan.CloseScan())) return (rc);
    
    char record_data[tuplelength];
    
    for (int s=0; s<nValues; s++) {
        
        switch (attributes[s].attrType) {
                
            case INT:{
                if(values[k].type!=attributes[s].attrType) return (QL_INVALID_ATTR_TYPE);
                int data_int=*((int*)(values[s].data));
                *((int*)(record_data+attributes[s].offset)) = data_int;
                break;
            }
            case FLOAT:{
                if(values[k].type!=attributes[s].attrType) return (QL_INVALID_ATTR_TYPE);
                int data_float=*((float*)(values[s].data));
                *((float*)(record_data+attributes[s].offset)) = data_float;
                break;
            }
                
            case STRING:{
                if(values[k].type!=attributes[s].attrType) return (QL_INVALID_ATTR_TYPE);
                char data_string[attributes[s].attrLength];
                memcpy(data_string,values[s].data,attributes[k].attrLength);
                memcpy(record_data+attributes[s].offset,data_string,attributes[k].attrLength);
                break;
            }
            default:
                // Test: wrong _attrType
                return (QL_INVALID_ATTR_TYPE);
        }

    }
    
    RM_FileHandle filehandle_r;
    
    // open relation file to store records read from file
    if((rc=_rmm->OpenFile(relName, filehandle_r))) return (rc);
    RID rid;
    if((rc=filehandle_r.InsertRec(record_data, rid))) return (rc);
    
    //close the relation file
    if((rc=_rmm->CloseFile(filehandle_r))) return (rc);
    
    //insert index if index file exists

    for (int k=0;k<attr_count;k++) {
        if(attributes[k].indexNo!=-1){
            // Call IX_IndexHandle::OpenIndex to open the index file
            IX_IndexHandle indexhandle;
            if((rc=_ixm->OpenIndex(relName, attributes[k].indexNo, indexhandle))) return (rc);
            char data_index[attributes[k].attrLength];
            memcpy(data_index,record_data+attributes[k].offset,attributes[k].attrLength);
            if((rc=indexhandle.InsertEntry(data_index, rid))) return (rc);
            //close index file
            if(_ixm->CloseIndex(indexhandle)) return (rc);
            
        }
    }

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
    //
    QueryNode * nodes = new QueryNode[nRelations];
    for(int i = 0; i<nRelations; i++){
        char * rel = relations[i];
        //get all the selection conditions about rel
        int nConReturn;
        Condition conditions[nConReturn];
        //getSelCondition
        if(nConReturn == 0){
        //create a node with the relation
            nodes[i] = ;
        }
        else{
            //create a subtree with the relation and its selection conditions
            nodes[i] = ;
        }
    }

    //reorder nRelations nodes to a tree
    queryTree->root = nodes[0];
    for(int i = 1; i<nRelations; i++){
        //get all the join conditions between relation i and relation 0 -> (i-1)
        int nConJoin;
        const Condition* condsJoin[nConJoin]; //condsJoin is a list of pointers to the conditions
        //TODO


        //create a new root node with old root and node[i] as children
        queryTree->root = new QueryNode();
        queryTree->root->left = queryTree->root;
        queryTree->root->right = node[i];
        //define nodeValue for the root
        NodeValue item;
        InternalNodeValue iv;
        iv.condition = condsJoin; //iv.condition points to an array of conditions
        iv.operation = NestedJoin; //if change algo, need change the value
        item.internalValue =iv;
        queryTree->root->nodeValue = item;
    }

//    //at the end, do projection
//    //create a new root node, who has only one child
//    //define nodeValue for the root
//    NodeValue item;
//    InternalNodeValue iv;
//    iv.condition = condsJoin; //iv.condition points to an array of conditions
//    iv.operation = Projection; //if change algo, need change the value
//    item.internalValue =iv;
//    queryTree->root->nodeValue = item;

}

RC QL_Manager::runQuery(){
    //create input page
    PF_FileHandle fileHandle;
    PF_PageHandle inputPageHandle;
    fileHandle.AllocatePage(inputPageHandle);

    //create output page
    PF_PageHandle outputPageHandle;
    fileHandle.AllocatePage(outputPageHandle);

    iteratorExecution(this,inputPageHandle,outputPageHandle)
}

RC QL_Manager::iteratorExecution(QueryTree * tree, const PF_PageHandle&inputPageHandle, const PF_PageHandle&outputPageHandle){
    char * inputData;
    inputPageHandle.GetData(inputData);

    char * outputData;
    outputPageHandle.GetData(outputData);

    //iterator to get inputData
    PF_FileHandle fileHandle;
    PF_PageHandle inputPageHandle1;
    fileHandle.AllocatePage(inputPageHandle1);

    //iterator to the subtree
    QueryTree * subTree = new QueryTree();
    subTree->root = tree->root->left;
    //input of this iteration is output of next iteration
    iteratorExecution(subTree,inputPageHandle1,inputPageHandle);

    //do query execution in this iteration

}


void getSelCondition(int nConditions, const Condition conditions[], const char * rel,
                     int &nConReturn, int &conNum[]){



}


//RC QL_Manager::createQueryTree(int nSelAttrs, const RelAttr selAttrs[],
//                               int nRelations, const char * const relations[],
//                               int nConditions, const Condition conditions[]){
//    int i;
//    char * relationL;
//    //if rhs is a value, it's restriction, must be done before join, as the most left node
//    for (i = 0; i < nConditions; i++){
//        if(!conditions[i].bRhsIsAttr) {
//            relationL = getRelNameForUniqueAttr(conditions[i].lhsAttr,nRelations,relations[]);
//            //verify if this relation has index on attribut
//            //TODO
//            bool isLeftIndexed = true;
//             //is a index scan selection
//            if(isLeftIndexed){
//                //define an internal node
//                NodeValue item;
//                InternalNodeValue iv;
//                iv.condition = &conditions[i];
//                iv.operation = IdexScanSelection;
//                item.internalValue =iv;
//                QueryNode *pptr = new QueryNode(item,0,0,0);
//                //define a leaf node
//                LeafValue leaf;
//                leaf.relation = relationL;
//                item.leafValue =leaf;
//                QueryNode * leafNode = new QueryNode(item,0,0,pptr);
//                pptr->left = leafNode;
//                queryTree = new QueryTree(pptr); //initialiser the tree
//                queryTree->numLayer = 2;
//                break;
//            }
//        }
//    }
//    //there is not index scan selection
//    if(i==nConditions) {
//        relationL = getRelNameForUniqueAttr(conditions[0].lhsAttr,nRelations,relations[]);
//        char * relationR = getRelNameForUniqueAttr(conditions[0].rhsAttr,nRelations,relations[]);
//        //verify if this relation has index on attribut
//        //TODO
//        bool isLeftIndexed = false;
//        bool isRightIndexed = true;

//        NodeValue item;
//        InternalNodeValue iv;
//        iv.condition = &conditions[0];
//        iv.operation = IdexScanSelection;
//        item.internalValue =iv;
//        QueryNode *pptr = new QueryNode(item,0,0,0);
//        //define a leaf node
//        LeafValue leaf;
//        leaf.relation = relationL;
//        item.leafValue =leaf;
//        QueryNode * leafNode = new QueryNode(item,0,0,pptr);
//        pptr->left = leafNode;
//        queryTree = new QueryTree(pptr); //initialiser the tree

//    }


//}

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
