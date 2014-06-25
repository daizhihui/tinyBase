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
#include "ql_internal.h"
#include "sm.h"
#include "ix.h"
#include "rm.h"
#include "ql_queryTree.h"
#include "ql_conditionconvertion.h"

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

    //define a printer
    Printer *p;

    //define a convertion of conditions
    ql_conditionConvertion pcc(this->_smm);

    int nRelAttrs = 0;
    //consider situdation : select * from where nSelAttrs = 1 selAttrs[0]:NULL.*
    if(nSelAttrs==1 && selAttrs[0].relName==NULL&&(!strcmp(selAttrs[0].attrName,"*"))){
        //show all the attributs of all the relations
        nRelAttrs = 0;
        DataAttrInfo attributes[MAXATTRS];
        for(int j=0;j<nRelations;j++){
            DataAttrInfo l_attributes[MAXATTRS];
            int l_nR=0;
            pcc.getDataAttributsByRelation(relations[j],l_attributes,l_nR);
            memcpy(attributes+nRelAttrs,l_attributes,l_nR*sizeof(DataAttrInfo));
            nRelAttrs+=l_nR;
        }

        for (int i=0;i<nRelAttrs;i++){
            //get attribut length
            DataAttrInfo dataAttr;
            dataAttr = attributes[i];
            strcpy(attributes[i].relName,"table_OUT");
            strcpy(attributes[i].attrName,dataAttr.attrName);
            //calculate offset
            int offset=0;
            for (int j=0;j<i;j++){
                offset+= attributes[j].attrLength;
            }
            attributes[i].offset = offset;
            attributes[i].attrType= dataAttr.attrType;
            attributes[i].attrLength= dataAttr.attrLength;
            attributes[i].indexNo=-1;
        }

        p = new Printer(attributes,nRelAttrs);
    }
    else{
        DataAttrInfo attributes[nSelAttrs];
        //DataAttrInfo records
        for (int i=0;i<nSelAttrs;i++){
            //get complete relation-attribut
            const char * relName = pcc.getRelName(selAttrs[i],nRelations,relations);
            char rN[MAXNAME];
            strcpy(rN,relName);
            RelAttr realRA = {rN,selAttrs[i].attrName};
            DataAttrInfo dataAttr;
            pcc.getDataAttributByRelAttr(realRA,dataAttr);
            strcpy(attributes[i].relName,"table_OUT");
            strcpy(attributes[i].attrName,selAttrs[i].attrName);
            //calculate offset
            int offset=0;
            for (int j=0;j<i;j++){
                offset+= attributes[j].attrLength;
            }
            attributes[i].offset = offset;
            attributes[i].attrType= dataAttr.attrType;
            attributes[i].attrLength= dataAttr.attrLength;
            attributes[i].indexNo=-1;
        }
        p = new Printer(attributes,nSelAttrs);
    }

    p->PrintHeader(cout);
    QueryTree *queryTree = new QueryTree(NULL,&pcc);
    queryTree->createQueryTree(nSelAttrs, selAttrs,nRelations, relations,nConditions, conditions);

    char* output;
    cout << "tree created" << endl;
    if(queryTree->root!=NULL) cout << "tree not empty" << endl;
    while(queryTree->root->nodeValue.iterator->get_next(output)!=endOfIteration){
        //TODO restructure the output to char* a[]
       p->Print(cout,output);
    }
    p->PrintFooter(cout);

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



RC QL_Manager::SelectPlan0(int   nSelAttrs,
               const RelAttr selAttrs[],
               int   nRelations,
               const char * const relations[],
               int   nConditions,
               const Condition conditions[],
               Condition &alwaysMatch,
               RM_FileHandle **pRmfhs,
               QL_Operator *&root)
{
    //Joins then filters
    //all tables have at least one join attribute between them
    int i =0;
    QL_Operator* leftSide = new QL_TblScanOp();;//always is left side
    while(i<nRelations)
    {//do joins left sided tree
        QL_TblScanOp* R2 = new QL_TblScanOp();//with 1 condition
        QL_NLJOp* NLJOP = new QL_NLJOp(QL_NLJOp(leftSide, R2, const Condition &, SM_Manager *);//do join on 1 attribute
        QL_Operator* tmp = NLJOP;
        //do filter for all remaining join attributes
        for(int jA = 0; jA<nbrOfJoinAttributes;jA++)
        {
            QL_FilterOp * fOP = new QL_FilterOp(tmp,..,);
            tmp=fOP;
        }
        leftSide=NLJOP;
        //join done
      
    }
    //all joins are done.
    //add filters:
   for(int filter=0;filter<numberOfRemainingFilters;fitler++)
   {
       QL_FilterOp * fOP = new QL_FilterOp(leftSide,..,);
       leftSide = fOP;
   }
   root = leftSide;
    
}
RC QL_Manager::SelectPlan1(int   nSelAttrs,
                           const RelAttr selAttrs[],
                           int   nRelations,
                           const char * const relations[],
                           int   nConditions,
                           const Condition conditions[],
                           Condition &alwaysMatch,
                           RM_FileHandle **pRmfhs,
                           QL_Operator *&root)
{
    /*
     
     RC QL_Manager::Select(int nSelAttrs, const RelAttr selAttrs[],
     int nRelations, const char * const relations[],
     int nConditions, const Condition conditions[])
     */
    
    //filters first then join
    
    
}
RC QL_Manager::SelectPlan2(int   nSelAttrs,
                           const RelAttr selAttrs[],
                           int   nRelations,
                           const char * const relations[],
                           int   nConditions,
                           const Condition conditions[],
                           Condition &alwaysMatch,
                           RM_FileHandle **pRmfhs,
                           QL_Operator *&root)
{
    /*
     
     RC QL_Manager::Select(int nSelAttrs, const RelAttr selAttrs[],
     int nRelations, const char * const relations[],
     int nConditions, const Condition conditions[])
     */
        
    
}

RC QL_Manager::SelectPrinter(DataAttrInfo *&attributes,
                 int nSelAttrs, const RelAttr selAttrs[],
                 int nRelations, const char * const relations[],
                 QL_Operator *root, int nTotalAttrs){

}

RC QL_Manager::DeleteUpdatePlan(const char *relName,
                    const char *pdAttrName,
                    int   nConditions,
                    const Condition conditions[],
                    Condition &alwaysMatch,
                    RM_FileHandle &rmfh,
                    QL_Operator *&root){}


RC QL_Manager::getSelectConditionsByRelation(const char* relationName,int   nConditions,
                                 const Condition conditions[],RM_FileHandle **pRmfhs,int   &nResultConditions,
                                 int* resultIndexConditions){
    for(int i=0; i< nConditions; i++){
        if(conditions[i].bRhsIsAttr!=1){
            if(!strcmp(conditions[i].lhsAttr.relName,relationName)){
                nResultConditions++;
                resultIndexConditions[nResultConditions] = i;
            }
        }
    }
}

RC QL_Manager::getJoinConditionsByRelation(const char * const relations[],
                                           int   nRelations,
                                           const char* relationName,int   nConditions,
                                 const Condition conditions[],RM_FileHandle **pRmfhs,int   &nResultConditions,
                                 int* resultIndexConditions, int   &nResultRelations, int * resultIndexRelations){
    nResultRelations = 0;
    for(int i=0; i< nConditions; i++){
        if(conditions[i].bRhsIsAttr==1){
            const char * rightRelationName = conditions[i].rhsAttr.relName;
            const char * leftRelationName = conditions[i].lhsAttr.relName;

            if(!strcmp(leftRelationName,relationName)){
                nResultConditions++;
                resultIndexConditions[nResultConditions] = i;
                for(int j=0; j< nRelations; j++){
                    if(!strcmp(rightRelationName,relations[j])) {
                        resultIndexRelations[nResultRelations] = j;
                        nResultRelations++;
                    }
                }
            }
            else if(!strcmp(rightRelationName,relationName)){
                nResultConditions++;
                resultIndexConditions[nResultConditions] = i;
                for(int j=0; j< nRelations; j++){
                    if(!strcmp(leftRelationName,relations[j])) {
                        resultIndexRelations[nResultRelations] = j;
                        nResultRelations++;
                    }
                }
            }
        }
    }
}


RC QL_Manager::getJoinConditions(const char * const relations[],
                                 int   nRelations,int   nConditions,
                                 const Condition conditions[],
                                 RM_FileHandle **pRmfhs,int   &nResultConditions[],
                                 int* resultIndexConditions[],int &numRightRelation[]){

    bool relationsUsed[nRelations];
    bool conditionsUsed[nConditions];
    for(int i=0; i<nRelations; i++){
        relationsUsed[i] = false;
    }
    for(int i=0; i<nConditions; i++){
        conditionsUsed[i] = false;
    }

    for(int i = 1; i<nRelations; i++){
        //search in the rested relations
        for(int j = i ; j< nRelations; j++)
        {
            int numResult;
            getRelation(relationsUsed,nRelation,j,numResult);
            //compare to find if this relation has conditions with existing relations
            for(int m=0; m< nConditions; m++){
                if(!conditionsUsed[m]) {
                    //get all the JoinRelations with this relation  and all the conditions about this relation
                    int result[nConditions];
                    int nResult = 0;
                    int nRelations = 0 ;
                    int joinRelations[nRelations];
                    //get all the conditions of this relation
                    getJoinConditionsByRelation(relations,nRelations,relations[numResult],
                                                nConditions,conditions,pRmfhs,nResult,result,nRelations,joinRelations);
                    int n=0;
                    while(!relationsUsed(joinRelations[n])) n++; //if the join relation is not used
                    if(n<=nRelations){
                        relationsUsed[numResult] = true;
                        numRightRelation[i] = numResult;

                        for(int h = 0; h < nResult; h++){
                            //find all the conditions between this relation and all existing relations
                            if(!strcmp(conditions[result[h]].lhsAttr,relations[numResult])
                                    ||!strcmp(conditions[result[h]].rhsAttr,relations[numResult])){
                                resultIndexConditions[i-1][nResultConditions] = result[h];
                                nResultConditions++;
                            }
                        }
                    }
                }
            }
        }
    }
    return 0;
}


//get the i ere relation in the unused relations
RC QL_Manager::getRelationByBoolMap(bool *used, int nRelation, int i, int &numResult){
    int nbr = 0;
    for(int i=0; i<nRelation; i++){
        if(!used[i]) nbr++;
        if(nbr == i) {
            numResult = i;
            break;
        }
    }
    return 0;
}

RC QL_Manager::printResultSelection(){

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
