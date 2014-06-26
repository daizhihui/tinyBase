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
#include "sm_internal.h"
#define QL_SELECT_DEBUG 1

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
    Condition alwaysMatch;
    RM_FileHandle **pRmfhs;
    QL_Operator *root;
    this->SelectPlan0(nSelAttrs,selAttrs,nRelations,relations, nConditions, conditions, alwaysMatch, pRmfhs, root
                );
    DataAttrInfo *attrInfo;
    int nTotalAttrs = 0;
    SelectPrinter(attrInfo,nSelAttrs,selAttrs,nRelations,relations, root, nTotalAttrs);
    //print the operator plan
    root->Print(cout);

    return 0;
}

//
// Insert the values into relName
//
RC QL_Manager::Insert(const char *relName,
                      int nValues, const Value values[])
{
    #ifndef QL_SELECT_DEBUG
    RC rc;
    RM_Record tmpRec;
    char *relcatData;
    char _relName[MAXNAME];
    SM_AttrcatRec *attributes;
    IX_IndexHandle *ihs;
    RM_FileHandle fh;
    RM_FileScan fs;
    RM_Record rec;
    char *data;
    FILE *fp;
    char *buf;
    int i = 0;
    int tupleCount = 0;
    
    // Sanity Check: relName should not be RELCAT or ATTRCAT
    if (strcmp(relName, RELCAT) == 0 || strcmp(relName, ATTRCAT) == 0) {
        rc = SM_INVALIDRELNAME;
        goto err_return;
    }
    
    // Get the attribute count
    if ((rc = GetRelationInfo(relName, tmpRec, relcatData)))
    goto err_return;
    
    // Allocate indexhandle array
    ihs = new IX_IndexHandle[((SM_RelcatRec *)relcatData)->attrCount];
    if (ihs == NULL) {
        rc = SM_NOMEM;
        goto err_return;
    }
    
    // Allocate attributes array
    attributes = new SM_AttrcatRec[((SM_RelcatRec *)relcatData)->attrCount];
    if (attributes == NULL) {
        rc = SM_NOMEM;
        goto err_deleteihs;
    }
    
    // Allocate buffer
    buf = new char[MAXLINE];
    if (buf == NULL) {
        rc = SM_NOMEM;
        goto err_deleteattributes;
    }
    
    // Allocate data
    data = new char[((SM_RelcatRec *)relcatData)->tupleLength];
    if (data == NULL) {
        rc = SM_NOMEM;
        goto err_deletebuf;
    }
    
    // Open a file scan for ATTRCAT
    memset(_relName, '\0', sizeof(_relName));
    strncpy(_relName, relName, MAXNAME);
    if ((rc = fs.OpenScan(fhAttrcat, STRING, MAXNAME,
                          OFFSET(SM_AttrcatRec, relName), EQ_OP, _relName)))
    goto err_deletedata;
    
    // Fill out attributes array
    while ((rc = fs.GetNextRec(rec)) != RM_EOF) {
        char *_data;
        
        if (rc != 0) {
            fs.CloseScan();
            goto err_deletedata;
        }
        if ((rc = rec.GetData(_data))) {
            fs.CloseScan();
            goto err_deletedata;
        }
        
        memcpy(&attributes[i], _data, sizeof(SM_AttrcatRec));
        if (++i == ((SM_RelcatRec *)relcatData)->attrCount)
        break;
    }
    
    // Close a file scan for ATTRCAT
    if ((rc = fs.CloseScan()))
    goto err_deletedata;
    
    qsort(attributes, ((SM_RelcatRec *)relcatData)->attrCount,
          sizeof(SM_AttrcatRec), compareSMAttrcatRec);
    
    
    // Open relation file
    if ((rc = pRmm->OpenFile(relName, fh)))
    goto err_fclose;
    
    // Open indexes
    for (i = 0; i < ((SM_RelcatRec *)relcatData)->attrCount; i++) {
        if (attributes[i].indexNo == -1)
        continue;
        if ((rc = pIxm->OpenIndex(relName, attributes[i].indexNo, ihs[i])))
        goto err_closeindexes;
    }
    
        // Make record data
        for (i = 0; i < ((SM_RelcatRec *)relcatData)->attrCount; i++) {
            int _i;
            float _f;
            char *delim = strpbrk(attr, ",\n");
            *delim = '\0';
            
            switch (attributes[i].attrType) {
                case INT:
                if(values[i].type!=attributes[i].attrType) return (QL_INVALID_ATTR_TYPE);
                _i = i*((int*)(values[i].data));
                memcpy(data + attributes[i].offset, &values[i], sizeof(int));
                break;
                case FLOAT:
                _f = atof(attr);
                memcpy(data + attributes[i].offset, &_f, sizeof(float));
                break;
                case STRING:
                memset(data + attributes[i].offset, '\0',
                       attributes[i].attrLength);
                strncpy(data + attributes[i].offset, attr,
                        attributes[i].attrLength);
                break;
            }
            attr = delim + 1;
        }
        
        // Insert the record
        if ((rc = fh.InsertRec(data, rid)))
        goto err_closeindexes;
        
        tupleCount++;
        
        // Update indexes
        for (i = 0; i < ((SM_RelcatRec *)relcatData)->attrCount; i++) {
            if (attributes[i].indexNo == -1)
            continue;
            if ((rc = ihs[i].InsertEntry(data + attributes[i].offset, rid)))
            goto err_closeindexes;
        }
    }
    
    // Update index statistics
    for (i = 0; i < ((SM_RelcatRec *)relcatData)->attrCount; i++) {
        char *attrcatData;
        
        if (attributes[i].indexNo == -1)
        continue;
        
        if ((rc = GetAttributeInfo(relName, attributes[i].attrName,
                                   rec, attrcatData)))
        goto err_closeindexes;
        
        ((SM_AttrcatRec *)attrcatData)->distinctVals += ihs[i].deltaDistinctVals();
        ((SM_AttrcatRec *)attrcatData)->ixDepth      += ihs[i].deltaDepth();
        if ((rc = ihs[i].GetMinKey(((SM_AttrcatRec *)attrcatData)->minValue)))
        goto err_closeindexes;
        if ((rc = ihs[i].GetMaxKey(((SM_AttrcatRec *)attrcatData)->maxValue)))
        goto err_closeindexes;
        if ((rc = fhAttrcat.UpdateRec(rec)))
        goto err_closeindexes;
    }
    if ((rc = fhAttrcat.ForcePages()))
    goto err_closeindexes;
    
    // Close indexes
    for (i = 0; i < ((SM_RelcatRec *)relcatData)->attrCount; i++) {
        if (attributes[i].indexNo == -1)
        continue;
        if ((rc = pIxm->CloseIndex(ihs[i])))
        goto err_closeindexes;
    }
    
    // Update the cardinality
    if (tupleCount)
    if ((rc = SetRelationInfo(relName, tupleCount, fh.numPages())))
    goto err_fclose;
    
    // Close relation file
    if ((rc = pRmm->CloseFile(fh)))
    goto err_fclose;
    
    // Close data file
    fclose(fp);
    
    // Deallocate
    delete [] data;
    delete [] buf;
    delete [] attributes;
    delete [] ihs;
    
    cout << "Insert\n";
    
    cout << "   relName = " << relName << "\n";
    cout << "   nValues = " << nValues << "\n";
    for (i = 0; i < nValues; i++)
    cout << "   values[" << i << "]:" << values[i] << "\n";
    
    // Return ok
    return (0);
    
    // Return error
err_closeindexes:
    for (i = 0; i < ((SM_RelcatRec *)relcatData)->attrCount; i++)
    if (attributes[i].indexNo != -1)
    pIxm->CloseIndex(ihs[i]);
    if (tupleCount)
    SetRelationInfo(relName, tupleCount, fh.numPages());
    //err_closefile:
    pRmm->CloseFile(fh);
err_fclose:
    fclose(fp);
err_deletedata:
    delete [] data;
err_deletebuf:
    delete [] buf;
err_deleteattributes:
    delete [] attributes;
err_deleteihs:
    delete [] ihs;
err_return:
    return (rc);
#endif
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
    //non indexed
    //filter then joins always.
    //all tables have at least one join attribute between them
    
    //get all filters to apply on first relation
    int * conditionArr;
    int numberOfResultConditions;
    getSelectConditionsByRelation(relations[0],nConditions,conditions,pRmfhs,&numberOfResultConditions,conditionArr);
    QL_Operator* leftSide = new QL_TblScanOp(relations[0],pRmfhs[0],conditionArr[0],pSmm);//always is left side
    //do filters on first relation
    for(int sC = 1;sC<numberOfResultConditions;sC++)
    {
        QL_FilterOp * fOP = new QL_FilterOp(leftSide,conditionArr[sC],pSmm);
        leftSide=fOp;
    }
    //left side is now filtered
    int* nResultCond = new int[nConditions];
    int** resultIndexConditions= new int*[nConditions];
    for(int i =0;i<nConditions;i++)
    {
        resultIndexConditions[i]=new int[nConditions];
    }
    int* rightRelationIndexes = new int[nRelations-1];
    getJoinConditions(relations,nRelations,nConditions,conditions,pRmfhs,nResultCond,resultIndexConditions,rightRelationIndexes);
    int i =0;
    //do joins as left sided tree
    while(i<nRelations)
    {
        //get join conditions, and right relation
        int * joinCondArr = resultindexCondtions[i];
        int numberOfJoinResultConditions=nResultCond[i];
        int rightRelationIndex=rightRelationIndexes[i];
        
        //get condition filters on right table
        int * conditionArrR2;
        int numberOfResultConditionsR2;
        getSelectConditionsByRelation(relations[rightRelationIndex],nConditions,conditions,pRmfhs,&numberOfResultConditionsR2,conditionArrR2);
        
        QL_TblScanOp* R2 = new QL_TblScanOp(relations[rightRelationIndex],pRmfhs[rightRelationIndex],conditionArrR2[0],pSmm);//with 1 condition
        
        //do remaining filters on right table R2
        for(int sC = 1;sC<numberOfResultConditionsR2;sC++)
        {
            QL_FilterOp * fOP = new QL_FilterOp(R2,conditionArrR2[sC],pSmm);
            R2=fOp;
        }
        
        //do nested loop join left side with filtered R2
        QL_NLJOp* NLJOP = new QL_NLJOp(leftSide, R2, joinCondArr[0], pSmm);//do join on 1 attribute
        //do filter for all remaining join attributes
        for(int jA = 1; jA<numberOfJoinResultConditions;jA++)
        {
            QL_FilterOp * fOP = new QL_FilterOp(NJLOP,joinCondArr[jA],pSmm);
            NJLOP=fOP;
        }
        leftSide=NLJOP;
        //join done
        i++;
    }
    root = leftSide;
    delete [] nResultCond;
    delete []rightRelationIndexes;
    for(int i=0;i<nConditions;i++)
    {
        delete[] resultIndexConditions[i];
    }
    delete [] resultIndexConditions;
    
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
    #ifndef QL_SELECT_DEBUG
   //indexed, filter then join
    //If index on restriction attribute AND on join attribute, we will chose restriction attribute index.
    //SelectPlan2 will do the other way around.
    //
    //filter then joins always.
    //all tables have at least one join attribute between them
    
    //get all filters to apply on first relation
    int * conditionArr;
    int numberOfResultConditions;
    getSelectConditionsByRelation(relations[0],nConditions,conditions,pRmfhs,&numberOfResultConditions,conditionArr);
    QL_Operator* leftSide;

    //find if it has an index.
    int indexNo;
//for all conditions not join
    if((indexNo=attributeHasIndex())==-1)
    {
        //no index;
        leftSide = new QL_TblScanOp(relations[0],pRmfhs[0],conditionArr[0],pSmm);//always is left side
    }
    else
    {
        leftSide = new QL_IxScanOp(cond.attr.tableName,cond,pIxm,pSmm);
    }
//end for
    
    //do filters on first relation
    for(int sC = 1;sC<numberOfResultConditions;sC++)
    {
        QL_FilterOp * fOP = new QL_FilterOp(leftSide,conditionArr[sC],pSmm);
        leftSide=fOp;
    }
    //left side is now filtered
    
    int i =0;
    //do joins as left sided tree
    while(i<nRelations)
    {
        //get join conditions, and right relation
        int * joinCondArr;
        int numberOfJoinResultConditions;
        int rightRelationIndex;
        getJoinConditions(nRelations, relations, nConditions, conditions, i, pRmfhs
                          , &numberOfResultConditions,joinCondArr,&rightRelationIndex));
        
        //get condition filters on right table
        int * conditionArrR2;
        int numberOfResultConditionsR2;
        getSelectConditionsByRelation(relations[rightRelationIndex],nConditions,conditions,pRmfhs,&numberOfResultConditionsR2,conditionArrR2);
        
        QL_TblScanOp* R2 = new QL_TblScanOp(relations[rightRelationIndex],pRmfhs[rightRelationIndex],conditionArrR2[0],pSmm);//with 1 condition
        
        //do remaining filters on right table R2
        for(int sC = 1;sC<numberOfResultConditionsR2;sC++)
        {
            QL_FilterOp * fOP = new QL_FilterOp(R2,conditionArrR2[sC],pSmm);
            R2=fOp;
        }
        
        //do nested loop join left side with filtered R2
        QL_NLJOp* NLJOP = new QL_NLJOp(leftSide, R2, joinCondArr[0], pSmm);//do join on 1 attribute
        //do filter for all remaining join attributes
        for(int jA = 1; jA<numberOfJoinResultConditions;jA++)
        {
            QL_FilterOp * fOP = new QL_FilterOp(NJLOP,joinCondArr[jA],pSmm);
            NJLOP=fOP;
        }
        leftSide=NLJOP;
        //join done
        i++;
    }
    root = leftSide;
    
#endif
    return 0;
    
    
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
     return 0;
    
}



RC QL_Manager::DeleteUpdatePlan(const char *relName,
                    const char *pdAttrName,
                    int   nConditions,
                    const Condition conditions[],
                    Condition &alwaysMatch,
                    RM_FileHandle &rmfh,
                    QL_Operator *&root){
    return 0;
}


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
    return 0;

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
    return 0;
}


RC QL_Manager::getJoinConditions(const char * const relations[],
                                 int   nRelations,int   nConditions,
                                 const Condition conditions[],
                                 RM_FileHandle **pRmfhs,int   nResultConditions[],
                                 int* resultIndexConditions[],int numRightRelation[]){

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

RC QL_Manager::SelectPrinter(DataAttrInfo *&attributes, int nSelAttrs, const RelAttr selAttrs[],
                             int nRelations, const char * const relations[], QL_Operator *root, int &nTotalAttrs){
    attributes = new DataAttrInfo[MAXATTRS];
    int nTotalAttrs = 0;
    //consider situdation : select * from where nSelAttrs = 1 selAttrs[0]:NULL.*
    if(nSelAttrs==1 && selAttrs[0].relName==NULL&&(!strcmp(selAttrs[0].attrName,"*"))){
        //show all the attributs of all the relations
        nTotalAttrs = 0;
        //DataAttrInfo attributes[MAXATTRS];
        //loop for all relations
        for(int j=0;j<nRelations;j++){
            //get all the attributs for a relation
            RM_FileScan fsAttrcat;
            if(rc = fsAttrcat.OpenScan(pSmm->fhAttrcat,STRING,MAXNAME,0,EQ_OP,relations[j],NO_HINT)) return rc;
            bool hasNext = true;
            while(hasNext){
                RM_Record recAttrcat;
                if(RM_EOF == fsAttrcat.GetNextRec(recAttrcat)) {hasNext = false; break;}
                char * dataAttrcat;
                recAttrcat.GetData(dataAttrcat);
                //change data to DataAttrInfo
                memcpy(attributes[nTotalAttrs],dataAttrcat,MAXNAME);
                memcpy(attributes[nTotalAttrs]+MAXNAME,"\0",1); //incompatable between DataAttrInfo and SM_AttrcatRec
                memcpy(attributes[nTotalAttrs]+MAXNAME+1,dataAttrcat+MAXNAME,MAXNAME);
                memcpy(attributes[nTotalAttrs]+2*MAXNAME+1,"\0",1); //incompatable between DataAttrInfo and SM_AttrcatRec
                memcpy(attributes[nTotalAttrs]+2*MAXNAME+2,dataAttrcat+2*MAXNAME,3*sizeof(INT)+sizeof(AttrType)); //incompatable between DataAttrInfo and SM_AttrcatRec
                nTotalAttrs++;
            }
        }

        for (int i=0;i<nTotalAttrs;i++){
            strcpy(attributes[i].relName,"table_OUT");
            //calculate offset
            int offset=0;
            for (int j=0;j<i;j++){
                offset+= attributes[j].attrLength;
            }
            attributes[i].offset = offset;
            attributes[i].indexNo=-1;
        }
    }
    else{
        //DataAttrInfo attributes[nSelAttrs];
        nTotalAttrs = nSelAttrs;
        //DataAttrInfo records
        for (int i=0;i<nSelAttrs;i++){
            //get complete relation-attribut
            const char * relName;
            DataAttrInfo dataAttr;
            if(!getRelName(selAttrs[i],nRelations,relations,relName, dataAttr)){
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
            else{
#ifdef SM_DEBUG
                assert(0);
#endif
            }
        }
    }
    p = new Printer(attributes,nSelAttrs);
    p->PrintHeader(cout);

    //print data
    RM_Record recOutput;
    while(!root->GetNext(recOutput))
    {
        p->Print(cout,output);
    }

    p->PrintFooter(cout);
    return 0;

}


//to get relation name for relAttr even relAttr is not "rel.attr"
RC QL_Manager::getRelName(const RelAttr &relAttr, int nRelations, const char * const relations[],
                          const char* relName, DataAttrInfo &dataAttr){
    if(relAttr.relName != NULL) {
        relName = relAttr.relName;
        RM_Record recAttr;
        char * pdata;
        pSmm->GetAttributeInfo(relName,relAttr.attrName,recAttr,pdata);

        memcpy(dataAttr,pdata,MAXNAME);
        memcpy(dataAttr+MAXNAME,"\0",1); //incompatable between DataAttrInfo and SM_AttrcatRec
        memcpy(dataAttr+MAXNAME+1,pdata+MAXNAME,MAXNAME);
        memcpy(dataAttr+2*MAXNAME+1,"\0",1); //incompatable between DataAttrInfo and SM_AttrcatRec
        memcpy(dataAttr+2*MAXNAME+2,pdata+2*MAXNAME,3*sizeof(INT)+sizeof(AttrType));

        //convert to DataAttrInfo

        return 0;
    }
    for(int j=0;j<nRelations;j++){
        RM_Record recAttr;
        char * pdata;
        if(!pSmm->GetAttributeInfo(relations[j],relAttr.attrName,recAttr,pdata)){
            relName = relations[j];
            memcpy(dataAttr,pdata,MAXNAME);
            memcpy(dataAttr+MAXNAME,"\0",1); //incompatable between DataAttrInfo and SM_AttrcatRec
            memcpy(dataAttr+MAXNAME+1,pdata+MAXNAME,MAXNAME);
            memcpy(dataAttr+2*MAXNAME+1,"\0",1); //incompatable between DataAttrInfo and SM_AttrcatRec
            memcpy(dataAttr+2*MAXNAME+2,pdata+2*MAXNAME,3*sizeof(INT)+sizeof(AttrType));
            return 0;
        }
    }
    return SM_ATTRNOTFOUND;
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
