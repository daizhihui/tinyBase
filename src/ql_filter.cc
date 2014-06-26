#include "ql_internal.h"
/*
private:
    QL_FilterOp  (const QL_FilterOp &);
    QL_FilterOp& operator=(const QL_FilterOp &);

    QL_Operator      *pChild;
    const Condition  *pCondition;
    SM_Manager       *pSmm;
    QL_RelAttrInfo   lhsAttrInfo;
    QL_RelAttrInfo   rhsAttrInfo;
*/
#define QL_DEBUG_OPERATOR

QL_FilterOp::QL_FilterOp(QL_Operator *pChd, const Condition &c,
                      SM_Manager *sm){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin constructor of QL_FilterOp" << endl;
#endif
        pCondition = &c;
        pSmm = sm;
        pChild = pChd;
        pChd->SchemaLookup(pCondition->lhsAttr,lhsAttrInfo); //lhsAttrInfo contains offset of attribut in returned tuple
        //fitrage apres la jointure
        if(pCondition->bRhsIsAttr){
            pChd->SchemaLookup(pCondition->rhsAttr,rhsAttrInfo);
        }
        this->tupleLength = pChd->tupleLength;
}

QL_FilterOp::~QL_FilterOp     (){
    delete pSmm;
    delete pChild;
}

RC QL_FilterOp::Initialize(AttrType type, int index, char * tname){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin initailise of QL_FilterOp" << endl;
#endif
    RC rc;
    if(rc=pChild->Initialize(type,index,tname)) return rc;
    return 0;
}
RC QL_FilterOp::GetNext(RM_Record &rec){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin getNExt of QL_FilterOp" << endl;
#endif
    RC rc;
    bool found = false;
    char *pData;
    while(!found){
        RM_Record lastRec;
        char *pLastData;
        if(rc=pChild->GetNext(lastRec)) return rc;
        lastRec.GetData(pLastData);
        if(pCondition->bRhsIsAttr){
            if(!memcmp(pLastData+lhsAttrInfo.offset,pLastData+rhsAttrInfo.offset,lhsAttrInfo.attrLength)) found = true;
        }
        else {
            if(!memcmp(pLastData+lhsAttrInfo.offset,pCondition->rhsValue.data,lhsAttrInfo.attrLength)) found = true;
        }
        if(found) {
            pData = new char[this->tupleLength];
            strcpy(pData,pLastData);
        }
    }
    memcpy(&rec+sizeof(RID),pData,sizeof(INT));
    return 0;
}
RC QL_FilterOp::Finalize      (){
    RC rc;
    if(rc=pChild->Finalize()) return rc;
    return 0;
}
RC QL_FilterOp::SchemaLookup  (const RelAttr &relattr, QL_RelAttrInfo &info){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin schemalookup of QL_FilterOp" << endl;
#endif
    RC rc;
    //search in left, if not found, search right
    if(rc=pChild->SchemaLookup(relattr,info)) {
#ifdef QL_DEBUG_OPERATOR
        if(!rc)
                assert(0);
#endif
    }
    return 0;
}
RC QL_FilterOp::EstimateCard  (double &){

}
RC QL_FilterOp::EstimateIO    (double &){

}
void QL_FilterOp::Print       (ostream &c, int indexDepth){
    c << "filter" << "\n";
    pChild->Print(c,indexDepth);
}



QL_ProjectOp::QL_ProjectOp     (QL_Operator *pCld, int nPAttrs , const RelAttr relattrs[]){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin constru of QL_ProjectOp" << endl;
#endif
    pChild = pCld;
    nProjAttrs = nPAttrs;
    projAttrs = const_cast<RelAttr*>(relattrs);
    projAttrInfos = new QL_RelAttrInfo[nProjAttrs]; //contains the output tuple's info
    QL_RelAttrInfo tInfo;
    //calculate projAttrInfos
    for(int i = 0; i< nProjAttrs; i++){
        pCld->SchemaLookup(projAttrs[i],tInfo); //tInfo contains the offset of returned record of previous getNext
        projAttrInfos[i].attrType = tInfo.attrType;
        projAttrInfos[i].attrLength = tInfo.attrLength;
        if(i==0) projAttrInfos[i].offset = 0;
        else projAttrInfos[i].offset += projAttrInfos[i-1].offset;
    }
    this->tupleLength = projAttrInfos[nProjAttrs-1].offset + projAttrInfos[nProjAttrs-1].attrLength;

}
QL_ProjectOp::~QL_ProjectOp    (){
    delete pChild;
    delete projAttrInfos;
    delete projAttrs;
}

RC QL_ProjectOp::Initialize(AttrType type, int index, char * tname){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin initalise of QL_ProjectOp" << endl;
#endif
    RC rc;
    if(rc=pChild->Initialize(type,index,tname)) return rc;
    return 0;
}

RC QL_ProjectOp::GetNext(RM_Record &rec){
    char * pData;
    rec.GetData(pData);
#ifdef QL_DEBUG_OPERATOR
    cout << "begin getNExt of QL_ProjectOp" << endl;
#endif
    RC rc;
    RM_Record lastRec;
    if(rc=pChild->GetNext(lastRec)) return rc;
    char * pDataRec;
    lastRec.GetData(pDataRec);

    char * pData = new char[this->tupleLength];
    for(int i = 0; i< nProjAttrs; i++){
        QL_RelAttrInfo tInfo;
        pChild->SchemaLookup(projAttrs[i],tInfo);
        memcpy(pData+projAttrInfos[i].offset,pDataRec+tInfo.offset,tInfo.attrLength);
    }
#ifdef QL_DEBUG_OPERATOR
    cout << "tupleLength" << this->tupleLength<<endl;
    cout << "projAttr OFFSET" << projAttrInfos[0].offset <<endl;
    cout << "data content" << pData <<endl;
#endif
    //memcpy(&rec,pData,sizeof(int));
#ifdef QL_DEBUG_OPERATOR
    char * testData;
    rec.GetData(testData);
    cout << "data content" << testData <<endl;
    assert(pData==testData);
#endif

    return 0;
}
RC QL_ProjectOp::Finalize      (){
    RC rc;
    if(rc=pChild->Finalize()) return rc;
    return 0;
}

RC QL_ProjectOp::SchemaLookup(const RelAttr &relAttr, QL_RelAttrInfo &info){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin schemalookup of QL_ProjectOp" << endl;
#endif
    for(int i = 0; i< nProjAttrs; i++){
        //compare relAttr ?= projAttrs[i]
        if(!strcmp(relAttr.relName,projAttrs[i].relName)&&
                !strcmp(relAttr.attrName,projAttrs[i].attrName)){
            info = this->projAttrInfos[i];
            return 0;
        }
    }
    return QL_UNCOMPATTYPE;
}

RC QL_ProjectOp::EstimateCard  (double &){}
RC QL_ProjectOp::EstimateIO    (double &){}
void QL_ProjectOp::Print(ostream & c, int indexDepth){
    c << "projection" << "\n";
    pChild->Print(c,indexDepth);
}


//QL_ProjectOp::QL_ProjectOp  (const QL_ProjectOp &){}
//QL_ProjectOp::QL_ProjectOp& operator=(const QL_ProjectOp &){}




// nested loop join

QL_NLJOp::QL_NLJOp(QL_Operator *pLchild, QL_Operator *pRchild, const Condition &c, SM_Manager *smm){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin constru of QL_NLJOp" << endl;
#endif
    pLeftChild = pLchild;
    pRightChild = pRchild;
    pCondition = &c;
    pSmm = smm;

    op = c.op;
    pLchild->SchemaLookup(c.lhsAttr,leftAttrInfo);
    pRchild->SchemaLookup(c.rhsAttr,rightAttrInfo);
    this->tupleLength = pLchild->tupleLength + pRchild->tupleLength - leftAttrInfo.attrLength;
}

QL_NLJOp::~QL_NLJOp(){
    delete pLeftChild;
    delete pRightChild;
    delete pSmm;

}
RC QL_NLJOp::Initialize    (AttrType at, int i, char * name){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin initialise of QL_NLJOp" << endl;
#endif
    RC rc;
    if(rc=pLeftChild->Initialize(at,i,name)) return rc;
    if(rc=pRightChild->Initialize(at,i,name)) return rc;
    //initialize leftRec
    if(rc=pLeftChild->GetNext(leftRec)) return rc;
    if(rc=leftRec.GetData(leftData)) return rc;
    return 0;
}

RC QL_NLJOp::GetNext(RM_Record &rec){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin getNext of QL_NLJOp" << endl;
#endif
    RC rc;
    bool found = false;
    char *pData;
    while(!found){
        RM_Record rightRec;
        if((rc=pRightChild->GetNext(rightRec))==QL_LASTWARN){
            //to re-initialize
            pRightChild->Initialize(rightAttrInfo.attrType);
            if(rc = pRightChild->GetNext(rightRec)) {
#ifdef QL_DEBUG_OPERATOR
                assert(0);
#endif
                return rc; //should not happen
            }
            pLeftChild->GetNext(leftRec);
            leftRec.GetData(leftData);
        }
        char *pRightData;
        rightRec.GetData(pRightData);
        //verify if two tuples satisfy condition
        if(!memcmp(leftData+leftAttrInfo.offset,pRightData+rightAttrInfo.offset,leftAttrInfo.attrLength)){
            //structure the result of join, delete the attribut value in pdata2
            // pdata1 + (pdata2 without attribut value)
            found = true;
            pData = new char[this->tupleLength];
            strcpy(pData,leftData);
            memcpy(pData+pLeftChild->tupleLength,pRightData,rightAttrInfo.offset);
            memcpy(pData+pLeftChild->tupleLength+rightAttrInfo.offset,
                   pRightData+rightAttrInfo.offset+rightAttrInfo.attrLength,
                   pRightChild->tupleLength-rightAttrInfo.attrLength);
        }
    }
    memcpy(&rec+sizeof(RID),pData,sizeof(INT));
    return 0;
}
RC QL_NLJOp::Finalize(){
    RC rc;
    if(rc=pLeftChild->Finalize()) return rc;
    if(rc=pRightChild->Finalize()) return rc;
    return 0;
}

RC QL_NLJOp::SchemaLookup  (const RelAttr &relattr, QL_RelAttrInfo &info){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin schemalookup of QL_NLJOp" << endl;
#endif
    RC rc;
    //search in left, if not found, search right
    QL_RelAttrInfo tInfo;
    if(rc=pLeftChild->SchemaLookup(relattr,tInfo)) {
        rc = pRightChild->SchemaLookup(relattr,tInfo);
#ifdef QL_DEBUG_OPERATOR
        if(!rc)
                assert(0);
#endif
        info.offset = pLeftChild->tupleLength + tInfo.offset;
        return 0;
    }
    info.offset = tInfo.offset;
    return 0;
}

RC QL_NLJOp::EstimateCard  (double &){
    return 0;
}
RC QL_NLJOp::EstimateIO    (double &){
    return 0;
}
void QL_NLJOp::Print(ostream & c, int indexDepth){
    c << "nested loop join" << "\n";
    pLeftChild->Print(c,0);
    pRightChild->Print(c,indexDepth);
}


//TODO
//shallow copy --> deep copy
//QL_NLJOp::QL_NLJOp  (const QL_NLJOp & nljOp){
//    this = &nljOp;
//}
//QL_NLJOp::QL_NLJOp& operator=(const QL_NLJOp & nljOp){
//    return &nljOp;
//}
