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

QL_FilterOp::QL_FilterOp(QL_Operator *pChd, const Condition &c,
                      SM_Manager *sm){
        pCondition = &c;
        pSmm = sm;
        pChild = pChd;
        pChd->SchemaLookup(pCondition->lhsAttr,lhsAttrInfo); //lhsAttrInfo contains offset of attribut in returned tuple
        //fitrage apres la jointure
        if(pCondition->bRhsIsAttr){

            SchemaLookup(pCondition->rhsAttr,rhsAttrInfo);
        }
);

}
QL_FilterOp::~QL_FilterOp     (){
    delete pSmm;
}

RC QL_FilterOp::Initialize(AttrType, int, char *){

}
RC QL_FilterOp::GetNext(RM_Record &rec){
    pChild->GetNext(rec);
    if(pCondition->bRhsIsAttr){

    }
    else;

}
RC QL_FilterOp::Finalize      (){

}
RC QL_FilterOp::SchemaLookup  (const RelAttr &, QL_RelAttrInfo &){

}
RC QL_FilterOp::EstimateCard  (double &){

}
RC QL_FilterOp::EstimateIO    (double &){

}
void QL_FilterOp::Print       (ostream &, int){

}



QL_ProjectOp::QL_ProjectOp     (QL_Operator *pCld, int nPAttrs , const RelAttr relattrs[]){
    pChild = pCld;
    nProjAttrs = nPAttrs;
    projAttrs = relattrs;
    projAttrInfos = QL_RelAttrInfo[nProjAttrs]; //contains the output tuple's info
    QL_RelAttrInfo tInfo;
    //calculate projAttrInfos
    for(int i = 0; i< nProjAttrs; i++){
        pCld->SchemaLookup(projAttrs[i],tInfo); //tInfo contains the offset of returned record of previous getNext
        projAttrInfos[i].attrType = tInfo.attrType;
        projAttrInfos[i].attrLength = tInfo.attrLength;
        if(i==0) projAttrInfos[i].offset = 0;
        else projAttrInfos[i].offset += projAttrInfos[i-1].offset;
    }

}
QL_ProjectOp::~QL_ProjectOp    (){
    delete pChild;
    delete projAttrInfos;
    delete projAttrs;
}

RC QL_ProjectOp::Initialize    (AttrType, int, char *){
    pChild->Initialize();
}
RC QL_ProjectOp::GetNext(RM_Record &rec){
    pChild->GetNext(rec);
    char * pDataRec;
    rec.GetData(pDataRec);
    char * pData = new char[this->tupleLength];
    for(int i = 0; i< nProjAttrs; i++){
        QL_RelAttrInfo tInfo;
        pChild->SchemaLookup(projAttrs[i],tInfo);
        memcpy(pData+projAttrInfos[i].offset,pDataRec+tInfo.offset,tInfo.attrLength);
    }
    memcpy(&rec+sizeof(RID),pData,sizeof(INT)); //to check
}
RC QL_ProjectOp::Finalize      (){
    pChild->Finalize();
}

RC QL_ProjectOp::SchemaLookup(const RelAttr &relAttr, QL_RelAttrInfo &info){
    for(int i = 0; i< nProjAttrs; i++){
        if(relAttr == this->projAttrs[i]){
            info = this->projAttrInfos[i];
            return 0;
        }
    }
    else return QL_UNCOMPATTYPE;
}

RC QL_ProjectOp::EstimateCard  (double &){}
RC QL_ProjectOp::EstimateIO    (double &){}
void QL_ProjectOp::Print       (ostream &, int){}


QL_ProjectOp::QL_ProjectOp  (const QL_ProjectOp &){}
QL_ProjectOp::QL_ProjectOp& operator=(const QL_ProjectOp &){}

//    QL_Operator      *pChild;
//    int              nProjAttrs;
//    RelAttr          *projAttrs;
//    QL_RelAttrInfo   *projAttrInfos;




// nested loop join

class QL_NLJOp : public QL_Operator {
public:
    QL_NLJOp         (QL_Operator *, QL_Operator *, const Condition &,
                      SM_Manager *);
    ~QL_NLJOp        ();
    RC Initialize    (AttrType, int, char *);
    RC GetNext       (RM_Record &);
    RC Finalize      ();
    RC SchemaLookup  (const RelAttr &, QL_RelAttrInfo &);
    RC EstimateCard  (double &);
    RC EstimateIO    (double &);
    void Print       (ostream &, int);

private:
    QL_NLJOp  (const QL_NLJOp &);
    QL_NLJOp& operator=(const QL_NLJOp &);

    QL_Operator      *pLeftChild;
    QL_Operator      *pRightChild;
    const Condition  *pCondition;
    SM_Manager       *pSmm;

    CompOp           op;
    QL_RelAttrInfo   leftAttrInfo;
    RM_Record        leftRec;
    char             *leftData;
    QL_RelAttrInfo   rightAttrInfo;
};

QL_NLJOp::QL_NLJOp(QL_Operator *pLchild, QL_Operator *pRchild, const Condition &c, SM_Manager *smm){
    pLeftChild = pLchild;
    pRightChild = pRchild;
    pCondition = &c;
    pSmm = smm;

    op = c.op;
    pLchild->SchemaLookup(c.lhsAttr,leftAttrInfo);
    pRchild->SchemaLookup(c.rhsAttr,rightAttrInfo);

}

QL_NLJOp::~QL_NLJOp(){
    //TODO to rethink
    delete pLeftChild;
    delete pRightChild;
    delete pSmm;

}
RC QL_NLJOp::Initialize    (AttrType at, int i, char * name){
    pLeftChild->Initialize(at,i,name);
    pRightChild->Initialize(at,i,name);

}
RC QL_NLJOp::GetNext(RM_Record &rec){
    RM_Record tuple1;
    RM_Record tuple2;
    pLeftChild->GetNext(tuple1);
    pRightChild->GetNext(tuple2);

    char *pdata1;
    char *pdata2;
    tuple1.GetData(pdata1);
    tuple2.GetData(pdata2);



    //verify if two tuples satisfy condition
    if(!memcmp(pdata1+leftAttrInfo.offset,pdata2+rightAttrInfo.offset,leftAttrInfo.attrLength)){
        //structure the result of join, delete the attribut value in pdata2
        // pdata1 + (pdata2 without attribut value)

        char * pData = new char[this->tupleLength];
        strcpy(pData,pdata1);

    }


}
RC QL_NLJOp::Finalize      ();
RC QL_NLJOp::SchemaLookup  (const RelAttr &relattr, QL_RelAttrInfo &info){
    //calculer offset
    RM_Record rc;
    char *pdata;
    pSmm->GetAttributeInfo(relattr.relName,relattr.attrName,rc,pdata);
    info.attrLength = pdata[2*MAXNAME+sizeof(INT)+sizeof(AttrType)];
    info.offset = *(int*)pdata[2*MAXNAME];
}
RC QL_NLJOp::EstimateCard  (double &);
RC QL_NLJOp::EstimateIO    (double &);
void QL_NLJOp::Print       (ostream &, int);
QL_NLJOp::QL_NLJOp  (const QL_NLJOp &);
QL_NLJOp::QL_NLJOp& operator=(const QL_NLJOp &);
