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
    char * pData = new char[this->tupleLength + sizeof(RID)];
    for(int i = 0; i< nProjAttrs; i++){
        QL_RelAttrInfo tInfo;
        pChild->SchemaLookup(projAttrs[i],tInfo);
        memcpy(pData+projAttrInfos[i].offset,pDataRec+tInfo.offset,tInfo.attrLength);
    }
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

