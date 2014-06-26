#include "ql_internal.h"
#define OFFSET(type, member) ((int)&((type *) 0)->member)
QL_TblScanOp::QL_TblScanOp(const char *tblName, RM_FileHandle &fh, const Condition & c,SM_Manager *sm){
    tableName = tblName;
    pRmfh = &fh; //not used
    pCondition = &c;
    pSmm = sm;
    op = pCondition->op;
    //rmfs = new RM_FileScan();
    SchemaLookup(pCondition->lhsAttr,fsAttrInfo);
    //valueData = c.rhsValue.data;
    alwaysEOF = false;
    pinHint = NO_HINT;
    RM_Record relcatRec;
    char * pdata;
    pSmm->GetRelationInfo(tableName,relcatRec,pdata);
    this->tupleLength = *(int*)(pdata+MAXNAME);
}

QL_TblScanOp::~QL_TblScanOp(){
    delete pRmfh;
    //delete pSmm;
    delete valueData;
    delete leftData;
}

RC QL_TblScanOp::Initialize(AttrType aT, int i, char * name){
    return rmfs.OpenScan((*pRmfh),fsAttrInfo.attrType,fsAttrInfo.attrLength,fsAttrInfo.offset,op,valueData,pinHint);

}

RC QL_TblScanOp::GetNext(RM_Record & rec){
        RC rc = rmfs.GetNextRec(rec);
        if(rc==RM_EOF)    {
            return QL_LASTWARN;
        }
        else return rc;

}

RC QL_TblScanOp::Finalize (){
    return rmfs.CloseScan();
}

RC QL_TblScanOp::SchemaLookup(const RelAttr &relattr, QL_RelAttrInfo &info){
    RC rc;
    //calculer offset
    RM_Record rec;
    char *pdata;
    if(rc = pSmm->GetAttributeInfo(relattr.relName,relattr.attrName,rec,pdata))
    {
#ifdef DEBUG_QL
        if(!rc)
                assert(0);
#endif
        return rc;
    }
    info.attrLength = *(int*)(pdata+2*MAXNAME+sizeof(int)+sizeof(AttrType));
    info.offset = *(int*)(pdata+2*MAXNAME);
    return 0;

}
RC QL_TblScanOp::EstimateCard  (double &){

}
RC QL_TblScanOp::EstimateIO    (double &){}

void QL_TblScanOp::Print(ostream &c, int t){
    c << "table scan: " << tableName <<"\n";
}

//QL_TblScanOp::QL_TblScanOp  (const QL_TblScanOp &tlScan){
//}
//QL_TblScanOp::QL_TblScanOp& operator=(const QL_TblScanOp &tlScan){
//}
