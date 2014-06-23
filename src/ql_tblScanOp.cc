#include "ql_internal.h"
#define OFFSET(type, member) ((int)&((type *) 0)->member)
QL_TblScanOp::QL_TblScanOp(const char *tblName, RM_FileHandle &fh, const Condition & c,SM_Manager *sm){
    tableName = tblName;
    pRmfh = &fh;
    pCondition = &c;
    pSmm = sm;
    op = pCondition->op;
    rmfs = new RM_FileScan();
    SchemaLookup(pCondition->lhsAttr,fsAttrInfo);
    //valueData = c.rhsValue.data;
    alwaysEOF = false;
    pinHint = NO_HINT;
}

QL_TblScanOp::~QL_TblScanOp(){
    delete pRmfh;
    delete pSmm;
    delete valueData;
    delete leftData;
}

RC QL_TblScanOp::Initialize(AttrType aT, int i, char * name){
    rmfs.OpenScan((*pRmfh),fsAttrInfo.attrType,fsAttrInfo.attrLength,fsAttrInfo.offset,op,valueData,pinHint);
}

RC QL_TblScanOp::GetNext(RM_Record & rec){
        RC rc = rmfs.GetNextRec(rec);
        if(rc==RM_EOF)    {
            return QL_LASTWARN;
        }
        else return rc;

}

RC QL_TblScanOp::Finalize      (){
    rmfs.CloseScan();
}

RC QL_TblScanOp::SchemaLookup(const RelAttr &relattr, QL_RelAttrInfo &info){
    //calculer offset
    RM_Record rc;
    char *pdata;
    pSmm->GetAttributeInfo(relattr.relName,relattr.attrName,rc,pdata);
    //TODO
    info.attrLength = pdata[2*MAXNAME+sizeof(INT)+sizeof(AttrType)];
    info.offset = *(int*)pdata[2*MAXNAME];

}
RC QL_TblScanOp::EstimateCard  (double &){

}
RC QL_TblScanOp::EstimateIO    (double &){}
void QL_TblScanOp::Print(ostream &, int){

}

QL_TblScanOp::QL_TblScanOp  (const QL_TblScanOp &tlScan){
}
QL_TblScanOp::QL_TblScanOp& operator=(const QL_TblScanOp &tlScan){
}
