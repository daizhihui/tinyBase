//QL_IxScanOp implementation
#include "ql_internal.h"

QL_IxScanOp::QL_IxScanOp(const char *tName, RM_FileHandle & fileHandle, const Condition & condition, IX_Manager * ix_manager, SM_Manager * sm_manager)
{
    tableName=tName;//?
    pRmfh=&fileHandle;
    pCondition=&condition;
    pIxm=ix_manager;
    pSmm=sm_manager;
    SchemaLookup(condition.lhsAttr,isAttrInfo);
    alwaysEOF=false;
    //valueData=condition.rhsValue.data;
    //leftData;
    RM_Record relcatRec;
    char * pdata;
    pSmm->GetRelationInfo(tableName,relcatRec,pdata);
    this->tupleLength = *(int*)(pdata+MAXNAME);
}

QL_IxScanOp::~QL_IxScanOp()
{
    delete pRmfh;
    delete pIxm;
    delete pSmm;
    //delete valueData;
   // delete leftData;
}

RC QL_IxScanOp::Initialize (AttrType aType, int indexNum, char *)
{
    RC rc;
    //TODO to change
    if((rc =pIxm->OpenIndex(tableName, indexNum, ixih))!=0)
        return rc;
    return ixis.OpenScan(ixih, pCondition->op, pCondition->rhsValue.data);
    
}


RC QL_IxScanOp::GetNext (RM_Record &rm)
{
    RC rc;
    RID rid;
    if((rc =ixis.GetNextEntry(rid))==IX_EOF)
        return QL_LASTWARN;
    else if(rc!=0)
        return rc;
   return pRmfh->GetRec(rid, rm);
}

RC QL_IxScanOp::Finalize()
{
    RC rc;
    if((rc=ixis.CloseScan())!=0)
        return rc;
   return pIxm->CloseIndex(ixih);
    
}
RC QL_IxScanOp::SchemaLookup  (const RelAttr & rA, QL_RelAttrInfo & rI)
{
    
    RM_Record rec;
    char* data;
    RC rc;
    if((rc = pSmm->GetAttributeInfo(rA.relName, rA.attrName, rec, data))!=0)
            return rc;
    
    rI.offset = data[2*MAXNAME];
    rI.attrType=(AttrType)data[2*MAXNAME+sizeof(int)];
    rI.attrLength=(int)data[2*MAXNAME+sizeof(int)+sizeof(AttrType)];
    return rc;
}

RC QL_IxScanOp::EstimateCard  (double &)
{
    return 0.5;
}
RC QL_IxScanOp::EstimateIO  (double &)
{
    return 0;
}
void QL_IxScanOp::Print  (ostream &c, int t){
    c << "index scan: " << tableName << "number of index" << t <<"\n";
}

//QL_IxScanOp::QL_IxScanOp  (const QL_IxScanOp &)
//{
    
//}
//QL_IxScanOp::QL_IxScanOp& operator=(const QL_IxScanOp &)
//{
    
//}


