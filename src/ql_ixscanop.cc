//QL_IxScanOp implementation
#include "ql_internal.h"
#define QL_DEBUG_OPERATOR
QL_IxScanOp::QL_IxScanOp(const char *tName, RM_FileHandle & fileHandle, const Condition & condition, IX_Manager * ix_manager, SM_Manager * sm_manager)
{
#ifdef QL_DEBUG_OPERATOR
    cout << "begin constructor of QL_IxScanOp" << endl;
#endif
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
#ifdef QL_DEBUG_OPERATOR
    cout << "begin initalise of QL_IxScanOp" << endl;
#endif
    RC rc;
    //TODO to change
    if((rc =pIxm->OpenIndex(tableName, indexNum, ixih))!=0)
        return rc;
    return ixis.OpenScan(ixih, pCondition->op, pCondition->rhsValue.data);
    
}


RC QL_IxScanOp::GetNext (RM_Record &rm)
{
#ifdef QL_DEBUG_OPERATOR
    cout << "begin getNext of QL_IxScanOp" << endl;
#endif
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
#ifdef QL_DEBUG_OPERATOR
    cout << "begin schemalookup of QL_IxScanOp" << endl;
#endif
    
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


