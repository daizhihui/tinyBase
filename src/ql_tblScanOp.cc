#include "ql_internal.h"
#define QL_DEBUG_OPERATOR

#define OFFSET(type, member) ((int)&((type *) 0)->member)
QL_TblScanOp::QL_TblScanOp(const char *tblName, RM_FileHandle &fh, const Condition & c,SM_Manager *sm){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin construct tblScan" << endl;
#endif

    tableName = tblName;
    pRmfh = &fh; //not used
    pCondition = &c;
    pSmm = sm;
    op = pCondition->op;
    //rmfs = new RM_FileScan();
    if(pCondition->op != NO_OP){
        SchemaLookup(pCondition->lhsAttr,fsAttrInfo);
    }
    //valueData = c.rhsValue.data;
    alwaysEOF = false;
    pinHint = NO_HINT;
    RM_Record relcatRec;
    char * pdata;
    pSmm->GetRelationInfo(tableName,relcatRec,pdata);
    this->tupleLength = *(int*)(pdata+MAXNAME);

#ifdef QL_DEBUG_OPERATOR
    cout << "tupleLength" << this->tupleLength<<endl;
#endif

#ifdef QL_DEBUG_OPERATOR
    cout << "END construct tblScan" << endl;
#endif
}

QL_TblScanOp::~QL_TblScanOp(){
    delete pRmfh;
    //delete pSmm;
    delete valueData;
    delete leftData;
}

RC QL_TblScanOp::Initialize(AttrType aT, int i, char * name){
    RC rc;
#ifdef QL_DEBUG_OPERATOR
    cout << "begin Initialize TblScan" << endl;
#endif
    if(op==NO_OP){
        //scan for all elements
        cout << "ALL MATCHES" << endl;
        rc = rmfs.OpenScan((*pRmfh),INT,sizeof(int),
                             0,op,NULL,pinHint);
    }
    else{
        rc = rmfs.OpenScan((*pRmfh),fsAttrInfo.attrType,fsAttrInfo.attrLength,
                         fsAttrInfo.offset,op,pCondition->rhsValue.data,pinHint);
//#ifdef QL_DEBUG_OPERATOR
//    cout << "fsAttrInfo.attrType" << fsAttrInfo.attrType << "=" << (char*)pCondition->rhsValue.data <<endl;
//#endif
    }
#ifdef QL_DEBUG_OPERATOR
    cout << "END Initialize TblScan" << endl;
    assert(rc==0);
#endif
    return rc;

}

RC QL_TblScanOp::GetNext(RM_Record & rec){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin getNext of TblScan" << endl;
#endif
        RC rc = rmfs.GetNextRec(rec);

        if(rc==RM_EOF)    {
            return QL_LASTWARN;
        }

        else if(rc==0){

#ifdef QL_DEBUG_OPERATOR
            char * data;
            rec.GetData(data);
            cout << "in TAble scan " << *(int*)data << endl;
#endif

            return rc;
        }
        else {
#ifdef QL_DEBUG_OPERATOR
            cout << "rc=" << rc << endl;
            assert(0);
#endif
#ifdef QL_DEBUG_OPERATOR
    cout << "END getNext of TblScan" << endl;
#endif
            return rc;
        }
}

RC QL_TblScanOp::Finalize (){
    return rmfs.CloseScan();
}

RC QL_TblScanOp::SchemaLookup(const RelAttr &relattr, QL_RelAttrInfo &info){
#ifdef QL_DEBUG_OPERATOR
    cout << "begin SchemaLookup of QL_TblScanOp" << endl;
    this->Print(cout,0);
    cout << "relAttre" << relattr << endl;
#endif
    RC rc;


    //compare relattr.relName with tableName
    if(strcmp(relattr.relName, tableName)) {

        return SM_ATTRNOTFOUND;
    }

    //calculer offset
    RM_Record rec;
    char *pdata;
    if(rc = pSmm->GetAttributeInfo(tableName,relattr.attrName,rec,pdata))
    {
#ifdef QL_DEBUG_OPERATOR
        if(!rc)
                assert(0);
#endif
        return rc;
    }
    info.attrType = *(AttrType*)(pdata+2*MAXNAME+sizeof(int));
    info.attrLength = *(int*)(pdata+2*MAXNAME+sizeof(int)+sizeof(AttrType));
    info.offset = *(int*)(pdata+2*MAXNAME);
#ifdef QL_DEBUG_OPERATOR
    cout << "in TAble scan,"<<info.attrType << "****" << info.attrLength << "*****" << info.offset << endl;
    cout << "END OF SchemaLookup of QL_TblScanOp" << endl;
#endif
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
