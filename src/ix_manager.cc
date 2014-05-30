#include "ix_internal.h"

IX_Manager::IX_Manager(PF_Manager &pfm){
    pPfm = &pfm;
}

IX_Manager::~IX_Manager(){
    pPfm = NULL;
}

//
// CreateFile
//
// Desc: Create a new Index file whose name is "fileName"
//       Allocate a file header page and fill out some information
// In:   fileName - name of file to create
//       indexNo - number of index
//       attrType   - INT|FLOAT|STRING
//       attrLength - 4 for INT|FLOAT, 1~MAXSTRING for STRING
// Ret:  ?? or PF return code
//
RC IX_Manager::CreateIndex(const char *fileName, int indexNo, AttrType attrType, int attrLength){



}

RC IX_Manager::DestroyIndex(const char *fileName, int indexNo){

}

RC IX_Manager::OpenIndex(const char *fileName, int indexNo, IX_IndexHandle &indexHandle){

}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle){

}
