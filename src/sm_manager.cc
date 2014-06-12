//
// File:        SM component stubs
// Description: Print parameters of all SM_Manager methods
// Authors:     Dallan Quass (quass@cs.stanford.edu)
//

#include <cstdio>
#include <iostream>
#include <cstring>
#include <unistd.h>
#include "redbase.h"
#include "sm.h"
#include "ix.h"
#include "rm.h"

using namespace std;

SM_Manager::SM_Manager(IX_Manager &ixm, RM_Manager &rmm)
{
    Ixm=&ixm;
    Rmm=&rmm;
    
}

SM_Manager::~SM_Manager()
{
    Ixm=NULL;
    Rmm=NULL;
}

RC SM_Manager::OpenDb(const char *dbName)
{
    //change to current directory
    if (chdir(dbName) < 0) {
        cerr << " chdir error to " << dbName << "\n";
        exit(1);
    }
    
    RC rc;
    
    // Call RM_Manager::OpenFile() to open Catalog relcat
    if((rc=Rmm->OpenFile("relcat", fileHandle_Relcat))) goto err_return;
    
    // Call RM_Manager::OpenFile() to open Catalog attrcat
    if((rc=Rmm->OpenFile("attrcat", fileHandle_Attrcat))) goto err_return;

err_return:
    return (rc);
    
    return (0);
}

RC SM_Manager::CloseDb()
{
    RC rc;
    
    //Call RM_Manager::CloseFile() to close Catalog relcat
    if((rc=Rmm->CloseFile(fileHandle_Relcat))) goto err_return;
    
    //Call RM_Manager::CloseFile() to close Catalog relcat
    if((rc=Rmm->CloseFile(fileHandle_Attrcat))) goto err_return;
    
err_return:
    return (rc);
    
    return (0);
}

// CreateTable
// IN: relName, attrCount, attributes
//RC: SM_INVALIDRELNAME, SM_INVALIDRELNAME, SM_EXCEEDMAXATTRS
RC SM_Manager::CreateTable(const char *relName,
                           int        attrCount,
                           AttrInfo   *attributes)
{
    RC rc;
    
    //check if relation name begin with letter
    if(!(relName[0] <= 'z' && relName[0] >= 'a') || (relName[0] <= 'Z' && relName[0] >= 'A')) return (SM_INVALIDRELNAME);
    
    // relation name can't be longer than MAXNAME
    if ((unsigned)strlen(relName)>=MAXNAME) return (SM_INVALIDRELNAME);
    
    // define de RM_FileScan
    RM_FileScan filescan;
    RM_Record rec;
    if((rc=filescan.OpenScan(fileHandle_Relcat, STRING, (unsigned)strlen(relName), 0, EQ_OP, (void *)relName))) return (rc);
    // check if exists the same attribute name in relcat
    if(filescan.GetNextRec(rec)!=RM_EOF) return (SM_INVALIDRELNAME);
    if((rc=filescan.CloseScan())) return (rc);
    
    // check number of attributes
    if(!(attrCount>=1 && attrCount<=MAXATTRS)) return (SM_EXCEEDMAXATTRS);
    
    // check if two attributes name are the same
    for(int j=0;j<attrCount-1;j++){
        for (int k=j+1;k<attrCount;k++){
            if(attributes[j].attrName==attributes[k].attrName) return (SM_INVALIDATTRNAME);
        }
    }
    
    int tuplelength=0;
    for (int i=0;i<attrCount;i++){
        
        //check if attribute name begin with letter
        if(!(attributes[i].attrName[0] <= 'z' && attributes[i].attrName[0] >= 'a') || (attributes[i].attrName[0] <= 'Z' && attributes[i].attrName[0] >= 'A')) return (SM_INVALIDATTRNAME);
        
        // attribute name can't be longer than MAXNAME
        if ((unsigned)strlen(attributes[i].attrName)>=MAXNAME) return (SM_INVALIDATTRNAME);
        
        // check attribute
        switch (attributes[i].attrType) {
            case INT:
                if(attributes[i].attrLength!=4)
                    // Test: wrong attribute length
                    return (SM_INVALIDATTR);
            case FLOAT:
                if (attributes[i].attrLength != 4)
                    // Test: wrong attribute length
                    return (SM_INVALIDATTR);
                break;
                
            case STRING:
                if (attributes[i].attrLength < 1 || attributes[i].attrLength > MAXSTRINGLEN)
                    // Test: wrong _attrLength
                    return (SM_INVALIDATTR);
                break;
                
            default:
                // Test: wrong _attrType
                return (SM_INVALIDATTR);
        }

        //calculate the total length of attributes
        tuplelength += attributes[i].attrLength;
    }
    
    // new Relation record
    RID rid;
    char data[sizeof(Relation)];
    Relation relation=Relation();
    relation.relName=relName;
    strcpy(data,relName);
    relation.tupleLength=tuplelength;
    relation.attrCount=attrCount;
    relation.indexCount=0;
    memcpy(data,&relation,sizeof(Relation));
    
    //store new Relation record to Catalog relcat
    if((rc=fileHandle_Relcat.InsertRec(data, rid))) return (rc);
    
    //call RM_Manager::ForcePage to force page relcat
    if((rc=fileHandle_Relcat.ForcePages())) return (rc);
    
    //Attr_Relation records
    for (int i=0;i<attrCount;i++){
        char data[sizeof(Attr_Relation)];
        Attr_Relation attr_relation[attrCount];
        attr_relation[i].attrName=attributes[i].attrName;
        //calculate offset
        int offset=0;
        for (int j=0;j<i;i++){
            offset+=attributes[j].attrLength;
        }
        attr_relation[i].offset=offset;
        attr_relation[i].attrType=attributes[i].attrType;
        attr_relation[i].attrLength=attributes[i].attrLength;
        attr_relation[i].indexNo=-1;
        
        memcpy(data,&attr_relation[i],sizeof(Attr_Relation));
        //store new Attr_Relation record to Catalog attrcat
        if((rc=fileHandle_Attrcat.InsertRec(data, rid))) return (rc);
    }
    
    //call RM_FileHandle::ForcePages to force page attrcat
    if((rc=fileHandle_Attrcat.ForcePages())) return (rc);
    
    
    //Call RM_Manager::CreateFile to create table file
    if((rc=Rmm->CreateFile(relName, tuplelength))) goto err_return;
    
    
    cout << "CreateTable\n"
         << "   relName     =" << relName << "\n"
         << "   attrCount   =" << attrCount << "\n";
    for (int i = 0; i < attrCount; i++)
        cout << "   attributes[" << i << "].attrName=" << attributes[i].attrName
             << "   attrType="
             << (attributes[i].attrType == INT ? "INT" :
                 attributes[i].attrType == FLOAT ? "FLOAT" : "STRING")
             << "   attrLength=" << attributes[i].attrLength << "\n";
err_return:
    return (rc);

    return (0);
    
}

RC SM_Manager::DropTable(const char *relName)
{
    cout << "DropTable\n   relName=" << relName << "\n";
    return (0);
}

RC SM_Manager::CreateIndex(const char *relName,
                           const char *attrName)
{
    RC rc;
    
    // define de RM_FileScan
    RM_FileScan filescan;
    RM_Record rec;
    
    // scan all the records in attrcat
    if((rc=filescan.OpenScan(fileHandle_Attrcat, STRING, (unsigned)strlen(relName), 0, EQ_OP, (void *)NULL))) return (rc);
    
    bool flag_rel_attr_exist=false;
    // check if the index already exists
    int max_index=0;
    Attr_Relation attr_relation;
    RC rc_scan;
    while(rc_scan!=RM_EOF){
        if((rc_scan=filescan.GetNextRec(rec))) return (rc_scan);
        //get records until the end
        if((rc=filescan.GetNextRec(rec))) return (rc);
        //char data[sizeof(Attr_Relation)];
        memcpy(&attr_relation,&rec,sizeof(Attr_Relation));

        //calculate the max index already exists, then create index max_index+1
        if(attr_relation.indexNo>max_index) max_index=attr_relation.indexNo;
        
        // the index already exists, return error SM_INDEXEXIST
        if(attr_relation.relName==relName && attr_relation.attrName==attrName && attr_relation.indexNo!=-1) {
            flag_rel_attr_exist=true;
            return (SM_INDEXEXIST);
        }
        if(attr_relation.relName==relName && attr_relation.attrName==attrName && attr_relation.indexNo==-1) {
            flag_rel_attr_exist=true;
            break;
        }
    }
    
    // relation name or attribute name not exist in attrcat
    if(flag_rel_attr_exist==false) return (SM_INVALIDRELNAME);
    
    // close the filescan
    if((rc=filescan.CloseScan())) return (rc);
    
    char filename[(unsigned)strlen(relName)+(unsigned)strlen(attrName)+1];//filename = relname.attribute name as the new filename
    strcpy(filename, relName);
    strcat(filename,".");
    strcat(filename, attrName);
    
    // Call IX_IndexHandle::CreateIndex to create a index file
    if((rc=Ixm->CreateIndex(filename, max_index+1, attr_relation.attrType, attr_relation.attrLength))) return (rc);
    
    // Call IX_IndexHandle::OpenIndex to open the index file
    IX_IndexHandle indexhandle;
    if((rc=Ixm->OpenIndex(filename, max_index+1, indexhandle))) return (rc);
    
    RM_FileHandle filehandle_rel;
    // Call RM_Manager::OpenFile to open relation file
    if((rc=Rmm->OpenFile(relName, filehandle_rel))) return (rc);
    
    // Call RM_FileScan::OpenScan to scan all the tuples in the table
    if((rc=filescan.OpenScan(filehandle_rel, attr_relation.attrType, attr_relation.attrLength, attr_relation.offset, NO_OP, (void *) NULL))) return (rc);
    
    // scan all the tuples in the table
    while(rc_scan!=RM_EOF){
        if((rc_scan=filescan.GetNextRec(rec))) return (rc_scan);
        RID rid;
        if((rc=rec.GetRid(rid))) return (rc);
        char * data;
        // get record data
        if((rc=rec.GetData(data))) return (rc);
        char data_rec[attr_relation.attrLength];
        // get attribute value
        strcpy(data_rec,data+attr_relation.offset);
        if((rc=indexhandle.InsertEntry(data_rec, rid))) return (rc);
    }
    
    //Close scan
    if((rc=filescan.CloseScan())) return (rc);
    
    //close relation file
    if((rc=Rmm->CloseFile(filehandle_rel))) return (rc);
    
    //close index file
    if(Ixm->CloseIndex(indexhandle)) return (rc);
    
    cout << "CreateIndex\n"
         << "   relName =" << relName << "\n"
         << "   attrName=" << attrName << "\n";
    return (0);
}

RC SM_Manager::DropIndex(const char *relName,
                         const char *attrName)
{
    cout << "DropIndex\n"
         << "   relName =" << relName << "\n"
         << "   attrName=" << attrName << "\n";
    return (0);
}

RC SM_Manager::Load(const char *relName,
                    const char *fileName)
{
    cout << "Load\n"
         << "   relName =" << relName << "\n"
         << "   fileName=" << fileName << "\n";
    return (0);
}

RC SM_Manager::Print(const char *relName)
{
    cout << "Print\n"
         << "   relName=" << relName << "\n";
    return (0);
}

RC SM_Manager::Set(const char *paramName, const char *value)
{
    cout << "Set\n"
         << "   paramName=" << paramName << "\n"
         << "   value    =" << value << "\n";
    return (0);
}

RC SM_Manager::Help()
{
    cout << "Help\n";
    return (0);
}

RC SM_Manager::Help(const char *relName)
{
    cout << "Help\n"
         << "   relName=" << relName << "\n";
    return (0);
}

void SM_PrintError(RC rc)
{
    cout << "SM_PrintError\n   rc=" << rc << "\n";
}

