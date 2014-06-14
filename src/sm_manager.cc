//
// File:        SM component stubs
// Description: Print parameters of all SM_Manager methods
// Authors:     Dallan Quass (quass@cs.stanford.edu)
//

#include <cstdio>
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <fstream>
#include "redbase.h"
#include "sm.h"
#include "ix.h"
#include "rm.h"
#include "printer.h"

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
    cout << "CreateTable\n"
    << "   relName     =" << relName << "\n"
    << "   attrCount   =" << attrCount << "\n";
    for (int i = 0; i < attrCount; i++)
        cout << "   attributes[" << i << "].attrName=" << attributes[i].attrName
        << "   attrType="
        << (attributes[i].attrType == INT ? "INT" :
            attributes[i].attrType == FLOAT ? "FLOAT" : "STRING")
        << "   attrLength=" << attributes[i].attrLength << "\n";
    
    RC rc;
    
    //check if relation name begin with letter
    if(!(relName[0] <= 'z' && relName[0] >= 'a') || (relName[0] <= 'Z' && relName[0] >= 'A')) return (SM_INVALIDRELNAME);
    
    // relation name can't be longer than MAXNAME
    if ((unsigned)strlen(relName)>=MAXNAME) return (SM_INVALIDRELNAME);
    
    // define de RM_FileScan
    RM_FileScan filescan;
    RM_Record rec;
    if((rc=filescan.OpenScan(fileHandle_Relcat, INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    // check if exists the same attribute name in relcat
    bool flag=false;
    Relation relation_r;
    char * data_r;
    //scan all the records in relcat
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF) return (rc);
        if(rc!=RM_EOF){
            //copy record to relation
            if((rc=rec.GetData(data_r))) return (rc);
            memcpy(&relation_r,data_r,sizeof(Relation));
            
            if(relation_r.relName==relName) {
                flag=true;
                break;
            }
        }
        
    }
    
    //if relName already exists
    if(flag) return (SM_INVALIDRELNAME);
    
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
    
    DataAttrInfo attr_relation;
    //DataAttrInfo records
    for (int i=0;i<attrCount;i++){
        char data[sizeof(DataAttrInfo)];
        strcpy(attr_relation.relName,relName);
        strcpy(attr_relation.attrName,attributes[i].attrName);
        //calculate offset
        int offset=0;
        for (int j=0;j<i;i++){
            offset+=attributes[j].attrLength;
        }
        attr_relation.offset=offset;
        attr_relation.attrType=attributes[i].attrType;
        attr_relation.attrLength=attributes[i].attrLength;
        attr_relation.indexNo=-1;
        
        memcpy(data,&attr_relation,sizeof(DataAttrInfo));
        //store new Attr_Relation record to Catalog attrcat
        if((rc=fileHandle_Attrcat.InsertRec(data, rid))) return (rc);
    }
    
    //call RM_FileHandle::ForcePages to force page attrcat
    if((rc=fileHandle_Attrcat.ForcePages())) return (rc);
    
    
    //Call RM_Manager::CreateFile to create table file
    if((rc=Rmm->CreateFile(relName, tuplelength))) goto err_return;
    
err_return:
    return (rc);

    return (0);
    
}

RC SM_Manager::DropTable(const char *relName)
{
    cout << "DropTable\n   relName=" << relName << "\n";
    
    RC rc;
    RM_FileScan filescan;
    
    // Call RM_Manager::DestroyFile to destroy the relation file
    if((rc=Rmm->DestroyFile(relName))) return (rc);
    
    //open scan of relcat
    if((rc=filescan.OpenScan(fileHandle_Relcat,INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    
    RC rc_scan=0;
    Relation relation_r;
    RM_Record rec;
    char * data;
    bool flag=false;
    //scan all the records in relcat
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF){
            return (rc);
        }
        if(rc!=RM_EOF){
            rec.GetData(data);
            //copy record to relation
        memcpy(&relation_r,data,sizeof(Relation));
        
        if(relation_r.relName==relName) {
            
            flag=true;
            RID rid;
            // get record RID
            if((rc=rec.GetRid(rid))) return (rc);
            
            //delete relation record in relcat
            if((rc=fileHandle_Relcat.DeleteRec(rid))) return (rc);
            
            //update relcat in disk
            if((rc=fileHandle_Relcat.ForcePages())) return (rc);
            break;
        }

        }
            }
   
    // if no relation records in relcat, return invalid relation name
    if(!flag) return (SM_INVALIDRELNAME);
    
    // close the scan
    if((rc=filescan.CloseScan())) return (rc);
    
    //open scan of attrcat
    if((rc=filescan.OpenScan(fileHandle_Attrcat,INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    
    //pointer to the attribute record in attrcat
    DataAttrInfo a_r;
    
    //scan all the records in attrcat
    while(rc!=RM_EOF){
        //get records until the end
        rc_scan=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF) return (rc);
        if(rc!=RM_EOF){
            rec.GetData(data);
            //copy record to a_r
            memcpy(&a_r,data,sizeof(DataAttrInfo));
            if(a_r.relName==relName) {
                
                RID rid;
                // get record RID
                if((rc=rec.GetRid(rid))) return (rc);
                
                //delete attribute record in attrcat
                if((rc=fileHandle_Attrcat.DeleteRec(rid))) return (rc);
                
                //update relcat in disk
                if((rc=fileHandle_Attrcat.ForcePages())) return (rc);
                
                int index=a_r.indexNo;
                /*char filename[(unsigned)strlen(relName)+(unsigned)strlen(a_r.attrName)+1];//filename = relname.attribute name as the new filename
                 strcpy(filename, relName);
                 strcat(filename,".");
                 strcat(filename, a_r.attrName);*/
                // destroy index file
                if((rc=Ixm->DestroyIndex(relName, index))) return (rc);
            }
        }
    }
    
    //c(lose scan
    if((rc=filescan.CloseScan())) return (rc);
    
    return (0);
}

RC SM_Manager::CreateIndex(const char *relName,
                           const char *attrName)
{
    cout << "CreateIndex\n"
    << "   relName =" << relName << "\n"
    << "   attrName=" << attrName << "\n";
    
    RC rc;
    
    // define de RM_FileScan
    RM_FileScan filescan;
    RM_Record rec;
    
    // scan all the records in attrcat
    if((rc=filescan.OpenScan(fileHandle_Attrcat, INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    
    bool flag_rel_attr_exist=false;
    // check if the index already exists
    int max_index=0;
    //pointer to the record
    DataAttrInfo attr_relation;
    char * data;
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF) return (rc);
        if(rc!=RM_EOF){
            if((rc=rec.GetData(data))) return (rc);
            // copy record to attr_relation
            memcpy(&attr_relation,data,sizeof(DataAttrInfo));
            
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
    }
    
    // relation name or attribute name not exist in attrcat
    if(flag_rel_attr_exist==false) return (SM_INVALIDRELNAME);
    
    // close the filescan
    if((rc=filescan.CloseScan())) return (rc);
    
    /*char filename[(unsigned)strlen(relName)+(unsigned)strlen(attrName)+1];//filename = relname.attribute name as the new filename
    strcpy(filename, relName);
    strcat(filename,".");
    strcat(filename, attrName);*/
    
    // Call IX_IndexHandle::CreateIndex to create a index file
    if((rc=Ixm->CreateIndex(relName, max_index+1, attr_relation.attrType, attr_relation.attrLength))) return (rc);
    
    // Call IX_IndexHandle::OpenIndex to open the index file
    IX_IndexHandle indexhandle;
    if((rc=Ixm->OpenIndex(relName, max_index+1, indexhandle))) return (rc);
    
    RM_FileHandle filehandle_rel;
    // Call RM_Manager::OpenFile to open relation file
    if((rc=Rmm->OpenFile(relName, filehandle_rel))) return (rc);
    
    // Call RM_FileScan::OpenScan to scan all the tuples in the table
    if((rc=filescan.OpenScan(filehandle_rel, INT, sizeof(int), 0, NO_OP, NULL))) return (rc);
    
    // scan all the tuples in the table
    while(rc!=RM_EOF){
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF) return (rc);
        if(rc!=RM_EOF){
            RID rid;
            if((rc=rec.GetRid(rid))) return (rc);
            char * data;
            // get record data
            if((rc=rec.GetData(data))) return (rc);
            char data_rec[attr_relation.attrLength];
            // get attribute value
            memcpy(data_rec,data+attr_relation.offset,attr_relation.attrLength);
            if((rc=indexhandle.InsertEntry(data_rec, rid))) return (rc);
        }
    }
    
    // update information in attrcat
    attr_relation.indexNo=max_index+1;
    
    // update attrcat to disk
    if((rc=fileHandle_Attrcat.ForcePages())) return (rc);
    
    //Close scan
    if((rc=filescan.CloseScan())) return (rc);
    
    //close relation file
    if((rc=Rmm->CloseFile(filehandle_rel))) return (rc);
    
    //close index file
    if(Ixm->CloseIndex(indexhandle)) return (rc);
    
    return (0);
}

RC SM_Manager::DropIndex(const char *relName,
                         const char *attrName)
{
    cout << "DropIndex\n"
    << "   relName =" << relName << "\n"
    << "   attrName=" << attrName << "\n";
    
    RC rc;
    RM_FileScan filescan;
 
    //open scan of attrcat
    if((rc=filescan.OpenScan(fileHandle_Attrcat,INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    
    DataAttrInfo a_r;
    RM_Record rec;
    
    bool flag_exist=false;
    //scan all the records in attrcat
    char * data;
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF)return (rc);
        if(rc!=RM_EOF){
            if((rc=rec.GetData(data))) return (rc);
            //copy record to a_r
            memcpy(&a_r,data,sizeof(DataAttrInfo));
            
            if(a_r.relName==relName && a_r.attrName==attrName && a_r.indexNo!=-1) {
                
                flag_exist=true;
                RID rid;
                // get record RID
                if((rc=rec.GetRid(rid))) return (rc);
                
                //delete attribute record in attrcat
                if((rc=fileHandle_Attrcat.DeleteRec(rid))) return (rc);
                
                //update attrcat in disk
                if((rc=fileHandle_Attrcat.ForcePages())) return (rc);
                
                int index=a_r.indexNo;
                // destroy index file
                if((rc=Ixm->DestroyIndex(relName, index))) return (rc);
                break;
            }
        }
    }

    // no attribute index record in attrcat
    if(!flag_exist) return (SM_ATTRINDEX_NOT_EXIST);
    
    //close scan
    if((rc=filescan.CloseScan())) return (rc);
    
    return (0);
}

RC SM_Manager::Load(const char *relName,
                    const char *fileName)
{
    cout << "Load\n"
    << "   relName =" << relName << "\n"
    << "   fileName=" << fileName << "\n";
    
    RC rc;

    DataAttrInfo a_r;
    RM_Record rec;
    RM_FileScan filescan;
    bool flag_exist=false;
    //store the attrlength of each attribute in relation relName
    int attrlength[MAXATTRS];
    int i=0;
    
    //open scan of attrcat
    if((rc=filescan.OpenScan(fileHandle_Attrcat,INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    
    //scan all the records in attrcat
    char * data;
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF) return (rc);
        if(rc!=RM_EOF){
            if((rc=rec.GetData(data))) return (rc);
            //copy record to a_r
            memcpy(&a_r,data,sizeof(DataAttrInfo));
            
            if(a_r.relName==relName) {
                
                flag_exist=true;
                attrlength[i]=a_r.attrLength;
                i++;
            }
        }
    }
    
//close the scan
    if((rc=filescan.CloseScan())) return (rc);
    
    if(!flag_exist) return (SM_INVALIDRELNAME);
    
    // open data file named fileName
    ifstream myfile (fileName);
    
    RM_FileHandle filehandle_r;
    
    // open relation file to store records read from file
    if((rc=Rmm->OpenFile(relName, filehandle_r))) return (rc);
    
    //load the file
    while(!myfile.eof()){
        int i=0;
        string str;
        //read file line by line
        getline(myfile, str);
        string delimiter = ",";
        
        size_t pos = 0;
        char * record_data;
        while ((pos = str.find(delimiter)) != string::npos) {
            char token[attrlength[i]];
            //ensure the length of attribute
            strncpy(token,str.substr(0, pos).c_str(),attrlength[i]);
            i++;
            strcat(record_data,token);
            //cout << token << endl;
            str.erase(0, pos + delimiter.length());
        }
        RID rid;
        //store the record to relation file
        if((rc=filehandle_r.InsertRec(record_data, rid))) return (rc);

    }
    
    //close the data file
    myfile.close();
    
    //close the relation file
    if((rc=Rmm->CloseFile(filehandle_r))) return (rc);

    return (0);
}

RC SM_Manager::Print(const char *relName)
{
    cout << "Print\n"
         << "   relName=" << relName << "\n";
    
    DataAttrInfo * attributes;
    
    RC rc;
    RM_FileScan filescan;
    //open scan of attrcat
    if((rc=filescan.OpenScan(fileHandle_Attrcat,INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    
    //scan all the records in attrcat
    DataAttrInfo a_r;
    bool flag_exist=false;
    RM_Record rec;
    int i=0;
    int attr_count=0;
    char * data;
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF)return (rc);
        if(rc!=RM_EOF){
            //copy record to a_r
            rc=rec.GetData(data);
            memcpy(&a_r,data,sizeof(DataAttrInfo));
            if(a_r.relName==relName) {
                // take all the DataAttrInfo of the relation relName
                attributes[i]=a_r;
                flag_exist=true;
                i++;
                attr_count++;
            }
        }
    }
    
    //close the scan
    if((rc=filescan.CloseScan())) return (rc);
    
    if(!flag_exist) return (SM_INVALIDRELNAME);
    
    //define a printer
    Printer p(attributes,attr_count);
    
    //print header;
    p.PrintHeader(cout);
    
    RM_FileHandle filehandle_r;
    if((rc=Rmm->OpenFile(relName, filehandle_r))) return (rc);
    
    //open scan of relation file
    if((rc=filescan.OpenScan(filehandle_r,INT, sizeof(int), 0, NO_OP , NULL))) return (rc);

    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF)return (rc);
        if(rc!=RM_EOF){
            //get record data
            if((rc=rec.GetData(data))) return (rc);
            
            //print record
            p.Print(cout, data);
        }
    }
    
    //print footer
    p.PrintFooter(cout);
    
    //close the sccan
    if((rc=filescan.CloseScan())) return (rc);
    
    //close relation file
    if((rc=Rmm->CloseFile(filehandle_r))) return (rc);
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
    DataAttrInfo * attributes;
    //initialize the attributes
    strcpy(attributes[0].relName,"Help");
    strcpy(attributes[0].attrName,"RelName");
    attributes[0].attrLength=MAXNAME;
    attributes[0].attrType=STRING;
    attributes[0].offset=0;
    attributes[0].indexNo=-1;
    
    // set a printer
    Printer p(attributes,1);
    
    //print the header
    p.PrintHeader(cout);
    
    RC rc;
    RM_FileScan filescan;
    RM_Record rec;
    char * data;
    Relation r;
    
    //open scan of relcat
    if((rc=filescan.OpenScan(fileHandle_Relcat,INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF)return (rc);
        if(rc!=RM_EOF){
            //get record data
            if((rc=rec.GetData(data))) return (rc);
            memcpy(&r,data,sizeof(Relation));
            //print the relation name
            p.Print(cout, r.relName);
        }
    }

    //print footer
    p.PrintFooter(cout);
    
    //close the scan
    if((rc=filescan.CloseScan())) return (rc);
    
    return (0);
}

RC SM_Manager::Help(const char *relName)
{
    cout << "Help\n"
    << "   relName=" << relName << "\n";
    
    DataAttrInfo attributes[6];
    //DataArrrInfo of relName
    strcpy(attributes[0].relName,"Help");
    strcpy(attributes[0].attrName,"RelName");
    attributes[0].attrType=STRING;
    attributes[0].attrLength=MAXNAME+1;
    attributes[0].offset=0;
    attributes[0].indexNo=-1;
    
    //DataArrrInfo of attrName
    strcpy(attributes[1].relName,"Help");
    strcpy(attributes[1].attrName,"AttrName");
    attributes[1].attrType=STRING;
    attributes[1].attrLength=MAXNAME+1;
    attributes[1].offset=MAXNAME+1;
    attributes[1].indexNo=-1;
    
    //DataArrrInfo of offset
    strcpy(attributes[2].relName,"Help");
    strcpy(attributes[2].attrName,"offset");
    attributes[2].attrType=INT;
    attributes[2].attrLength=4;
    attributes[2].offset=2*(MAXNAME+1);
    attributes[2].indexNo=-1;
    
    //DataArrrInfo of attribute type
    strcpy(attributes[3].relName,"Help");
    strcpy(attributes[3].attrName,"AttrType");
    attributes[3].attrType=STRING;
    attributes[3].attrLength=sizeof(AttrType);
    attributes[3].offset=2*(MAXNAME+1)+4;
    attributes[3].indexNo=-1;
  
    //DataArrrInfo of attribute length
    strcpy(attributes[4].relName,"Help");
    strcpy(attributes[4].attrName,"AttrLength");
    attributes[4].attrType=INT;
    attributes[4].attrLength=4;
    attributes[4].offset=2*(MAXNAME+1)+4+sizeof(AttrType);
    attributes[4].indexNo=-1;
    
    //DataArrrInfo of attribute indexNo
    strcpy(attributes[5].relName,"Help");
    strcpy(attributes[5].attrName,"IndexNo");
    attributes[5].attrType=INT;
    attributes[5].attrLength=4;
    attributes[5].offset=2*(MAXNAME+1)+8+sizeof(AttrType);
    attributes[5].indexNo=-1;
    
    Printer p(attributes,6);
    
    //print the header
    p.PrintHeader(cout);
    
    RC rc;
    RM_FileScan filescan;
    RM_Record rec;
    char * data;
    DataAttrInfo r;
    
    //open scan of relcat
    if((rc=filescan.OpenScan(fileHandle_Attrcat,INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF)return (rc);
        if(rc!=RM_EOF){
            //get record data
            if((rc=rec.GetData(data))) return (rc);
            memcpy(&r,data,sizeof(DataAttrInfo));
            if(r.relName==relName){
                //print the relation name
                p.Print(cout, data);
            }
        }
    }
    
    //print footer
    p.PrintFooter(cout);
    
    //close the scan
    if((rc=filescan.CloseScan())) return (rc);
    
    
    return (0);
}

//
// SM_PrintError
//
// Desc: Send a message corresponding to a SM return code to cerr
// In:   rc - return code for which a message is desired
//
void SM_PrintError(RC rc)
{
    
    // Check the return code is within proper limits
    if (rc >= START_SM_WARN && rc <= SM_LASTWARN)
        // Print warning
        cerr << "SM warning: " << SM_WarnMsg[rc - START_SM_WARN] << "\n";
    else if (rc == 0)
        cerr << "SM_PrintError called with return code of 0\n";
    else
        cerr << "SM error: " << rc << " is out of bounds\n";

    //cout << "SM_PrintError\n   rc=" << rc << "\n";
}

