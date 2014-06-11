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

RC SM_Manager::CreateTable(const char *relName,
                           int        attrCount,
                           AttrInfo   *attributes)
{
    RC rc;
    
    //check if relation name begin with letter
    if(!(relName[0] <= 'z' && relName[0] >= 'a') || (relName[0] <= 'Z' && relName[0] >= 'A')) return (SM_INVALIDRELNAME);
    
    // relation name can't be longer than MAXNAME
    if ((unsigned)strlen(relName)>=MAXNAME) return (SM_INVALIDRELNAME);
    
    int recordsize=0;
    for (int i=0;i<attrCount;i++){
        
        //check if attribute name begin with letter
        if(!(attributes[i].attrName[0] <= 'z' && attributes[i].attrName[0] >= 'a') || (attributes[i].attrName[0] <= 'Z' && attributes[i].attrName[0] >= 'A')) return (SM_INVALIATTRNAME);
        
        // attribute name can't be longer than MAXNAME
        if ((unsigned)strlen(attributes[i].attrName)>=MAXNAME) return (SM_INVALIATTRNAME);
        
        //calculate the total length of attributes
        recordsize += attributes[i].attrLength;
        
        
    }
    
    //Call RM_Manager::CreateFile to create table file
    if((rc=Rmm->CreateFile(relName, recordsize))) goto err_return;
    
err_return:
    return (rc);
    
    
    cout << "CreateTable\n"
         << "   relName     =" << relName << "\n"
         << "   attrCount   =" << attrCount << "\n";
    for (int i = 0; i < attrCount; i++)
        cout << "   attributes[" << i << "].attrName=" << attributes[i].attrName
             << "   attrType="
             << (attributes[i].attrType == INT ? "INT" :
                 attributes[i].attrType == FLOAT ? "FLOAT" : "STRING")
             << "   attrLength=" << attributes[i].attrLength << "\n";
    

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

