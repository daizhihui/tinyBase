//
// redbase.cc
//
// Author: Jason McHugh (mchughj@cs.stanford.edu)
//
// This shell is provided for the student.

#include <iostream>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include "redbase.h"
#include "rm.h"
#include "sm.h"
#include "ix.h"
#include "ql.h"

using namespace std;

//
// main
//
int main(int argc, char *argv[])
{
    char *dbname;
    RC rc;

    // Look for 2 arguments.  The first is always the name of the program
    // that was executed, and the second should be the name of the
    // database.
    if (argc != 2) {
        cerr << "Usage: " << argv[0] << " dbname \n";
        exit(1);
    }

    // *********************************
    //
    // Fair amount to be filled in here!!
    //
    // *********************************

    // The database name is the second argument
   // dbname = strrchr( argv[1], '/' );
    dbname=argv[1];
    
// initialize RedBase components
    PF_Manager pfm;
    RM_Manager rmm(pfm);
    IX_Manager ixm(pfm);
    SM_Manager smm(ixm, rmm);
    QL_Manager qlm(smm, ixm, rmm);
    // open the database
    if ((rc = smm.OpenDb(dbname))) goto err_return;
        // call the parser
        RBparse(pfm, smm, qlm);
    
    // close the database
    if ((rc = smm.CloseDb())) goto err_return;
        
    cout << "Bye.\n";
    
err_return:
    return (rc);
}
