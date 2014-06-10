//
// dbcreate.cc
//
// Author: Jason McHugh (mchughj@cs.stanford.edu)
//
// This shell is provided for the student.

#include <iostream>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include "rm.h"
#include "sm.h"
#include "redbase.h"

using namespace std;

//
// main
//
main(int argc, char *argv[])
{
    char *dbname;
    char command[255] = "mkdir ";
    RC rc;

    // Look for 2 arguments. The first is always the name of the program
    // that was executed, and the second should be the name of the
    // database.
    if (argc != 2) {
        cerr << "Usage: " << argv[0] << " dbname \n";
        exit(1);
    }

    // The database name is the second argument
    dbname = argv[1];

    // Create a subdirectory for the database
    system (strcat(command,dbname));

    if (chdir(dbname) < 0) {
        cerr << argv[0] << " chdir error to " << dbname << "\n";
        exit(1);
    }

    // Create the system catalogs...
    //add by Xiao
    PF_Manager pfm;
    RM_Manager rmn(pfm);
    //create Catelog relcat, record type: Struct Relation
    rmn.CreateFile("relcat", sizeof(Relation));
    //create Catelog attrcat, record type: Struct Attr_Relation
    rmn.CreateFile("attrcat", sizeof(Attr_Relation));

    // Fair amount to be filled in here!!

    return(0);
}
