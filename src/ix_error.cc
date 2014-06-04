//
// File:        ix_error.cc
// Description: IX_PrintError function
// Author:      Zhihui DAI
//

#include <cerrno>
#include <cstdio>
#include <iostream>
#include "ix_internal.h"

using namespace std;

//
// Error table
// TODO
static char *IX_WarnMsg[] = {
  (char*)"invalid comparison operator",
  (char*)"invalid attribute parameters",
  (char*)"null pointer",
  (char*)"scan open",
  (char*)"scan closed",
  (char*)"file closed"
};

// 
// IX_PrintError
//
// Desc: Send a message corresponding to a RC return code to cerr
// In:   rc - return code for which a message is desired
//
void IX_PrintError(RC rc)
{
  // Check the return code is within proper limits
  if (rc >= START_IX_WARN && rc <= END_IX_WARN)
    // Print warning
    cerr << "IX warning: " << IX_WarnMsg[rc - START_IX_WARN] << "\n";
  else if (rc == 0)
    cerr << "IX_PrintError called with return code of 0\n";
  else
    cerr << "IX error: " << rc << " is out of bounds\n";
}

