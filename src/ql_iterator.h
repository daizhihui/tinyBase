#ifndef QL_ITERATOR_H
#define QL_ITERATOR_H
//#include "ql.h"
#include "rm.h"
#include "redbase.h"
#include "parser.h"
#include <cstring>

enum Status{
    endOfIteration,
    success,
    NotRoot
};

//iterator
class Iterator{
public:
    virtual Status get_next(char * &tuple) = 0;

};

//
// This file contains the interface for the joins.
// We name the two relations being joined as R and S.
//


class filterIterator : public Iterator{

public:
    filterIterator(const char *relName,
                   int n, const Condition **selCon, int attrLength[], int offset[] );
    Status get_next(char * &tuple);  // The tuple is returned.
    ~filterIterator();
private:
    PF_Manager pfm;
    RM_Manager* rmm;
    RM_FileHandle handle;
    RM_FileScan fileScan; //to file scan
    const char *relName;
    int n;
    const Condition **selCon;
    int * offset;
    int *attrLength;
    float Compare(void *_value, void *value1,AttrType attrType, int attrLength1,int attrLength2);

};

/*
class Select : public Iterator
{
 public:
   Select(AttrType in[],             // Array containing field types of R.
          int     len_in,            // # of columns in R.

          Iterator *  am,            // access method for input to select
          Condition * outFilter       // Ptr to a predicate list
         );

   Status get_next(RM_Record * &tuple);  // The tuple is returned.

   ~Select();
};

class IndexSelect : public Iterator
{
 public:
   IndexSelect(AttrType  in[],       // Array containing field types
               int      len_in,      // # of columns in

               int      indexCol,    // The column on which the index is on.
               Value    lowerBound,  // The lower bound
               Value    upperBound,  // The upper bound
               CompOp op1,         // op1 is either < or <=
               CompOp op2,         // and so is op2. Together they capture all
                                     // ALL index conditions.
               char   * indexName,   // Name of the index file.
               char   * relationName,
               Condition *outFilter
              );

   Status get_next(RM_Record * &tuple);
   ~IndexSelect();
};

class project : public Iterator
{

 public:
   project(AttrType in[],        // Array containing field types of R.
           int     len_in,       // # of columns in R.

           int     permutation_array[], // array of permutations.
                                 // If a field i is filtered out,
                                 // the value p_a[i] = -1. Else, if field i is
                                 // in position j in the output, p_a[i] = j.
           Iterator *am          // access method for left input to join.
         );

  ~project();

   ProjectError get_next_tuple(char * &tuple);     // The tuple is returned.

   void perror(ProjectError error_code); // Prints an error message correlated
                                         // to error_code
};

class FileScanIter : public Iterator {

// File Scan iterator will sequentially scan the file and perform
// selections and projections.

public:
    FileScanIter(
        char* relName, 	// Name of the input relation
        AttrType types[],	// Array of types in this relation
        short* str_sizes,	// Array of string sizes
        int noInFlds,		// Number of fields in input tuple
        int noOutFlds,		// Number of fields in output tuple
        FldSpec outFlds[],	// Fields to project
        CondExpr* selects[],	// Conditions to apply
        Status& s
    );

    Status reset(){};

    // Reset is needed in nested loop join for the inner relation.

    Status get_next(Tuple*& tuple){}
    ~FileScanIter(){}
};

class IndexScanIter : public Iterator {

// Index Scan iterator will directly access the required tuple using
// the provided key. It will also perform selections and projections.

public:
    IndexScanIter(
        IndexType index,	// Type of the index
        char* relName, 	// Name of the input relation
        char* indName, 	// Name of the input index
        AttrType types[],	// Array of types in this relation
        short* str_sizes,	// Array of string sizes
        int noInFlds,		// Number of fields in input tuple
        int noOutFlds,		// Number of fields in output tuple
        FldSpec outFlds[],	// Fields to project
        CondExpr* selects[],	// Conditions to apply
        Status& s
    );

    Status get_next(Tuple*& tuple){}
    ~IndexScanIter(){}
};


class nested_loops_join : public Iterator
{
 public:
    nested_loops_join(
                      AttrType    in1[],      // Array containing field types of R.
                      int     len_in1,        // # of columns in R.
                      short   t1_str_sizes[],

                      AttrType    in2[],      // Array containing field types of S.
                      int     len_in2,        // # of columns in S.
                      short   t2_str_sizes[],
                      int     amt_of_mem,     // IN PAGES

                      Iterator *    am1,      // access method for left i/p to join
                      Iterator *    am2,
                      const char* relationName, // access method for right i/p to join

                      Condition **outFilter,   // Ptr to the output filter
                      Condition **rightFilter, // Ptr to filter applied on right i/p

                      //output info
                      FldSpec   * proj_list,  //projectionList
                      int        n_out_flds,
                      Status   & s
                      );
   Status get_next(RM_Record * &tuple);          // The tuple is returned.

  ~nested_loops_join();
private:
   AttrType    in1[];      // Array containing field types of S.
   int     len_in1;       // # of columns in S.
   short   t1_str_sizes[];

   AttrType    in2[];      // Array containing field types of S.
   int     len_in2;       // # of columns in S.
   short   t2_str_sizes[];

   int     amt_of_mem;     // IN PAGES

   Iterator *    am1;      // access method for left i/p to join
   //Iterator *    am2;
   const char* relationName;// access method for right i/p to join

   //CondExpr **outFilter,   // Ptr to the output filter
   Condition **rightFilter; // Ptr to filter applied on right i/p
   FldSpec  * proj_list;
   int        n_out_flds;
   Status  s ;
   RM_FileHandle handle;
   RM_Manager *rmm;
   RM_FileScan fileScan;
   char * leftData;
   char * rightData;
   int offset1, offset2;
   int     join_col_in1;   // The col of R to be joined with
   int     join_col_in2;
};
*/
#endif // QL_ITERATOR_H
