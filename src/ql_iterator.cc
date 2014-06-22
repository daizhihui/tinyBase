#include "ql_iterator.h"

using namespace std;
filterIterator::filterIterator(const char *l_relName,
                               int l_n, const Condition **l_selCon, int l_attrLength[], int l_offset[]){
    rmm = new RM_Manager(pfm);
    rmm->OpenFile(l_relName,handle);
    this->relName = l_relName;
    this->n = l_n;
    this->selCon = l_selCon;
    this->offset = l_offset;
    this->attrLength = l_attrLength;
    RC rc = fileScan.OpenScan(handle,selCon[0]->rhsValue.type,attrLength[0],offset[0],selCon[0]->op,selCon[0]->rhsValue.data,NO_HINT);
//    std::cout << "RC=" << rc <<endl;
}
filterIterator::~filterIterator(){
    fileScan.CloseScan();
    rmm->CloseFile(handle);
    delete rmm;
}

Status filterIterator::get_next(char * &tuple){
//    cout << "getnext" << endl;
    RM_Record rec;
    //search left relation to match the current rightvalue
    bool tupleFound = false;
    char * pdata;
    while(!tupleFound){
        RC rc = fileScan.GetNextRec(rec);
        if(rc==RM_EOF)    {
            return endOfIteration;
        }

        rc = rec.GetData(pdata);
//        cout << *(int*)(tuple) << endl;
        tupleFound = true;
        //compare all other data
        for(int i = 1; i<n; i++){
            float cmp = Compare(pdata+offset[i],selCon[i]->rhsValue.data,
                                selCon[i]->rhsValue.type,0,attrLength[i]);       
            if(cmp==0&&(selCon[i]->op!=EQ_OP||selCon[i]->op!=LE_OP||selCon[i]->op!=GE_OP)) {
                tupleFound=false;
                break;
            }
            else if(cmp>0&&(selCon[i]->op!=GT_OP||selCon[i]->op!=GE_OP)){
                tupleFound=false;
                break;
            }
            else if(cmp<0&&(selCon[i]->op!=LT_OP||selCon[i]->op!=LE_OP)){
                tupleFound=false;
                break;
            }
        }
    }
    tuple = new char[strlen(pdata)+1];  //to remember rec is local variable, at the end of this function, deconstructer is called to delete the pdata
    strcpy(tuple,pdata);
    return success;
}  // The tuple is returned.

//
// Compare
//
float filterIterator::Compare(void *_value, void *value1,AttrType attrType, int attrLength1,int attrLength2 )
{
   float cmp;
   int i1, i2;
   float f1, f2;

   // Do comparison according to the attribute type
   switch (attrType) {
   case INT:
      memcpy(&i1, _value, sizeof(int));
      memcpy(&i2, _value, sizeof(int));
      cmp = i1 - i2;
      break;

   case FLOAT:
      memcpy(&f1, _value, sizeof(float));
      memcpy(&f2, _value, sizeof(float));
      cmp = f1 - f2;
      break;

   case STRING:
      cmp = attrLength1 - attrLength2;
      if(cmp ==0){
        cmp = memcmp(_value, value1, attrLength1);
      }
      break;
   }
    return cmp;
}
/*
nested_loops_join::nested_loops_join(AttrType    in1[],      // Array containing field types of R.
                                     int     len_in1,        // # of columns in R.
                                     short   t1_str_sizes[],

                                     AttrType    in2[],      // Array containing field types of S.
                                     int     len_in2,        // # of columns in S.
                                     short   t2_str_sizes[],

                                     //int     join_col_in1,   // The col of R to be joined with
//                                     int     join_col_in2,   // the col of S.
//                                     CompOp comps,
//                                     int     sizeConds,

                                     int     amt_of_mem,     // IN PAGES

                                     Iterator *    am1,      // access method for left i/p to join
//                                     Iterator *    am2,
                                     const char* relationName,// access method for right i/p to join

                                     Condition **outFilter,   // Ptr to the output filter
                                     Condition **rightFilter, // Ptr to filter applied on right i/p
                                     //result
                                     const RelAttr  * proj_list,  //projectionList
                                     int        n_out_flds,
                                     Status   & s)
{

    if((*outFilter)!=NULL){
        this->join_col_in1 = (*outFilter);
        this->join_col_in2 = join_col_in2;
    }
    this->am1 = am1;
    this->in1 = in1;

    this->rmm->OpenFile(relationName,handle);
    offset1=0;
    offset2=0;
    for(int i=0; i<join_col_in1; i++)
        offset1 += t1_str_sizes[i];
    for(int i=0; i<join_col_in2; i++)
        offset2 += t2_str_sizes[i];
}


Status nested_loops_join::get_next(RM_Record *&tuple){
    RM_Record *tuple1;
    RM_Record *tuple2;
    char *pdata1;
    char *pdata2;
    memcpy(pdata1,leftData+offset1,in1[join_col_in1]);
    //search right relation to match the current leftvalue
    fileScan.OpenScan(handle,in2[join_col_in2],offset2,comps,pdata1,NO_HINT);
    //if right relation has not tuples that matches
    if(fileScan.GetNextRec(tuple2)==RM_EOF) {
        //get a new left tuple
        while(_am1->get_next(tuple1)!=endOfIteration)
        {
            //update current leftValue
            tuple1->GetData(leftData);
            memcpy(pdata1,leftData+offset1,in1[join_col_in1]);
            //compare the new left value
            fileScan.OpenScan(handle,in2[join_col_in2],offset2,comps,pdata1,NO_HINT);
            //find a match right tuple
            if(fileScan.GetNextRec(tuple2)!=RM_EOF) {
                tuple2->GetData(rightData);
                break;
            }
        }
        //left relation has no longer tuples
        if(am1->get_next(tuple1)==endOfIteration)  return endOfIteration;
    }
    else{
        tuple2->GetData(rightData);
    }
    char * pdata;
    tuple->GetData(pdata);
    //RelAttr relA = proj_list[i];//a attribut to project
    //getDataAttribut(relA);

    //no projection
    memcpy(pdata,leftData,sizeof(leftData));
    memcpy(pdata+sizeof(leftData),rightData,sizeof(rightData));
    return success;
}

*/
