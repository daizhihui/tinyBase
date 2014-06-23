#ifndef QL_CONDITIONCONVERTION_H
#define QL_CONDITIONCONVERTION_H
#include"sm.h"
#include "redbase.h"
#include "ql.h"
class ql_conditionConvertion
{
public:
    ql_conditionConvertion(SM_Manager *ps):psmm(ps){}
    RC getDataAttributsByRelation(const char *relName, DataAttrInfo attributs[], int &attr_count);
    RC getDataAttributByRelAttr(const RelAttr &relAttr, DataAttrInfo &attribut);
    void getSelCondition(int nConditions, const Condition conditions[], const char * rel,
                         int &nConReturn, const Condition *selConds[], int offset[], int length[]);
    void getJoinConditions(const int &numRel, int nConditions, const Condition conditions[],
                      int &nConReturn, Condition **selConds, int offset[], int  length[]);
    //Status iteratorExecution(QueryTree * tree, RM_Record *&input, RM_Record *&output);
    const char* getRelName(const RelAttr &relAttr, int nRelations, const char * const relations[]);
private:
     SM_Manager *psmm;
};

#endif // QL_CONDITIONCONVERTION_H
