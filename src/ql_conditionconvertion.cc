#include "ql_conditionconvertion.h"


RC ql_conditionConvertion::getDataAttributsByRelation(const char *relName, DataAttrInfo attributs[], int &attr_count){
    //DataAttrInfo attributes[MAXATTRS];
    RC rc;
    RM_FileScan filescan;

    //open scan of attrcat
    if((rc=filescan.OpenScan(psmm->fileHandle_Attrcat,INT, sizeof(int), 0, NO_OP , NULL))) return (rc);

    //scan all the records in attrcat
    bool flag_exist=false;
    RM_Record rec;
    int i=0;
    attr_count=0;
    char * data;
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF)return (rc);
        if(rc!=RM_EOF){
            //copy record to a_r
            rc=rec.GetData(data);
            if(!strcmp(((DataAttrInfo*)data)->relName,relName)) {
                // take all the DataAttrInfo of the relation relName
                memcpy(&attributs[i],data,sizeof(DataAttrInfo));
                flag_exist=true;
                i++;
                attr_count++;
            }
        }
    }
    if(flag_exist) return 0;
    else return -1;
}

RC ql_conditionConvertion::getDataAttributByRelAttr(const RelAttr &relAttr, DataAttrInfo &attribut){
    //DataAttrInfo attributes[MAXATTRS];
    RC rc;
    RM_FileScan filescan;
    //open scan of attrcat
    if((rc=filescan.OpenScan(psmm->fileHandle_Attrcat,INT, sizeof(int), 0, NO_OP , NULL))) return (rc);
    //scan all the records in attrcat
    bool flag_exist=false;
    RM_Record rec;
    char * data;
    while(rc!=RM_EOF){
        //get records until the end
        rc=filescan.GetNextRec(rec);
        if(rc!=0 && rc!=RM_EOF)return (rc);
        if(rc!=RM_EOF){
            //copy record to a_r
            rc=rec.GetData(data);
            if(!strcmp(((DataAttrInfo*)data)->relName,relAttr.relName)) {
                // take all the DataAttrInfo of the relation relName
                memcpy(&attribut,data,sizeof(DataAttrInfo));
                if(!strcmp(attribut.attrName,relAttr.attrName)){
                    flag_exist=true;
                    break;
                }

            }
        }
    }
    if(flag_exist) return 0;
    else return -1;

}


 //get all selection conditions
void ql_conditionConvertion::getSelCondition(int nConditions, const Condition conditions[], const char * rel,
                                 int &nConReturn, const Condition *selConds[], int offset[], int length[]){
    DataAttrInfo attributs[MAXATTRS];
    int attr_count = 0;
    getDataAttributsByRelation(rel,attributs,attr_count);
    nConReturn = 0;
    for(int i=0; i< nConditions; i++){
        if(conditions[i].bRhsIsAttr!=1){
            if(!strcmp(conditions[i].lhsAttr.relName,rel)){
            selConds[i] = &conditions[i];
            nConReturn++;
            //get selection attributs' positions
            for(int j = 0; j< attr_count; j++){
                if(!strcmp(attributs[j].attrName,conditions[i].lhsAttr.attrName)){
                    offset[i] = attributs[j].offset;
                    length[i] = attributs[j].attrLength;
                    break;
                }
            }

        }
        }
    }
}


 void ql_conditionConvertion::getJoinConditions(const int &numRel, int nConditions, const Condition conditions[],
                   int &nConReturn, Condition **selConds, int offset[], int length[]){

 }


//to get relation name for relAttr even relAttr is not "rel.attr"
const char* ql_conditionConvertion::getRelName(const RelAttr &relAttr, int nRelations, const char * const relations[]){
    if(relAttr.relName != NULL) return relAttr.relName;
    for(int j=0;j<nRelations;j++){
        DataAttrInfo l_attribut;
        char emptyName[MAXNAME+1]; //initial
        memset(emptyName, 0, MAXNAME + 1);
        char r[MAXNAME+1], a[MAXNAME+1];
        strcpy(r,relations[j]);
        strcpy(a,relAttr.attrName);
        RelAttr l_RA = {r,a};
        getDataAttributByRelAttr(l_RA,l_attribut);
        if(memcmp(l_attribut.relName,emptyName,MAXNAME+1)){
            return l_attribut.relName;
        }

    }
}
