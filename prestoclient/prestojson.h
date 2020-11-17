#ifndef PRESTOJSON_HH
#define PRESTOJSON_HH

#include "json.h"
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include "prestoclient.h"
#include "prestoclienttypes.h"

enum JSON_RESULT_SECTION {
    ROOT = 0,
    COLUMNS ,
    DATA ,
    STATS ,
    ERROR_SECTION , 
    WARNINGS 
};

enum JSON_RESULT_HEADER {
    ID = 0,
    INFO ,
    NEXT ,
    CANCEL
};

typedef struct IT_PARSINGSTATE {
    size_t level;
    size_t aopen;
    size_t aclose;    
    size_t oopen;
    size_t oclose;
    enum JSON_RESULT_SECTION section;    //!< top level section
    enum JSON_RESULT_HEADER  header;     //!< top level direct values (urls, ids)
    int currentdatacolumn;
    int inColumnName;                   //!< column tags, name
    int inRawType;                      //!< column tags, raw type name
    int inValue;                        //!< column tag parameter to type name (such as length of varchar)
    int state;    
    int inErrorType;
    int inErrorMessage;
} PARSINGSTATE;

int presto_json_parser(JSON_TYPE typ, const char* data, size_t size, void* userdata);

#endif
