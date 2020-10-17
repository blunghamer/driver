#include "prestojson.h"
#include <assert.h>

#define min(a, b) \
    ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _b : _a; })

static void debug_print_value(const char *data, size_t size, char *sep)
{    
    printf("%.*s%s", (int)size, data, sep);	    
}

// FIXME: This should be an util library
// malloc/realloc memory for the variable and copy the newvalue to the variable. Exit on failure
static void alloc_copyn(char **var, const char *newvalue, size_t len)
{
    size_t newlength, currlength = 0;
    assert(var);
    assert(newvalue);
    newlength = (len + 1) * sizeof(char);

    if (*var)
    {
        currlength = (strlen(*var)) * sizeof(char);
        if (currlength < newlength)
            *var = (char *)realloc((char *)*var, newlength);
    }
    else
    {
        *var = (char *)malloc(newlength);
        // Not doing rigorous checking and handling of all malloc's because:
        // - On the intended platforms for this code (Linux, windows boxes with lots of (virtual)memory) malloc failures are very rare
        // - Because such failures are rare it's very difficult to test proper malloc handling code
        // - Handling failures will likely also fail
        // Whenever an alloc fails we're doing an: exit(1)
        if (!var)
            exit(1);
    }

    strncpy(*var, newvalue, len);
    (*var)[len] = '\0';
}

static void write_column_value(const char *data, size_t size, int colidx, PRESTOCLIENT_RESULT *result)
{
    size_t increment;
    // printf("currentcolumn: %i, columcount: %li\n", result->currentdatacolumn, result->columncount);
    result->columns[colidx]->dataisnull = false;
    if (size > result->columns[colidx]->databuffersize)
    {
        increment = 1000;
        if (size + 1 > increment) 
            increment = size + 1;

        result->columns[colidx]->data = (char *)realloc((char *)result->columns[colidx]->data, (result->columns[colidx]->databuffersize + increment) * sizeof(char));
        if (!result->columns[colidx]->data)
            exit(1);
        result->columns[colidx]->databuffersize += increment;           
    }

    result->columns[colidx]->dataactualsize = size;  
    strncpy(result->columns[colidx]->data, data, size);
    result->columns[colidx]->data[size] = 0;
}

static void append_column_value(const char *data, size_t size, int colidx, PRESTOCLIENT_RESULT *result, char delimiter)
{
    // printf("Appending: >%.*s< adding: %li, buffersize: %li\n", (int)size, data, size, result->columns[colidx]->dataactualsize);	
    size_t increment;
    result->columns[colidx]->dataisnull = false;

    // maybe we should not allocate in very small steps but just increase in blocks 
    if ((result->columns[colidx]->dataactualsize) + size + 2 > result->columns[colidx]->databuffersize)
    {
        increment = 1000;
        if (size + 2 > increment) 
            increment = size + 2;

        result->columns[colidx]->data = (char *)realloc((char *)result->columns[colidx]->data, (result->columns[colidx]->databuffersize + increment) * sizeof(char) );
        if (!result->columns[colidx]->data)
            exit(1);

        result->columns[colidx]->databuffersize += increment;
    }

    // overwriting the '\0' of last string....
    strncpy((result->columns[colidx]->data + result->columns[colidx]->dataactualsize), data, size);
    result->columns[colidx]->dataactualsize += size + 1;    
    result->columns[colidx]->data[result->columns[colidx]->dataactualsize-1] = delimiter;
    
    result->columns[colidx]->data[result->columns[colidx]->dataactualsize] = 0;
}


static enum E_FIELDTYPES str_to_type(const char * typestr, size_t size) {
    if (strncmp(typestr, "tinyint", min(size, (size_t)7)) == 0)
    {
        return PRESTOCLIENT_TYPE_TINYINT;
    }
    else if (strncmp(typestr, "smallint", min(size, (size_t)8)) == 0)
    {
        return PRESTOCLIENT_TYPE_SMALLINT;
    }
    else if (strncmp(typestr, "integer", min(size, (size_t)7)) == 0)
    {
        return PRESTOCLIENT_TYPE_INTEGER;
    }
    else if (strncmp(typestr, "bigint", min(size, (size_t)6)) == 0)
    {
        return PRESTOCLIENT_TYPE_BIGINT;
    }
    else if (strncmp(typestr, "boolean", min(size, (size_t)7)) == 0)
    {
        return PRESTOCLIENT_TYPE_BOOLEAN;
    }
    else if (strncmp(typestr, "real", min(size, (size_t)4)) == 0)
    {
        return PRESTOCLIENT_TYPE_REAL;
    }
    else if (strncmp(typestr, "double", min(size, (size_t)6)) == 0)
    {
        return PRESTOCLIENT_TYPE_DOUBLE;
    }
    else if (strncmp(typestr, "date", min(size, (size_t)4)) == 0)
    {
        return PRESTOCLIENT_TYPE_DATE;
    }
    else if (strncmp(typestr, "timestamp(3) with time zone", min(size, (size_t)24)) == 0)
    {
        return PRESTOCLIENT_TYPE_TIMESTAMP_WITH_TIME_ZONE;
    }
    else if (strncmp(typestr, "timestamp with time zone", min(size, (size_t)24)) == 0)
    {
        return PRESTOCLIENT_TYPE_TIMESTAMP_WITH_TIME_ZONE;
    }
    else if (strncmp(typestr, "timestamp", min(size, (size_t)9)) == 0)
    {
        return PRESTOCLIENT_TYPE_TIMESTAMP;
    }
    else if (strncmp(typestr, "time(3) with time zone", min(size, (size_t)19)) == 0)
    {
        return PRESTOCLIENT_TYPE_TIME_WITH_TIME_ZONE;
    }
    else if (strncmp(typestr, "time with time zone", min(size, (size_t)19)) == 0)
    {
        return PRESTOCLIENT_TYPE_TIME_WITH_TIME_ZONE;
    }
    else if (strncmp(typestr, "time", min(size, (size_t)4)) == 0)
    {
        return PRESTOCLIENT_TYPE_TIME;
    }
    else if (strncmp(typestr, "interval year to month", min(size, (size_t)22)) == 0)
    {
        return PRESTOCLIENT_TYPE_INTERVAL_YEAR_TO_MONTH;
    }
    else if (strncmp(typestr, "interval day to second", min(size, (size_t)22)) == 0)
    {
        return PRESTOCLIENT_TYPE_INTERVAL_DAY_TO_SECOND;
    }
    else if (strncmp(typestr, "varchar", min(size, (size_t)7)) == 0)
    {
        return PRESTOCLIENT_TYPE_VARCHAR;
    }
    else if (strncmp(typestr, "array", min(size, (size_t)5)) == 0)
    {
        return PRESTOCLIENT_TYPE_ARRAY;
    }
    else if (strncmp(typestr, "map", min(size, (size_t)3)) == 0)
    {
        return PRESTOCLIENT_TYPE_MAP;
    }
    else if (strncmp(typestr, "json", min(size, (size_t)4)) == 0)
    {
        return PRESTOCLIENT_TYPE_JSON;
    }
    else
    {
        // so we have a type we cannot work with, bail out? or set to VARCHAR
        // exiting
        printf("unable to work with type >%.*s< setting to varchar\n", (int)size, typestr);
        return PRESTOCLIENT_TYPE_VARCHAR;
        // we are lacking those...
        // IPADDRESS
        // UUID
        // HyperLogLog
        // P4HyperLogLog
        // QDigest
    }
}

static void apply_raw_type(const char *data, size_t size, PRESTOCLIENT_RESULT *result)
{
    enum E_FIELDTYPES ft = str_to_type(data, size);
    result->columns[result->columncount - 1]->type = ft;
    result->columns[result->columncount - 1]->bytesize = E_FIELDTYPES_SIZES[ft];

    /*
    if (strncmp(data, "tinyint", min(size, (size_t)7)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TINYINT;
        result->columns[result->columncount - 1]->bytesize = 1;
    }
    else if (strncmp(data, "smallint", min(size, (size_t)8)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_SMALLINT;
        result->columns[result->columncount - 1]->bytesize = 2;
    }
    else if (strncmp(data, "integer", min(size, (size_t)7)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_INTEGER;
        result->columns[result->columncount - 1]->bytesize = 4;
    }
    else if (strncmp(data, "bigint", min(size, (size_t)6)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_BIGINT;
        result->columns[result->columncount - 1]->bytesize = 8;
    }
    else if (strncmp(data, "boolean", min(size, (size_t)7)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_BOOLEAN;
        result->columns[result->columncount - 1]->bytesize = 1;
    }
    else if (strncmp(data, "real", min(size, (size_t)4)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_REAL;
        result->columns[result->columncount - 1]->bytesize = 4;
    }
    else if (strncmp(data, "double", min(size, (size_t)6)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_DOUBLE;
        result->columns[result->columncount - 1]->bytesize = 8;
    }
    else if (strncmp(data, "date", min(size, (size_t)4)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_DATE;
        result->columns[result->columncount - 1]->bytesize = 10;
    }
    else if (strncmp(data, "timestamp with time zone", min(size, (size_t)24)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIMESTAMP_WITH_TIME_ZONE;
        result->columns[result->columncount - 1]->bytesize = 30;
    }
    else if (strncmp(data, "timestamp", min(size, (size_t)9)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIMESTAMP;
        result->columns[result->columncount - 1]->bytesize = 23;
    }
    else if (strncmp(data, "time with time zone", min(size, (size_t)19)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIME_WITH_TIME_ZONE;
        result->columns[result->columncount - 1]->bytesize = 20;
    }
    else if (strncmp(data, "time", min(size, (size_t)4)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIME;
        result->columns[result->columncount - 1]->bytesize = 14;
    }
    else if (strncmp(data, "interval year to month", min(size, (size_t)22)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_INTERVAL_YEAR_TO_MONTH;
        result->columns[result->columncount - 1]->bytesize = 20;
    }
    else if (strncmp(data, "interval day to second", min(size, (size_t)22)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_INTERVAL_DAY_TO_SECOND;
        result->columns[result->columncount - 1]->bytesize = 20;
    }
    else if (strncmp(data, "varchar", min(size, (size_t)7)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_VARCHAR;
        // will get it from the arguments in json
    }
    else if (strncmp(data, "array", min(size, (size_t)5)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_ARRAY;
        result->columns[result->columncount - 1]->bytesize = 100;
        // result->columns[result->columncount - 1]->bytesize = 2147483647;
    }
    else if (strncmp(data, "map", min(size, (size_t)3)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_MAP;
        result->columns[result->columncount - 1]->bytesize = 100;
    }
    else if (strncmp(data, "json", min(size, (size_t)4)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_JSON;
        result->columns[result->columncount - 1]->bytesize = 100;
    }
    else
    {
        // so we have a type we cannot work with, bail out? or set to VARCHAR
        // exiting
        printf("unable to work with type >%.*s< setting to varchar\n", (int)size, data);
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_VARCHAR;
        // we are lacking those...
        // IPADDRESS
        // UUID
        // HyperLogLog
        // P4HyperLogLog
        // QDigest
    }
    */
}



int presto_json_parser(JSON_TYPE typ, const char *data, size_t size, void *user_data)
{
    // debug_print_value(data,size,"DEBUG ");
    PRESTOCLIENT_RESULT *result = (PRESTOCLIENT_RESULT *)user_data;
    PARSINGSTATE *pstate = (PARSINGSTATE *)result->parserstate;
    switch (typ)
    {
    case JSON_ARRAY_BEG:
        pstate->level++;
        pstate->aopen++;
        if (pstate->section == DATA)
        {
            if (pstate->level > 3)
            {                           
                append_column_value("[", 1, pstate->currentdatacolumn, result, pstate->level == 4 ? ' ' : ' ' );
            }
            else if (pstate->level == 3)
            {
                pstate->currentdatacolumn = -1;
            }
        }
        break;
    case JSON_ARRAY_END:
        pstate->level--;
        pstate->aclose++;
        if (pstate->section == DATA)
        {
            if (pstate->level == 2)
            {
                // this is where the write callback function is invoked end of array (meaning end of row in result set)
                result->write_callback_function(result->user_data, result);
            }
            else if (pstate->level >= 3)
            {             
                append_column_value("]", 1, pstate->currentdatacolumn, result, pstate->level == 3 ? ' ' : ' ' );
            }
        }
        break;
    case JSON_OBJECT_BEG:
        pstate->level++;
        pstate->oopen++;
        if (pstate->section == DATA)
        {
            if (pstate->level > 3)
            {                
                pstate->currentdatacolumn++;
                append_column_value("{", 1, pstate->currentdatacolumn, result, pstate->level == 4 ? ' ' : ' ' );
            }
        }
        break;
    case JSON_OBJECT_END:
        pstate->level--;
        pstate->oclose++;
        if (pstate->section == DATA)
        {
            if (pstate->level >= 3)
            {
                append_column_value("}", 1, pstate->currentdatacolumn, result, pstate->level == 3 ? ' ' : ' ' );
            }
        }
        break;
    case JSON_KEY:
        if (pstate->level == 1)
        {
            if (strncmp(data, "id", 2) == 0)
            {
                pstate->header = ID;
            }
            else if (strncmp(data, "infoUri", 7) == 0)
            {
                pstate->header = INFO;
            }
            else if (strncmp(data, "nextUri", 7) == 0)
            {
                pstate->header = NEXT;
            }
            else if (strncmp(data, "partialCancelUri", 16) == 0)
            {
                pstate->header = CANCEL;
            }
            else if (strncmp(data, "columns", 7) == 0)
            {
                // skip parsing if we have already found columns / they are sent at the end of the message again
                if (result->columncount == 0 ) {
                    pstate->section = COLUMNS;
                }
            }
            else if (strncmp(data, "data", 4) == 0)
            {
                pstate->section = DATA;
            }
            else if (strncmp(data, "stats", 5) == 0)
            {
                pstate->section = STATS;
            }
            else if (strncmp(data, "error", 5) == 0)
            {
                pstate->section = ERROR;
            }
            else if (strncmp(data, "warnings", 8) == 0)
            {
                pstate->section = WARNINGS;
            }
        }
        else if (pstate->section == COLUMNS)
        {            
            if (strncmp(data, "rawType", 7) == 0)
            {
                pstate->inRawType = 1;
            }
            else if (strncmp(data, "name", 4) == 0)
            {
                pstate->inColumnName = 1;
            }
            else if (strncmp(data, "value", 5) == 0)
            {
                pstate->inValue = 1;
            }
        }
        else if (pstate->section == DATA)
        {
            if (pstate->level > 3)
            {
                append_column_value(data, size, pstate->currentdatacolumn, result, ':');                
            }
        }
        else if (pstate->section == ERROR)
        {
            if (pstate->level == 2)
            {
                if (strncmp(data, "type", 4) == 0)
                {
                    pstate->inErrorType = 1;
                }
                else if (strncmp(data, "errorType", 9) == 0)
                {
                    pstate->inErrorMessage = 1;
                }
            }
        }
        else if (pstate->section == STATS)
        {
            if (pstate->level == 2)
            {
                if (strncmp(data, "state", 5) == 0)
                {
                    pstate->state = 1;
                }
            }
        }
        break;
    case JSON_STRING:
        if (pstate->level == 1)
        {
            if (pstate->header == ID)
            {
                // debug_print_value(data, size, " = ID\n");
            }
            else if (pstate->header == INFO)
            {
                // debug_print_value(data, size, " = INFO_URL\n");
                alloc_copyn(&result->lastinfouri, data, size);
            }
            else if (pstate->header == NEXT)
            {
                // debug_print_value(data, size, " = NEXT_URL\n");
                alloc_copyn(&result->lastnexturi, data, size);
            }
            else if (pstate->header == CANCEL)
            {
                // debug_print_value(data, size, " = PARTIAL_CANCEL_URL\n");
                alloc_copyn(&result->lastcanceluri, data, size);
            }
        }
        else if (pstate->section == COLUMNS)
        {
            if (pstate->inColumnName)
            {
                pstate->inColumnName = 0;
                result->columncount++;

                // debug_print_value(data, size, "Column Name\n");
                if (result->columncount == 1)
                    result->columns = (PRESTOCLIENT_COLUMN **)malloc(sizeof(PRESTOCLIENT_COLUMN *) *1 );
                else
                    result->columns = (PRESTOCLIENT_COLUMN **)realloc((PRESTOCLIENT_COLUMN **)result->columns, result->columncount * sizeof(PRESTOCLIENT_COLUMN *));

                if (!result->columns)
                {
                    exit(1);
                }
                result->columns[result->columncount - 1] = new_prestocolumn();               
                alloc_copyn(&result->columns[result->columncount - 1]->name, data, size);
                alloc_copyn(&result->columns[result->columncount - 1]->catalog, "unknown", 7 );
                alloc_copyn(&result->columns[result->columncount - 1]->schema, "unknown", 7 );
                alloc_copyn(&result->columns[result->columncount - 1]->table, "unknown", 7);
            }
            else if (pstate->inRawType)
            {
                pstate->inRawType = 0;
                // debug_print_value(data, size, ";");
                apply_raw_type(data, size, result);
            }
        }
        else if (pstate->section == DATA)
        {
            if (pstate->level == 3)
            {
                pstate->currentdatacolumn++;
                // debug_print_value(data, size, ";");
                write_column_value(data, size, pstate->currentdatacolumn, result);
            }
            else if (pstate->level > 3)
            {
                append_column_value(data, size, pstate->currentdatacolumn, result, ',');                
            }
        }
        else if (pstate->section == STATS)
        {
            if (pstate->state)
            {
                // debug_print_value(data, size, "\n");
                alloc_copyn(&result->laststate, data, size);
                pstate->state = 0;
            }
        }
        else if (pstate->section == ERROR)
        {
            if (pstate->inErrorType)
            {
                // debug_print_value(data, size, " => Errortype\n");
                alloc_copyn(&result->lasterrormessage, data, size);
                pstate->inErrorType = 0;
            }
            else if (pstate->inErrorMessage)
            {
                // debug_print_value(data, size, " => Errormessage\n");
                alloc_copyn(&result->lasterrormessage, data, size);
                pstate->inErrorMessage = 0;
            }
        }
        break;
    case JSON_NUMBER:
        switch (pstate->section)
        {
        case DATA:
            if (pstate->level == 3)
            {
                pstate->currentdatacolumn++;
                // debug_print_value(data, size, ";");
                write_column_value(data, size, pstate->currentdatacolumn, result);
            }
            else if (pstate->level > 3)
            {
                // debug_print_value(data, size, ",");
                append_column_value(data, size, pstate->currentdatacolumn, result, ',');
            }
            break;
        case COLUMNS:
            if (pstate->inValue)
            {
                // debug_print_value(data, size, ";");
                pstate->inValue = 0;

                char *wherestopped;
                char *tmp = malloc(sizeof(char) * (size + 1));
                strncpy(tmp, data, size);
                tmp[size] = 0;

                result->columns[result->columncount - 1]->bytesize = strtol(tmp, &wherestopped, 0);
                if (result->columns[result->columncount - 1]->bytesize == 2147483647)
                {
                    result->columns[result->columncount - 1]->bytesize = 100;
                }

                free(tmp);
            }
            break;
        case ROOT:
        case ERROR:
        case WARNINGS:
        case STATS:
            break;
        }
        break;
    case JSON_FALSE:
        if (pstate->section == DATA)
        {
            if (pstate->level == 3)
            {
                pstate->currentdatacolumn++;
                // debug_print_value(data, size, ";");
                write_column_value("false", 5, pstate->currentdatacolumn, result);
            }
            else if (pstate->level > 3)
            {
                // debug_print_value(data, size, ",");
                append_column_value("false", 5, pstate->currentdatacolumn, result, ',');
            }
        }
        break;
    case JSON_TRUE:
        if (pstate->section == DATA)
        {
            if (pstate->level == 3)
            {
                pstate->currentdatacolumn++;
                // debug_print_value(data, size, ";");
                write_column_value("true", 4, pstate->currentdatacolumn, result);
            }
            else if (pstate->level > 3)
            {
                // debug_print_value(data, size, ",");
                append_column_value("true", 4, pstate->currentdatacolumn, result, ',');
            }
        }
        break;
    case JSON_NULL:
        if (pstate->section == DATA)
        {
            if (pstate->level == 3)
            {
                pstate->currentdatacolumn++;
                // debug_print_value(data, size, ";");
                write_column_value("null", 4, pstate->currentdatacolumn, result);
                result->columns[pstate->currentdatacolumn]->dataisnull = true;
                // result->columns[result->currentdatacolumn]->data[0] = 0;
            }
            else if (pstate->level > 3)
            {
                // should append to column value, probably
                // debug_print_value(data, size, ",");
                append_column_value("null", 4, pstate->currentdatacolumn, result, ',');
            }
        }
        break;
    }
    return 0;
}
