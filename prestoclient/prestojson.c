#include "prestojson.h"
#include <assert.h>

#define min(a, b) \
    ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _b : _a; })

static void debug_print_value(const char *data, size_t size, char *sep)
{    
    printf("%.*s%s", (int)size, data, sep);	
    /*
    char *dat = (char *)malloc(sizeof(char) * (size + 1));
    strncpy(dat, data, size);
    dat[size] = '\0';
    printf("%s%s", dat, sep);
    free(dat);
    */    
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

static void write_column_value(const char *data, size_t size, PRESTOCLIENT_RESULT *result)
{
    // printf("currentcolumn: %i, columcount: %li\n", result->currentdatacolumn, result->columncount);
    result->columns[result->currentdatacolumn]->dataisnull = false;
    if (size > result->columns[result->currentdatacolumn]->databuffersize)
    {
        result->columns[result->currentdatacolumn]->data = (char *)realloc((char *)result->columns[result->currentdatacolumn]->data, (size + 1) * sizeof(char));
        if (!result->columns[result->currentdatacolumn]->data)
            exit(1);
        result->columns[result->currentdatacolumn]->databuffersize = size;           
    }
    result->columns[result->currentdatacolumn]->dataactualsize = size;  
    strncpy(result->columns[result->currentdatacolumn]->data, data, size);
    result->columns[result->currentdatacolumn]->data[size] = 0;
}

static void append_column_value(const char *data, size_t size, PRESTOCLIENT_RESULT *result)
{
    write_column_value(data, size, result);
    /*
    result->columns[result->currentdatacolumn]->dataisnull = false;
    if ((size + result->columns[result->currentdatacolumn]->datasize) > result->columns[result->currentdatacolumn]->datasize)
    {
        result->columns[result->currentdatacolumn]->data = (char *)realloc((char *)result->columns[result->currentdatacolumn]->data, sizeof(char) * (size + result->columns[result->currentdatacolumn]->datasize) + 1);
        if (!result->columns[result->currentdatacolumn]->data)
            exit(1);
    }

    // lets try to append to preexisting buffer;
    result->columns[result->currentdatacolumn]->data[result->columns[result->currentdatacolumn]->datasize] = ',';
    strncpy(result->columns[result->currentdatacolumn]->data + (result->columns[result->currentdatacolumn]->datasize + 1), data, size);
    result->columns[result->currentdatacolumn]->datasize += size;
    result->columns[result->currentdatacolumn]->data[size] = 0;
    */
}

static void apply_raw_type(const char *data, size_t size, PRESTOCLIENT_RESULT *result)
{
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
        result->columns[result->columncount - 1]->bytesize = 23;
    }
    else if (strncmp(data, "time", min(size, (size_t)4)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIME;
        result->columns[result->columncount - 1]->bytesize = 23;
    }
    else if (strncmp(data, "time with time zone", min(size, (size_t)19)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIME_WITH_TIME_ZONE;
        result->columns[result->columncount - 1]->bytesize = 30;
    }
    else if (strncmp(data, "timestamp", min(size, (size_t)9)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIMESTAMP;
        result->columns[result->columncount - 1]->bytesize = 23;
    }
    else if (strncmp(data, "timestamp with time zone", min(size, (size_t)24)) == 0)
    {
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_TIMESTAMP_WITH_TIME_ZONE;
        result->columns[result->columncount - 1]->bytesize = 30;
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
        printf("unable to work with type %s setting to varchar\n", data);
        result->columns[result->columncount - 1]->type = PRESTOCLIENT_TYPE_VARCHAR;
        // we are lacking those...
        // IPADDRESS
        // UUID
        // HyperLogLog
        // P4HyperLogLog
        // QDigest
    }
}

int presto_json_parser(JSON_TYPE typ, const char *data, size_t size, void *user_data)
{
    // debug_print_value(data,size,"DEBUG ");
    PRESTOCLIENT_RESULT *result = (PRESTOCLIENT_RESULT *)user_data;
    PARSINGSTATE *journal = (PARSINGSTATE *)result->parserstate;
    switch (typ)
    {
    case JSON_ARRAY_BEG:
        journal->level++;
        journal->aopen++;
        if (journal->section == DATA)
        {
            if (journal->level > 3)
            {
                printf("[");                
                write_column_value(data, size, result);
            }
            else if (journal->level == 3)
            {
                result->currentdatacolumn = -1;
            }
        }
        break;
    case JSON_ARRAY_END:
        journal->level--;
        journal->aclose++;
        if (journal->section == DATA)
        {
            if (journal->level == 2)
            {
                printf("\n");
                // this is where the write callback function is invoked end of array (meaning end of row in result set)
                result->write_callback_function(result->user_data, result);
            }
            else if (journal->level == 3)
            {
                printf("]");
                append_column_value("]", 1, result);
            }
        }
        break;
    case JSON_OBJECT_BEG:
        journal->level++;
        journal->oopen++;
        if (journal->section == DATA)
        {
            if (journal->level > 3)
            {
                printf("{");
                result->currentdatacolumn++;
                write_column_value(data, size, result);
            }
        }
        break;
    case JSON_OBJECT_END:
        journal->level--;
        journal->oclose++;
        if (journal->section == DATA)
        {
            if (journal->level == 3)
            {
                printf("}");
                append_column_value("}", 1, result);
            }
        }
        break;
    case JSON_KEY:
        if (journal->level == 1)
        {
            if (strncmp(data, "id", 2) == 0)
            {
                journal->header = ID;
            }
            else if (strncmp(data, "infoUri", 7) == 0)
            {
                journal->header = INFO;
            }
            else if (strncmp(data, "nextUri", 7) == 0)
            {
                journal->header = NEXT;
            }
            else if (strncmp(data, "partialCancelUri", 16) == 0)
            {
                journal->header = CANCEL;
            }
            else if (strncmp(data, "columns", 7) == 0)
            {
                journal->section = COLUMNS;
            }
            else if (strncmp(data, "data", 4) == 0)
            {
                journal->section = DATA;
            }
            else if (strncmp(data, "stats", 5) == 0)
            {
                journal->section = STATS;
            }
            else if (strncmp(data, "error", 5) == 0)
            {
                journal->section = ERROR;
            }
            else if (strncmp(data, "warnings", 8) == 0)
            {
                journal->section = WARNINGS;
            }
        }
        else if (journal->section == COLUMNS)
        {
            if (strncmp(data, "rawType", 7) == 0)
            {
                journal->inRawType = 1;
            }
            else if (strncmp(data, "name", 4) == 0)
            {
                journal->inColumnName = 1;
            }
            else if (strncmp(data, "value", 5) == 0)
            {
                journal->inValue = 1;
            }
        }
        else if (journal->section == DATA)
        {
            if (journal->level > 3)
            {
                debug_print_value(data, size, ":");
            }
        }
        else if (journal->section == ERROR)
        {
            if (journal->level == 2)
            {
                if (strncmp(data, "type", 4) == 0)
                {
                    journal->inErrorType = 1;
                }
                else if (strncmp(data, "errorType", 9) == 0)
                {
                    journal->inErrorMessage = 1;
                }
            }
        }
        else if (journal->section == STATS)
        {
            if (journal->level == 2)
            {
                if (strncmp(data, "state", 5) == 0)
                {
                    journal->state = 1;
                }
            }
        }
        break;
    case JSON_STRING:
        if (journal->level == 1)
        {
            if (journal->header == ID)
            {
                debug_print_value(data, size, " = ID\n");
            }
            else if (journal->header == INFO)
            {
                debug_print_value(data, size, " = INFO_URL\n");
                alloc_copyn(&result->lastinfouri, data, size);
            }
            else if (journal->header == NEXT)
            {
                debug_print_value(data, size, " = NEXT_URL\n");
                alloc_copyn(&result->lastnexturi, data, size);
            }
            else if (journal->header == CANCEL)
            {
                debug_print_value(data, size, " = PARTIAL_CANCEL_URL\n");
                alloc_copyn(&result->lastcanceluri, data, size);
            }
        }
        else if (journal->section == COLUMNS)
        {
            if (journal->inColumnName)
            {
                journal->inColumnName = 0;
                result->columncount++;

                debug_print_value(data, size, ";");
                if (result->columncount == 1)
                    result->columns = (PRESTOCLIENT_COLUMN **)malloc(sizeof(PRESTOCLIENT_COLUMN *) *1 );
                else
                    result->columns = (PRESTOCLIENT_COLUMN **)realloc((PRESTOCLIENT_COLUMN **)result->columns, result->columncount * sizeof(PRESTOCLIENT_COLUMN *));

                if (!result->columns)
                {
                    exit(1);
                }
                result->columns[result->columncount - 1] = new_prestocolumn();
            }
            else if (journal->inRawType)
            {
                journal->inRawType = 0;
                debug_print_value(data, size, ";");
                apply_raw_type(data, size, result);
            }
        }
        else if (journal->section == DATA)
        {
            if (journal->level == 3)
            {
                result->currentdatacolumn++;
                debug_print_value(data, size, ";");
                write_column_value(data, size, result);
            }
            else if (journal->level > 3)
            {
                debug_print_value(data, size, ",");
            }
        }
        else if (journal->section == STATS)
        {
            if (journal->state)
            {
                debug_print_value(data, size, "\n");
                alloc_copyn(&result->laststate, data, size);
                journal->state = 0;
            }
        }
        else if (journal->section == ERROR)
        {
            if (journal->inErrorType)
            {
                debug_print_value(data, size, " => Errortype\n");
                alloc_copyn(&result->lasterrormessage, data, size);
                journal->inErrorType = 0;
            }
            else if (journal->inErrorMessage)
            {
                debug_print_value(data, size, " => Errormessage\n");
                alloc_copyn(&result->lasterrormessage, data, size);
                journal->inErrorMessage = 0;
            }
        }
        break;
    case JSON_NUMBER:
        switch (journal->section)
        {
        case DATA:
            if (journal->level == 3)
            {
                result->currentdatacolumn++;
                debug_print_value(data, size, ";");
                write_column_value(data, size, result);
            }
            else if (journal->level > 3)
            {
                debug_print_value(data, size, ",");
                append_column_value(data, size, result);
            }
            break;
        case COLUMNS:
            if (journal->inValue)
            {
                debug_print_value(data, size, ";");
                journal->inValue = 0;

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
        if (journal->section == DATA)
        {
            if (journal->level == 3)
            {
                result->currentdatacolumn++;
                debug_print_value(data, size, ";");
                write_column_value("false", 5, result);
            }
            else if (journal->level > 3)
            {
                debug_print_value(data, size, ",");
                append_column_value("false", 5, result);
            }
        }
        break;
    case JSON_TRUE:
        if (journal->section == DATA)
        {
            if (journal->level == 3)
            {
                result->currentdatacolumn++;
                debug_print_value(data, size, ";");
                write_column_value("true", 4, result);
            }
            else if (journal->level > 3)
            {
                debug_print_value(data, size, ",");
                append_column_value("true", 4, result);
            }
        }
        break;
    case JSON_NULL:
        if (journal->section == DATA)
        {
            if (journal->level == 3)
            {
                result->currentdatacolumn++;
                debug_print_value(data, size, ";");
                write_column_value("null", 4, result);
                result->columns[result->currentdatacolumn]->dataisnull = true;
                // result->columns[result->currentdatacolumn]->data[0] = 0;
            }
            else if (journal->level > 3)
            {
                // should append to column value, probably
                debug_print_value(data, size, ",");
            }
        }
        break;
    }
    return 0;
}
