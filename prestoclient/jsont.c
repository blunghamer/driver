#include "json.h"

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>


const char* const toks[10] = {
    "JSON_NULL",
    "JSON_FALSE",
    "JSON_TRUE",
    "JSON_NUMBER",
    "JSON_STRING",
    "JSON_KEY",     
    "JSON_ARRAY_BEG",
    "JSON_ARRAY_END",
    "JSON_OBJECT_BEG",
    "JSON_OBJECT_END"
};

static int printval(const char* data, size_t size, char* sep) {
    char *dat = (char*)malloc(sizeof(char)* (size + 1));
    strncpy(dat, data, size);
    dat[size] = '\0';
    printf("%s%s", dat,sep);
    free(dat);
}

typedef struct JOURNAL {
    size_t level;
    size_t aopen;
    size_t aclose;    
    size_t oopen;
    size_t oclose;
    int inColumns;
    int inData;
    int inStats;
    int inWarnings;
    int inRawType;
    int inColumnName;
    int inValue;
} JOURNAL;

static int
test_dump_callback(JSON_TYPE typ, const char* data, size_t size, void* userdata)
{
    JOURNAL* journal = (JOURNAL*)userdata;
    switch (typ) {  
        case JSON_ARRAY_BEG:
            journal->level++;
            journal->aopen++;
            if (journal->inData){
                if (journal->level > 3) {
                    printf("[");
                }
            }
            break;
        case JSON_ARRAY_END:
            journal->level--;
            journal->aclose++;
            if (journal->inData){
                if (journal->level == 2){
                    printf("\n");
                } else if (journal->level = 3) {
                    printf("]");
                }
            }
            break;
        case JSON_OBJECT_BEG:
            journal->level++;
            journal->oopen++;
            if (journal->inData) {
                if (journal->level > 3) {
                     printf("{");
                }
            }
            break;
        case JSON_OBJECT_END:
            journal->level--;
            journal->oclose++;
            if (journal->inData) {
                if (journal->level = 3) {
                     printf("}");
                }
            }
            break;
        case JSON_KEY:
            if (journal->level == 1) {
                if (strncmp(data, "columns", 7) == 0) {
                    journal->inColumns = 1;
                }
                else if (strncmp(data, "data", 4) == 0 ) {
                    journal->inData = 1;
                    journal->inColumns = 0;
                }
                else if (strncmp(data, "stats", 5) == 0 ) {
                    journal->inStats = 1;
                    journal->inData = 0;
                }
                else if (strncmp(data, "warnings", 8) == 0 ) {
                    journal->inWarnings = 1;
                    journal->inStats = 0;
                }
            }
            else if (journal->inColumns) {
                if (strncmp(data, "rawType", 7) == 0) {
                    journal->inRawType = 1;
                } 
                else if (strncmp(data, "name", 4) == 0) {
                    journal->inColumnName = 1;
                }
                else if (strncmp(data, "value", 5) == 0) {
                    journal->inValue = 1;
                }
            }
            else if (journal->inData) {
                if (journal->level > 3) {
                    printval(data,size,":");
                }
            }
            break;
        case JSON_STRING:
            if (journal->inColumns) {
                if (journal->inColumnName) {
                    printval(data,size,";");
                    journal->inColumnName = 0;
                }
                else if (journal->inRawType) {
                    printval(data,size,";");
                    journal->inRawType = 0;
                }
            }
            if (journal->inData) {
                if (journal->level == 3){
                    printval(data,size,";");
                }
                else if (journal->level > 3) {
                    printval(data,size,",");
                }
            }
            break;
        case JSON_NUMBER:
            if (journal->inData) {
                if (journal->level == 3){
                    printval(data,size,";");
                } else if (journal->level > 3) {
                    printval(data,size,",");
                }
            }
            if (journal->inColumns && journal->inValue) {
                printval(data,size,";");
                journal->inValue = 0;
            }
            break;
    }

    return 0;
}

static const JSON_CALLBACKS callbacks = {
    test_dump_callback
};

int main() {
    int fd = open("../.testdata/03_results.json", O_RDONLY);
    printf("Result of open is %i\n", fd);
    int len = lseek(fd, 0, SEEK_END);
    void *data = mmap(0, len, PROT_READ, MAP_PRIVATE, fd, 0);

    // json_parse((const char*) data, len , &callbacks, NULL, NULL, 0);    

    JSON_PARSER parser;
    JSON_INPUT_POS pdbg = {0};
    JOURNAL jrnl = {0};
    int ret;

    ret = json_init(&parser, &callbacks, NULL, &jrnl);
    if(ret != 0)
        return ret;

    /* We rely on propagation of any error code into json_fini(). */
    json_feed(&parser, (char*)data, len);

    json_fini(&parser, &pdbg);

    munmap(data, len);
    return 0;
}