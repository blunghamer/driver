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

int dummy_json_parser(JSON_TYPE typ, const char *data, size_t size, void *user_data)
{
    printf("%s \n", toks[typ]);// debug_print_value(data,size,"DEBUG ");
    return 0;
}

static const JSON_CALLBACKS callbacks = {
    dummy_json_parser
};


int main() {
    char * fix = "\"]]}";

    int fd = open("../.testdata/06_result_chunk.json", O_RDONLY);    
    int len = lseek(fd, 0, SEEK_END);
    printf("Result of open is %i length is %i\n ", fd, len);
    void *data = mmap(0, len, PROT_READ, MAP_PRIVATE, fd, 0);

    JSON_PARSER parser;
    JSON_INPUT_POS pdbg = {0};    
    int ret;

    ret = json_init(&parser, &callbacks, NULL, NULL);
    if(ret != 0) {
        printf("Unable to init parser, retcode %i\n", ret);
        return ret;
    }

    /* We rely on propagation of any error code into json_fini(). */
    ret = json_feed(&parser, (char*)data, len);
    if (ret != 0) {
        printf("Unable to feed 1 parser, retcode %i\n", ret);
        ret = json_fini(&parser, &pdbg);  
        printf("Unable to finish 1 parser, retcode %i (offset: %li, column %li, line %li)\n", ret, pdbg.offset, pdbg.column_number, pdbg.line_number);  
        return ret;
    }

    printf("End of part one\n");

    ret = json_feed(&parser, fix, strlen(fix));
    if (ret != 0) {
        printf("Unable to feed 2 parser, retcode %i\n", ret);
        ret = json_fini(&parser, &pdbg);  
        printf("Unable to finish 2 parser, retcode %i (offset: %li, column %li, line %li)\n", ret, pdbg.offset, pdbg.column_number, pdbg.line_number);  
        return ret;
    }

    printf("End of part two\n");

    ret = json_fini(&parser, &pdbg);
    if (ret != 0) {
        printf("Unable to finish parser, retcode %i (offset: %li, column %li, line %li)\n", ret, pdbg.offset, pdbg.column_number, pdbg.line_number);
    }

    munmap(data, len);
    return 0;
}