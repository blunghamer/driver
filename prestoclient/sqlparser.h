#ifndef sqlparser__h
#define sqlparser__h

#include <stdlib.h>
#include <string.h>
#include <stddef.h>

enum STATMENT_TYPE {
    OTHER = 0,
    SELECT,
    DDL,
    NON_PREPARABLE
};

char* fixupsql(char *sql, int sqlLen, int cte, size_t *nparam, int *isselect, char **errmsg);

#endif