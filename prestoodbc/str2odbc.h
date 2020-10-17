#ifndef _str2odbc_H
#define _str2odbc_H

#include <sql.h>
#include <stddef.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <sys/time.h>
#include <sqltypes.h>
#include <stdlib.h>


double ln_strtod(const char *data, char **endp);
int    str2time(char *str, TIME_STRUCT *ts);
int    str2timestamp(char *str, TIMESTAMP_STRUCT *tss);
int    str2date(char *str, DATE_STRUCT *ds);

typedef long long TIMESTAMP_MS;


TIMESTAMP_MS timestamp_to_long(char *str);
void ts_to_odbc(char *str, TIMESTAMP_STRUCT* ts); 
void dt_to_odbc(char *str, DATE_STRUCT * ds);

#endif