#ifndef _wcutils_H
#define _wcutils_H

#include <sql.h>
#include <sqltypes.h>
#include <stddef.h>
#include <stdlib.h>

int 
uc_strlen(SQLWCHAR *str);

char *
uc_to_utf(SQLWCHAR *str, int len);

char *
uc_to_utf_c(SQLWCHAR *str, int len);

void
uc_free(void *str);

#endif