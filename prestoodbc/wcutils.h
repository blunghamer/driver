#ifndef _wcutils_H
#define _wcutils_H

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#endif

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

#if defined(_WIN32) || defined(_WIN64)

char *
wmb_to_utf(char *str, int len);

#ifndef WINTERFACE

char *
wmb_to_utf_c(char *str, int len);

#endif

char *
utf_to_wmb(char *str, int len);

#ifdef WINTERFACE

WCHAR *
wmb_to_uc(char *str, int len);

char *
uc_to_wmb(WCHAR *wstr, int len);

#endif /* WINTERFACE */

#endif /* _WIN32 || _WIN64 */

#endif