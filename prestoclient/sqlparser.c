#include "sqlparser.h"

static const char space_chars[] = " \f\n\r\t\v";
#define ISSPACE(c) ((c) && (strchr(space_chars, (c)) != 0))
#define SQL_NTS (-3)

/**
 * Free memory given pointer to memory pointer.
 * @param x pointer to pointer to memory to be free'd
 */

static void
freep(void *x)
{
    if (x && ((char **)x)[0])
    {
        free(((char **)x)[0]);
        ((char **)x)[0] = NULL;
    }
}

/**
 * Check if query is a DDL statement.
 * @param sql query string
 * @result true or false
 */

static int
checkddl(char *sql)
{
    int isddl = 0;

    while (*sql && ISSPACE(*sql))
    {
        ++sql;
    }
    if (*sql && *sql != ';')
    {
        int i, size;
        static const struct
        {
            int len;
            const char *str;
        } ddlstr[10] = {
            {5, "alter"},
            {7, "analyze"},
            {5, "begin"},
            {7, "comment"},
            {6, "commit"},
            {6, "create"},
            {10, "deallocate"},
            {4, "drop"},
            {7, "explain"},
            {8, "rollback"},
        };

        size = strlen(sql);
        for (i = 0; i < 10; i++)
        {
            if (size >= ddlstr[i].len &&
                strncasecmp(sql, ddlstr[i].str, ddlstr[i].len) == 0)
            {
                isddl = 1;
                break;
            }
        }
    }
    return isddl;
}

/**
 * Fixup query string with optional parameter markers.
 * @param sql original query string
 * @param sqlLen length of query string or SQL_NTS
 * @param cte when true, WITH is treated as SELECT
 * @param nparam output number of parameters
 * @param sqltype output indicator for SELECT (1) or DDL statement (2) or Statement that cannot be prepared (3)
 * @param errmsg output error message
 * @result newly allocated string containing query string for SQLite or NULL
 */

char *fixupsql(char *sql, int sqlLen, int cte, size_t *nparam, int *sqltype, char **errmsg)
{
    char *q = sql, *qz = NULL, *p, *inq = NULL, *out;
    int np = 0, isddl = -1, size;

    if (errmsg)
    {
        *errmsg = NULL;
    }
    if (sqlLen != SQL_NTS)
    {
        qz = q = (char *)malloc(sqlLen + 1);
        if (!qz)
        {
            return NULL;
        }
        memcpy(q, sql, sqlLen);
        q[sqlLen] = '\0';
        size = sqlLen * 4;
    }
    else
    {
        size = strlen(sql) * 4;
    }
    size += sizeof(char *) - 1;
    size &= ~(sizeof(char *) - 1);
    p = (char *)malloc(size);
    if (!p)
    {
    errout:
        freep(&qz);
        return NULL;
    }
    memset(p, 0, size);
    out = p;
    while (*q)
    {
        switch (*q)
        {
        case '\'':
        case '\"':
            if (q == inq)
            {
                inq = NULL;
            }
            else if (!inq)
            {
                inq = q + 1;

                while (*inq)
                {
                    if (*inq == *q)
                    {
                        if (inq[1] == *q)
                        {
                            inq++;
                        }
                        else
                        {
                            break;
                        }
                    }
                    inq++;
                }
            }
            *p++ = *q;
            break;
        case '?':
            *p++ = *q;
            if (!inq)
            {
                np++;
            }
            break;
        case ';':
            if (!inq)
            {
                if (isddl < 0)
                {
                    isddl = checkddl(out);
                }
                if (isddl == 0)
                {
                    char *qq = q;

                    do
                    {
                        ++qq;
                    } while (*qq && ISSPACE(*qq));
                    if (*qq && *qq != ';')
                    {
                        freep(&out);
                        if (errmsg)
                        {
                            *errmsg = "only one SQL statement allowed";
                        }
                        goto errout;
                    }
                }
            }
            *p++ = *q;
            break;
        case '{':
            /*
	     * Deal with escape sequences:
	     * {d 'YYYY-MM-DD'}, {t ...}, {ts ...}
	     * {oj ...}, {fn ...} etc.
	     */
            if (!inq)
            {
                int ojfn = 0, brc = 0;
                char *inq2 = NULL, *end = q + 1, *start;

                while (*end && ISSPACE(*end))
                {
                    ++end;
                }
                if (*end != 'd' && *end != 'D' &&
                    *end != 't' && *end != 'T')
                {
                    ojfn = 1;
                }
                start = end;
                while (*end)
                {
                    if (inq2 && *end == *inq2)
                    {
                        inq2 = NULL;
                    }
                    else if (inq2 == NULL && *end == '{')
                    {
                        char *nerr = 0, *nsql;

                        nsql = fixupsql(end, SQL_NTS, cte, 0, 0, &nerr);
                        if (nsql && !nerr)
                        {
                            strcpy(end, nsql);
                        }
                        else
                        {
                            brc++;
                        }
                        freep(&nsql);
                    }
                    else if (inq2 == NULL && *end == '}')
                    {
                        if (brc-- <= 0)
                        {
                            break;
                        }
                    }
                    else if (inq2 == NULL && (*end == '\'' || *end == '"'))
                    {
                        inq2 = end;
                    }
                    else if (inq2 == NULL && *end == '?')
                    {
                        np++;
                    }
                    ++end;
                }
                if (*end == '}')
                {
                    char *end2 = end - 1;

                    if (ojfn)
                    {
                        while (start < end)
                        {
                            if (ISSPACE(*start))
                            {
                                break;
                            }
                            ++start;
                        }
                        while (start < end)
                        {
                            *p++ = *start;
                            ++start;
                        }
                        q = end;
                        break;
                    }
                    else
                    {
                        while (start < end2 && *start != '\'')
                        {
                            ++start;
                        }
                        while (end2 > start && *end2 != '\'')
                        {
                            --end2;
                        }
                        if (*start == '\'' && *end2 == '\'')
                        {
                            while (start <= end2)
                            {
                                *p++ = *start;
                                ++start;
                            }
                            q = end;
                            break;
                        }
                    }
                }
            }
            /* FALL THROUGH */
        default:
            *p++ = *q;
        }
        ++q;
    }
    freep(&qz);
    *p = '\0';
    if (nparam)
    {
        *nparam = np;
    }
    if (sqltype)
    {
        if (isddl < 0)
        {
            isddl = checkddl(out);
        }
        if (isddl > 0)
        {
            *sqltype = DDL;
        }
        else
        {
            int incom = 0;

            p = out;
            while (*p)
            {
                switch (*p)
                {
                case '-':
                    if (!incom && p[1] == '-')
                    {
                        incom = -1;
                    }
                    break;
                case '\n':
                    if (incom < 0)
                    {
                        incom = 0;
                    }
                    break;
                case '/':
                    if (incom > 0 && p[-1] == '*')
                    {
                        incom = 0;
                        p++;
                        continue;
                    }
                    else if (!incom && p[1] == '*')
                    {
                        incom = 1;
                    }
                    break;
                }
                if (!incom && !ISSPACE(*p))
                {
                    break;
                }
                p++;
            }
            size = strlen(p);
            if (size >= 6 && strncasecmp(p, "select", 6) == 0 )
            {
                *sqltype = SELECT;
            }
            else if ( size >= 4 && 
                     ( 
                         strncasecmp(p, "use", 4) == 0 ||
                        strncasecmp(p, "show", 4) == 0
                     ) 
                    )
            {
                *sqltype = NON_PREPARABLE;
            }
            else if (cte && size >= 4 && strncasecmp(p, "with", 4) == 0)
            {
                *sqltype = SELECT;
            }
            else if (size >= 7 && strncasecmp(p, "explain", 7) == 0)
            {
                *sqltype = NON_PREPARABLE;
            }
            else
            {
                *sqltype = OTHER;
            }
        }
    }
    return out;
}