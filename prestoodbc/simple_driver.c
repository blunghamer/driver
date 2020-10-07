#include "simple_driver.h"

#define PRESTO_OK 0 /* Successful result */
/* beginning-of-error-codes */
#define PRESTO_ERROR 1       /* Generic error */
#define PRESTO_INTERNAL 2    /* Internal logic error in SQLite */
#define PRESTO_PERM 3        /* Access permission denied */
#define PRESTO_ABORT 4       /* Callback routine requested an abort */
#define PRESTO_BUSY 5        /* The database file is locked */
#define PRESTO_LOCKED 6      /* A table in the database is locked */
#define PRESTO_NOMEM 7       /* A malloc() failed */
#define PRESTO_READONLY 8    /* Attempt to write a readonly database */
#define PRESTO_INTERRUPT 9   /* Operation terminated by sqlite3_interrupt()*/
#define PRESTO_IOERR 10      /* Some kind of disk I/O error occurred */
#define PRESTO_CORRUPT 11    /* The database disk image is malformed */
#define PRESTO_NOTFOUND 12   /* Unknown opcode in sqlite3_file_control() */
#define PRESTO_FULL 13       /* Insertion failed because database is full */
#define PRESTO_CANTOPEN 14   /* Unable to open the database file */
#define PRESTO_PROTOCOL 15   /* Database lock protocol error */
#define PRESTO_EMPTY 16      /* Internal use only */
#define PRESTO_SCHEMA 17     /* The database schema changed */
#define PRESTO_TOOBIG 18     /* String or BLOB exceeds size limit */
#define PRESTO_CONSTRAINT 19 /* Abort due to constraint violation */
#define PRESTO_MISMATCH 20   /* Data type mismatch */
#define PRESTO_MISUSE 21     /* Library used incorrectly */
#define PRESTO_NOLFS 22      /* Uses OS features not supported on host */
#define PRESTO_AUTH 23       /* Authorization denied */
#define PRESTO_FORMAT 24     /* Not used */
#define PRESTO_RANGE 25      /* 2nd parameter to sqlite3_bind out of range */
#define PRESTO_NOTADB 26     /* File opened that is not a database file */
#define PRESTO_NOTICE 27     /* Notifications from sqlite3_log() */
#define PRESTO_WARNING 28    /* Warnings from sqlite3_log() */
#define PRESTO_ROW 100       /* sqlite3_step() has another row ready */
#define PRESTO_DONE 101      /* sqlite3_step() has finished executing */

#undef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#undef max
#define max(a, b) ((a) < (b) ? (b) : (a))

#ifdef _WIN32
#include <windows.h>
#define strcasecmp  _stricmp
#define strncasecmp _strnicmp
#else
#include <unistd.h>
#endif

#define array_size(x) (sizeof (x) / sizeof (x[0]))

static const char space_chars[] = " \f\n\r\t\v";
#define ISSPACE(c) ((c) && (strchr(space_chars, (c)) != 0))

#if defined(_WIN32) || defined(_WIN64)
#include "resource3.h"
#define ODBC_INI "ODBC.INI"
#ifndef DRIVER_VER_INFO
#define DRIVER_VER_INFO VERSION
#endif
#else
#define ODBC_INI ".odbc.ini"
#endif

#if defined(_WIN32) || defined(_WIN64)

/*
 * SQLHENV, SQLHDBC, and SQLHSTMT synchronization
 * is done using a critical section in ENV and DBC
 * structures.
 */

#define HDBC_LOCK(hdbc)                  \
    {                                    \
        DBC *d;                          \
                                         \
        if ((hdbc) == SQL_NULL_HDBC)     \
        {                                \
            return SQL_INVALID_HANDLE;   \
        }                                \
        d = (DBC *)(hdbc);               \
        if (d->magic != DBC_MAGIC)       \
        {                                \
            return SQL_INVALID_HANDLE;   \
        }                                \
        EnterCriticalSection(&d->cs);    \
        d->owner = GetCurrentThreadId(); \
    }

#define HDBC_UNLOCK(hdbc)                 \
    if ((hdbc) != SQL_NULL_HDBC)          \
    {                                     \
        DBC *d;                           \
                                          \
        d = (DBC *)(hdbc);                \
        if (d->magic == DBC_MAGIC)        \
        {                                 \
            d->owner = 0;                 \
            LeaveCriticalSection(&d->cs); \
        }                                 \
    }

#define HSTMT_LOCK(hstmt)                  \
    {                                      \
        DBC *d;                            \
                                           \
        if ((hstmt) == SQL_NULL_HSTMT)     \
        {                                  \
            return SQL_INVALID_HANDLE;     \
        }                                  \
        d = (DBC *)((STMT *)(hstmt))->dbc; \
        if (d->magic != DBC_MAGIC)         \
        {                                  \
            return SQL_INVALID_HANDLE;     \
        }                                  \
        EnterCriticalSection(&d->cs);      \
        d->owner = GetCurrentThreadId();   \
    }

#define HSTMT_UNLOCK(hstmt)                \
    if ((hstmt) != SQL_NULL_HSTMT)         \
    {                                      \
        DBC *d;                            \
                                           \
        d = (DBC *)((STMT *)(hstmt))->dbc; \
        if (d->magic == DBC_MAGIC)         \
        {                                  \
            d->owner = 0;                  \
            LeaveCriticalSection(&d->cs);  \
        }                                  \
    }

#else

/*
 * On UN*X assume that we are single-threaded or
 * the driver manager provides serialization for us.
 *
 * In iODBC (3.52.x) serialization can be turned
 * on using the DSN property "ThreadManager=yes".
 *
 * In unixODBC that property is named
 * "Threading=0-3" and takes one of these values:
 *
 *   0 - no protection
 *   1 - statement level protection
 *   2 - connection level protection
 *   3 - environment level protection
 *
 * unixODBC 2.2.11 uses environment level protection
 * by default when it has been built with pthread
 * support.
 */

#define HDBC_LOCK(hdbc)
#define HDBC_UNLOCK(hdbc)
#define HSTMT_LOCK(hdbc)
#define HSTMT_UNLOCK(hdbc)

#endif

#define ENV_MAGIC 0x53544145
#define DBC_MAGIC 0x53544144
#define DEAD_MAGIC 0xdeadbeef

#define verinfo(maj, min, lev) ((maj) << 16 | (min) << 8 | (lev))

static void
lt_error_core(const char *message, va_list ap)
{
    vfprintf(stderr, message, ap);
    fprintf(stderr, ".\n");

    exit(1);
}

void lt_fatal(const char *message, ...)
{
    va_list ap;
    va_start(ap, message);
    lt_error_core(message, ap);
    va_end(ap);
}

void *
xmalloc(size_t num)
{
    void *p = (void *)malloc(num);
    if (!p)
        lt_fatal("Memory exhausted");

    return p;
}

char *
xstrdup(const char *string)
{
    return string ? strcpy((char *)xmalloc(strlen(string) + 1),
                           string)
                  : NULL;
}

void xfree(void *p)
{
    if (p)
    {
        free(p);
        p = NULL;
    }
}

#ifdef USE_DLOPEN_FOR_GPPS

#include <dlfcn.h>

/**
 * Get boolean flag from string.
 * @param string string to be inspected
 * @result true or false
 */

static int
getbool(char *string)
{
    if (string)
    {
        return string[0] && strchr("Yy123456789Tt", string[0]) != NULL;
    }
    return 0;
}

#define SQLGetPrivateProfileString(A, B, C, D, E, F) drvgpps(d, A, B, C, D, E, F)

/*
 * EXPERIMENTAL: SQLGetPrivateProfileString infrastructure using
 * dlopen(), in theory this makes the driver independent from the
 * driver manager, i.e. the same driver binary can run with iODBC
 * and unixODBC.
 */

static void
drvgetgpps(DBC *d)
{
    void *lib;
    int (*gpps)();

    lib = dlopen("libodbcinst.so.2", RTLD_LAZY);
    if (!lib)
    {
        lib = dlopen("libodbcinst.so.1", RTLD_LAZY);
    }
    if (!lib)
    {
        lib = dlopen("libodbcinst.so", RTLD_LAZY);
    }
    if (!lib)
    {
        lib = dlopen("libiodbcinst.so.2", RTLD_LAZY);
    }
    if (!lib)
    {
        lib = dlopen("libiodbcinst.so", RTLD_LAZY);
    }
    if (lib)
    {
        gpps = (int (*)())dlsym(lib, "SQLGetPrivateProfileString");
        if (!gpps)
        {
            dlclose(lib);
            return;
        }
        d->instlib = lib;
        d->gpps = gpps;
    }
}

static void
drvrelgpps(DBC *d)
{
    if (d->instlib)
    {
        dlclose(d->instlib);
        d->instlib = 0;
    }
}

static int
drvgpps(DBC *d, char *sect, char *ent, char *def, char *buf,
        int bufsiz, char *fname)
{
    if (d->gpps)
    {
        return d->gpps(sect, ent, def, buf, bufsiz, fname);
    }
    strncpy(buf, def, bufsiz);
    buf[bufsiz - 1] = '\0';
    return 1;
}
#else
#include <odbcinst.h>
#define drvgetgpps(d)
#define drvrelgpps(d)
#endif

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
 * Set error message and SQL state on DBC
 * @param d database connection pointer
 * @param naterr native error code
 * @param msg error message
 * @param st SQL state
 */

#if defined(__GNUC__) && (__GNUC__ >= 2)
static void setstatd(DBC *, int, char *, char *, ...)
    __attribute__((format(printf, 3, 5)));
#endif

static void
setstatd(DBC *d, int naterr, char *msg, char *st, ...)
{
    va_list ap;

    if (!d)
    {
        return;
    }
    d->naterr = naterr;
    d->logmsg[0] = '\0';
    if (msg)
    {
        int count;

        va_start(ap, st);
        count = vsnprintf((char *)d->logmsg, sizeof(d->logmsg), msg, ap);
        va_end(ap);
        if (count < 0)
        {
            d->logmsg[sizeof(d->logmsg) - 1] = '\0';
        }
    }
    if (!st)
    {
        st = "?????";
    }
    strncpy(d->sqlstate, st, 5);
    d->sqlstate[5] = '\0';
}

/**
 * Set error message and SQL state on statement
 * @param s statement pointer
 * @param naterr native error code
 * @param msg error message
 * @param st SQL state
 */

#if defined(__GNUC__) && (__GNUC__ >= 2)
static void setstat(STMT *, int, char *, char *, ...)
    __attribute__((format(printf, 3, 5)));
#endif

static void
setstat(STMT *s, int naterr, char *msg, char *st, ...)
{
    va_list ap;

    if (!s)
    {
        return;
    }
    s->naterr = naterr;
    s->logmsg[0] = '\0';
    if (msg)
    {
        int count;

        va_start(ap, st);
        count = vsnprintf((char *)s->logmsg, sizeof(s->logmsg), msg, ap);
        va_end(ap);
        if (count < 0)
        {
            s->logmsg[sizeof(s->logmsg) - 1] = '\0';
        }
    }
    if (!st)
    {
        st = "?????";
    }
    strncpy(s->sqlstate, st, 5);
    s->sqlstate[5] = '\0';
}

/**
 * Report S1000 (out of memory) SQL error given STMT.
 * @param s statement pointer
 * @result ODBC error code
 */

static SQLRETURN
nomem(STMT *s)
{
    setstat(s, -1, "out of memory", (*s->ov3) ? "HY000" : "S1000");
    return SQL_ERROR;
}

/**
 * Report S1000 (not connected) SQL error given STMT.
 * @param s statement pointer
 * @result ODBC error code
 */

static SQLRETURN
noconn(STMT *s)
{
    setstat(s, -1, "not connected", (*s->ov3) ? "HY000" : "S1000");
    return SQL_ERROR;
}

/**
 * Trace function for SQLite API calls
 * @param d pointer to database connection handle
 * @param fn SQLite function name
 * @param sql SQL string
 */

static void
dbtraceapi(DBC *d, char *fn, const char *sql)
{
    if (fn && d->trace)
    {
        if (sql)
        {
            fprintf(d->trace, "-- %s: %s\n", fn, sql);
        }
        else
        {
            fprintf(d->trace, "-- %s\n", fn);
        }
        fflush(d->trace);
    }
}

/**
 * Drop running sqlite statement in STMT
 * @param s statement pointer
 */

static void
presto_stmt_drop(STMT *s)
{
    if (s->presto_stmt)
    {
        DBC *d = (DBC *)s->dbc;

        if (d)
        {
            dbtraceapi(d, "sqlite3_finalize", 0);
        }

        prestoclient_cancelquery(s->presto_stmt);
        s->presto_stmt = NULL;
        s->presto_stmt_rownum = 0;
    }
}

/**
 * Free dynamically allocated column descriptions of STMT.
 * @param s statement pointer
 */

static void
freedyncols(STMT *s)
{
    if (s->dyncols)
    {
        for (size_t i = 0; i < s->dcols; i++)
        {
            freep(&s->dyncols[i].typname);
        }
        if (s->cols == s->dyncols)
        {
            s->cols = NULL;
            s->ncols = 0;
        }
        freep(&s->dyncols);
    }
    s->dcols = 0;
}

/**
 * Clear out parameter bindings, if any.
 * @param s statement pointer
 */

static SQLRETURN
freeparams(STMT *s)
{
    if (s->bindparms)
    {
        int n;

        for (n = 0; n < s->nbindparms; n++)
        {
            freep(&s->bindparms[n].parbuf);
            memset(&s->bindparms[n], 0, sizeof(BINDPARM));
        }
    }
    return SQL_SUCCESS;
}

/**
 * Free statement's result.
 * @param s statement pointer
 * @param clrcols flag to clear column information
 *
 * The result rows are free'd using the rowfree function pointer.
 * If clrcols is greater than zero, then column bindings and dynamic column
 * descriptions are free'd.
 * If clrcols is less than zero, then dynamic column descriptions are free'd.
 */

static void
freeresult(STMT *s, int clrcols)
{
    freep(&s->bincache);
    s->bincell = NULL;
    s->binlen = 0;
    if (s->rows)
    {
        if (s->rowfree)
        {
            s->rowfree(s->rows);
            s->rowfree = NULL;
        }
        s->rows = NULL;
    }
    s->nrows = -1;
    if (clrcols > 0)
    {
        freep(&s->bindcols);
        s->nbindcols = 0;
    }
    if (clrcols)
    {
        freedyncols(s);
        s->cols = NULL;
        s->ncols = 0;
        s->nowchar[1] = 0;
        s->one_tbl = -1;
        s->has_pk = -1;
        s->has_rowid = -1;
    }
}

/**
 * Internal free function for HSTMT.
 * @param stmt statement handle
 * @result ODBC error code
 */

static SQLRETURN
freestmt(SQLHSTMT stmt)
{
    STMT *s;
    DBC *d;

    if (stmt == SQL_NULL_HSTMT)
    {
        return SQL_INVALID_HANDLE;
    }
    s = (STMT *)stmt;
    presto_stmt_drop(s);
    freeresult(s, 1);
    freep(&s->query);
    d = (DBC *)s->dbc;
    if (d && d->magic == DBC_MAGIC)
    {
        STMT *p, *n;

        p = NULL;
        n = d->stmt;
        while (n)
        {
            if (n == s)
            {
                break;
            }
            p = n;
            n = n->next;
        }
        if (n)
        {
            if (p)
            {
                p->next = s->next;
            }
            else
            {
                d->stmt = s->next;
            }
        }
    }
    freeparams(s);
    freep(&s->bindparms);
    if (s->row_status0 != &s->row_status1)
    {
        freep(&s->row_status0);
        s->rowset_size = 1;
        s->row_status0 = &s->row_status1;
    }
    xfree(s);
    return SQL_SUCCESS;
}

/**
 * Internal allocate HENV.
 * @param env pointer to environment handle
 * @result ODBC error code
 */

static SQLRETURN
drvallocenv(SQLHENV *env)
{
    ENV *e;

    if (env == NULL)
    {
        return SQL_INVALID_HANDLE;
    }
    e = (ENV *)malloc(sizeof(ENV));
    if (e == NULL)
    {
        *env = SQL_NULL_HENV;
        return SQL_ERROR;
    }
    e->magic = ENV_MAGIC;
    e->ov3 = 0;
    e->pool = 0;
#if defined(_WIN32) || defined(_WIN64)
    InitializeCriticalSection(&e->cs);
#else
#if defined(ENABLE_NVFS) && (ENABLE_NVFS)
    nvfs_init();
#endif
#endif
    e->dbcs = NULL;
    *env = (SQLHENV)e;
    return SQL_SUCCESS;
}

/**
 * Allocate HSTMT given HDBC (driver internal version).
 * @param dbc database connection handle
 * @param stmt pointer to statement handle
 * @result ODBC error code
 */

static SQLRETURN
drvallocstmt(SQLHDBC dbc, SQLHSTMT *stmt)
{
    DBC *d;
    STMT *s, *sl, *pl;

    if (dbc == SQL_NULL_HDBC)
    {
        return SQL_INVALID_HANDLE;
    }
    d = (DBC *)dbc;
    if (d->magic != DBC_MAGIC || stmt == NULL)
    {
        return SQL_INVALID_HANDLE;
    }
    s = (STMT *)xmalloc(sizeof(STMT));
    if (s == NULL)
    {
        *stmt = SQL_NULL_HSTMT;
        return SQL_ERROR;
    }
    *stmt = (SQLHSTMT)s;
    memset(s, 0, sizeof(STMT));
    s->dbc = dbc;
    s->ov3 = d->ov3;
    s->bkmrk = SQL_UB_OFF;
    s->bkmrkptr = 0;
    s->oemcp = &d->oemcp;
    s->jdconv = &d->jdconv;
    s->nowchar[0] = d->nowchar;
    s->nowchar[1] = 0;
    s->dobigint = d->dobigint;
    s->curtype = d->curtype;
    s->row_status0 = &s->row_status1;
    s->rowset_size = 1;
    s->longnames = d->longnames;
    s->retr_data = SQL_RD_ON;
    s->max_rows = 0;
    s->bind_type = SQL_BIND_BY_COLUMN;
    s->bind_offs = NULL;
    s->paramset_size = 1;
    s->parm_bind_type = SQL_PARAM_BIND_BY_COLUMN;
    s->one_tbl = -1;
    s->has_pk = -1;
    s->has_rowid = -1;
#ifdef _WIN64
    sprintf((char *)s->cursorname, "CUR_%I64X", (SQLUBIGINT)*stmt);
#else
    sprintf((char *)s->cursorname, "CUR_%016lX", (long)*stmt);
#endif
    sl = d->stmt;
    pl = NULL;
    while (sl)
    {
        pl = sl;
        sl = sl->next;
    }
    if (pl)
    {
        pl->next = s;
    }
    else
    {
        d->stmt = s;
    }
    return SQL_SUCCESS;
}

/**
 * Allocate HENV.
 * @param env pointer to environment handle
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLAllocEnv(SQLHENV *env)
{
    return drvallocenv(env);
}

/**
 * Internal free HENV.
 * @param env environment handle
 * @result ODBC error code
 */

static SQLRETURN
drvfreeenv(SQLHENV env)
{
    ENV *e;

    if (env == SQL_NULL_HENV)
    {
        return SQL_INVALID_HANDLE;
    }
    e = (ENV *)env;
    if (e->magic != ENV_MAGIC)
    {
        return SQL_SUCCESS;
    }
#if defined(_WIN32) || defined(_WIN64)
    EnterCriticalSection(&e->cs);
#endif
    if (e->dbcs)
    {
#if defined(_WIN32) || defined(_WIN64)
        LeaveCriticalSection(&e->cs);
#endif
        return SQL_ERROR;
    }
    e->magic = DEAD_MAGIC;
#if defined(_WIN32) || defined(_WIN64)
    LeaveCriticalSection(&e->cs);
    DeleteCriticalSection(&e->cs);
#endif
    free(e);
    return SQL_SUCCESS;
}

/**
 * Free HENV.
 * @param env environment handle
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLFreeEnv(SQLHENV env)
{
    return drvfreeenv(env);
}

/**
 * Internal allocate HDBC.
 * @param env environment handle
 * @param dbc pointer to database connection handle
 * @result ODBC error code
 */

static SQLRETURN
drvallocconnect(SQLHENV env, SQLHDBC *dbc)
{
    DBC *d;
    ENV *e;
    const char *verstr;
    int maj = 0, min = 0, lev = 0;

    if (dbc == NULL)
    {
        return SQL_ERROR;
    }
    d = (DBC *)malloc(sizeof(DBC));
    if (d == NULL)
    {
        *dbc = SQL_NULL_HDBC;
        return SQL_ERROR;
    }
    memset(d, 0, sizeof(DBC));
    d->curtype = SQL_CURSOR_STATIC;
    d->ov3 = &d->ov3val;
    verstr = "0.0.1";
    sscanf(verstr, "%d.%d.%d", &maj, &min, &lev);
    d->version = verinfo(maj & 0xFF, min & 0xFF, lev & 0xFF);
    e = (ENV *)env;
#if defined(_WIN32) || defined(_WIN64)
    if (e->magic == ENV_MAGIC)
    {
        EnterCriticalSection(&e->cs);
    }
#endif
    if (e->magic == ENV_MAGIC)
    {
        DBC *n, *p;

        d->env = e;
        d->ov3 = &e->ov3;
        p = NULL;
        n = e->dbcs;
        while (n)
        {
            p = n;
            n = n->next;
        }
        if (p)
        {
            p->next = d;
        }
        else
        {
            e->dbcs = d;
        }
    }
#if defined(_WIN32) || defined(_WIN64)
    InitializeCriticalSection(&d->cs);
    d->owner = 0;
    if (e->magic == ENV_MAGIC)
    {
        LeaveCriticalSection(&e->cs);
    }
    d->oemcp = 1;
#endif
    d->autocommit = 1;
    d->magic = DBC_MAGIC;
    *dbc = (SQLHDBC)d;
    drvgetgpps(d);
    return SQL_SUCCESS;
}

/**
 * Allocate HDBC.
 * @param env environment handle
 * @param dbc pointer to database connection handle
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLAllocConnect(SQLHENV env, SQLHDBC *dbc)
{
    return drvallocconnect(env, dbc);
}

/**
 * Internal free connection (HDBC).
 * @param dbc database connection handle
 * @result ODBC error code
 */

static SQLRETURN
drvfreeconnect(SQLHDBC dbc)
{
    DBC *d;
    ENV *e;
    SQLRETURN ret = SQL_ERROR;

    if (dbc == SQL_NULL_HDBC)
    {
        return SQL_INVALID_HANDLE;
    }
    d = (DBC *)dbc;
    if (d->magic != DBC_MAGIC)
    {
        return SQL_INVALID_HANDLE;
    }
    e = d->env;
    if (e && e->magic == ENV_MAGIC)
    {
#if defined(_WIN32) || defined(_WIN64)
        EnterCriticalSection(&e->cs);
#endif
    }
    else
    {
        e = NULL;
    }
    HDBC_LOCK(dbc);
    if (d->presto_client)
    {
        setstatd(d, -1, "not disconnected", (*d->ov3) ? "HY000" : "S1000");
        HDBC_UNLOCK(dbc);
        goto done;
    }
    while (d->stmt)
    {
        freestmt((HSTMT)d->stmt);
    }
    if (e && e->magic == ENV_MAGIC)
    {
        DBC *n, *p;

        p = NULL;
        n = e->dbcs;
        while (n)
        {
            if (n == d)
            {
                break;
            }
            p = n;
            n = n->next;
        }
        if (n)
        {
            if (p)
            {
                p->next = d->next;
            }
            else
            {
                e->dbcs = d->next;
            }
        }
    }
    drvrelgpps(d);
    d->magic = DEAD_MAGIC;
    if (d->trace)
    {
        fclose(d->trace);
    }
#if defined(_WIN32) || defined(_WIN64)
    d->owner = 0;
    LeaveCriticalSection(&d->cs);
    DeleteCriticalSection(&d->cs);
#endif
    free(d);
    ret = SQL_SUCCESS;
done:
#if defined(_WIN32) || defined(_WIN64)
    if (e)
    {
        LeaveCriticalSection(&e->cs);
    }
#endif
    return ret;
}

/**
 * Free connection (HDBC).
 * @param dbc database connection handle
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLFreeConnect(SQLHDBC dbc)
{
    return drvfreeconnect(dbc);
}

/**
 * Open SQLite database file given file name and flags.
 * @param d DBC pointer
 * @param name file name
 * @param isu true/false: file name is UTF8 encoded
 * @param dsn data source name
 * @param sflag STEPAPI flag
 * @param spflag SyncPragma string
 * @param ntflag NoTransaction string
 * @param jmode JournalMode string
 * @param busy busy/lock timeout
 * @result ODBC error code
 */

static SQLRETURN
dbopen(DBC *d, char *name, int isu, char *dsn, char *sflag,
       char *spflag, char *ntflag, char *jmode, char *busy)
{
    char *endp = NULL;
    int rc, tmp, busyto = 100000;
#if defined(HAVE_SQLITE3VFS) && (HAVE_SQLITE3VFS)
    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
    char *uname = name;
    const char *vfs_name = NULL;
#endif

    if (d->presto_client)
    {
        if (d->trace)
        {
            fprintf(d->trace, "-- sqlite3_close (deferred): '%s'\n",
                    d->dbname);
            fflush(d->trace);
        }
#if defined(HAVE_SQLITE3CLOSEV2) && (HAVE_SQLITE3CLOSEV2)
        sqlite3_close_v2(d->sqlite);
#else
        prestoclient_close(d->presto_client);
#endif
        d->presto_client = NULL;
    }
#if defined(HAVE_SQLITE3VFS) && (HAVE_SQLITE3VFS)
    if (d->nocreat)
    {
        flags &= ~SQLITE_OPEN_CREATE;
    }
#if defined(_WIN32) || defined(_WIN64)
    if (!isu)
    {
        char expname[SQL_MAX_MESSAGE_LENGTH * 2];

        expname[0] = '\0';
        rc = ExpandEnvironmentStrings(name, expname, sizeof(expname));
        if (rc <= sizeof(expname))
        {
            uname = wmb_to_utf(expname, rc - 1);
        }
        else
        {
            uname = wmb_to_utf(name, -1);
        }
        if (!uname)
        {
            rc = SQLITE_NOMEM;
            setstatd(d, rc, "out of memory", (*d->ov3) ? "HY000" : "S1000");
            return SQL_ERROR;
        }
    }
#endif
#if defined(ENABLE_NVFS) && (ENABLE_NVFS)
    vfs_name = nvfs_makevfs(uname);
#endif
#ifdef SQLITE_OPEN_URI
    flags |= SQLITE_OPEN_URI;
#endif
    rc = sqlite3_open_v2(uname, &d->sqlite, flags, vfs_name);
#if defined(WINTERFACE) || defined(_WIN32) || defined(_WIN64)
    if (uname != name)
    {
        uc_free(uname);
    }
#endif
#else
#if defined(_WIN32) || defined(_WIN64)
    if (d->nocreat)
    {
        char *cname = NULL;

        if (isu)
        {
            cname = utf_to_wmb(name, -1);
        }
        if (GetFileAttributesA(cname ? cname : name) ==
            INVALID_FILE_ATTRIBUTES)
        {
            uc_free(cname);
            rc = PRESTO_CANTOPEN;
            setstatd(d, rc, "cannot open database",
                     (*d->ov3) ? "HY000" : "S1000");
            return SQL_ERROR;
        }
        uc_free(cname);
    }
#else
    if (d->nocreat)
    {
        rc = PRESTO_CANTOPEN;
        setstatd(d, rc, "cannot open database", (*d->ov3) ? "HY000" : "S1000");
        return SQL_ERROR;
    }
#endif
#if defined(_WIN32) || defined(_WIN64)
    if (!isu)
    {
        WCHAR *wname = wmb_to_uc(name, -1);

        if (!wname)
        {
            rc = SQLITE_NOMEM;
            setstatd(d, rc, "out of memory", (*d->ov3) ? "HY000" : "S1000");
            return SQL_ERROR;
        }
        rc = sqlite3_open16(wname, &d->sqlite);
        uc_free(wname);
    }
    else
#endif
        d->presto_client = prestoclient_init("localhost", NULL, NULL, NULL, NULL, NULL, NULL);
    if (!d->presto_client)
    {
        rc = PRESTO_ERROR;
    }
    else
    {
        rc = PRESTO_OK;
    }

#endif /* !HAVE_SQLITE3VFS */
    if (rc != PRESTO_OK)
    {
    connfail:
        setstatd(d, rc, "connect failed", (*d->ov3) ? "HY000" : "S1000");
        if (d->presto_client)
        {
            prestoclient_close(d->presto_client);
            d->presto_client = NULL;
        }
        return SQL_ERROR;
    }
#if defined(SQLITE_DYNLOAD) || defined(SQLITE_HAS_CODEC)
    if (d->pwd)
    {
        sqlite3_key(d->sqlite, d->pwd, d->pwdLen);
    }
#endif
    d->pwd = NULL;
    d->pwdLen = 0;
    if (d->trace)
    {
#if defined(HAVE_SQLITE3PROFILE) && (HAVE_SQLITE3PROFILE)
        sqlite3_profile(d->sqlite, dbtrace, d);
#else
        //sqlite3_trace(d->sqlite, dbtrace, d);
#endif
    }
    d->step_enable = getbool(sflag);
    d->trans_disable = getbool(ntflag);
    d->curtype = d->step_enable ? SQL_CURSOR_FORWARD_ONLY : SQL_CURSOR_STATIC;
    tmp = strtol(busy, &endp, 0);
    if (endp && *endp == '\0' && endp != busy)
    {
        busyto = tmp;
    }
    if (busyto < 1 || busyto > 1000000)
    {
        busyto = 1000000;
    }
    d->timeout = busyto;
    freep(&d->dbname);
    d->dbname = xstrdup(name);
    freep(&d->dsn);
    d->dsn = xstrdup(dsn);
    /*
    if ((rc = setsqliteopts(d->sqlite, d)) != SQLITE_OK)
    {
        if (d->trace)
        {
            fprintf(d->trace, "-- sqlite3_close: '%s'\n",
                    d->dbname);
            fflush(d->trace);
        }
        sqlite3_close(d->sqlite);
        d->sqlite = NULL;
        goto connfail;
    }
    */
    if (!spflag || spflag[0] == '\0')
    {
        spflag = "NORMAL";
    }
    if (spflag[0] != '\0')
    {
        char syncp[128];

        sprintf(syncp, "PRAGMA synchronous = %8.8s;", spflag);
        //sqlite3_exec(d->sqlite, syncp, NULL, NULL, NULL);
    }
    if (jmode[0] != '\0')
    {
        char jourp[128];

        sprintf(jourp, "PRAGMA journal_mode = %16.16s;", jmode);
        //sqlite3_exec(d->sqlite, jourp, NULL, NULL, NULL);
    }
    if (d->trace)
    {
        fprintf(d->trace, "-- sqlite3_open: '%s'\n", d->dbname);
        fflush(d->trace);
    }
#if defined(_WIN32) || defined(_WIN64)
    {
        char pname[MAX_PATH];
        HANDLE h = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ,
                               FALSE, GetCurrentProcessId());

        pname[0] = '\0';
        if (h)
        {
            HMODULE m = NULL, l = LoadLibrary("psapi.dll");
            DWORD need;
            typedef BOOL(WINAPI * epmfunc)(HANDLE, HMODULE *, DWORD, LPDWORD);
            typedef BOOL(WINAPI * gmbfunc)(HANDLE, HMODULE, LPSTR, DWORD);
            epmfunc epm;
            gmbfunc gmb;

            if (l)
            {
                epm = (epmfunc)GetProcAddress(l, "EnumProcessModules");
                gmb = (gmbfunc)GetProcAddress(l, "GetModuleBaseNameA");
                if (epm && gmb && epm(h, &m, sizeof(m), &need))
                {
                    gmb(h, m, pname, sizeof(pname));
                }
                FreeLibrary(l);
            }
            CloseHandle(h);
        }
        d->xcelqrx = strncasecmp(pname, "EXCEL", 5) == 0 ||
                     strncasecmp(pname, "MSQRY", 5) == 0;
        if (d->trace && d->xcelqrx)
        {

            fprintf(d->trace, "-- enabled EXCEL quirks\n");
            fflush(d->trace);
        }
    }
#endif
    //sqlite3_create_function(d->sqlite, "blob_import", 1, SQLITE_UTF8,
    //                        d, blob_import, 0, 0);
    //sqlite3_create_function(d->sqlite, "blob_export", 2, SQLITE_UTF8,
    //                        d, blob_export, 0, 0);
    return SQL_SUCCESS;
}

/**
 * Internal connect to SQLite database.
 * @param dbc database connection handle
 * @param dsn DSN string
 * @param dsnLen length of DSN string or SQL_NTS
 * @param pwd password or NULL
 * @param pwdLen length of password or SQL_NTS
 * @param isu true/false: file name is UTF8 encoded
 * @result ODBC error code
 */

static SQLRETURN
drvconnect(SQLHDBC dbc, SQLCHAR *dsn, SQLSMALLINT dsnLen, char *pwd,
           int pwdLen, int isu)
{
    DBC *d;
    int len;
    SQLRETURN ret;
    char buf[SQL_MAX_MESSAGE_LENGTH * 6], dbname[SQL_MAX_MESSAGE_LENGTH];
    char busy[SQL_MAX_MESSAGE_LENGTH / 4], tracef[SQL_MAX_MESSAGE_LENGTH];
    char loadext[SQL_MAX_MESSAGE_LENGTH];
    char sflag[32], spflag[32], ntflag[32], nwflag[32], biflag[32];
    char snflag[32], lnflag[32], ncflag[32], fkflag[32], jmode[32];
    char jdflag[32];
#if defined(_WIN32) || defined(_WIN64)
    char oemcp[32];
#endif

    if (dbc == SQL_NULL_HDBC)
    {
        return SQL_INVALID_HANDLE;
    }
    d = (DBC *)dbc;
    if (d->magic != DBC_MAGIC)
    {
        return SQL_INVALID_HANDLE;
    }
    if (d->presto_client != NULL)
    {
        setstatd(d, -1, "connection already established", "08002");
        return SQL_ERROR;
    }
    buf[0] = '\0';
    if (dsnLen == SQL_NTS)
    {
        len = sizeof(buf) - 1;
    }
    else
    {
        len = min(sizeof(buf) - 1, dsnLen);
    }
    if (dsn != NULL)
    {
        strncpy(buf, (char *)dsn, len);
    }
    buf[len] = '\0';
    if (buf[0] == '\0')
    {
        setstatd(d, -1, "invalid DSN", (*d->ov3) ? "HY090" : "S1090");
        return SQL_ERROR;
    }
#if defined(_WIN32) || defined(_WIN64)
    /*
     * When DSN is in UTF it must be converted to ANSI
     * here for ANSI SQLGetPrivateProfileString()
     */
    if (isu)
    {
        char *cdsn = utf_to_wmb(buf, len);

        if (!cdsn)
        {
            setstatd(d, -1, "out of memory", (*d->ov3) ? "HY000" : "S1000");
            return SQL_ERROR;
        }
        strcpy(buf, cdsn);
        uc_free(cdsn);
    }
#endif
    busy[0] = '\0';
    dbname[0] = '\0';
#ifdef WITHOUT_DRIVERMGR
    getdsnattr(buf, "database", dbname, sizeof(dbname));
    if (dbname[0] == '\0')
    {
        strncpy(dbname, buf, sizeof(dbname));
        dbname[sizeof(dbname) - 1] = '\0';
    }
    getdsnattr(buf, "timeout", busy, sizeof(busy));
    sflag[0] = '\0';
    getdsnattr(buf, "stepapi", sflag, sizeof(sflag));
    spflag[0] = '\0';
    getdsnattr(buf, "syncpragma", spflag, sizeof(spflag));
    ntflag[0] = '\0';
    getdsnattr(buf, "notxn", ntflag, sizeof(ntflag));
    nwflag[0] = '\0';
    getdsnattr(buf, "nowchar", nwflag, sizeof(nwflag));
    snflag[0] = '\0';
    getdsnattr(buf, "shortnames", snflag, sizeof(snflag));
    lnflag[0] = '\0';
    getdsnattr(buf, "longnames", lnflag, sizeof(lnflag));
    ncflag[0] = '\0';
    getdsnattr(buf, "nocreat", ncflag, sizeof(ncflag));
    fkflag[0] = '\0';
    getdsnattr(buf, "fksupport", fkflag, sizeof(fkflag));
    loadext[0] = '\0';
    getdsnattr(buf, "loadext", loadext, sizeof(loadext));
    jmode[0] = '\0';
    getdsnattr(buf, "journalmode", jmode, sizeof(jmode));
    jdflag[0] = '\0';
    getdsnattr(buf, "jdconv", jdflag, sizeof(jdflag));
#if defined(_WIN32) || defined(_WIN64)
    oemcp[0] = '\0';
    getdsnattr(buf, "oemcp", oemcp, sizeof(oemcp));
#endif
    biflag[0] = '\0';
    getdsnattr(buf, "bigint", biflag, sizeof(biflag));
#else
    SQLGetPrivateProfileString(buf, "timeout", "100000",
                               busy, sizeof(busy), ODBC_INI);
    SQLGetPrivateProfileString(buf, "database", "",
                               dbname, sizeof(dbname), ODBC_INI);
#if defined(_WIN32) || defined(_WIN64)
    /* database name read from registry is not UTF8 !!! */
    isu = 0;
#endif
    SQLGetPrivateProfileString(buf, "stepapi", "",
                               sflag, sizeof(sflag), ODBC_INI);
    SQLGetPrivateProfileString(buf, "syncpragma", "NORMAL",
                               spflag, sizeof(spflag), ODBC_INI);
    SQLGetPrivateProfileString(buf, "notxn", "",
                               ntflag, sizeof(ntflag), ODBC_INI);
    SQLGetPrivateProfileString(buf, "nowchar", "",
                               nwflag, sizeof(nwflag), ODBC_INI);
    SQLGetPrivateProfileString(buf, "shortnames", "",
                               snflag, sizeof(snflag), ODBC_INI);
    SQLGetPrivateProfileString(buf, "longnames", "",
                               lnflag, sizeof(lnflag), ODBC_INI);
    SQLGetPrivateProfileString(buf, "nocreat", "",
                               ncflag, sizeof(ncflag), ODBC_INI);
    SQLGetPrivateProfileString(buf, "fksupport", "",
                               fkflag, sizeof(fkflag), ODBC_INI);
    SQLGetPrivateProfileString(buf, "loadext", "",
                               loadext, sizeof(loadext), ODBC_INI);
    SQLGetPrivateProfileString(buf, "journalmode", "",
                               jmode, sizeof(jmode), ODBC_INI);
    SQLGetPrivateProfileString(buf, "jdconv", "",
                               jdflag, sizeof(jdflag), ODBC_INI);
#if defined(_WIN32) || defined(_WIN64)
    SQLGetPrivateProfileString(buf, "oemcp", "1",
                               oemcp, sizeof(oemcp), ODBC_INI);
#endif
    SQLGetPrivateProfileString(buf, "bigint", "",
                               biflag, sizeof(biflag), ODBC_INI);
#endif
    tracef[0] = '\0';
#ifdef WITHOUT_DRIVERMGR
    getdsnattr(buf, "tracefile", tracef, sizeof(tracef));
#else
    SQLGetPrivateProfileString(buf, "tracefile", "",
                               tracef, sizeof(tracef), ODBC_INI);
#endif
    if (tracef[0] != '\0')
    {
        d->trace = fopen(tracef, "a");
    }
    d->nowchar = getbool(nwflag);
    d->shortnames = getbool(snflag);
    d->longnames = getbool(lnflag);
    d->nocreat = getbool(ncflag);
    d->fksupport = getbool(fkflag);
    d->jdconv = getbool(jdflag);
#if defined(_WIN32) || defined(_WIN64)
    d->oemcp = getbool(oemcp);
#else
    d->oemcp = 0;
#endif
    d->dobigint = getbool(biflag);
    d->pwd = pwd;
    d->pwdLen = 0;
    if (d->pwd)
    {
        d->pwdLen = (pwdLen == SQL_NTS) ? strlen(d->pwd) : pwdLen;
    }
    ret = dbopen(d, dbname, isu, (char *)dsn, sflag, spflag, ntflag,
                 jmode, busy);
    /*
    if (ret == SQL_SUCCESS)
    {
        dbloadext(d, loadext);
    }
    */
    return ret;
}

#ifndef WINTERFACE
/**
 * Connect to SQLite database.
 * @param dbc database connection handle
 * @param dsn DSN string
 * @param dsnLen length of DSN string or SQL_NTS
 * @param uid user id string or NULL
 * @param uidLen length of user id string or SQL_NTS
 * @param pwd password string or NULL
 * @param pwdLen length of password string or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLConnect(SQLHDBC dbc, SQLCHAR *dsn, SQLSMALLINT dsnLen,
           SQLCHAR *uid, SQLSMALLINT uidLen,
           SQLCHAR *pwd, SQLSMALLINT pwdLen)
{
    SQLRETURN ret;

    HDBC_LOCK(dbc);
    ret = drvconnect(dbc, dsn, dsnLen, (char *)pwd, pwdLen, 0);
    HDBC_UNLOCK(dbc);
    return ret;
}
#endif

#ifdef WINTERFACE
/**
 * Connect to SQLite database.
 * @param dbc database connection handle
 * @param dsn DSN string
 * @param dsnLen length of DSN string or SQL_NTS
 * @param uid user id string or NULL
 * @param uidLen length of user id string or SQL_NTS
 * @param pwd password string or NULL
 * @param pwdLen length of password string or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLConnectW(SQLHDBC dbc, SQLWCHAR *dsn, SQLSMALLINT dsnLen,
            SQLWCHAR *uid, SQLSMALLINT uidLen,
            SQLWCHAR *pwd, SQLSMALLINT pwdLen)
{
    char *dsna = NULL;
    char *pwda = NULL;
    SQLRETURN ret;

    HDBC_LOCK(dbc);
    if (dsn)
    {
        dsna = uc_to_utf_c(dsn, dsnLen);
        if (!dsna)
        {
            DBC *d = (DBC *)dbc;

            setstatd(d, -1, "out of memory", (*d->ov3) ? "HY000" : "S1000");
            ret = SQL_ERROR;
            goto done;
        }
    }
    if (pwd)
    {
        pwda = uc_to_utf_c(pwd, pwdLen);
        if (!pwda)
        {
            DBC *d = (DBC *)dbc;

            setstatd(d, -1, "out of memory", (*d->ov3) ? "HY000" : "S1000");
            ret = SQL_ERROR;
            goto done;
        }
    }
    ret = drvconnect(dbc, (SQLCHAR *)dsna, SQL_NTS, pwda, SQL_NTS, 1);
done:
    HDBC_UNLOCK(dbc);
    uc_free(dsna);
    uc_free(pwda);
    return ret;
}
#endif

static void
s3stmt_end(STMT *s)
{
    DBC *d;

    if (!s || !s->presto_stmt)
    {
        return;
    }
    d = (DBC *)s->dbc;
    if (d)
    {
        d->busyint = 0;
    }
    if (!s->s3stmt_noreset)
    {
        dbtraceapi(d, "sqlite3_reset", 0);
        // FIXME: have to deallocate query
        // DEALLOCATE PREPARE my_query;
        // sqlite3_reset(s->s3stmt);
        s->s3stmt_noreset = 1;
        s->presto_stmt_rownum = -1;
    }
    if (d->cur_s3stmt == s)
    {
        d->cur_s3stmt = NULL;
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

    while (*sql && ISSPACE(*sql)) {
	++sql;
    }
    if (*sql && *sql != ';') {
	int i, size;
	static const struct {
	    int len;
	    const char *str;
	} ddlstr[] = {
	    { 5, "alter" },
	    { 7, "analyze" },
	    { 6, "attach" },
	    { 5, "begin" },
	    { 6, "commit" },
	    { 6, "create" },
	    { 6, "detach" },
	    { 4, "drop" },
	    { 3, "end" },
	    { 7, "reindex" },
	    { 7, "release" },
	    { 8, "rollback" },
	    { 9, "savepoint" },
	    { 6, "vacuum" }
	};

	size = strlen(sql);
	for (i = 0; i < array_size(ddlstr); i++) {
	    if (size >= ddlstr[i].len &&
		strncasecmp(sql, ddlstr[i].str, ddlstr[i].len) == 0) {
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
 * @param isselect output indicator for SELECT (1) or DDL statement (2)
 * @param errmsg output error message
 * @result newly allocated string containing query string for SQLite or NULL
 */

static char *
fixupsql(char *sql, int sqlLen, int cte, int *nparam, int *isselect,
         char **errmsg)
{
    char *q = sql, *qz = NULL, *p, *inq = NULL, *out;
    int np = 0, isddl = -1, size;

    if (errmsg)
    {
        *errmsg = NULL;
    }
    if (sqlLen != SQL_NTS)
    {
        qz = q = xmalloc(sqlLen + 1);
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
    p = xmalloc(size);
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
    if (isselect)
    {
        if (isddl < 0)
        {
            isddl = checkddl(out);
        }
        if (isddl > 0)
        {
            *isselect = 2;
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
            if (size >= 6 &&
                (strncasecmp(p, "select", 6) == 0 ||
                 strncasecmp(p, "pragma", 6) == 0))
            {
                *isselect = 1;
            }
            else if (cte && size >= 4 && strncasecmp(p, "with", 4) == 0)
            {
                *isselect = 1;
            }
            else if (size >= 7 && strncasecmp(p, "explain", 7) == 0)
            {
                *isselect = 1;
            }
            else
            {
                *isselect = 0;
            }
        }
    }
    return out;
}

/**
 * Internal query preparation used by SQLPrepare() and SQLExecDirect().
 * @param stmt statement handle
 * @param query query string
 * @param queryLen length of query string or SQL_NTS
 * @result ODBC error code
 */
static SQLRETURN
drvprepare(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER queryLen)
{
    STMT *s;
    DBC *d;
    char *errp = NULL;
    SQLRETURN sret;

    if (stmt == SQL_NULL_HSTMT)
    {
        return SQL_INVALID_HANDLE;
    }
    s = (STMT *)stmt;
    if (s->dbc == SQL_NULL_HDBC)
    {
    noconn:
        return noconn(s);
    }
    d = (DBC *)s->dbc;
    if (!d->presto_client)
    {
        goto noconn;
    }
    s3stmt_end(s);
    presto_stmt_drop(s);
    //sret = starttran(s);
    //if (sret != SQL_SUCCESS)
    //{
    //    return sret;
    //}

    freep(&s->query);
    s->query = (SQLCHAR *)fixupsql((char *)query, queryLen,
                                   (d->version >= 0x030805),
                                   &s->nparams, &s->isselect, &errp);
    if (!s->query)
    {
        if (errp)
        {
            setstat(s, -1, "%s", (*s->ov3) ? "HY000" : "S1000", errp);
            return SQL_ERROR;
        }
        return nomem(s);
    }
    errp = NULL;
    freeresult(s, -1);
    if (s->isselect == 1)
    {
        int ret, ncols, nretry = 0;
        const char *rest;
        sqlite3_stmt *s3stmt = NULL;

#if defined(HAVE_SQLITE3PREPAREV2) && (HAVE_SQLITE3PREPAREV2)
        dbtraceapi(d, "sqlite3_prepare_v2", (char *)s->query);
#else
        dbtraceapi(d, "sqlite3_prepare", (char *)s->query);
#endif
        do
        {
            s3stmt = NULL;
#if defined(HAVE_SQLITE3PREPAREV2) && (HAVE_SQLITE3PREPAREV2)
            ret = sqlite3_prepare_v2(d->sqlite, (char *)s->query, -1,
                                     &s3stmt, &rest);
#else
            ret = sqlite3_prepare(d->sqlite, (char *)s->query, -1,
                                  &s3stmt, &rest);
#endif
            if (ret != PRESTO_OK)
            {
                if (s3stmt)
                {
                    sqlite3_finalize(s3stmt);
                    s3stmt = NULL;
                }
            }
        } while (ret == SQLITE_SCHEMA && (++nretry) < 2);
        dbtracerc(d, ret, NULL);
        if (ret != SQLITE_OK)
        {
            if (s3stmt)
            {
                dbtraceapi(d, "sqlite3_finalize", 0);
                sqlite3_finalize(s3stmt);
            }
            setstat(s, ret, "%s (%d)", (*s->ov3) ? "HY000" : "S1000",
                    sqlite3_errmsg(d->sqlite), ret);
            return SQL_ERROR;
        }
        if (sqlite3_bind_parameter_count(s3stmt) != s->nparams)
        {
            dbtraceapi(d, "sqlite3_finalize", 0);
            sqlite3_finalize(s3stmt);
            setstat(s, SQLITE_ERROR, "parameter marker count incorrect",
                    (*s->ov3) ? "HY000" : "S1000");
            return SQL_ERROR;
        }
        ncols = sqlite3_column_count(s3stmt);
        s->guessed_types = 0;
        setupdyncols(s, s3stmt, &ncols);
        s->ncols = ncols;
        s->presto_stmt = s3stmt;
    }
    mkbindcols(s, s->ncols);
    s->paramset_count = 0;
    return SQL_SUCCESS;
}

/*
static int
drvgettable(STMT *s, const char *sql, char ***resp, int *nrowp,
            int *ncolp, char **errp, int nparam, BINDPARM *p)
{
    DBC *d = (DBC *)s->dbc;
    int rc = PRESTO_OK, keep = sql == NULL;
    TBLRES tres;
    const char *sqlleft = 0;
    int nretry = 0, haveerr = 0;

    if (!resp)
    {
        return PRESTO_ERROR;
    }
    *resp = NULL;
    if (nrowp)
    {
        *nrowp = 0;
    }
    if (ncolp)
    {
        *ncolp = 0;
    }
    tres.errmsg = NULL;
    tres.nrow = 0;
    tres.ncol = 0;
    tres.ndata = 1;
    tres.nalloc = 20;
    tres.rc = PRESTO_OK;
    tres.resarr = xmalloc(sizeof(char *) * tres.nalloc);
    tres.stmt = NULL;
    tres.s = s;
    if (!tres.resarr)
    {
        return PRESTO_NOMEM;
    }
    tres.resarr[0] = 0;
    if (sql == NULL)
    {
        tres.stmt = s->s3stmt;
        if (tres.stmt == NULL)
        {
            return PRESTO_NOMEM;
        }
        goto retrieve;
    }
    while (sql && *sql && (rc == PRESTO_OK || (rc == PRESTO_SCHEMA && (++nretry) < 2)))
    {
        int ncol;

        tres.stmt = NULL;
#if defined(HAVE_SQLITE3PREPAREV2) && (HAVE_SQLITE3PREPAREV2)
        dbtraceapi(d, "sqlite3_prepare_v2", sql);
        rc = sqlite3_prepare_v2(d->sqlite, sql, -1, &tres.stmt, &sqlleft);
#else
        dbtraceapi(d, "sqlite3_prepare", sql);
        rc = sqlite3_prepare(d->sqlite, sql, -1, &tres.stmt, &sqlleft);
#endif
        if (rc != PRESTO_OK)
        {
            if (tres.stmt)
            {
                dbtraceapi(d, "sqlite3_finalize", 0);
                sqlite3_finalize(tres.stmt);
                tres.stmt = NULL;
            }
            continue;
        }
        if (!tres.stmt)
        {
            // this happens for a comment or white-space
            sql = sqlleft;
            continue;
        }
    retrieve:
        if (sqlite3_bind_parameter_count(tres.stmt) != nparam)
        {
            if (errp)
            {
                *errp =
                    sqlite3_mprintf("%s", "parameter marker count incorrect");
            }
            haveerr = 1;
            rc = SQLITE_ERROR;
            goto tbldone;
        }
        s3bind(d, tres.stmt, nparam, p);
        ncol = sqlite3_column_count(tres.stmt);
        while (1)
        {
            if (s->max_rows && tres.nrow >= s->max_rows)
            {
                rc = PRESTO_OK;
                break;
            }
            rc = sqlite3_step(tres.stmt);
            if (rc == PRESTO_ROW || rc == PRESTO_DONE)
            {
                if (drvgettable_row(&tres, ncol, rc))
                {
                    rc = PRESTO_ABORT;
                    goto tbldone;
                }
            }
            if (rc != PRESTO_ROW)
            {
                if (keep)
                {
                    dbtraceapi(d, "sqlite3_reset", 0);
                    rc = sqlite3_reset(tres.stmt);
                    s->s3stmt_noreset = 1;
                }
                else
                {
                    dbtraceapi(d, "sqlite3_finalize", 0);
                    rc = sqlite3_finalize(tres.stmt);
                }
                tres.stmt = 0;
                if (rc != PRESTO_SCHEMA)
                {
                    nretry = 0;
                    sql = sqlleft;
                    while (sql && ISSPACE(*sql))
                    {
                        sql++;
                    }
                }
                if (rc == SQLITE_DONE)
                {
                    rc = PRESTO_OK;
                }
                break;
            }
        }
    }
tbldone:
    if (tres.stmt)
    {
        if (keep)
        {
            if (!s->s3stmt_noreset)
            {
                dbtraceapi(d, "sqlite3_reset", 0);
                sqlite3_reset(tres.stmt);
                s->s3stmt_noreset = 1;
            }
        }
        else
        {
            dbtraceapi(d, "sqlite3_finalize", 0);
            sqlite3_finalize(tres.stmt);
        }
    }
    if (haveerr)
    {
        // message already in *errp if any
    }
    else if (rc != PRESTO_OK && rc == sqlite3_errcode(d->presto_client) && errp)
    {
        *errp = sqlite3_mprintf("%s", sqlite3_errmsg(d->presto_client));
    }
    else if (errp)
    {
        *errp = NULL;
    }
    if (tres.resarr)
    {
        tres.resarr[0] = (char *)(tres.ndata - 1);
    }
    if (rc == SQLITE_ABORT)
    {
        freerows(&tres.resarr[1]);
        if (tres.errmsg)
        {
            if (errp)
            {
                if (*errp)
                {
                    sqlite3_free(*errp);
                }
                *errp = tres.errmsg;
            }
            else
            {
                sqlite3_free(tres.errmsg);
            }
        }
        return tres.rc;
    }
    sqlite3_free(tres.errmsg);
    if (rc != PRESTO_OK)
    {
        freerows(&tres.resarr[1]);
        return rc;
    }
    *resp = &tres.resarr[1];
    if (ncolp)
    {
        *ncolp = tres.ncol;
    }
    if (nrowp)
    {
        *nrowp = tres.nrow;
    }
    return rc;
}
*/

/**
 * Internal query execution used by SQLExecute() and SQLExecDirect().
 * @param stmt statement handle
 * @param initial false when called from SQLPutData()
 * @result ODBC error code
 */

static SQLRETURN
drvexecute(SQLHSTMT stmt, int initial)
{
    SQLRETURN ret;
    STMT *s;
    DBC *d;
    char *errp = NULL;
    int rc, i, ncols = 0, nrows = 0, busy_count;

    if (stmt == SQL_NULL_HSTMT)
    {
        return SQL_INVALID_HANDLE;
    }
    s = (STMT *)stmt;
    if (s->dbc == SQL_NULL_HDBC)
    {
    noconn:
        return noconn(s);
    }
    d = (DBC *)s->dbc;
    if (!d->presto_client)
    {
        goto noconn;
    }
    if (!s->query)
    {
        setstat(s, -1, "no query prepared", (*s->ov3) ? "HY000" : "S1000");
        return SQL_ERROR;
    }
    if (s->nbindparms < s->nparams)
    {
    unbound:
        setstat(s, -1, "unbound parameters in query",
                (*s->ov3) ? "HY000" : "S1000");
        return SQL_ERROR;
    }
    for (i = 0; i < s->nparams; i++)
    {
        BINDPARM *p = &s->bindparms[i];

        if (!p->bound)
        {
            goto unbound;
        }
        if (initial)
        {
            SQLLEN *lenp = p->lenp;

            if (lenp && *lenp < 0 && *lenp > SQL_LEN_DATA_AT_EXEC_OFFSET &&
                *lenp != SQL_NTS && *lenp != SQL_NULL_DATA &&
                *lenp != SQL_DATA_AT_EXEC)
            {
                setstat(s, -1, "invalid length reference", "HY009");
                return SQL_ERROR;
            }
            if (lenp && (*lenp <= SQL_LEN_DATA_AT_EXEC_OFFSET ||
                         *lenp == SQL_DATA_AT_EXEC))
            {
                p->need = 1;
                p->offs = 0;
                p->len = 0;
            }
        }
    }
    //ret = starttran(s);
    //if (ret != SQL_SUCCESS)
    //{
    //    goto cleanup;
    //}
    busy_count = 0;
again:
    s3stmt_end(s);
    if (initial)
    {
        // fixup data-at-execution parameters and alloc'ed blobs
        s->pdcount = -1;
        for (i = 0; i < s->nparams; i++)
        {
            BINDPARM *p = &s->bindparms[i];

            if (p->param == p->parbuf)
            {
                p->param = NULL;
            }
            freep(&p->parbuf);
            if (p->need <= 0 &&
                p->lenp && (*p->lenp <= SQL_LEN_DATA_AT_EXEC_OFFSET || *p->lenp == SQL_DATA_AT_EXEC))
            {
                p->need = 1;
                p->offs = 0;
                p->len = 0;
            }
        }
    }
    if (s->nparams)
    {
        for (i = 0; i < s->nparams; i++)
        {
            ret = setupparam(s, (char *)s->query, i);
            if (ret != SQL_SUCCESS)
            {
                goto cleanup;
            }
        }
    }
    freeresult(s, 0);
    if (s->isselect == 1 && !d->intrans &&
        s->curtype == SQL_CURSOR_FORWARD_ONLY &&
        d->step_enable && s->nparams == 0 && d->cur_s3stmt == NULL)
    {
        s->nrows = -1;
        ret = s3stmt_start(s);
        if (ret == SQL_SUCCESS)
        {
            goto done2;
        }
    }
    rc = drvgettable(s, s->presto_stmt ? NULL : (char *)s->query, &s->rows,
                     &s->nrows, &ncols, &errp, s->nparams, s->bindparms);
    dbtracerc(d, rc, errp);
    if (rc == PRESTO_BUSY)
    {
        if (busy_handler((void *)d, ++busy_count))
        {
            if (errp)
            {
                sqlite3_free(errp);
                errp = NULL;
            }
            for (i = 0; i < s->nparams; i++)
            {
                BINDPARM *p = &s->bindparms[i];

                if (p->param == p->parbuf)
                {
                    p->param = NULL;
                }
                freep(&p->parbuf);
                if (!p->lenp || (*p->lenp > SQL_LEN_DATA_AT_EXEC_OFFSET &&
                                 *p->lenp != SQL_DATA_AT_EXEC))
                {
                    p->param = p->param0;
                }
                p->lenp = p->lenp0;
            }
            s->nrows = 0;
            goto again;
        }
    }
    if (rc != PRESTO_OK)
    {
        setstat(s, rc, "%s (%d)", (*s->ov3) ? "HY000" : "S1000",
                errp ? errp : "unknown error", rc);
        if (errp)
        {
            sqlite3_free(errp);
            errp = NULL;
        }
        ret = SQL_ERROR;
        goto cleanup;
    }
    if (errp)
    {
        sqlite3_free(errp);
        errp = NULL;
    }
    s->rowfree = freerows;
    if (s->isselect <= 0 || s->isselect > 1)
    {
        //
        // INSERT/UPDATE/DELETE or DDL results are immediately released.
        //
        freeresult(s, -1);
        nrows += sqlite3_changes(d->presto_client);
        s->nrows = nrows;
        goto done;
    }
    if (s->ncols != ncols)
    {
        //
        // Weird result.
        //
        setstat(s, -1, "broken result set %d/%d",
                (*s->ov3) ? "HY000" : "S1000", s->ncols, ncols);
        ret = SQL_ERROR;
        goto cleanup;
    }
done:
    mkbindcols(s, s->ncols);
done2:
    ret = SQL_SUCCESS;
    s->rowp = s->rowprs = -1;
    s->paramset_count++;
    s->paramset_nrows = s->nrows;
    if (s->paramset_count < s->paramset_size)
    {
        for (i = 0; i < s->nparams; i++)
        {
            BINDPARM *p = &s->bindparms[i];

            if (p->param == p->parbuf)
            {
                p->param = NULL;
            }
            freep(&p->parbuf);
            if (p->lenp0 &&
                s->parm_bind_type != SQL_PARAM_BIND_BY_COLUMN)
            {
                p->lenp = (SQLLEN *)((char *)p->lenp0 +
                                     s->paramset_count * s->parm_bind_type);
            }
            else if (p->lenp0 && p->inc > 0)
            {
                p->lenp = p->lenp0 + s->paramset_count;
            }
            if (!p->lenp || (*p->lenp > SQL_LEN_DATA_AT_EXEC_OFFSET &&
                             *p->lenp != SQL_DATA_AT_EXEC))
            {
                if (p->param0 &&
                    s->parm_bind_type != SQL_PARAM_BIND_BY_COLUMN)
                {
                    p->param = (char *)p->param0 +
                               s->paramset_count * s->parm_bind_type;
                }
                else if (p->param0 && p->inc > 0)
                {
                    p->param = (char *)p->param0 +
                               s->paramset_count * p->inc;
                }
            }
            else if (p->lenp && (*p->lenp <= SQL_LEN_DATA_AT_EXEC_OFFSET ||
                                 *p->lenp == SQL_DATA_AT_EXEC))
            {
                p->need = 1;
                p->offs = 0;
                p->len = 0;
            }
        }
        goto again;
    }
cleanup:
    if (ret != SQL_NEED_DATA)
    {
        for (i = 0; i < s->nparams; i++)
        {
            BINDPARM *p = &s->bindparms[i];

            if (p->param == p->parbuf)
            {
                p->param = NULL;
            }
            freep(&p->parbuf);
            if (!p->lenp || (*p->lenp > SQL_LEN_DATA_AT_EXEC_OFFSET &&
                             *p->lenp != SQL_DATA_AT_EXEC))
            {
                p->param = p->param0;
            }
            p->lenp = p->lenp0;
        }
        s->nrows = s->paramset_nrows;
        if (s->parm_proc)
        {
            *s->parm_proc = s->paramset_count;
        }
        s->paramset_count = 0;
        s->paramset_nrows = 0;
    }
    //
    // For INSERT/UPDATE/DELETE statements change the return code
    // to SQL_NO_DATA if the number of rows affected was 0.
    //
    if (*s->ov3 && s->isselect == 0 &&
        ret == SQL_SUCCESS && nrows == 0)
    {
        ret = SQL_NO_DATA;
    }
    return ret;
}

/**
 * Execute query directly.
 * @param stmt statement handle
 * @param query query string
 * @param queryLen length of query string or SQL_NTS
 * @result ODBC error code
 */
SQLRETURN SQL_API
SQLExecDirect(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER queryLen)
{
    SQLRETURN ret;
#if defined(_WIN32) || defined(_WIN64)
    char *q;
#endif

    HSTMT_LOCK(stmt);
#if defined(_WIN32) || defined(_WIN64)
    if (!((STMT *)stmt)->oemcp[0])
    {
        ret = drvprepare(stmt, query, queryLen);
        if (ret == SQL_SUCCESS)
        {
            ret = drvexecute(stmt, 1);
        }
        goto done;
    }
    q = wmb_to_utf_c((char *)query, queryLen);
    if (!q)
    {
        ret = nomem((STMT *)stmt);
        goto done;
    }
    query = (SQLCHAR *)q;
    queryLen = SQL_NTS;
#endif
    ret = drvprepare(stmt, query, queryLen);
    if (ret == SQL_SUCCESS)
    {
        ret = drvexecute(stmt, 1);
    }
#if defined(_WIN32) || defined(_WIN64)
    uc_free(q);
done:;
#endif
    HSTMT_UNLOCK(stmt);
    return ret;
}

/**
 * Execute query directly (UNICODE version).
 * @param stmt statement handle
 * @param query query string
 * @param queryLen length of query string or SQL_NTS
 * @result ODBC error code
 */
SQLRETURN SQL_API
SQLExecDirectW(SQLHSTMT stmt, SQLWCHAR *query, SQLINTEGER queryLen)
{
    SQLRETURN ret;
    char *q = uc_to_utf_c(query, queryLen);

    HSTMT_LOCK(stmt);
    if (!q)
    {
        ret = nomem((STMT *)stmt);
        goto done;
    }
    ret = drvprepare(stmt, (SQLCHAR *)q, SQL_NTS);
    uc_free(q);
    if (ret == SQL_SUCCESS)
    {
        ret = drvexecute(stmt, 1);
    }
done:
    HSTMT_UNLOCK(stmt);
    return ret;
}

/**
 * Allocate a HENV, HDBC, or HSTMT handle.
 * @param type handle type
 * @param input input handle (HENV, HDBC)
 * @param output pointer to output handle (HENV, HDBC, HSTMT)
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLAllocHandle(SQLSMALLINT type, SQLHANDLE input, SQLHANDLE *output)
{
    SQLRETURN ret;

    switch (type)
    {
    case SQL_HANDLE_ENV:
        ret = drvallocenv((SQLHENV *)output);
        if (ret == SQL_SUCCESS)
        {
            ENV *e = (ENV *)*output;

            if (e && e->magic == ENV_MAGIC)
            {
                e->ov3 = 1;
            }
        }
        return ret;
    case SQL_HANDLE_DBC:
        return drvallocconnect((SQLHENV)input, (SQLHDBC *)output);
    case SQL_HANDLE_STMT:
        HDBC_LOCK((SQLHDBC)input);
        ret = drvallocstmt((SQLHDBC)input, (SQLHSTMT *)output);
        HDBC_UNLOCK((SQLHDBC)input);
        return ret;
    }
    return SQL_ERROR;
}

/**
 * Free a HENV, HDBC, or HSTMT handle.
 * @param type handle type
 * @param h handle (HENV, HDBC, or HSTMT)
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLFreeHandle(SQLSMALLINT type, SQLHANDLE h)
{
    switch (type)
    {
    case SQL_HANDLE_ENV:
        return drvfreeenv((SQLHENV)h);
    case SQL_HANDLE_DBC:
        return drvfreeconnect((SQLHDBC)h);
    case SQL_HANDLE_STMT:
        return drvfreestmt((SQLHSTMT)h, SQL_DROP);
    }
    return SQL_ERROR;
}

#ifndef WINTERFACE
/**
 * Prepare HSTMT.
 * @param stmt statement handle
 * @param query query string
 * @param queryLen length of query string or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLPrepare(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER queryLen)
{
    SQLRETURN ret;
#if defined(_WIN32) || defined(_WIN64)
    char *q;
#endif

    HSTMT_LOCK(stmt);
#if defined(_WIN32) || defined(_WIN64)
    if (!((STMT *)stmt)->oemcp[0])
    {
        ret = drvprepare(stmt, query, queryLen);
        goto done;
    }
    q = wmb_to_utf_c((char *)query, queryLen);
    if (!q)
    {
        ret = nomem((STMT *)stmt);
        goto done;
    }
    query = (SQLCHAR *)q;
    queryLen = SQL_NTS;
#endif
    ret = drvprepare(stmt, query, queryLen);
#if defined(_WIN32) || defined(_WIN64)
    uc_free(q);
done:;
#endif
    HSTMT_UNLOCK(stmt);
    return ret;
}
#endif

#ifdef WINTERFACE
/**
 * Prepare HSTMT (UNICODE version).
 * @param stmt statement handle
 * @param query query string
 * @param queryLen length of query string or SQL_NTS
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLPrepareW(SQLHSTMT stmt, SQLWCHAR *query, SQLINTEGER queryLen)
{
    SQLRETURN ret;
    char *q = uc_to_utf_c(query, queryLen);

    HSTMT_LOCK(stmt);
    if (!q)
    {
        ret = nomem((STMT *)stmt);
        goto done;
    }
    ret = drvprepare(stmt, (SQLCHAR *)q, SQL_NTS);
    uc_free(q);
done:
    HSTMT_UNLOCK(stmt);
    return ret;
}
#endif

/**
 * Execute query.
 * @param stmt statement handle
 * @result ODBC error code
 */
SQLRETURN SQL_API
SQLExecute(SQLHSTMT stmt)
{
    SQLRETURN ret;

    HSTMT_LOCK(stmt);
    ret = drvexecute(stmt, 1);
    HSTMT_UNLOCK(stmt);
    return ret;
}
