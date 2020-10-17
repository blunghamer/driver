#include "driver.h"

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

static const char *xdigits = "0123456789ABCDEFabcdef";

#undef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#undef max
#define max(a, b) ((a) < (b) ? (b) : (a))

#ifdef _WIN32
#include <windows.h>
#define strcasecmp _stricmp
#define strncasecmp _strnicmp
#else
#include <unistd.h>
#endif

#define array_size(x) (sizeof(x) / sizeof(x[0]))

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

// forward
static void unbindcols(STMT *s);
static SQLRETURN mkbindcols(STMT *s, size_t ncols);



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
    setstat(s, -1, "out of memory", (*s->ov3) ? (char *)"HY000" : (char *)"S1000");
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
    setstat(s, -1, "not connected", (*s->ov3) ? (char *)"HY000" : (char *)"S1000");
    return SQL_ERROR;
}

/**
 * Report IM001 (not implemented) SQL error code for HSTMT.
 * @param stmt statement handle
 * @result ODBC error code
 */

static SQLRETURN
drvunimplstmt(HSTMT stmt)
{
	STMT *s;

	if (stmt == SQL_NULL_HSTMT)
	{
		return SQL_INVALID_HANDLE;
	}
	s = (STMT *)stmt;
	setstat(s, -1, "not supported", "IM001");
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
 * SQLite trace or profile callback
 * @param arg DBC pointer
 * @param msg log message, SQL text
 * @param et  elapsed time
 */

static void
#if defined(HAVE_SQLITE3PROFILE) && (HAVE_SQLITE3PROFILE)
dbtrace(void *arg, const char *msg, sqlite_uint64 et)
#else
dbtrace(void *arg, const char *msg)
#endif
{
    DBC *d = (DBC *)arg;

    if (msg && d->trace)
    {
        int len = strlen(msg);
#if defined(HAVE_SQLITE3PROFILE) && (HAVE_SQLITE3PROFILE)
        unsigned long s, f;
#endif

        if (len > 0)
        {
            char *end = "\n";

            if (msg[len - 1] != ';')
            {
                end = ";\n";
            }
            fprintf(d->trace, "%s%s", msg, end);
#if defined(HAVE_SQLITE3PROFILE) && (HAVE_SQLITE3PROFILE)
            s = et / 1000000000LL;
            f = et % 1000000000LL;
            fprintf(d->trace, "-- took %lu.%09lu seconds\n", s, f);
#endif
            fflush(d->trace);
        }
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
        prestoclient_deleteresult(s->presto_stmt->client, s->presto_stmt);
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
        for (size_t n = 0; n < s->nbindparms; n++)
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
        if (s->presto_stmt)
            prestoclient_deleteresult(s->presto_stmt->client, s->presto_stmt);        
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
    // s->jdconv = &d->jdconv;
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
        setstatd(d, -1, "not disconnected", (*d->ov3) ? (char *)"HY000" : (char *)"S1000");
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
 * @param dsn data source name
 * @param busy busy/lock timeout
 * @result ODBC error code
 */

static SQLRETURN
dbopen(DBC *d, char *name, char *dsn, char *sflag, char *ntflag, char *busy)
{
    char *endp = NULL;
    int rc, tmp, busyto = 100000;

    if (d->presto_client)
    {
        if (d->trace)
        {
            fprintf(d->trace, "closing existing presto handle: '%s'\n",
                    d->dbname);
            fflush(d->trace);
        }
        prestoclient_close(d->presto_client);
        d->presto_client = NULL;
    }
    unsigned int prt = 8080;
    d->presto_client = prestoclient_init("http", "localhost", &prt, NULL, NULL, NULL, NULL, NULL, NULL, true);
    if (!d->presto_client)
    {
        rc = PRESTO_ERROR;
    }
    else
    {
        rc = PRESTO_OK;
    }

    if (rc != PRESTO_OK)
    {
    connfail:
        setstatd(d, rc, "connect failed", (*d->ov3) ? (char *)"HY000" : (char *)"S1000");
        if (d->presto_client)
        {
            prestoclient_close(d->presto_client);
            d->presto_client = NULL;
        }
        return SQL_ERROR;
    }
    d->pwd = NULL;
    d->pwdLen = 0;
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
   
    if (d->trace)
    {
        fprintf(d->trace, "-- sqlite3_open:  '%s'\n", d->dbname);
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
    return SQL_SUCCESS;
}

/**
 * Internal connect to Presto database.
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
        len = min(sizeof(buf) - 1, (size_t)dsnLen);
    }
    if (dsn != NULL)
    {
        strncpy(buf, (char *)dsn, len);
    }
    buf[len] = '\0';
    if (buf[0] == '\0')
    {
        setstatd(d, -1, "invalid DSN", (*d->ov3) ? (char *)"HY090" : (char *)"S1090");
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
        d->pwdLen = (pwdLen == SQL_NTS) ? strlen(d->pwd) : (size_t)pwdLen;
    }
    ret = dbopen(d, dbname, (char *)dsn, sflag, ntflag, busy);    
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
 * Internal query preparation used by SQLPrepare().
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
    int rc;
    char *errp = NULL;
    SQLRETURN sret = SQL_ERROR;
    PRESTOCLIENT_RESULT *presto_stmt = NULL; 

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

    // recycle stmt
    s3stmt_end(s);
    presto_stmt_drop(s);
    freep(&s->query);

    s->query = (SQLCHAR *)fixupsql((char *)query, queryLen,
                                   (d->version >= 0x030805),
                                   &s->nparams, &s->isselect, &errp);
    if (!s->query)
    {
        if (errp)
        {
            setstat(s, -1, "%s", (*s->ov3) ? (char *)"HY000" : (char *)"S1000", errp);
            return SQL_ERROR;
        }
        return nomem(s);
    }
    errp = NULL;
    freeresult(s, -1);

    if (s->isselect == 1)
    {
        dbtraceapi(d, "prestoclient_prepare", (char *)s->query);
        rc = prestoclient_prepare(d->presto_client, &presto_stmt, (char *)s->query);
        if (rc != PRESTO_OK)
        {            
            if (presto_stmt)
            {
                dbtraceapi(d, "prestoclient_deleteresult", 0);
                prestoclient_deleteresult(d->presto_client, presto_stmt);
            }
            setstat(s, rc, "%s (%s)", (*s->ov3) ? (char *)"HY000" : (char *)"S1000", "ERROR preparing query", s->query);
            sret = SQL_ERROR;            
        } else {
            s->presto_stmt = presto_stmt;
            sret = PRESTO_OK;
        }
    } else {
        // there are statements presto cannot prepare so we execute direct (dms, special keywords)
        dbtraceapi(d, "prestoclient_prepare execute direct...", (char *)s->query);
        rc = prestoclient_query(d->presto_client, &presto_stmt, (char *)s->query, NULL, NULL);
        if (rc != PRESTO_OK)
        {            
            if (presto_stmt)
            {
                dbtraceapi(d, "prestoclient_deleteresult", 0);
                prestoclient_deleteresult(d->presto_client, presto_stmt);
            }
            setstat(s, rc, "%s (%s)", (*s->ov3) ? (char *)"HY000" : (char *)"S1000", "ERROR executing non preparable query", s->query);
            sret = SQL_ERROR;            
        } else {
            s->presto_stmt = presto_stmt;
            alloc_copy(&(presto_stmt->query), (char*)s->query);
            sret = PRESTO_OK;
        }        
    }
    return sret;
}

/**
 * Return number of columns of result set given HSTMT.
 * @param stmt statement handle
 * @param ncols output number of columns
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT *ncols)
{
    STMT *s;

    HSTMT_LOCK(stmt);
    if (stmt == SQL_NULL_HSTMT)
    {
        return SQL_INVALID_HANDLE;
    }
    s = (STMT *)stmt;
    if (ncols)
    {
        *ncols = s->presto_stmt->columncount;
    }
    HSTMT_UNLOCK(stmt);
    return SQL_SUCCESS;
}

/**
 * Return number of affected rows of HSTMT.
 * @param stmt statement handle
 * @param nrows output number of rows
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLRowCount(SQLHSTMT stmt, SQLLEN *nrows)
{
    STMT *s;

    HSTMT_LOCK(stmt);
    if (stmt == SQL_NULL_HSTMT)
    {
        return SQL_INVALID_HANDLE;
    }
    s = (STMT *)stmt;
    if (nrows)
    {
        *nrows = s->isselect ? 0 : 0;
    }
    HSTMT_UNLOCK(stmt);
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
 * Reallocate space for bound columns.
 * @param s statement pointer
 * @param ncols number of columns
 * @result ODBC error code
 */

static SQLRETURN
mkbindcols(STMT *s, size_t ncols)
{
    if (s->bindcols)
    {
        if (s->nbindcols < ncols)
        {
            int i;
            BINDCOL *bindcols =
                s->bindcols = (BINDCOL *)realloc(s->bindcols, ncols * sizeof(BINDCOL));

            if (!bindcols)
            {
                return nomem(s);
            }
            for (i = s->nbindcols; i < ncols; i++)
            {
                bindcols[i].type = SQL_UNKNOWN_TYPE;
                bindcols[i].max = 0;
                bindcols[i].lenp = NULL;
                bindcols[i].valp = NULL;
                bindcols[i].index = i;
                bindcols[i].offs = 0;
            }
            s->bindcols = bindcols;
            s->nbindcols = ncols;
        }
    }
    else if (ncols > 0)
    {
        s->bindcols = (BINDCOL *)xmalloc(ncols * sizeof(BINDCOL));
        if (!s->bindcols)
        {
            return nomem(s);
        }
        s->nbindcols = ncols;
        unbindcols(s);
    }
    return SQL_SUCCESS;
}

/**
 * Internal query execution used by SQLExecute().
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
    int rc, busy_count;
    size_t i, ncols = 0, nrows = 0;

    if (stmt == SQL_NULL_HSTMT)
    {
        printf("Invalid handle");
        return SQL_INVALID_HANDLE;
    }
    s = (STMT *)stmt;
    if (s->dbc == SQL_NULL_HDBC)
    {
    noconn:
        printf("No connection");
        return noconn(s);
    }
    d = (DBC *)s->dbc;
    if (!d->presto_client)
    {
        goto noconn;
    }
    if (!s->query)
    {
        setstat(s, -1, "no query prepared", (*s->ov3) ? (char *)"HY000" : (char *)"S1000");
        return SQL_ERROR;
    }

    ret = prestoclient_execute(d->presto_client, s->presto_stmt, NULL, NULL);
    if (ret != PRESTO_OK)
    {
        printf("Execute error %i", ret);
        setstat(s, -1, "unable to execute query", (*s->ov3) ? (char *)"HY000" : (char *)"S1000");
        ret = SQL_ERROR;
    } else {
        ret = mkbindcols(s, s->presto_stmt->columncount);
    }

    // For INSERT/UPDATE/DELETE statements change the return code
    // to SQL_NO_DATA if the number of rows affected was 0.
    if (*s->ov3 && s->isselect == 0 && ret == SQL_SUCCESS && nrows == 0)
    {
        ret = SQL_NO_DATA;
    }
    return ret;
}

/**
 * Internal query execution used by SQLExecDirect().
 * @param stmt statement handle
 * @param initial false when called from SQLPutData()
 * @result ODBC error code
 */

static SQLRETURN
drvexecutedirect(SQLHSTMT stmt, SQLCHAR *query, SQLINTEGER queryLen)
{
    SQLRETURN ret;
    STMT *s;
    DBC *d;
    char *errp = NULL;
    int rc, busy_count;
    size_t i, ncols = 0, nrows = 0;

    if (stmt == SQL_NULL_HSTMT)
    {
        printf("Invalid handle");
        return SQL_INVALID_HANDLE;
    }
    s = (STMT *)stmt;
    if (s->dbc == SQL_NULL_HDBC)
    {
    noconn:
        printf("No connection");
        return noconn(s);
    }
    d = (DBC *)s->dbc;
    if (!d->presto_client)
    {
        goto noconn;
    }

    s3stmt_end(s);
    presto_stmt_drop(s);
    freep(&s->query);

    s->query = (SQLCHAR *)fixupsql((char *)query, queryLen,
                                   (d->version >= 0x030805),
                                   &s->nparams, &s->isselect, &errp);
    if (!s->query)
    {
        if (errp)
        {
            setstat(s, -1, "%s", (*s->ov3) ? (char *)"HY000" : (char *)"S1000", errp);
            return SQL_ERROR;
        }
        return nomem(s);
    }
    errp = NULL;
    freeresult(s, -1);

    ret = prestoclient_query(d->presto_client, &(s->presto_stmt), (char*)s->query, NULL, NULL);
    if (ret != PRESTO_OK)
    {
        printf("Execute error %i", ret);
        setstat(s, -1, "unable to execute query direct", (*s->ov3) ? (char *)"HY000" : (char *)"S1000");
        ret = SQL_ERROR;
    } else {
        ret = mkbindcols(s, s->presto_stmt->columncount);        
    }

    // For INSERT/UPDATE/DELETE statements change the return code
    // to SQL_NO_DATA if the number of rows affected was 0.
    // if (*s->ov3 && s->isselect == 0 && ret == SQL_SUCCESS && s->presto_stmt->tablebuff->nrow == 0)
    //{
    //    ret = SQL_NO_DATA;
    //}    
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
        ret = drvexecutedirect(stmt, query, queryLen);        
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
    ret = drvexecutedirect(stmt, query, queryLen);    
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
 * Reset bound columns to unbound state.
 * @param s statement pointer
 */

static void
unbindcols(STMT *s)
{
    size_t i;

    for (i = 0; s->bindcols && i < s->nbindcols; i++)
    {
        s->bindcols[i].type = SQL_UNKNOWN_TYPE;
        s->bindcols[i].max = 0;
        s->bindcols[i].lenp = NULL;
        s->bindcols[i].valp = NULL;
        s->bindcols[i].index = i;
        s->bindcols[i].offs = 0;
    }
}

/**
 * Conditionally stop running sqlite statement
 * @param s statement pointer
 */

static void
s3stmt_end_if(STMT *s)
{
    DBC *d = (DBC *)s->dbc;

    if (d)
    {
        d->busyint = 0;
    }
    if (d && d->cur_s3stmt == s)
    {
        s3stmt_end(s);
    }
}

/**
 * Internal function to perform certain kinds of free/close on STMT.
 * @param stmt statement handle
 * @param opt SQL_RESET_PARAMS, SQL_UNBIND, SQL_CLOSE, or SQL_DROP
 * @result ODBC error code
 */

static SQLRETURN
drvfreestmt(SQLHSTMT stmt, SQLUSMALLINT opt)
{
    STMT *s;
    SQLRETURN ret = SQL_SUCCESS;
    SQLHDBC dbc;

    if (stmt == SQL_NULL_HSTMT)
    {
        return SQL_INVALID_HANDLE;
    }
    HSTMT_LOCK(stmt);
    s = (STMT *)stmt;
    dbc = s->dbc;
    switch (opt)
    {
    case SQL_RESET_PARAMS:
        freeparams(s);
        break;
    case SQL_UNBIND:
        unbindcols(s);
        break;
    case SQL_CLOSE:
        s3stmt_end_if(s);
        freeresult(s, 0);
        break;
    case SQL_DROP:
        s3stmt_end_if(s);
        ret = freestmt(stmt);
        break;
    default:
        setstat(s, -1, "unsupported option", (*s->ov3) ? (char *)"HYC00" : (char *)"S1C00");
        ret = SQL_ERROR;
        break;
    }
    HDBC_UNLOCK(dbc);
    return ret;
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

/**
 * Internal retrieve column attributes.
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param id attribute id
 * @param val output buffer
 * @param valMax length of output buffer
 * @param valLen output length
 * @param val2 integer output buffer
 * @result ODBC error code
 */

static SQLRETURN
drvcolattribute(SQLHSTMT stmt, SQLUSMALLINT col, SQLUSMALLINT id,
                SQLPOINTER val, SQLSMALLINT valMax, SQLSMALLINT *valLen,
                SQLPOINTER val2)
{
    STMT *s;
    PRESTOCLIENT_COLUMN *c;
    int v = 0;
    char *valc = (char *)val;
    SQLSMALLINT dummy;

    if (stmt == SQL_NULL_HSTMT)
    {
        return SQL_INVALID_HANDLE;
    }
    s = (STMT *)stmt;
    if (!s->presto_stmt->columns)
    {
        setstat(s, -1, "no columns", (*s->ov3) ? (char *)"07009" : (char *)"S1002");
        return SQL_ERROR;
    }
    if (col < 1 || col > s->presto_stmt->columncount)
    {
        setstat(s, -1, "invalid column", (*s->ov3) ? (char *)"07009" : (char *)"S1002");
        return SQL_ERROR;
    }
    if (!valLen)
    {
        valLen = &dummy;
    }
    c = s->presto_stmt->columns[col - 1];
    switch (id)
    {
    case SQL_DESC_COUNT:
        v = s->presto_stmt->columncount;
        break;
    case SQL_DESC_CATALOG_NAME:
        if (valc && valMax > 0)
        {
            strncpy(valc, c->catalog, valMax);
            valc[valMax - 1] = '\0';
        }
        *valLen = strlen(c->catalog);
    checkLen:
        if (*valLen >= valMax)
        {
            setstat(s, -1, "data right truncated", "01004");
            return SQL_SUCCESS_WITH_INFO;
        }
        break;
    case SQL_COLUMN_LENGTH:
    case SQL_DESC_LENGTH:
        v = strlen(c->name);
        // v = c->bytesize;
        break;
    case SQL_COLUMN_LABEL:
        if (c->name) {
           if (valc && valMax > 0)
           {
               strncpy(valc, c->name, valMax);
               valc[valMax - 1] = '\0';
           }
           *valLen = strlen(c->name);
           goto checkLen;
        }
        break;
    case SQL_COLUMN_NAME:
    case SQL_DESC_NAME:
        if (valc && valMax > 0)
        {
            strncpy(valc, c->name, valMax);
            valc[valMax - 1] = '\0';
        }
        *valLen = strlen(c->name);
        goto checkLen;        
    case SQL_DESC_SCHEMA_NAME:    
        if (c->schema) {
           if (valc && valMax > 0)
           {
               strncpy(valc, c->schema, valMax);
               valc[valMax - 1] = '\0';
           }
           *valLen = strlen(c->schema);
           goto checkLen;
        }
        break;
        // char *z = "";
        // if (valc && valMax > 0)
        // {
        //     strncpy(valc, z, valMax);
        //     valc[valMax - 1] = '\0';
        // }
        // *valLen = strlen(z);
        // goto checkLen;
#ifdef SQL_DESC_BASE_COLUMN_NAME
    // case SQL_DESC_BASE_COLUMN_NAME:
    //     if (strchr(c->column, '(') || strchr(c->column, ')'))
    //     {
    //         valc[0] = '\0';
    //         *valLen = 0;
    //     }
    //     else if (valc && valMax > 0)
    //     {
    //         strncpy(valc, c->column, valMax);
    //         valc[valMax - 1] = '\0';
    //         *valLen = strlen(c->column);
    //     }
    //     goto checkLen;
#endif
    case SQL_DESC_TYPE_NAME:
    {
        char *p = NULL, *tn = c->type ? (char *)"varchar" : (char *)"varchar";

#ifdef WINTERFACE
        if (c->type == SQL_WCHAR ||
            c->type == SQL_WVARCHAR ||
            c->type == SQL_WLONGVARCHAR)
        {
            if (!(s->nowchar[0] || s->nowchar[1]))
            {
                if (strcasecmp(tn, "varchar") == 0)
                {
                    tn = "wvarchar";
                }
            }
        }
#endif
        if (valc && valMax > 0)
        {
            strncpy(valc, tn, valMax);
            valc[valMax - 1] = '\0';
            p = strchr(valc, '(');
            if (p)
            {
                *p = '\0';
                while (p > valc && ISSPACE(p[-1]))
                {
                    --p;
                    *p = '\0';
                }
            }
            *valLen = strlen(valc);
        }
        else
        {
            *valLen = strlen(tn);
            p = strchr(tn, '(');
            if (p)
            {
                *valLen = p - tn;
                while (p > tn && ISSPACE(p[-1]))
                {
                    --p;
                    *valLen -= 1;
                }
            }
        }
        goto checkLen;
    }
    case SQL_DESC_OCTET_LENGTH:
        //  v = c->size;
#ifdef WINTERFACE
        if (c->type == SQL_WCHAR ||
            c->type == SQL_WVARCHAR ||
            c->type == SQL_WLONGVARCHAR)
        {
            if (!(s->nowchar[0] || s->nowchar[1]))
            {
                v *= sizeof(SQLWCHAR);
            }
        }
#endif
        break;
#if (SQL_COLUMN_TABLE_NAME != SQL_DESC_TABLE_NAME)
    case SQL_COLUMN_TABLE_NAME:
#endif
#ifdef SQL_DESC_BASE_TABLE_NAME
    case SQL_DESC_BASE_TABLE_NAME:
#endif
    case SQL_DESC_TABLE_NAME:
        if (valc && valMax > 0)
        {
            strncpy(valc, c->table, valMax);
            valc[valMax - 1] = '\0';
        }
        *valLen = strlen(c->table);
        goto checkLen;
    case SQL_DESC_TYPE:        
        switch (c->type) {
            case PRESTOCLIENT_TYPE_TIMESTAMP_WITH_TIME_ZONE:
                /*fallthrough*/
            case PRESTOCLIENT_TYPE_TIMESTAMP:
                v = SQL_TIMESTAMP;
                break;
            case PRESTOCLIENT_TYPE_TIME_WITH_TIME_ZONE:
                /*fallthrough*/
            case PRESTOCLIENT_TYPE_TIME:
                v = SQL_TIME;
                break;
            case PRESTOCLIENT_TYPE_VARCHAR:
                v = SQL_VARCHAR;
                break;
            default:
                SQL_VARCHAR;
        }
        break;
#ifdef WINTERFACE
        if (s->nowchar[0] || s->nowchar[1])
        {
            switch (v)
            {
            case SQL_WCHAR:
                v = SQL_CHAR;
                break;
            case SQL_WVARCHAR:
                v = SQL_VARCHAR;
                break;
#ifdef SQL_LONGVARCHAR
            case SQL_WLONGVARCHAR:
                v = SQL_LONGVARCHAR;
                break;
#endif
            }
        }
#endif
        break;
        /*
    case SQL_DESC_CONCISE_TYPE:
        switch (c->type)
        {
        case SQL_INTEGER:
            v = SQL_C_LONG;
            break;
        case SQL_TINYINT:
            v = SQL_C_TINYINT;
            break;
        case SQL_SMALLINT:
            v = SQL_C_SHORT;
            break;
        case SQL_FLOAT:
            v = SQL_C_FLOAT;
            break;
        case SQL_DOUBLE:
            v = SQL_C_DOUBLE;
            break;
        case SQL_TIMESTAMP:
            v = SQL_C_TIMESTAMP;
            break;
        case SQL_TIME:
            v = SQL_C_TIME;
            break;
        case SQL_DATE:
            v = SQL_C_DATE;
            break;
#ifdef SQL_C_TYPE_TIMESTAMP
        case SQL_TYPE_TIMESTAMP:
            v = SQL_C_TYPE_TIMESTAMP;
            break;
#endif
#ifdef SQL_C_TYPE_TIME
        case SQL_TYPE_TIME:
            v = SQL_C_TYPE_TIME;
            break;
#endif
#ifdef SQL_C_TYPE_DATE
        case SQL_TYPE_DATE:
            v = SQL_C_TYPE_DATE;
            break;
#endif
#ifdef SQL_BIT
        case SQL_BIT:
            v = SQL_C_BIT;
            break;
#endif
#ifdef SQL_BIGINT
        case SQL_BIGINT:
            v = SQL_C_SBIGINT;
            break;
#endif
        default:
#ifdef WINTERFACE
            v = (s->nowchar[0] || s->nowchar[1]) ? SQL_C_CHAR : SQL_C_WCHAR;
#else
            v = SQL_C_CHAR;
#endif
            break;
        }
        break;
*/
    case SQL_DESC_UPDATABLE:
        v = SQL_TRUE;
        break;
    case SQL_COLUMN_DISPLAY_SIZE:
        //v = strlen(c->name) + 1;
        v = c->bytesize + 1;
        break;
    case SQL_COLUMN_UNSIGNED:
        v = SQL_FALSE;
        break;
    case SQL_COLUMN_SEARCHABLE:
        v = SQL_SEARCHABLE;
        break;
    case SQL_COLUMN_SCALE:
    case SQL_DESC_SCALE:
        v = 20;
        break;
        /*
    case SQL_COLUMN_PRECISION:
    case SQL_DESC_PRECISION:
        switch (c->type)
        {
        case SQL_SMALLINT:
            v = 5;
            break;
        case SQL_INTEGER:
            v = 10;
            break;
        case SQL_FLOAT:
        case SQL_REAL:
        case SQL_DOUBLE:
            v = 15;
            break;
        case SQL_DATE:
            v = 0;
            break;
        case SQL_TIME:
            v = 0;
            break;
#ifdef SQL_TYPE_TIMESTAMP
        case SQL_TYPE_TIMESTAMP:
#endif
        case SQL_TIMESTAMP:
            v = (c->prec >= 0 && c->prec <= 3) ? c->prec : 3;
            break;
        default:
            v = c->prec;
            break;
        }
        break;
*/
    case SQL_COLUMN_MONEY:
        v = SQL_FALSE;
        break;
    case SQL_COLUMN_AUTO_INCREMENT:
        v = SQL_FALSE;
        break;
    case SQL_DESC_NULLABLE:
        v = SQL_TRUE;
        break;
        /*
#ifdef SQL_DESC_NUM_PREC_RADIX
    case SQL_DESC_NUM_PREC_RADIX:
        switch (c->type)
        {
#ifdef WINTERFACE
        case SQL_WCHAR:
        case SQL_WVARCHAR:
#ifdef SQL_LONGVARCHAR
        case SQL_WLONGVARCHAR:
#endif
#endif
        case SQL_CHAR:
        case SQL_VARCHAR:
#ifdef SQL_LONGVARCHAR
        case SQL_LONGVARCHAR:
#endif
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
            v = 0;
            break;
        default:
            v = 2;
        }
        break;  
#endif
*/
    default:
        setstat(s, -1, "unsupported column attribute %d", "HY091", id);
        return SQL_ERROR;
    }
    if (val2)
    {
        *(SQLLEN *)val2 = v;
    }
    return SQL_SUCCESS;
}

#ifndef WINTERFACE
/**
 * Retrieve column attributes.
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param id attribute id
 * @param val output buffer
 * @param valMax length of output buffer
 * @param valLen output length
 * @param val2 integer output buffer
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT col, SQLUSMALLINT id,
                SQLPOINTER val, SQLSMALLINT valMax, SQLSMALLINT *valLen,
                SQLLEN *val2)
{
#if defined(_WIN32) || defined(_WIN64)
    SQLSMALLINT len = 0;
#endif
    SQLRETURN ret;

    HSTMT_LOCK(stmt);
#if defined(_WIN32) || defined(_WIN64)
    if (!((STMT *)stmt)->oemcp[0])
    {
        ret = drvcolattribute(stmt, col, id, val, valMax, valLen,
                              (SQLPOINTER)val2);
        goto done;
    }
    ret = drvcolattribute(stmt, col, id, val, valMax, &len,
                          (SQLPOINTER)val2);
    if (SQL_SUCCEEDED(ret))
    {
        char *v = NULL;

        switch (id)
        {
        case SQL_DESC_SCHEMA_NAME:
        case SQL_DESC_CATALOG_NAME:
        case SQL_COLUMN_LABEL:
        case SQL_DESC_NAME:
        case SQL_DESC_TABLE_NAME:
#ifdef SQL_DESC_BASE_TABLE_NAME
        case SQL_DESC_BASE_TABLE_NAME:
#endif
#ifdef SQL_DESC_BASE_COLUMN_NAME
        case SQL_DESC_BASE_COLUMN_NAME:
#endif
        case SQL_DESC_TYPE_NAME:
            if (val && valMax > 0)
            {
                int vmax = valMax;

                v = utf_to_wmb((char *)val, SQL_NTS);
                if (v)
                {
                    strncpy(val, v, vmax);
                    len = min(vmax, strlen(v));
                    uc_free(v);
                }
                if (vmax > 0)
                {
                    v = (char *)val;
                    v[vmax - 1] = '\0';
                }
            }
            if (len <= 0)
            {
                len = 0;
            }
            break;
        }
        if (valLen)
        {
            *valLen = len;
        }
    }
done:;
#else
    ret = drvcolattribute(stmt, col, id, val, valMax, valLen,
                          (SQLPOINTER)val2);
#endif
    HSTMT_UNLOCK(stmt);
    return ret;
}
#endif

#ifdef WINTERFACE
/**
 * Retrieve column attributes (UNICODE version).
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param id attribute id
 * @param val output buffer
 * @param valMax length of output buffer
 * @param valLen output length
 * @param val2 integer output buffer
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLColAttributeW(SQLHSTMT stmt, SQLUSMALLINT col, SQLUSMALLINT id,
                 SQLPOINTER val, SQLSMALLINT valMax, SQLSMALLINT *valLen,
                 COLATTRIBUTE_LAST_ARG_TYPE val2)
{
    SQLRETURN ret;
    SQLSMALLINT len = 0;

    HSTMT_LOCK(stmt);
    ret = drvcolattribute(stmt, col, id, val, valMax, &len,
                          (SQLPOINTER)val2);
    if (SQL_SUCCEEDED(ret))
    {
        SQLWCHAR *v = NULL;

        switch (id)
        {
        case SQL_DESC_SCHEMA_NAME:
        case SQL_DESC_CATALOG_NAME:
        case SQL_COLUMN_LABEL:
        case SQL_DESC_NAME:
        case SQL_DESC_TABLE_NAME:
#ifdef SQL_DESC_BASE_TABLE_NAME
        case SQL_DESC_BASE_TABLE_NAME:
#endif
#ifdef SQL_DESC_BASE_COLUMN_NAME
        case SQL_DESC_BASE_COLUMN_NAME:
#endif
        case SQL_DESC_TYPE_NAME:
            if (val && valMax > 0)
            {
                int vmax = valMax / sizeof(SQLWCHAR);

                v = uc_from_utf((SQLCHAR *)val, SQL_NTS);
                if (v)
                {
                    uc_strncpy(val, v, vmax);
                    len = min(vmax, uc_strlen(v));
                    uc_free(v);
                    len *= sizeof(SQLWCHAR);
                }
                if (vmax > 0)
                {
                    v = (SQLWCHAR *)val;
                    v[vmax - 1] = '\0';
                }
            }
            if (len <= 0)
            {
                len = 0;
            }
            break;
        }
        if (valLen)
        {
            *valLen = len;
        }
    }
    HSTMT_UNLOCK(stmt);
    return ret;
}
#endif

/**
 * Internal fetch function for SQLFetchScroll() and SQLExtendedFetch().
 * @param stmt statement handle
 * @param orient fetch direction
 * @param offset offset for fetch direction
 * @result ODBC error code
 */

static SQLRETURN
drvfetchscroll(SQLHSTMT stmt, SQLSMALLINT orient, SQLINTEGER offset)
{
    STMT *s;
    int i, withinfo = 0;
    SQLRETURN ret;

    if (stmt == SQL_NULL_HSTMT)
    {
        return SQL_INVALID_HANDLE;
    }
    s = (STMT *)stmt;
    for (i = 0; i < s->rowset_size; i++)
    {
        s->row_status0[i] = SQL_ROW_NOROW;
    }
    if (s->row_status)
    {
        memcpy(s->row_status, s->row_status0,
               sizeof(SQLUSMALLINT) * s->rowset_size);
    }
    s->row_count0 = 0;
    if (s->row_count)
    {
        *s->row_count = s->row_count0;
    }
    if (!s->bindcols)
    {
        printf("Error: %s\n", "no bindcols");
        for (i = 0; i < s->rowset_size; i++)
        {
            s->row_status0[i] = SQL_ROW_ERROR;
        }
        ret = SQL_ERROR;
        i = 0;
        goto done2;
    }
    if (s->isselect != 1 && s->isselect != 3 && s->isselect != -1)
    {
        printf("Error: %s\n", "no result set!");
        setstat(s, -1, "no result set available", "24000");
        ret = SQL_ERROR;
        i = s->nrows;
        goto done2;
    }
    if (s->curtype == SQL_CURSOR_FORWARD_ONLY && orient != SQL_FETCH_NEXT)
    {
        printf("Error: %s\n", "wrong cursor type");
        setstat(s, -1, "wrong fetch direction", "01000");
        ret = SQL_ERROR;
        i = 0;
        goto done2;
    }    
    ret = SQL_SUCCESS;
    
    // if (((DBC *)(s->dbc))->cur_s3stmt == s && s->presto_stmt)
    // {
    //     s->rowp = s->rowprs = 0;
    //     for ( ; i < s->rowset_size; i++)
    //     {
    //         if (s->max_rows && s->presto_stmt_rownum + 1 >= s->max_rows)
    //         {
    //             printf("Error: %s\n", "ended up with no data but should have");
    //             ret = (i == 0) ? SQL_NO_DATA : SQL_SUCCESS;
    //             break;
    //         }
    //         ret = s3stmt_step(s);
    //         if (ret != SQL_SUCCESS)
    //         {
    //             s->row_status0[i] = SQL_ROW_ERROR;
    //             break;
    //         }
    //         if (s->nrows < 1)
    //         {
    //             break;
    //         }
    //         ret = dofetchbind(s, i);
    //         if (!SQL_SUCCEEDED(ret))
    //         {
    //             break;
    //         }
    //         else if (ret == SQL_SUCCESS_WITH_INFO)
    //         {
    //             withinfo = 1;
    //         }
    //     }
    // }
    
    // guard, if query with no results, tablebuff is NULL
    if (s->presto_stmt->tablebuff && s->presto_stmt->tablebuff->rowbuff)
    {        
        switch (orient)
        {
        case SQL_FETCH_NEXT:
            if ((s->presto_stmt->tablebuff->rowidx + 1) < s->presto_stmt->tablebuff->nrow ) {
                s->presto_stmt->tablebuff->rowidx += 1;                
            } else {
                ret = SQL_NO_DATA;
            }
            break;
            // if (s->nrows < 1)
            // {
            //     printf("Error: %s\n", "SQL_FETCH_NEXT no data");
            //     return SQL_NO_DATA;
            // }
            // if (s->rowp < 0)
            // {
            //     s->rowp = -1;
            // }
            // if (s->rowp >= s->nrows)
            // {
            //     s->rowp = s->rowprs = s->nrows;
            //     return SQL_NO_DATA;
            // }
            // break;
        case SQL_FETCH_PRIOR:            
            // if (s->nrows < 1 || s->rowp <= 0)
            // {
            //     printf("Error: %s\n", "SQL_FETCH_PRIOR no data");
            //     s->rowp = s->rowprs = -1;
            //     return SQL_NO_DATA;
            // }
            // s->rowp -= s->rowset_size + 1;
            // if (s->rowp < -1)
            // {
            //     printf("Error: %s\n", "SQL_FETCH_PRIOR no data");
            //     s->rowp = s->rowprs = -1;
            //     return SQL_NO_DATA;
            // }
            // break;
        case SQL_FETCH_FIRST:
            // if (s->nrows < 1)
            // {
            //     printf("Error: %s\n", "SQL_FETCH_FIRST no data");
            //     return SQL_NO_DATA;
            // }
            // s->rowp = -1;
            // break;
        case SQL_FETCH_LAST:
            // if (s->nrows < 1)
            // {
            //     printf("Error: %s\n", "SQL_FETCH_LAST no data");
            //     return SQL_NO_DATA;
            // }
            // s->rowp = s->nrows - s->rowset_size;
            // if (--s->rowp < -1)
            // {
            //     s->rowp = -1;
            // }
            // break;
        case SQL_FETCH_ABSOLUTE:
            // if (offset == 0)
            // {
            //     printf("Error: %s\n", "SQL_FETCH_ABSOLUTE no data");
            //     s->rowp = s->rowprs = -1;
            //     return SQL_NO_DATA;
            // }
            // else if (offset < 0)
            // {
            //     if (0 - offset <= s->nrows)
            //     {
            //         s->rowp = s->nrows + offset - 1;
            //         break;
            //     }
            //     printf("Error: %s\n", "SQL_FETCH_ABSOLUTE no data");
            //     s->rowp = s->rowprs = -1;
            //     return SQL_NO_DATA;
            // }
            // else if (offset > s->nrows)
            // {
            //     printf("Error: %s\n", "SQL_FETCH_ABSOLUTE no data");
            //     s->rowp = s->rowprs = s->nrows;
            //     return SQL_NO_DATA;
            // }
            // s->rowp = offset - 1 - 1;
            // break;
        case SQL_FETCH_RELATIVE:
            // if (offset >= 0)
            // {
            //     s->rowp += offset * s->rowset_size - 1;
            //     if (s->rowp >= s->nrows)
            //     {
            //         printf("Error: %s\n", "SQL_FETCH_RELATIVE no data");
            //         s->rowp = s->rowprs = s->nrows;
            //         return SQL_NO_DATA;
            //     }
            // }
            // else
            // {
            //     s->rowp += offset * s->rowset_size - 1;
            //     if (s->rowp < -1)
            //     {
            //         printf("Error: %s\n", "SQL_FETCH_RELATIVE no data");
            //         s->rowp = s->rowprs = -1;
            //         return SQL_NO_DATA;
            //     }
            // }
            // break;

            // 		case SQL_FETCH_BOOKMARK:
            // 			if (s->bkmrk == SQL_UB_ON && !s->bkmrkptr)
            // 			{
            // 				if (offset < 0 || offset >= s->nrows)
            // 				{
            // 					return SQL_NO_DATA;
            // 				}
            // 				s->rowp = offset - 1;
            // 				break;
            // 			}
            // 			if (s->bkmrk != SQL_UB_OFF && s->bkmrkptr)
            // 			{
            // 				int rowp;

            // 				if (s->bkmrk == SQL_UB_VARIABLE)
            // 				{
            // 					if (s->has_rowid >= 0)
            // 					{
            // 						sqlite_int64 bkmrk, rowid;

            // 						bkmrk = *(sqlite_int64 *)s->bkmrkptr;
            // 						for (rowp = 0; rowp < s->nrows; rowp++)
            // 						{
            // 							char **data, *endp = 0;

            // 							data = s->rows + s->ncols + (rowp * s->ncols) + s->has_rowid;
            // #ifdef __osf__
            // 							rowid = strtol(*data, &endp, 0);
            // #else
            // 							rowid = strtoll(*data, &endp, 0);
            // #endif
            // 							if (rowid == bkmrk)
            // 							{
            // 								break;
            // 							}
            // 						}
            // 					}
            // 					else
            // 					{
            // 						rowp = *(sqlite_int64 *)s->bkmrkptr;
            // 					}
            // 				}
            // 				else
            // 				{
            // 					rowp = *(int *)s->bkmrkptr;
            // 				}
            // 				if (rowp + offset < 0 || rowp + offset >= s->nrows)
            // 				{
            // 					return SQL_NO_DATA;
            // 				}
            // 				s->rowp = rowp + offset - 1;
            // 				break;
            // 			}
            /* fall through */
        default:
            printf("Unsupported fetch orient %i\n", orient);
            s->row_status0[0] = SQL_ROW_ERROR;
            ret = SQL_ERROR;
            goto done;
        }        
        // s->rowprs = s->rowp + 1;
        // for (; i < s->rowset_size; i++)
        // {
        //     ++s->rowp;
        //     if (s->rowp < 0 || s->rowp >= s->nrows)
        //     {
        //         break;
        //     }
        //     ret = dofetchbind(s, i);
        //     if (!SQL_SUCCEEDED(ret))
        //     {
        //         break;
        //     }
        //     else if (ret == SQL_SUCCESS_WITH_INFO)
        //     {
        //         withinfo = 1;
        //     }
        // }        
    } else {
        printf("Error: %s\n", "unable to fetch from non existent buffer");
        ret = SQL_NO_DATA;
    }
done:    
    // if (!could_fetch)
    // {
    //     if (SQL_SUCCEEDED(ret))
    //     {
    //         printf("Error: %s\n", "Succeeded but had no data (nothing to iterate on)");            
    //         return SQL_NO_DATA;
    //     }
    //     return ret;
    // }
    if (SQL_SUCCEEDED(ret))
    {
        ret = withinfo ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
    }
done2:    
    // if (s->row_status)
    // {
    //     memcpy(s->row_status, s->row_status0,
    //            sizeof(SQLUSMALLINT) * s->rowset_size);
    // }
    // s->row_count0 = i;
    // if (s->row_count)
    // {
    //     *s->row_count = s->row_count0;
    // }
    return ret;
}

/**
 * Fetch next result row.
 * @param stmt statement handle
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLFetch(SQLHSTMT stmt)
{
    SQLRETURN ret;

    HSTMT_LOCK(stmt);
    ret = drvfetchscroll(stmt, SQL_FETCH_NEXT, 0);
    HSTMT_UNLOCK(stmt);
    return ret;
}

/**
 * Perform bulk operation on HSTMT.
 * @param stmt statement handle
 * @param oper operation to be performed
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLBulkOperations(SQLHSTMT stmt, SQLSMALLINT oper)
{
    SQLRETURN ret;

    HSTMT_LOCK(stmt);
    ret = drvunimplstmt(stmt);
    HSTMT_UNLOCK(stmt);
    return ret;
}

/**
 * Function not implemented.
 */

SQLRETURN SQL_API
SQLSetScrollOptions(SQLHSTMT stmt, SQLUSMALLINT concur, SQLLEN rowkeyset,
                    SQLUSMALLINT rowset)
{
    SQLRETURN ret;

    HSTMT_LOCK(stmt);
    ret = drvunimplstmt(stmt);
    HSTMT_UNLOCK(stmt);
    return ret;
}

#define strmak(dst, src, max, lenp)      \
    {                                    \
        int len = strlen(src);           \
        int cnt = min(len + 1, max);     \
        strncpy(dst, src, cnt);          \
        *lenp = (cnt > len) ? len : cnt; \
    }

/**
 * Internal return information about what this ODBC driver supports.
 * @param dbc database connection handle
 * @param type type of information to be retrieved
 * @param val output buffer
 * @param valMax length of output buffer
 * @param valLen output length
 * @result ODBC error code
 */

static SQLRETURN
drvgetinfo(SQLHDBC dbc, SQLUSMALLINT type, SQLPOINTER val, SQLSMALLINT valMax,
           SQLSMALLINT *valLen)
{
    DBC *d;
    char dummyc[301];
    SQLSMALLINT dummy;
#if defined(_WIN32) || defined(_WIN64)
    char pathbuf[301], *drvname;
#else
    static char drvname[] = "libPrestoODBC.so";
#endif

    if (dbc == SQL_NULL_HDBC)
    {
        return SQL_INVALID_HANDLE;
    }
    d = (DBC *)dbc;
    if (valMax)
    {
        valMax--;
    }
    if (!valLen)
    {
        valLen = &dummy;
    }
    if (!val)
    {
        val = dummyc;
        valMax = sizeof(dummyc) - 1;
    }
    switch (type)
    {
    case SQL_MAX_USER_NAME_LEN:
        *((SQLSMALLINT *)val) = 16;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_USER_NAME:
        strmak(val, "", valMax, valLen);
        break;
    case SQL_DRIVER_ODBC_VER:
#if 0
	strmak(val, (*d->ov3) ? "03.00" : "02.50", valMax, valLen);
#else
        strmak(val, "03.00", valMax, valLen);
#endif
        break;
    case SQL_ACTIVE_CONNECTIONS:
    case SQL_ACTIVE_STATEMENTS:
        *((SQLSMALLINT *)val) = 0;
        *valLen = sizeof(SQLSMALLINT);
        break;
#ifdef SQL_ASYNC_MODE
    case SQL_ASYNC_MODE:
        *((SQLUINTEGER *)val) = SQL_AM_NONE;
        *valLen = sizeof(SQLUINTEGER);
        break;
#endif
#ifdef SQL_CREATE_TABLE
    case SQL_CREATE_TABLE:
        *((SQLUINTEGER *)val) = SQL_CT_CREATE_TABLE |
                                SQL_CT_COLUMN_DEFAULT |
                                SQL_CT_COLUMN_CONSTRAINT |
                                SQL_CT_CONSTRAINT_NON_DEFERRABLE;
        *valLen = sizeof(SQLUINTEGER);
        break;
#endif
#ifdef SQL_CREATE_VIEW
    case SQL_CREATE_VIEW:
        *((SQLUINTEGER *)val) = SQL_CV_CREATE_VIEW;
        *valLen = sizeof(SQLUINTEGER);
        break;
#endif
#ifdef SQL_DDL_INDEX
    case SQL_DDL_INDEX:
        *((SQLUINTEGER *)val) = SQL_DI_CREATE_INDEX | SQL_DI_DROP_INDEX;
        *valLen = sizeof(SQLUINTEGER);
        break;
#endif
#ifdef SQL_DROP_TABLE
    case SQL_DROP_TABLE:
        *((SQLUINTEGER *)val) = SQL_DT_DROP_TABLE;
        *valLen = sizeof(SQLUINTEGER);
        break;
#endif
#ifdef SQL_DROP_VIEW
    case SQL_DROP_VIEW:
        *((SQLUINTEGER *)val) = SQL_DV_DROP_VIEW;
        *valLen = sizeof(SQLUINTEGER);
        break;
#endif
#ifdef SQL_INDEX_KEYWORDS
    case SQL_INDEX_KEYWORDS:
        *((SQLUINTEGER *)val) = SQL_IK_ALL;
        *valLen = sizeof(SQLUINTEGER);
        break;
#endif
    case SQL_DATA_SOURCE_NAME:
        strmak(val, d->dsn ? d->dsn : "", valMax, valLen);
        break;
    case SQL_DRIVER_NAME:
#if defined(_WIN32) || defined(_WIN64)
        GetModuleFileName(hModule, pathbuf, sizeof(pathbuf));
        drvname = strrchr(pathbuf, '\\');
        if (drvname == NULL)
        {
            drvname = strrchr(pathbuf, '/');
        }
        if (drvname == NULL)
        {
            drvname = pathbuf;
        }
        else
        {
            drvname++;
        }
#endif
        strmak(val, drvname, valMax, valLen);
        break;
    case SQL_DRIVER_VER:
        strmak(val, "0.343.0", valMax, valLen);
        break;
    case SQL_FETCH_DIRECTION:
        *((SQLUINTEGER *)val) = SQL_FD_FETCH_NEXT | SQL_FD_FETCH_FIRST |
                                SQL_FD_FETCH_LAST | SQL_FD_FETCH_PRIOR | SQL_FD_FETCH_ABSOLUTE;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_ODBC_VER:
        strmak(val, (*d->ov3) ? "03.00" : "02.50", valMax, valLen);
        break;
    case SQL_ODBC_SAG_CLI_CONFORMANCE:
        *((SQLSMALLINT *)val) = SQL_OSCC_NOT_COMPLIANT;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_STANDARD_CLI_CONFORMANCE:
        *((SQLUINTEGER *)val) = SQL_SCC_XOPEN_CLI_VERSION1;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_SQL_CONFORMANCE:
        *((SQLUINTEGER *)val) = SQL_SC_SQL92_ENTRY;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_SERVER_NAME:
    case SQL_DATABASE_NAME:
        strmak(val, d->dbname ? d->dbname : "", valMax, valLen);
        break;
    case SQL_SEARCH_PATTERN_ESCAPE:
        strmak(val, "\\", valMax, valLen);
        break;
    case SQL_ODBC_SQL_CONFORMANCE:
        *((SQLSMALLINT *)val) = SQL_OSC_MINIMUM;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_ODBC_API_CONFORMANCE:
        *((SQLSMALLINT *)val) = SQL_OAC_LEVEL1;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_DBMS_NAME:
        strmak(val, "prestosql", valMax, valLen);
        break;
    case SQL_DBMS_VER:
        strmak(val, "0.343.0", valMax, valLen);
        break;
    case SQL_COLUMN_ALIAS:
    case SQL_NEED_LONG_DATA_LEN:
    case SQL_OUTER_JOINS:
        strmak(val, "Y", valMax, valLen);
        break;
    case SQL_ROW_UPDATES:
    case SQL_ACCESSIBLE_PROCEDURES:
    case SQL_PROCEDURES:
    case SQL_EXPRESSIONS_IN_ORDERBY:
    case SQL_ODBC_SQL_OPT_IEF:
    case SQL_LIKE_ESCAPE_CLAUSE:
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:
    case SQL_ACCESSIBLE_TABLES:
    case SQL_MULT_RESULT_SETS:
    case SQL_MULTIPLE_ACTIVE_TXN:
    case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
        strmak(val, "N", valMax, valLen);
        break;
#ifdef SQL_CATALOG_NAME
    case SQL_CATALOG_NAME:
#if defined(_WIN32) || defined(_WIN64)
        strmak(val, d->xcelqrx ? "Y" : "N", valMax, valLen);
#else
        strmak(val, "N", valMax, valLen);
#endif
        break;
#endif
    case SQL_DATA_SOURCE_READ_ONLY:
        strmak(val, "N", valMax, valLen);
        break;
#ifdef SQL_OJ_CAPABILITIES
    case SQL_OJ_CAPABILITIES:
        *((SQLUINTEGER *)val) = SQL_OJ_LEFT;
        *valLen = sizeof(SQLUINTEGER);
        break;
#endif
#ifdef SQL_MAX_IDENTIFIER_LEN
    case SQL_MAX_IDENTIFIER_LEN:
        *((SQLUSMALLINT *)val) = 255;
        *valLen = sizeof(SQLUSMALLINT);
        break;
#endif
    case SQL_CONCAT_NULL_BEHAVIOR:
        *((SQLSMALLINT *)val) = SQL_CB_NULL;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_CURSOR_COMMIT_BEHAVIOR:
    case SQL_CURSOR_ROLLBACK_BEHAVIOR:
        *((SQLSMALLINT *)val) = SQL_CB_PRESERVE;
        *valLen = sizeof(SQLSMALLINT);
        break;
#ifdef SQL_CURSOR_SENSITIVITY
    case SQL_CURSOR_SENSITIVITY:
        *((SQLUINTEGER *)val) = SQL_UNSPECIFIED;
        *valLen = sizeof(SQLUINTEGER);
        break;
#endif
    case SQL_DEFAULT_TXN_ISOLATION:
        *((SQLUINTEGER *)val) = SQL_TXN_SERIALIZABLE;
        *valLen = sizeof(SQLUINTEGER);
        break;
#ifdef SQL_DESCRIBE_PARAMETER
    case SQL_DESCRIBE_PARAMETER:
        strmak(val, "Y", valMax, valLen);
        break;
#endif
    case SQL_TXN_ISOLATION_OPTION:
        *((SQLUINTEGER *)val) = SQL_TXN_SERIALIZABLE;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_IDENTIFIER_CASE:
        *((SQLSMALLINT *)val) = SQL_IC_SENSITIVE;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_IDENTIFIER_QUOTE_CHAR:
        strmak(val, "\"", valMax, valLen);
        break;
    case SQL_MAX_TABLE_NAME_LEN:
    case SQL_MAX_COLUMN_NAME_LEN:
        *((SQLSMALLINT *)val) = 255;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_MAX_CURSOR_NAME_LEN:
        *((SQLSMALLINT *)val) = 255;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_MAX_PROCEDURE_NAME_LEN:
        *((SQLSMALLINT *)val) = 0;
        break;
    case SQL_MAX_QUALIFIER_NAME_LEN:
    case SQL_MAX_OWNER_NAME_LEN:
        *((SQLSMALLINT *)val) = 255;
        break;
    case SQL_OWNER_TERM:
        strmak(val, "", valMax, valLen);
        break;
    case SQL_PROCEDURE_TERM:
        strmak(val, "PROCEDURE", valMax, valLen);
        break;
    case SQL_QUALIFIER_NAME_SEPARATOR:
        strmak(val, ".", valMax, valLen);
        break;
    case SQL_QUALIFIER_TERM:
#if defined(_WIN32) || defined(_WIN64)
        strmak(val, d->xcelqrx ? "catalog" : "", valMax, valLen);
#else
        strmak(val, "", valMax, valLen);
#endif
        break;
    case SQL_QUALIFIER_USAGE:
#if defined(_WIN32) || defined(_WIN64)
        *((SQLUINTEGER *)val) = d->xcelqrx ? (SQL_CU_DML_STATEMENTS | SQL_CU_INDEX_DEFINITION |
                                              SQL_CU_TABLE_DEFINITION)
                                           : 0;
#else
        *((SQLUINTEGER *)val) = 0;
#endif
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_SCROLL_CONCURRENCY:
        *((SQLUINTEGER *)val) = SQL_SCCO_LOCK;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_SCROLL_OPTIONS:
        *((SQLUINTEGER *)val) = SQL_SO_STATIC | SQL_SO_FORWARD_ONLY;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_TABLE_TERM:
        strmak(val, "TABLE", valMax, valLen);
        break;
    case SQL_TXN_CAPABLE:
        *((SQLSMALLINT *)val) = SQL_TC_ALL;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_CONVERT_FUNCTIONS:
        *((SQLUINTEGER *)val) = 0;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_SYSTEM_FUNCTIONS:
    case SQL_NUMERIC_FUNCTIONS:
    case SQL_STRING_FUNCTIONS:
    case SQL_TIMEDATE_FUNCTIONS:
        *((SQLUINTEGER *)val) = 0;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_CONVERT_BIGINT:
    case SQL_CONVERT_BIT:
    case SQL_CONVERT_CHAR:
    case SQL_CONVERT_DATE:
    case SQL_CONVERT_DECIMAL:
    case SQL_CONVERT_DOUBLE:
    case SQL_CONVERT_FLOAT:
    case SQL_CONVERT_INTEGER:
    case SQL_CONVERT_LONGVARCHAR:
    case SQL_CONVERT_NUMERIC:
    case SQL_CONVERT_REAL:
    case SQL_CONVERT_SMALLINT:
    case SQL_CONVERT_TIME:
    case SQL_CONVERT_TIMESTAMP:
    case SQL_CONVERT_TINYINT:
    case SQL_CONVERT_VARCHAR:
        *((SQLUINTEGER *)val) =
            SQL_CVT_CHAR | SQL_CVT_NUMERIC | SQL_CVT_DECIMAL |
            SQL_CVT_INTEGER | SQL_CVT_SMALLINT | SQL_CVT_FLOAT | SQL_CVT_REAL |
            SQL_CVT_DOUBLE | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
            SQL_CVT_BIT | SQL_CVT_TINYINT | SQL_CVT_BIGINT |
            SQL_CVT_DATE | SQL_CVT_TIME | SQL_CVT_TIMESTAMP;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_CONVERT_BINARY:
    case SQL_CONVERT_VARBINARY:
    case SQL_CONVERT_LONGVARBINARY:
        *((SQLUINTEGER *)val) = 0;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_POSITIONED_STATEMENTS:
        *((SQLUINTEGER *)val) = 0;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_LOCK_TYPES:
        *((SQLUINTEGER *)val) = SQL_LCK_NO_CHANGE;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_BOOKMARK_PERSISTENCE:
        *((SQLUINTEGER *)val) = SQL_BP_SCROLL;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_UNION:
        *((SQLUINTEGER *)val) = SQL_U_UNION | SQL_U_UNION_ALL;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_OWNER_USAGE:
    case SQL_SUBQUERIES:
    case SQL_TIMEDATE_ADD_INTERVALS:
    case SQL_TIMEDATE_DIFF_INTERVALS:
        *((SQLUINTEGER *)val) = 0;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_QUOTED_IDENTIFIER_CASE:
        *((SQLUSMALLINT *)val) = SQL_IC_SENSITIVE;
        *valLen = sizeof(SQLUSMALLINT);
        break;
    case SQL_POS_OPERATIONS:
        *((SQLUINTEGER *)val) = SQL_POS_POSITION | SQL_POS_UPDATE |
                                SQL_POS_DELETE | SQL_POS_ADD | SQL_POS_REFRESH;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_ALTER_TABLE:
        *((SQLUINTEGER *)val) = 0;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_CORRELATION_NAME:
        *((SQLSMALLINT *)val) = SQL_CN_DIFFERENT;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_NON_NULLABLE_COLUMNS:
        *((SQLSMALLINT *)val) = SQL_NNC_NON_NULL;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_NULL_COLLATION:
        *((SQLSMALLINT *)val) = SQL_NC_START;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_MAX_COLUMNS_IN_GROUP_BY:
    case SQL_MAX_COLUMNS_IN_ORDER_BY:
    case SQL_MAX_COLUMNS_IN_SELECT:
    case SQL_MAX_COLUMNS_IN_TABLE:
    case SQL_MAX_ROW_SIZE:
    case SQL_MAX_TABLES_IN_SELECT:
        *((SQLSMALLINT *)val) = 0;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_MAX_BINARY_LITERAL_LEN:
    case SQL_MAX_CHAR_LITERAL_LEN:
        *((SQLUINTEGER *)val) = 0;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_MAX_COLUMNS_IN_INDEX:
        *((SQLSMALLINT *)val) = 0;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_MAX_INDEX_SIZE:
        *((SQLUINTEGER *)val) = 0;
        *valLen = sizeof(SQLUINTEGER);
        break;
#ifdef SQL_MAX_IDENTIFIER_LENGTH
    case SQL_MAX_IDENTIFIER_LENGTH:
        *((SQLUINTEGER *)val) = 255;
        *valLen = sizeof(SQLUINTEGER);
        break;
#endif
    case SQL_MAX_STATEMENT_LEN:
        *((SQLUINTEGER *)val) = 16384;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_QUALIFIER_LOCATION:
        *((SQLSMALLINT *)val) = SQL_QL_START;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_GETDATA_EXTENSIONS:
        *((SQLUINTEGER *)val) =
            SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER | SQL_GD_BOUND;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_STATIC_SENSITIVITY:
        *((SQLUINTEGER *)val) = 0;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_FILE_USAGE:
#if defined(_WIN32) || defined(_WIN64)
        *((SQLSMALLINT *)val) =
            d->xcelqrx ? SQL_FILE_CATALOG : SQL_FILE_NOT_SUPPORTED;
#else
        *((SQLSMALLINT *)val) = SQL_FILE_NOT_SUPPORTED;
#endif
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_GROUP_BY:
        *((SQLSMALLINT *)val) = SQL_GB_GROUP_BY_EQUALS_SELECT;
        *valLen = sizeof(SQLSMALLINT);
        break;
    case SQL_KEYWORDS:
        strmak(val, "CREATE,SELECT,DROP,DELETE,UPDATE,INSERT,"
                    "INTO,VALUES,TABLE,INDEX,FROM,SET,WHERE,AND,CURRENT,OF",
               valMax, valLen);
        break;
    case SQL_SPECIAL_CHARACTERS:
#ifdef SQL_COLLATION_SEQ
    case SQL_COLLATION_SEQ:
#endif
        strmak(val, "", valMax, valLen);
        break;
    case SQL_BATCH_SUPPORT:
    case SQL_BATCH_ROW_COUNT:
    case SQL_PARAM_ARRAY_ROW_COUNTS:
        *((SQLUINTEGER *)val) = 0;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
        *((SQLUINTEGER *)val) = SQL_CA1_NEXT | SQL_CA1_BOOKMARK;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_STATIC_CURSOR_ATTRIBUTES1:
        *((SQLUINTEGER *)val) = SQL_CA1_NEXT | SQL_CA1_ABSOLUTE |
                                SQL_CA1_RELATIVE | SQL_CA1_BOOKMARK | SQL_CA1_POS_POSITION |
                                SQL_CA1_POS_DELETE | SQL_CA1_POS_UPDATE | SQL_CA1_POS_REFRESH |
                                SQL_CA1_LOCK_NO_CHANGE | SQL_CA1_BULK_ADD |
                                SQL_CA1_BULK_UPDATE_BY_BOOKMARK | SQL_CA1_BULK_DELETE_BY_BOOKMARK;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
    case SQL_STATIC_CURSOR_ATTRIBUTES2:
        *((SQLUINTEGER *)val) = SQL_CA2_READ_ONLY_CONCURRENCY |
                                SQL_CA2_LOCK_CONCURRENCY;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_KEYSET_CURSOR_ATTRIBUTES1:
    case SQL_KEYSET_CURSOR_ATTRIBUTES2:
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
        *((SQLUINTEGER *)val) = 0;
        *valLen = sizeof(SQLUINTEGER);
        break;
    case SQL_ODBC_INTERFACE_CONFORMANCE:
        *((SQLUINTEGER *)val) = SQL_OIC_CORE;
        *valLen = sizeof(SQLUINTEGER);
        break;
    default:
        setstatd(d, -1, "unsupported info option %d",
                 (*d->ov3) ? "HYC00" : "S1C00", type);
        return SQL_ERROR;
    }
    return SQL_SUCCESS;
}

#if (defined(HAVE_UNIXODBC) && (HAVE_UNIXODBC)) || !defined(WINTERFACE)
/**
 * Return information about what this ODBC driver supports.
 * @param dbc database connection handle
 * @param type type of information to be retrieved
 * @param val output buffer
 * @param valMax length of output buffer
 * @param valLen output length
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetInfo(SQLHDBC dbc, SQLUSMALLINT type, SQLPOINTER val, SQLSMALLINT valMax,
           SQLSMALLINT *valLen)
{
    SQLRETURN ret;

    HDBC_LOCK(dbc);
    ret = drvgetinfo(dbc, type, val, valMax, valLen);
    HDBC_UNLOCK(dbc);
    return ret;
}
#endif

#ifdef WINTERFACE
/**
 * Return information about what this ODBC driver supports.
 * @param dbc database connection handle
 * @param type type of information to be retrieved
 * @param val output buffer
 * @param valMax length of output buffer
 * @param valLen output length
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetInfoW(SQLHDBC dbc, SQLUSMALLINT type, SQLPOINTER val, SQLSMALLINT valMax,
            SQLSMALLINT *valLen)
{
    SQLRETURN ret;
    SQLSMALLINT len = 0;

    HDBC_LOCK(dbc);
    ret = drvgetinfo(dbc, type, val, valMax, &len);
    HDBC_UNLOCK(dbc);
    if (ret == SQL_SUCCESS)
    {
        SQLWCHAR *v = NULL;

        switch (type)
        {
        case SQL_USER_NAME:
        case SQL_DRIVER_ODBC_VER:
        case SQL_DATA_SOURCE_NAME:
        case SQL_DRIVER_NAME:
        case SQL_DRIVER_VER:
        case SQL_ODBC_VER:
        case SQL_SERVER_NAME:
        case SQL_DATABASE_NAME:
        case SQL_SEARCH_PATTERN_ESCAPE:
        case SQL_DBMS_NAME:
        case SQL_DBMS_VER:
        case SQL_NEED_LONG_DATA_LEN:
        case SQL_ROW_UPDATES:
        case SQL_ACCESSIBLE_PROCEDURES:
        case SQL_PROCEDURES:
        case SQL_EXPRESSIONS_IN_ORDERBY:
        case SQL_ODBC_SQL_OPT_IEF:
        case SQL_LIKE_ESCAPE_CLAUSE:
        case SQL_ORDER_BY_COLUMNS_IN_SELECT:
        case SQL_OUTER_JOINS:
        case SQL_COLUMN_ALIAS:
        case SQL_ACCESSIBLE_TABLES:
        case SQL_MULT_RESULT_SETS:
        case SQL_MULTIPLE_ACTIVE_TXN:
        case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
        case SQL_DATA_SOURCE_READ_ONLY:
#ifdef SQL_DESCRIBE_PARAMETER
        case SQL_DESCRIBE_PARAMETER:
#endif
        case SQL_IDENTIFIER_QUOTE_CHAR:
        case SQL_OWNER_TERM:
        case SQL_PROCEDURE_TERM:
        case SQL_QUALIFIER_NAME_SEPARATOR:
        case SQL_QUALIFIER_TERM:
        case SQL_TABLE_TERM:
        case SQL_KEYWORDS:
        case SQL_SPECIAL_CHARACTERS:
#ifdef SQL_CATALOG_NAME
        case SQL_CATALOG_NAME:
#endif
#ifdef SQL_COLLATION_SEQ
        case SQL_COLLATION_SEQ:
#endif
            if (val)
            {
                if (len > 0)
                {
                    v = uc_from_utf((SQLCHAR *)val, len);
                    if (v)
                    {
                        int vmax = valMax / sizeof(SQLWCHAR);

                        uc_strncpy(val, v, vmax);
                        if (len < vmax)
                        {
                            len = min(vmax, uc_strlen(v));
                            v[len] = 0;
                        }
                        else
                        {
                            len = vmax;
                        }
                        uc_free(v);
                        len *= sizeof(SQLWCHAR);
                    }
                    else
                    {
                        len = 0;
                    }
                }
                if (len <= 0)
                {
                    len = 0;
                    if (valMax >= sizeof(SQLWCHAR))
                    {
                        *((SQLWCHAR *)val) = 0;
                    }
                }
            }
            else
            {
                len *= sizeof(SQLWCHAR);
            }
            break;
        }
        if (valLen)
        {
            *valLen = len;
        }
    }
    return ret;
}
#endif


/**
 * Map SQL_C_DEFAULT to proper C type.
 * @param type input C type
 * @param stype input SQL type
 * @param nosign 0=signed, 0>unsigned, 0<undefined
 * @param nowchar when compiled with WINTERFACE don't use WCHAR
 * @result C type
 */
static int
mapdeftype(int type, int stype, int nosign, int nowchar)
{
	if (type == SQL_C_DEFAULT)
	{
		switch (stype)
		{
		case SQL_INTEGER:
			type = (nosign > 0) ? SQL_C_ULONG : SQL_C_LONG;
			break;
		case SQL_TINYINT:
			type = (nosign > 0) ? SQL_C_UTINYINT : SQL_C_TINYINT;
			break;
		case SQL_SMALLINT:
			type = (nosign > 0) ? SQL_C_USHORT : SQL_C_SHORT;
			break;
		case SQL_FLOAT:
			type = SQL_C_FLOAT;
			break;
		case SQL_DOUBLE:
			type = SQL_C_DOUBLE;
			break;
		case SQL_TIMESTAMP:
			type = SQL_C_TIMESTAMP;
			break;
		case SQL_TIME:
			type = SQL_C_TIME;
			break;
		case SQL_DATE:
			type = SQL_C_DATE;
			break;
#ifdef SQL_C_TYPE_TIMESTAMP
		case SQL_TYPE_TIMESTAMP:
			type = SQL_C_TYPE_TIMESTAMP;
			break;
#endif
#ifdef SQL_C_TYPE_TIME
		case SQL_TYPE_TIME:
			type = SQL_C_TYPE_TIME;
			break;
#endif
#ifdef SQL_C_TYPE_DATE
		case SQL_TYPE_DATE:
			type = SQL_C_TYPE_DATE;
			break;
#endif
#ifdef WINTERFACE
		case SQL_WVARCHAR:
		case SQL_WCHAR:
#ifdef SQL_WLONGVARCHAR
		case SQL_WLONGVARCHAR:
#endif
			type = nowchar ? SQL_C_CHAR : SQL_C_WCHAR;
			break;
#endif
		case SQL_BINARY:
		case SQL_VARBINARY:
		case SQL_LONGVARBINARY:
			type = SQL_C_BINARY;
			break;
#ifdef SQL_BIT
		case SQL_BIT:
			type = SQL_C_BIT;
			break;
#endif
#ifdef SQL_BIGINT
		case SQL_BIGINT:
			type = SQL_C_CHAR;
			break;
#endif
		default:
#ifdef WINTERFACE
			type = nowchar ? SQL_C_CHAR : SQL_C_WCHAR;
#else
			type = SQL_C_CHAR;
#endif
			break;
		}
	}
	return type;
}



/**
 * Internal bind C variable to column of result set.
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param type output type
 * @param val output buffer
 * @param max length of output buffer
 * @param lenp output length pointer
 * @result ODBC error code
 */

static SQLRETURN
drvbindcol(SQLHSTMT stmt, SQLUSMALLINT col, SQLSMALLINT type,
           SQLPOINTER val, SQLLEN max, SQLLEN *lenp)
{
    STMT *s;
    int sz = 0;

    if (stmt == SQL_NULL_HSTMT)
    {
        return SQL_INVALID_HANDLE;
    }
    s = (STMT *)stmt;
    if (col < 1)
    {
        // if (col == 0 && s->bkmrk == SQL_UB_ON &&
        // 	type == SQL_C_BOOKMARK)
        // {
        // 	s->bkmrkcol.type = val ? type : SQL_UNKNOWN_TYPE;
        // 	s->bkmrkcol.max = val ? sizeof(SQLINTEGER) : 0;
        // 	s->bkmrkcol.lenp = val ? lenp : 0;
        // 	s->bkmrkcol.valp = val;
        // 	s->bkmrkcol.offs = 0;
        // 	if (val && lenp)
        // 	{
        // 		*lenp = 0;
        // 	}
        // 	return SQL_SUCCESS;
        // }
        // else if (col == 0 && s->bkmrk == SQL_UB_VARIABLE &&
        // 		 type == SQL_C_VARBOOKMARK &&
        // 		 max >= sizeof(sqlite_int64))
        // {
        // 	s->bkmrkcol.type = val ? type : SQL_UNKNOWN_TYPE;
        // 	s->bkmrkcol.max = val ? max : 0;
        // 	s->bkmrkcol.lenp = val ? lenp : 0;
        // 	s->bkmrkcol.valp = val;
        // 	s->bkmrkcol.offs = 0;
        // 	if (val && lenp)
        // 	{
        // 		*lenp = 0;
        // 	}
        // 	return SQL_SUCCESS;
        // }
        setstat(s, -1, "invalid column", (*s->ov3) ? "07009" : "S1002");
        return SQL_ERROR;
    }
    if (mkbindcols(s, col) != SQL_SUCCESS)
    {
        return SQL_ERROR;
    }
    --col;
    if (type == SQL_C_DEFAULT)
    {
        type = mapdeftype(type, s->presto_stmt->columns[col]->type, 0,
                          s->nowchar[0] || s->nowchar[1]);
    }
    switch (type)
    {
    case SQL_C_LONG:
    case SQL_C_ULONG:
    case SQL_C_SLONG:
        sz = sizeof(SQLINTEGER);
        break;
    case SQL_C_TINYINT:
    case SQL_C_UTINYINT:
    case SQL_C_STINYINT:
        sz = sizeof(SQLCHAR);
        break;
    case SQL_C_SHORT:
    case SQL_C_USHORT:
    case SQL_C_SSHORT:
        sz = sizeof(SQLSMALLINT);
        break;
    case SQL_C_FLOAT:
        sz = sizeof(SQLFLOAT);
        break;
    case SQL_C_DOUBLE:
        sz = sizeof(SQLDOUBLE);
        break;
    case SQL_C_TIMESTAMP:
        sz = sizeof(SQL_TIMESTAMP_STRUCT);
        break;
    case SQL_C_TIME:
        sz = sizeof(SQL_TIME_STRUCT);
        break;
    case SQL_C_DATE:
        sz = sizeof(SQL_DATE_STRUCT);
        break;
    case SQL_C_CHAR:
        break;
#ifdef WCHARSUPPORT
    case SQL_C_WCHAR:
        break;
#endif
#ifdef SQL_C_TYPE_DATE
    case SQL_C_TYPE_DATE:
        sz = sizeof(SQL_DATE_STRUCT);
        break;
#endif
#ifdef SQL_C_TYPE_TIME
    case SQL_C_TYPE_TIME:
        sz = sizeof(SQL_TIME_STRUCT);
        break;
#endif
#ifdef SQL_C_TYPE_TIMESTAMP
    case SQL_C_TYPE_TIMESTAMP:
        sz = sizeof(SQL_TIMESTAMP_STRUCT);
        break;
#endif
#ifdef SQL_BIT
    case SQL_C_BIT:
        sz = sizeof(SQLCHAR);
        break;
#endif
    case SQL_C_BINARY:
        break;
#ifdef SQL_BIGINT
    case SQL_C_SBIGINT:
    case SQL_C_UBIGINT:
        sz = sizeof(SQLBIGINT);
        break;
#endif
    default:
        if (val == NULL)
        {
            /* fall through, unbinding column */
            break;
        }
        setstat(s, -1, "invalid type %d", "HY003", type);
        return SQL_ERROR;
    }
    if (val == NULL)
    {
        /* unbind column */
        s->bindcols[col].type = SQL_UNKNOWN_TYPE;
        s->bindcols[col].max = 0;
        s->bindcols[col].lenp = NULL;
        s->bindcols[col].valp = NULL;
        s->bindcols[col].offs = 0;
    }
    else
    {
        if (sz == 0 && max < 0)
        {
            setstat(s, -1, "invalid length", "HY090");
            return SQL_ERROR;
        }
        s->bindcols[col].type = type;
        s->bindcols[col].max = (sz == 0) ? max : sz;
        s->bindcols[col].lenp = lenp;
        s->bindcols[col].valp = val;
        s->bindcols[col].offs = 0;
        if (lenp)
        {
            *lenp = 0;
        }
    }
    return SQL_SUCCESS;
}

/**
 * Bind C variable to column of result set.
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param type output type
 * @param val output buffer
 * @param max length of output buffer
 * @param lenp output length pointer
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT col, SQLSMALLINT type,
           SQLPOINTER val, SQLLEN max, SQLLEN *lenp)
{
    SQLRETURN ret;

    HSTMT_LOCK(stmt);
    ret = drvbindcol(stmt, col, type, val, max, lenp);
    HSTMT_UNLOCK(stmt);
    return ret;
}

/**
 * Free HSTMT.
 * @param stmt statement handle
 * @param opt SQL_RESET_PARAMS, SQL_UNBIND, SQL_CLOSE, or SQL_DROP
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT opt)
{
    return drvfreestmt(stmt, opt);
}

/**
 * Internal disconnect given HDBC.
 * @param dbc database connection handle
 * @result ODBC error code
 */

static SQLRETURN
drvdisconnect(SQLHDBC dbc)
{
    DBC *d;

    if (dbc == SQL_NULL_HDBC)
    {
        return SQL_INVALID_HANDLE;
    }
    d = (DBC *)dbc;
    if (d->magic != DBC_MAGIC)
    {
        return SQL_INVALID_HANDLE;
    }
    if (d->intrans)
    {
        setstatd(d, -1, "incomplete transaction", "25000");
        return SQL_ERROR;
    }   
    if (d->presto_client)
    {
        prestoclient_close(d->presto_client);        
        d->presto_client = NULL;
    }
    freep(&d->dbname);
    freep(&d->dsn);
    return SQL_SUCCESS;
}

/**
 * Disconnect given HDBC.
 * @param dbc database connection handle
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLDisconnect(SQLHDBC dbc)
{
    SQLRETURN ret;

    HDBC_LOCK(dbc);
    ret = drvdisconnect(dbc);
    HDBC_UNLOCK(dbc);
    return ret;
}


/**
 * Internal function to retrieve row data, used by SQLFetch() and
 * friends and SQLGetData().
 * @param s statement pointer
 * @param col column number, 0 based
 * @param otype output data type
 * @param val output buffer
 * @param len length of output buffer
 * @param lenp output length
 * @param partial flag for partial data retrieval
 * @result ODBC error code
 */

static SQLRETURN
getrowdata(STMT *s, SQLUSMALLINT col, SQLSMALLINT otype,
		   SQLPOINTER val, SQLINTEGER len, SQLLEN *lenp, int partial)
{
	char *data, valdummy[16];
	SQLLEN dummy;
	SQLINTEGER *ilenp = NULL;
	int valnull = 0;
	int type = otype;
	SQLRETURN sret = SQL_NO_DATA;

	if (!lenp)
	{
		lenp = &dummy;
	}
	/* workaround for JDK 1.7.0 on x86_64 */
	if (((SQLINTEGER *)lenp) + 1 == (SQLINTEGER *)val)
	{
		ilenp = (SQLINTEGER *)lenp;
		lenp = &dummy;
	}
	if (col >= s->presto_stmt->tablebuff->ncol)
	{
		setstat(s, -1, "invalid column", (*s->ov3) ? (char*)"07009" : (char*)"S1002");
		return SQL_ERROR;
	}
	if (s->retr_data != SQL_RD_ON)
	{
		return SQL_SUCCESS;
	}
	if (!s->presto_stmt->tablebuff->rowbuff)
	{
		*lenp = SQL_NULL_DATA;
		goto done;
	}
	if (s->presto_stmt->tablebuff->rowidx > (s->presto_stmt->tablebuff->nrow-1))
	{
		*lenp = SQL_NULL_DATA;
		goto done;
	}

	//type = mapdeftype(type, s->cols[col].type, s->cols[col].nosign ? 1 : 0,
	//				  s->nowchar[0]);

    // just some types are mapped    
    switch (otype) {
        case SQL_CHAR:
        case SQL_VARCHAR:
            type = SQL_C_CHAR;
            break;
        case SQL_TIMESTAMP:
            type = SQL_C_TIMESTAMP;
            break;
        default:
            printf("client wants %i, map to default char", otype);
            type = SQL_C_CHAR;            
    }
    
#if (defined(_WIN32) || defined(_WIN64)) && defined(WINTERFACE)
	/* MS Access hack part 3 (map SQL_C_DEFAULT to SQL_C_CHAR) */
	if (type == SQL_C_WCHAR && otype == SQL_C_DEFAULT)
	{
		type = SQL_C_CHAR;
	}
#endif
    // calc the array offset of the column
	data = (char*)s->presto_stmt->tablebuff->rowbuff[ (s->presto_stmt->tablebuff->ncol* s->presto_stmt->tablebuff->rowidx + col) ];
    // printf("\nparams %s %i %i %i %i %i\n", data, type, col, otype, len, *lenp);
	if (!val)
	{
		valnull = 1;
		val = (SQLPOINTER)valdummy;
	}
	if (data == NULL)
	{
		*lenp = SQL_NULL_DATA;
		switch (type)
		{
		case SQL_C_UTINYINT:
		case SQL_C_TINYINT:
		case SQL_C_STINYINT:
#ifdef SQL_BIT
		case SQL_C_BIT:
#endif
			*((SQLCHAR *)val) = 0;
			break;
		case SQL_C_USHORT:
		case SQL_C_SHORT:
		case SQL_C_SSHORT:
			*((SQLSMALLINT *)val) = 0;
			break;
		case SQL_C_ULONG:
		case SQL_C_LONG:
		case SQL_C_SLONG:
			*((SQLINTEGER *)val) = 0;
			break;
#ifdef SQL_BIGINT
		case SQL_C_SBIGINT:
		case SQL_C_UBIGINT:
			*((SQLBIGINT *)val) = 0;
			break;
#endif
		case SQL_C_FLOAT:
			*((float *)val) = 0;
			break;
		case SQL_C_DOUBLE:
			*((double *)val) = 0;
			break;
		case SQL_C_BINARY:
		case SQL_C_CHAR:
			if (len > 0)
			{
				*((SQLCHAR *)val) = '\0';
			}
			break;
#ifdef WCHARSUPPORT
		case SQL_C_WCHAR:
			if (len > 0)
			{
				*((SQLWCHAR *)val) = '\0';
			}
			break;
#endif
#ifdef SQL_C_TYPE_DATE
		case SQL_C_TYPE_DATE:
#endif
		case SQL_C_DATE:
			memset((DATE_STRUCT *)val, 0, sizeof(DATE_STRUCT));
			break;
#ifdef SQL_C_TYPE_TIME
		case SQL_C_TYPE_TIME:
#endif
		case SQL_C_TIME:
			memset((TIME_STRUCT *)val, 0, sizeof(TIME_STRUCT));
			break;
#ifdef SQL_C_TYPE_TIMESTAMP
		case SQL_C_TYPE_TIMESTAMP:
#endif
		case SQL_C_TIMESTAMP:
			memset((TIMESTAMP_STRUCT *)val, 0, sizeof(TIMESTAMP_STRUCT));
			break;
		default:
			return SQL_ERROR;
		}
	}
	else
	{
		char *endp = NULL;
#if defined(_WIN32) || defined(_WIN64)
#ifdef SQL_BIGINT
		char endc;
#endif
#endif

		switch (type)
		{
		case SQL_C_UTINYINT:
		case SQL_C_TINYINT:
		case SQL_C_STINYINT:
			*((SQLCHAR *)val) = strtol(data, &endp, 0);
			if (endp && endp == data)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(SQLCHAR);
			}
			break;
#ifdef SQL_BIT
		case SQL_C_BIT:
			*((SQLCHAR *)val) = getbool(data);
			*lenp = sizeof(SQLCHAR);
			break;
#endif
		case SQL_C_USHORT:
		case SQL_C_SHORT:
		case SQL_C_SSHORT:
			*((SQLSMALLINT *)val) = strtol(data, &endp, 0);
			if (endp && endp == data)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(SQLSMALLINT);
			}
			break;
		case SQL_C_ULONG:
		case SQL_C_LONG:
		case SQL_C_SLONG:
			*((SQLINTEGER *)val) = strtol(data, &endp, 0);
			if (endp && endp == data)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(SQLINTEGER);
			}
			break;
#ifdef SQL_BIGINT
		case SQL_C_UBIGINT:
#if defined(_WIN32) || defined(_WIN64)
			if (sscanf(*data, "%I64u%c", (SQLUBIGINT *)val, &endc) != 1)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(SQLUBIGINT);
			}
#else
#ifdef __osf__
			*((SQLUBIGINT *)val) = strtoul(*data, &endp, 0);
#else
			*((SQLUBIGINT *)val) = strtoull(data, &endp, 0);
#endif
			if (endp && endp == data)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(SQLUBIGINT);
			}
#endif
			break;
		case SQL_C_SBIGINT:
#if defined(_WIN32) || defined(_WIN64)
			if (sscanf(*data, "%I64d%c", (SQLBIGINT *)val, &endc) != 1)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(SQLBIGINT);
			}
#else
#ifdef __osf__
			*((SQLBIGINT *)val) = strtol(data, &endp, 0);
#else
			*((SQLBIGINT *)val) = strtoll(data, &endp, 0);
#endif
			if (endp && endp == data)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(SQLBIGINT);
			}
#endif
			break;
#endif
		case SQL_C_FLOAT:
			*((float *)val) = ln_strtod(data, &endp);
			if (endp && endp == data)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(float);
			}
			break;
		case SQL_C_DOUBLE:
			*((double *)val) = ln_strtod(data, &endp);
			if (endp && endp == data)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(double);
			}
			break;
		case SQL_C_BINARY:
		{
			int dlen, offs = 0;
			char *bin;

			if (valnull)
			{
				freep(&s->bincache);
				s->binlen = 0;
				goto doCHAR;
			}
			if (data == s->bincell)
			{
				if (s->bincache)
				{
					bin = s->bincache;
					dlen = s->binlen;
				}
				else
				{
					goto doCHAR;
				}
			}
			else
			{
				char *dp;
				int i;

				freep(&s->bincache);
				dp = data;
				dlen = strlen(dp);
				s->bincell = dp;
				s->binlen = 0;
				if (!(dp[0] == 'x' || dp[0] == 'X') || dp[1] != '\'' ||
					dp[dlen - 1] != '\'')
				{
					goto doCHAR;
				}
				dlen -= 2;
				dp += 2;
				dlen = dlen / 2;
				s->bincache = bin = xmalloc(dlen + 1);
				if (!bin)
				{
					return nomem(s);
				}
				s->binlen = dlen;
				memset(bin, 0, dlen);
				bin[dlen] = '\0'; /* terminator, just in case */
				for (i = 0; i < dlen; i++)
				{
					char *x;
					int v;

					if (!*dp || !(x = strchr(xdigits, *dp)))
					{
						goto converr;
					}
					v = x - xdigits;
					bin[i] = (v >= 16) ? ((v - 6) << 4) : (v << 4);
					++dp;
					if (!*dp || !(x = strchr(xdigits, *dp)))
					{
					converr:
						freep(&s->bincache);
						s->binlen = 0;
						setstat(s, -1, "conversion error",
								(*s->ov3) ? "HY000" : "S1000");
						return SQL_ERROR;
					}
					v = x - xdigits;
					bin[i] |= (v >= 16) ? (v - 6) : v;
					++dp;
				}
				bin = s->bincache;
			}
			if (partial && len && s->bindcols)
			{
				if (s->bindcols[col].offs >= dlen)
				{
					*lenp = 0;
					if (!dlen && s->bindcols[col].offs == dlen)
					{
						s->bindcols[col].offs = 1;
						sret = SQL_SUCCESS;
						goto done;
					}
					s->bindcols[col].offs = 0;
					sret = SQL_NO_DATA;
					goto done;
				}
				offs = s->bindcols[col].offs;
				dlen -= offs;
			}
			if (val && len)
			{
				memcpy(val, bin + offs, min(len, dlen));
			}
			if (len < 1)
			{
				*lenp = dlen;
			}
			else
			{
				*lenp = min(len, dlen);
				if (*lenp == len && *lenp != dlen)
				{
					*lenp = SQL_NO_TOTAL;
				}
			}
			if (partial && len && s->bindcols)
			{
				if (*lenp == SQL_NO_TOTAL)
				{
					*lenp = dlen;
					s->bindcols[col].offs += len;
					setstat(s, -1, "data right truncated", "01004");
					if (s->bindcols[col].lenp)
					{
						*s->bindcols[col].lenp = dlen;
					}
					sret = SQL_SUCCESS_WITH_INFO;
					goto done;
				}
				s->bindcols[col].offs += *lenp;
			}
			if (*lenp == SQL_NO_TOTAL)
			{
				*lenp = dlen;
				setstat(s, -1, "data right truncated", "01004");
				sret = SQL_SUCCESS_WITH_INFO;
				goto done;
			}
			break;
		}
		doCHAR:
#ifdef WCHARSUPPORT
		case SQL_C_WCHAR:
#endif
		case SQL_C_CHAR:
		{
			int doz, zlen = len - 1;
			int dlen = strlen(data);
			int offs = 0;
            strcpy(val, data + offs);
            *lenp = dlen + 1;
            break;

#ifdef WCHARSUPPORT
			SQLWCHAR *ucdata = NULL;
			SQLCHAR *cdata = (SQLCHAR *)*data;
#endif

#if (defined(_WIN32) || defined(_WIN64)) && defined(WINTERFACE)
			/* MS Access hack part 2 (reserved error -7748) */
			if (!valnull &&
				(s->cols == statSpec2P || s->cols == statSpec3P) &&
				type == SQL_C_WCHAR)
			{
				if (len > 0 && len <= sizeof(SQLWCHAR))
				{
					((char *)val)[0] = data[0][0];
					memset((char *)val + 1, 0, len - 1);
					*lenp = 1;
					sret = SQL_SUCCESS;
					goto done;
				}
			}
#endif

#ifdef WCHARSUPPORT
			switch (type)
			{
			case SQL_C_CHAR:
				doz = 1;
				break;
			case SQL_C_WCHAR:
				doz = sizeof(SQLWCHAR);
				break;
			default:
				doz = 0;
				break;
			}
			if (type == SQL_C_WCHAR)
			{
				ucdata = uc_from_utf(cdata, dlen);
				if (!ucdata)
				{
					return nomem(s);
				}
				dlen = uc_strlen(ucdata) * sizeof(SQLWCHAR);
			}
#if defined(_WIN32) || defined(_WIN64)
			else if (*s->oemcp && type == SQL_C_CHAR)
			{
				ucdata = (SQLWCHAR *)utf_to_wmb((char *)cdata, dlen);
				if (!ucdata)
				{
					return nomem(s);
				}
				cdata = (SQLCHAR *)ucdata;
				dlen = strlen((char *)cdata);
			}
#endif
#else
			doz = (type == SQL_C_CHAR) ? 1 : 0;
#endif
			if (partial && len && s->bindcols)
			{
				if (s->bindcols[col].offs >= dlen)
				{
#ifdef WCHARSUPPORT
					uc_free(ucdata);
#endif
					*lenp = 0;
					if (doz && val)
					{
#ifdef WCHARSUPPORT
						if (type == SQL_C_WCHAR)
						{
							((SQLWCHAR *)val)[0] = 0;
						}
						else
						{
							((char *)val)[0] = '\0';
						}
#else
						((char *)val)[0] = '\0';
#endif
					}
					if (!dlen && s->bindcols[col].offs == dlen)
					{
						s->bindcols[col].offs = 1;
						sret = SQL_SUCCESS;
						goto done;
					}
					s->bindcols[col].offs = 0;
					sret = SQL_NO_DATA;
					goto done;
				}
				offs = s->bindcols[col].offs;
				dlen -= offs;
			}
			if (val && !valnull && len)
			{
#ifdef WCHARSUPPORT
				if (type == SQL_C_WCHAR)
				{
					uc_strncpy(val, ucdata + offs / sizeof(SQLWCHAR),
							   (len - doz) / sizeof(SQLWCHAR));
				}
				else
				{
					strncpy(val, (char *)cdata + offs, len - doz);
				}
#else
				strncpy(val, data + offs, len - doz);
#endif
			}
			if (valnull || len < 1)
			{
				*lenp = dlen;
			}
			else
			{
				*lenp = min(len - doz, dlen);
				if (*lenp == len - doz && *lenp != dlen)
				{
					*lenp = SQL_NO_TOTAL;
				}
				else if (*lenp < zlen)
				{
					zlen = *lenp;
				}
			}
			if (len && !valnull && doz)
			{
#ifdef WCHARSUPPORT
				if (type == SQL_C_WCHAR)
				{
					((SQLWCHAR *)val)[zlen / sizeof(SQLWCHAR)] = 0;
				}
				else
				{
					((char *)val)[zlen] = '\0';
				}
#else
				((char *)val)[zlen] = '\0';
#endif
			}
#ifdef WCHARSUPPORT
			uc_free(ucdata);
#endif
			if (partial && len && s->bindcols)
			{
				if (*lenp == SQL_NO_TOTAL)
				{
					*lenp = dlen;
					s->bindcols[col].offs += len - doz;
					setstat(s, -1, "data right truncated", "01004");
					if (s->bindcols[col].lenp)
					{
						*s->bindcols[col].lenp = dlen;
					}
					sret = SQL_SUCCESS_WITH_INFO;
					goto done;
				}
				s->bindcols[col].offs += *lenp;
			}
			if (*lenp == SQL_NO_TOTAL)
			{
				*lenp = dlen;
				setstat(s, -1, "data right truncated", "01004");
				sret = SQL_SUCCESS_WITH_INFO;
				goto done;
			}
			break;
		}
#ifdef SQL_C_TYPE_DATE
		case SQL_C_TYPE_DATE:
#endif
		case SQL_C_DATE:
			if (str2date(data, (DATE_STRUCT *)val) < 0)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(DATE_STRUCT);
			}
			break;
#ifdef SQL_C_TYPE_TIME
		case SQL_C_TYPE_TIME:
#endif
		case SQL_C_TIME:
			if (str2time(data, (TIME_STRUCT *)val) < 0)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(TIME_STRUCT);
			}
			break;
#ifdef SQL_C_TYPE_TIMESTAMP
		case SQL_C_TYPE_TIMESTAMP:
#endif
		case SQL_C_TIMESTAMP:
			if (str2timestamp(data, (TIMESTAMP_STRUCT *)val) < 0)
			{
				*lenp = SQL_NULL_DATA;
			}
			else
			{
				*lenp = sizeof(TIMESTAMP_STRUCT);
			}
			switch (s->presto_stmt->columns[col]->precision)
			{
			case 0:
				((TIMESTAMP_STRUCT *)val)->fraction = 0;
				break;
			case 1:
				((TIMESTAMP_STRUCT *)val)->fraction /= 100000000;
				((TIMESTAMP_STRUCT *)val)->fraction *= 100000000;
				break;
			case 2:
				((TIMESTAMP_STRUCT *)val)->fraction /= 10000000;
				((TIMESTAMP_STRUCT *)val)->fraction *= 10000000;
				break;
			}
			break;
		default:
			return SQL_ERROR;
		}
	}
	sret = SQL_SUCCESS;
done:
	if (ilenp)
	{
		*ilenp = *lenp;
	}    
	return sret;
}

/**
 * Retrieve row data after fetch.
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param type output type
 * @param val output buffer
 * @param len length of output buffer
 * @param lenp output length
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLGetData(SQLHSTMT stmt, SQLUSMALLINT col, SQLSMALLINT type,
		   SQLPOINTER val, SQLLEN len, SQLLEN *lenp)
{
	STMT *s;
	SQLRETURN ret = SQL_ERROR;

	HSTMT_LOCK(stmt);
	if (stmt == SQL_NULL_HSTMT)
	{
		ret = SQL_INVALID_HANDLE;
        goto done;
	}
	s = (STMT *)stmt;

    if (!s->presto_stmt->tablebuff || s->presto_stmt->tablebuff->nrow == 0) {
        setstat(s, -1, "invalid result set buffer", (*s->ov3) ? (char*)"07009" : (char*)"S1002");
        ret = SQL_INVALID_HANDLE;
        goto done;        
    }

	if (col < 1 || col > s->presto_stmt->tablebuff->ncol )
	{
		setstat(s, -1, "invalid column reference", (*s->ov3) ? (char*)"07009" : (char*)"S1002");
		goto done;
	}
	--col;
	ret = getrowdata(s, col, type, val, len, lenp, 1);    
    // printf("return %s %i %i %i %i\n", val, type, col, len, *lenp);
done:
	HSTMT_UNLOCK(stmt);
	return ret;
}


/**
 * Internal describe column information.
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param name buffer for column name
 * @param nameMax length of name buffer
 * @param nameLen output length of column name
 * @param type output SQL type
 * @param size output column size
 * @param digits output number of digits
 * @param nullable output NULL allowed indicator
 * @result ODBC error code
 */

static SQLRETURN
drvdescribecol(SQLHSTMT stmt, SQLUSMALLINT col, SQLCHAR *name,
			   SQLSMALLINT nameMax, SQLSMALLINT *nameLen,
			   SQLSMALLINT *type, SQLULEN *size,
			   SQLSMALLINT *digits, SQLSMALLINT *nullable)
{
	STMT *s;
	PRESTOCLIENT_COLUMN *c;
	int didname = 0;

	if (stmt == SQL_NULL_HSTMT)
	{
		return SQL_INVALID_HANDLE;
	}
	s = (STMT *)stmt;
	if (!s->presto_stmt->columns)
	{
		setstat(s, -1, "no columns", (*s->ov3) ? "07009" : "S1002");
		return SQL_ERROR;
	}
	if (col < 1 || col > s->presto_stmt->columncount)
	{
		setstat(s, -1, "invalid column", (*s->ov3) ? "07009" : "S1002");
		return SQL_ERROR;
	}
	c = s->presto_stmt->columns[col - 1];
	if (name && nameMax > 0)
	{
		strncpy((char *)name, c->name, nameMax);
		name[nameMax - 1] = '\0';
		didname = 1;
	}
	if (nameLen)
	{
		if (didname)
		{
			*nameLen = strlen((char *)name);
		}
		else
		{
			*nameLen = strlen(c->name);
		}
	}
	if (type)
	{
		*type = c->type;
#ifdef WINTERFACE
		if (s->nowchar[0] || s->nowchar[1])
		{
			switch (c->type)
			{
			case SQL_WCHAR:
				*type = SQL_CHAR;
				break;
			case SQL_WVARCHAR:
				*type = SQL_VARCHAR;
				break;
#ifdef SQL_LONGVARCHAR
			case SQL_WLONGVARCHAR:
				*type = SQL_LONGVARCHAR;
				break;
#endif
			}
		}
#endif
	}
	if (size)
	{
		*size = c->bytesize;
	}
	if (digits)
	{
		*digits = 0;
	}
	if (nullable)
	{
		*nullable = 1;
	}
	return SQL_SUCCESS;
}

#ifndef WINTERFACE
/**
 * Describe column information.
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param name buffer for column name
 * @param nameMax length of name buffer
 * @param nameLen output length of column name
 * @param type output SQL type
 * @param size output column size
 * @param digits output number of digits
 * @param nullable output NULL allowed indicator
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLDescribeCol(SQLHSTMT stmt, SQLUSMALLINT col, SQLCHAR *name,
			   SQLSMALLINT nameMax, SQLSMALLINT *nameLen,
			   SQLSMALLINT *type, SQLULEN *size,
			   SQLSMALLINT *digits, SQLSMALLINT *nullable)
{
#if defined(_WIN32) || defined(_WIN64)
	SQLSMALLINT len = 0;
#endif
	SQLRETURN ret;

	HSTMT_LOCK(stmt);
#if defined(_WIN32) || defined(_WIN64)
	if (!((STMT *)stmt)->oemcp[0])
	{
		ret = drvdescribecol(stmt, col, name, nameMax, nameLen,
							 type, size, digits, nullable);
		goto done;
	}
	ret = drvdescribecol(stmt, col, name, nameMax,
						 &len, type, size, digits, nullable);
	if (ret == SQL_SUCCESS)
	{
		if (name)
		{
			if (len > 0)
			{
				SQLCHAR *n = NULL;

				n = (SQLCHAR *)utf_to_wmb((char *)name, len);
				if (n)
				{
					strncpy((char *)name, (char *)n, nameMax);
					n[len] = 0;
					len = min(nameMax, strlen((char *)n));
					uc_free(n);
				}
				else
				{
					len = 0;
				}
			}
			if (len <= 0)
			{
				len = 0;
				if (nameMax > 0)
				{
					name[0] = 0;
				}
			}
		}
		else
		{
			STMT *s = (STMT *)stmt;
			COL *c = s->cols + col - 1;

			len = 0;
			if (c->column)
			{
				len = strlen(c->column);
			}
		}
		if (nameLen)
		{
			*nameLen = len;
		}
	}
done:;
#else
	ret = drvdescribecol(stmt, col, name, nameMax, nameLen,
						 type, size, digits, nullable);
#endif
	HSTMT_UNLOCK(stmt);
	return ret;
}
#endif

#ifdef WINTERFACE
/**
 * Describe column information (UNICODE version).
 * @param stmt statement handle
 * @param col column number, starting at 1
 * @param name buffer for column name
 * @param nameMax length of name buffer
 * @param nameLen output length of column name
 * @param type output SQL type
 * @param size output column size
 * @param digits output number of digits
 * @param nullable output NULL allowed indicator
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLDescribeColW(SQLHSTMT stmt, SQLUSMALLINT col, SQLWCHAR *name,
				SQLSMALLINT nameMax, SQLSMALLINT *nameLen,
				SQLSMALLINT *type, SQLULEN *size,
				SQLSMALLINT *digits, SQLSMALLINT *nullable)
{
	SQLRETURN ret;
	SQLSMALLINT len = 0;

	HSTMT_LOCK(stmt);
	ret = drvdescribecol(stmt, col, (SQLCHAR *)name,
						 (SQLSMALLINT)(nameMax * sizeof(SQLWCHAR)),
						 &len, type, size, digits, nullable);
	if (ret == SQL_SUCCESS)
	{
		if (name)
		{
			if (len > 0)
			{
				SQLWCHAR *n = NULL;

				n = uc_from_utf((SQLCHAR *)name, len);
				if (n)
				{
					uc_strncpy(name, n, nameMax);
					n[len] = 0;
					len = min(nameMax, uc_strlen(n));
					uc_free(n);
				}
				else
				{
					len = 0;
				}
			}
			if (len <= 0)
			{
				len = 0;
				if (nameMax > 0)
				{
					name[0] = 0;
				}
			}
		}
		else
		{
			STMT *s = (STMT *)stmt;
			COL *c = s->cols + col - 1;

			len = 0;
			if (c->column)
			{
				len = strlen(c->column);
			}
		}
		if (nameLen)
		{
			*nameLen = len;
		}
	}
	HSTMT_UNLOCK(stmt);
	return ret;
}
#endif


/**
 * Internal return last HDBC or HSTMT error message.
 * @param env environment handle or NULL
 * @param dbc database connection handle or NULL
 * @param stmt statement handle or NULL
 * @param sqlState output buffer for SQL state
 * @param nativeErr output buffer for native error code
 * @param errmsg output buffer for error message
 * @param errmax length of output buffer for error message
 * @param errlen output length of error message
 * @result ODBC error code
 */

static SQLRETURN
drverror(SQLHENV env, SQLHDBC dbc, SQLHSTMT stmt,
		 SQLCHAR *sqlState, SQLINTEGER *nativeErr,
		 SQLCHAR *errmsg, SQLSMALLINT errmax, SQLSMALLINT *errlen)
{
	SQLCHAR dummy0[6];
	SQLINTEGER dummy1;
	SQLSMALLINT dummy2;

	if (env == SQL_NULL_HENV &&
		dbc == SQL_NULL_HDBC &&
		stmt == SQL_NULL_HSTMT)
	{
		return SQL_INVALID_HANDLE;
	}
	if (sqlState)
	{
		sqlState[0] = '\0';
	}
	else
	{
		sqlState = dummy0;
	}
	if (!nativeErr)
	{
		nativeErr = &dummy1;
	}
	*nativeErr = 0;
	if (!errlen)
	{
		errlen = &dummy2;
	}
	*errlen = 0;
	if (errmsg)
	{
		if (errmax > 0)
		{
			errmsg[0] = '\0';
		}
	}
	else
	{
		errmsg = dummy0;
		errmax = 0;
	}
	if (stmt)
	{
		STMT *s = (STMT *)stmt;

		HSTMT_LOCK(stmt);
		if (s->logmsg[0] == '\0')
		{
			HSTMT_UNLOCK(stmt);
			goto noerr;
		}
		*nativeErr = s->naterr;
		strcpy((char *)sqlState, s->sqlstate);
		if (errmax == SQL_NTS)
		{
			strcpy((char *)errmsg, "[SQLite]");
			strcat((char *)errmsg, (char *)s->logmsg);
			*errlen = strlen((char *)errmsg);
		}
		else
		{
			strncpy((char *)errmsg, "[SQLite]", errmax);
			if (errmax - 8 > 0)
			{
				strncpy((char *)errmsg + 8, (char *)s->logmsg, errmax - 8);
			}
			*errlen = min(strlen((char *)s->logmsg) + 8, errmax);
		}
		s->logmsg[0] = '\0';
		HSTMT_UNLOCK(stmt);
		return SQL_SUCCESS;
	}
	if (dbc)
	{
		DBC *d = (DBC *)dbc;

		HDBC_LOCK(dbc);
		if (d->magic != DBC_MAGIC || d->logmsg[0] == '\0')
		{
			HDBC_UNLOCK(dbc);
			goto noerr;
		}
		*nativeErr = d->naterr;
		strcpy((char *)sqlState, d->sqlstate);
		if (errmax == SQL_NTS)
		{
			strcpy((char *)errmsg, "[SQLite]");
			strcat((char *)errmsg, (char *)d->logmsg);
			*errlen = strlen((char *)errmsg);
		}
		else
		{
			strncpy((char *)errmsg, "[SQLite]", errmax);
			if (errmax - 8 > 0)
			{
				strncpy((char *)errmsg + 8, (char *)d->logmsg, errmax - 8);
			}
			*errlen = min(strlen((char *)d->logmsg) + 8, errmax);
		}
		d->logmsg[0] = '\0';
		HDBC_UNLOCK(dbc);
		return SQL_SUCCESS;
	}
noerr:
	sqlState[0] = '\0';
	errmsg[0] = '\0';
	*nativeErr = 0;
	*errlen = 0;
	return SQL_NO_DATA;
}

#ifndef WINTERFACE
/**
 * Return last HDBC or HSTMT error message.
 * @param env environment handle or NULL
 * @param dbc database connection handle or NULL
 * @param stmt statement handle or NULL
 * @param sqlState output buffer for SQL state
 * @param nativeErr output buffer for native error code
 * @param errmsg output buffer for error message
 * @param errmax length of output buffer for error message
 * @param errlen output length of error message
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLError(SQLHENV env, SQLHDBC dbc, SQLHSTMT stmt,
		 SQLCHAR *sqlState, SQLINTEGER *nativeErr,
		 SQLCHAR *errmsg, SQLSMALLINT errmax, SQLSMALLINT *errlen)
{
	return drverror(env, dbc, stmt, sqlState, nativeErr,
					errmsg, errmax, errlen);
}
#endif

#ifdef WINTERFACE
/**
 * Return last HDBC or HSTMT error message (UNICODE version).
 * @param env environment handle or NULL
 * @param dbc database connection handle or NULL
 * @param stmt statement handle or NULL
 * @param sqlState output buffer for SQL state
 * @param nativeErr output buffer for native error code
 * @param errmsg output buffer for error message
 * @param errmax length of output buffer for error message
 * @param errlen output length of error message
 * @result ODBC error code
 */

SQLRETURN SQL_API
SQLErrorW(SQLHENV env, SQLHDBC dbc, SQLHSTMT stmt,
		  SQLWCHAR *sqlState, SQLINTEGER *nativeErr,
		  SQLWCHAR *errmsg, SQLSMALLINT errmax, SQLSMALLINT *errlen)
{
	char state[16];
	SQLSMALLINT len = 0;
	SQLRETURN ret;

	ret = drverror(env, dbc, stmt, (SQLCHAR *)state, nativeErr,
				   (SQLCHAR *)errmsg, errmax, &len);
	if (ret == SQL_SUCCESS)
	{
		if (sqlState)
		{
			uc_from_utf_buf((SQLCHAR *)state, -1, sqlState,
							6 * sizeof(SQLWCHAR));
		}
		if (errmsg)
		{
			if (len > 0)
			{
				SQLWCHAR *e = NULL;

				e = uc_from_utf((SQLCHAR *)errmsg, len);
				if (e)
				{
					if (errmax > 0)
					{
						uc_strncpy(errmsg, e, errmax);
						e[len] = 0;
						len = min(errmax, uc_strlen(e));
					}
					else
					{
						len = uc_strlen(e);
					}
					uc_free(e);
				}
				else
				{
					len = 0;
				}
			}
			if (len <= 0)
			{
				len = 0;
				if (errmax > 0)
				{
					errmsg[0] = 0;
				}
			}
		}
		else
		{
			len = 0;
		}
		if (errlen)
		{
			*errlen = len;
		}
	}
	else if (ret == SQL_NO_DATA)
	{
		if (sqlState)
		{
			sqlState[0] = 0;
		}
		if (errmsg)
		{
			if (errmax > 0)
			{
				errmsg[0] = 0;
			}
		}
		if (errlen)
		{
			*errlen = 0;
		}
	}
	return ret;
}
#endif