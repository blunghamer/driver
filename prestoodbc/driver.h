#ifndef _simple_driver_H
#define _simple_driver_H

#include <odbcinst.h>
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include "../prestoclient/prestoclient.h"
#include "../prestoclient/prestoclienttypes.h"
#include "wcutils.h"
#include "str2odbc.h"

#define USE_DLOPEN_FOR_GPPS

struct dbc;
struct stmt;

/**
 * @typedef ENV
 * @struct ENV
 * Driver internal structure for environment (HENV).
 */

typedef struct {
    int magic;			/**< Magic cookie */
    int ov3;			/**< True for SQL_OV_ODBC3 */
    int pool;			/**< True for SQL_CP_ONE_PER_DRIVER */
#if defined(_WIN32) || defined(_WIN64)
    CRITICAL_SECTION cs;	/**< For serializing most APIs */
#endif
    struct dbc *dbcs;		/**< Pointer to first DBC */
} ENV;

#endif


/**
 * @typedef DBC
 * @struct dbc
 * Driver internal structure for database connection (HDBC).
 */

typedef struct dbc {
    int magic;			/**< Magic cookie */
    ENV *env;			/**< Pointer to environment */
    struct dbc *next;		/**< Pointer to next DBC */
    PRESTOCLIENT		*presto_client; /**< Pointer to PrestoClient */
    // sqlite3 *sqlite;		/**< SQLITE database handle */
    int version;		/**< SQLITE version number */
    char *dbname;		/**< SQLITE database name */
    char *dsn;			/**< ODBC data source name */
    int timeout;		/**< Lock timeout value */
    long t0;			/**< Start time for SQLITE busy handler */
    int busyint;		/**< Interrupt busy handler from SQLCancel() */
    int *ov3;			/**< True for SQL_OV_ODBC3 */
    int ov3val;			/**< True for SQL_OV_ODBC3 */
    int autocommit;		/**< Auto commit state */
    int intrans;		/**< True when transaction started */
    struct stmt *stmt;		/**< STMT list of this DBC */
    int naterr;			/**< Native error code */
    char sqlstate[6];		/**< SQL state for SQLError() */
    SQLCHAR logmsg[1024];	/**< Message for SQLError() */
    int nowchar;		/**< Don't try to use WCHAR */
    int dobigint;		/**< Force SQL_BIGINT for INTEGER columns */
    int shortnames;		/**< Always use short column names */
    int longnames;		/**< Don't shorten column names */
    int nocreat;		/**< Don't auto create database file */
    int fksupport;		/**< Foreign keys on or off */
    int curtype;		/**< Default cursor type */
    int step_enable;		/**< True for sqlite_compile/step/finalize */
    int trans_disable;		/**< True for no transaction support */
    int oemcp;			/**< True for Win32 OEM CP translation */
    int jdconv;			/**< True for julian day conversion */
    struct stmt *cur_s3stmt;	/**< Current STMT executing sqlite statement */
    int s3stmt_needmeta;	/**< True to get meta data in s3stmt_step(). */
    FILE *trace;		/**< sqlite3_trace() file pointer or NULL */
    char *pwd;			/**< Password or NULL */
    int pwdLen;			/**< Length of password */
#ifdef USE_DLOPEN_FOR_GPPS
    void *instlib;
    int (*gpps)();
#endif
#if defined(_WIN32) || defined(_WIN64)
    CRITICAL_SECTION cs;	/**< For serializing most APIs */
    DWORD owner;		/**< Current owner of CS or 0 */
    int xcelqrx;
#endif
} DBC;


/**
 * @typedef COL
 * @struct COL
 * Internal structure to describe a column in a result set.
 */

typedef struct {
    char *db;			/**< Database name */
    char *table;		/**< Table name */
    char *column;		/**< Column name */
    int type;			/**< Data type of column */
    int size;			/**< Size of column */
    int index;			/**< Index of column in result */
    int nosign;			/**< Unsigned type */
    int scale;			/**< Scale of column */
    int prec;			/**< Precision of column */
    int autoinc;		/**< AUTO_INCREMENT column */
    int notnull;		/**< NOT NULL constraint on column */
    int ispk;			/**< Flag for primary key (> 0) */
    int isrowid;		/**< Flag for ROWID column (> 0) */
    char *typname;		/**< Column type name or NULL */
    char *label;		/**< Column label or NULL */
} COL;

/**
 * @typedef BINDCOL
 * @struct BINDCOL
 * Internal structure for bound column (SQLBindCol).
 */

typedef struct {
    SQLSMALLINT type;	/**< ODBC type */
    SQLINTEGER max;	/**< Max. size of value buffer */
    SQLLEN *lenp;	/**< Value return, actual size of value buffer */
    SQLPOINTER valp;	/**< Value buffer */
    int index;		/**< Index of column in result */
    int offs;		/**< Byte offset for SQLGetData() */
} BINDCOL;

/**
 * @typedef BINDPARM
 * @struct BINDPARM
 * Internal structure for bound parameter (SQLBindParameter).
 */

typedef struct {
    int type, stype;	/**< ODBC and SQL types */
    int coldef, scale;	/**< from SQLBindParameter() */
    SQLLEN max;		/**< Max. size size of parameter buffer */
    SQLLEN *lenp;	/**< Actual size of parameter buffer */
    SQLLEN *lenp0;	/**< Actual size of parameter buffer, initial value */
    void *param;	/**< Parameter buffer */
    void *param0;	/**< Parameter buffer, initial value */
    int inc;		/**< Increment for paramset size > 1 */
    int need;		/**< True when SQL_LEN_DATA_AT_EXEC */
    int bound;		/**< True when SQLBindParameter() called */
    int offs, len;	/**< Offset/length for SQLParamData()/SQLPutData() */
    void *parbuf;	/**< Buffer for SQL_LEN_DATA_AT_EXEC etc. */
    char strbuf[64];	/**< String buffer for scalar data */
    int s3type;		/**< SQLite3 type */
    int s3size;		/**< SQLite3 size */
    void *s3val;	/**< SQLite3 value buffer */
    int s3ival;		/**< SQLite3 integer value */
    long long int s3lival;	/**< SQLite3 64bit integer value */
    double s3dval;	/**< SQLite3 float value */
} BINDPARM;


/**
 * @typedef STMT
 * @struct stmt
 * Driver internal structure representing SQL statement (HSTMT).
 */

typedef struct stmt {
    struct stmt *next;		/**< Linkage for STMT list in DBC */
    HDBC dbc;			/**< Pointer to DBC */
    SQLCHAR cursorname[32];	/**< Cursor name */
    SQLCHAR *query;		/**< Current query, raw string */
    int *ov3;			/**< True for SQL_OV_ODBC3 */
    int *oemcp;			/**< True for Win32 OEM CP translation */
    // int *jdconv;		/**< True for julian day conversion */
    int isselect;		/**< > 0 if query is a SELECT statement */
    size_t ncols;			/**< Number of result columns */
    COL *cols;			/**< Result column array */
    COL *dyncols;		/**< Column array, but malloc()ed */
    size_t dcols;			/**< Number of entries in dyncols */
    int bkmrk;			/**< True when bookmarks used */
    SQLINTEGER *bkmrkptr;	/**< SQL_ATTR_FETCH_BOOKMARK_PTR */
    BINDCOL bkmrkcol;		/**< Bookmark bound column */
    BINDCOL *bindcols;		/**< Array of bound columns */
    size_t nbindcols;		/**< Number of entries in bindcols */
    size_t nbindparms;		/**< Number bound parameters */
    BINDPARM *bindparms;	/**< Array of bound parameters */
    size_t nparams;		/**< Number of parameters in query */
    size_t pdcount;		/**< SQLParamData() counter */
    size_t nrows;			/**< Number of result rows */
    int rowp;			/**< Current result row */
    int rowprs;			/**< Current start row of rowset */
    char **rows;		/**< 2-dim array, result set */
    void (*rowfree)();		/**< Free function for rows */
    int naterr;			/**< Native error code */
    char sqlstate[6];		/**< SQL state for SQLError() */
    SQLCHAR logmsg[1024];	/**< Message for SQLError() */
    int nowchar[2];		/**< Don't try to use WCHAR */
    int dobigint;		/**< Force SQL_BIGINT for INTEGER columns */
    int longnames;		/**< Don't shorten column names */
    SQLULEN retr_data;		/**< SQL_ATTR_RETRIEVE_DATA */
    SQLULEN rowset_size;	/**< Size of rowset */
    SQLUSMALLINT *row_status;	/**< Row status pointer */
    SQLUSMALLINT *row_status0;	/**< Internal status array */
    SQLUSMALLINT row_status1;	/**< Internal status array for 1 row rowsets */
    SQLULEN *row_count;		/**< Row count pointer */
    SQLULEN row_count0;		/**< Row count */
    SQLULEN paramset_size;	/**< SQL_ATTR_PARAMSET_SIZE */
    SQLULEN paramset_count;	/**< Internal for paramset */
    SQLUINTEGER paramset_nrows;	/**< Row count for paramset handling */
    SQLULEN max_rows;		/**< SQL_ATTR_MAX_ROWS */
    SQLULEN bind_type;		/**< SQL_ATTR_ROW_BIND_TYPE */
    SQLULEN *bind_offs;		/**< SQL_ATTR_ROW_BIND_OFFSET_PTR */
    /* Dummies to make ADO happy */
    SQLULEN *parm_bind_offs;	/**< SQL_ATTR_PARAM_BIND_OFFSET_PTR */
    SQLUSMALLINT *parm_oper;	/**< SQL_ATTR_PARAM_OPERATION_PTR */
    SQLUSMALLINT *parm_status;	/**< SQL_ATTR_PARAMS_STATUS_PTR */
    SQLULEN *parm_proc;		/**< SQL_ATTR_PARAMS_PROCESSED_PTR */
    SQLULEN parm_bind_type;	/**< SQL_ATTR_PARAM_BIND_TYPE */
    int curtype;		/**< Cursor type */
    // sqlite3_stmt *s3stmt;	/**< SQLite statement handle or NULL */
    PRESTOCLIENT_RESULT *presto_stmt; /**< Presto statement handle or NULL */
    int s3stmt_noreset;		/**< False when sqlite3_reset() needed. */
    int presto_stmt_rownum;		/**< Current row number */
    char *bincell;		/**< Cache for blob data */
    char *bincache;		/**< Cache for blob data */
    int binlen;			/**< Length of blob data */
    int guessed_types;		/**< Flag for drvprepare()/drvexecute() */
    int one_tbl;		/**< Flag for single table (> 0) */
    int has_pk;			/**< Flag for primary key (> 0) */
    int has_rowid;		/**< Flag for ROWID (>= 0 or -1) */
} STMT;