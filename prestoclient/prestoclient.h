/**
 * \file prestoclient.h
 *
 * \brief prestoclient implements the client protocol to communicate with a Presto server
 *   
 * Presto (http://prestodb.io/) is a fast query engine developed
 * by Facebook that runs distributed queries against a (cluster of)
 * Hadoop HDFS servers (http://hadoop.apache.org/).
 * Presto uses SQL as its query language. Presto is an alternative for Hadoop-Hive.
 *
 * This file is part of cPrestoClient
 *
 * Copyright (C) 2014 Ivo Herweijer
 *
 * cPrestoClient is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact me via email: info@easydatawarehousing.com
 */

#ifndef EASYPTORA_PRESTOCLIENT_HH
#define EASYPTORA_PRESTOCLIENT_HH

#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* --- Defines -------------------------------------------------------------------------------------------------------- */
#define PRESTOCLIENT_SOURCE              "cPrestoClient"  //!< Client name sent to Presto server
#define PRESTOCLIENT_VERSION             "0.3.2"          //!< PrestoClient version string
#define PRESTOCLIENT_URLTIMEOUT           5000            //!< Timeout in millisec to wait for Presto server to respond
#define PRESTOCLIENT_UPDATEWAITTIMEMSEC   20              //!< Wait time in millisec to wait between requests to Presto server
#define PRESTOCLIENT_RETRIEVEWAITTIMEMSEC 20              //!< Wait time in millisec to wait before getting next data packet
#define PRESTOCLIENT_RETRYWAITTIMEMSEC    100             //!< Wait time in millisec to wait before retrying a request
#define PRESTOCLIENT_MAXIMUMRETRIES       5               //!< Maximum number of retries for request in case of 503 errors
#define PRESTOCLIENT_DEFAULT_PORT         8080            //!< Default tcp port of presto server
#define PRESTOCLIENT_DEFAULT_CATALOG      "system"        //!< Default presto catalog name
#define PRESTOCLIENT_DEFAULT_SCHEMA       "runtime"       //!< Default presto schema name

enum PRESTO_RESULTCODES
{
	PRESTO_OK = 0,		          //!< all went well
	PRESTO_BAD_REQUEST,			  //!< caller did not provide sufficient parameters
	PRESTO_NO_MEMORY,			  //!< memory allocation error 
	PRESTO_BACKEND_ERROR		  //!< presto backend issued an error
};

/* --- Enums ---------------------------------------------------------------------------------------------------------- */
/**
 * \brief Fieldtypes returned by prestoclient
 */
enum E_FIELDTYPES
{
	PRESTOCLIENT_TYPE_UNDEFINED = 0,
	// char types
	PRESTOCLIENT_TYPE_VARCHAR, 				// 1
	PRESTOCLIENT_TYPE_CHAR,					// 2
	PRESTOCLIENT_TYPE_VARBINARY,			// 3
	// integers
	PRESTOCLIENT_TYPE_TINYINT,				// 4
	PRESTOCLIENT_TYPE_SMALLINT,				// 5
	PRESTOCLIENT_TYPE_INTEGER,				// 6
	PRESTOCLIENT_TYPE_BIGINT,				// 7
	PRESTOCLIENT_TYPE_BOOLEAN,				// 8
	// floating
	PRESTOCLIENT_TYPE_REAL,					// 9
	PRESTOCLIENT_TYPE_DOUBLE,				// 19
	PRESTOCLIENT_TYPE_DECIMAL,				// 11
	// date and time
	PRESTOCLIENT_TYPE_DATE,					// 12
	PRESTOCLIENT_TYPE_TIME,					// 13
	PRESTOCLIENT_TYPE_TIME_WITH_TIME_ZONE,	// 14
	PRESTOCLIENT_TYPE_TIMESTAMP,			// 15
	PRESTOCLIENT_TYPE_TIMESTAMP_WITH_TIME_ZONE, 	// 16
	PRESTOCLIENT_TYPE_INTERVAL_YEAR_TO_MONTH, 		// 17
	PRESTOCLIENT_TYPE_INTERVAL_DAY_TO_SECOND, 		// 18
	// complex structural types
	PRESTOCLIENT_TYPE_ARRAY,						// 19
    PRESTOCLIENT_TYPE_MAP, 							// 20
	PRESTOCLIENT_TYPE_JSON							// 21
};


static size_t E_FIELDTYPES_SIZES[22] = {
    0,
    2147483647,
	2147483647,
	2147483647,
	1,
	2,
	4,
	8,
	1,
	8,
	8,
	8,
	10,
	12,
	20,
	23,
	30,
	20,
	20,
	2147483647,
	2147483647,
	2147483647
};


/**
 * \brief Status of a query determined by prestoclient
 */
enum E_CLIENTSTATUS
{
    PRESTOCLIENT_STATUS_NONE = 0,
    PRESTOCLIENT_STATUS_RUNNING,
    PRESTOCLIENT_STATUS_SUCCEEDED,
    PRESTOCLIENT_STATUS_FAILED
};

// actually there is QUEUED, RUNNING, FAILED, SUCCESS (TOBE FIXED) in the presto backend

/* --- Structs -------------------------------------------------------------------------------------------------------- */
/**
 * \brief  Query resultset used to interface with prestoclient. All members are private.
 */
typedef struct ST_PRESTOCLIENT_RESULT PRESTOCLIENT_RESULT;

/**
 * \brief  Client interface for prestoclient. All members are private.
 */
typedef struct ST_PRESTOCLIENT        PRESTOCLIENT;

/* --- Functions ------------------------------------------------------------------------------------------------------ */
/**
 * \brief               Get the version string of prestoclient
 *
 * \return              Null terminated version string
 */
char*                   prestoclient_getversion();


/**
 * \brief               Initiate a client connection
 *
 * \param in_server     String contaning the servername or address. Should be without the port number. Not NULL
 * \param in_port       Unsigned int containing the tcp port of the Presto server. May be NULL
 * \param in_catalog    String contaning the Hive catalog name. May be NULL
 * \param in_user       String contaning the username for the Presto server. May be NULL
 * \param in_pwd        String contaning the password for the Presto server. May be NULL. Currently not used
 * \param in_timezone   String contaning the Timezone for the Presto server. May be NULL. (IANA timezone format)
 * \param in_language   String contaning the Language for the Presto server. May be NULL. (ISO-639-1 code)
 *
 * \return              A handle to the PRESTOCLIENT object if successful or NULL if init failed
 */
PRESTOCLIENT*           prestoclient_init                       ( const char *protocol
																, const char *in_server
                                                                , const unsigned int *in_port
                                                                , const char *in_catalog
																, const char *in_schema
                                                                , const char *in_user
                                                                , const char *in_pwd
																, const char *in_timezone
																, const char *in_language
																, bool trace_http);

/**
 * \brief               Get the serverinfo to see if there is a server we even an conncect to...
 * 
 * \param prestoclient  Handle to PRESTOCLIENT object
 * \return              Null terminated version string
 */
char*                   prestoclient_serverinfo(PRESTOCLIENT *prestoclient);

/**
 * \brief               Close client connection
 *                      Close client connection and delete all used memory. Handle to object is NULL after
 *                      calling this function.
 * 
 * \param prestoclient  Handle to PRESTOCLIENT object
 */
void                    prestoclient_close                      (PRESTOCLIENT *prestoclient);

/**
 * \brief               Execute a query
 *                      Executes a query and calls callback functions when columninfo or data
 *                      is available.
 *
 * \param prestoclient                  Handle to PRESTOCLIENT object
 * \param in_sql_statement              String containing the sql statement that should be executed on the Presto server
 * \param in_schema                     String contaning the Hive schema name. May be NULL
 * \param in_write_callback_function    Pointer to function called when columninfo is available
 * \param in_describe_callback_function Pointer to function called for every available row of data
 * \param in_client_object              Pointer to a user object, passed to callback functions
 *
 * \return              A handle to the PRESTOCLIENT_RESULT object if successful or NULL if starting the query failed
 */
int    					prestoclient_query                      (PRESTOCLIENT *prestoclient
																, PRESTOCLIENT_RESULT** result
                                                                , const char *in_sql_statement
                                                                , void (*in_write_callback_function)(void*, void*)                                                                
                                                                , void *in_client_object
                                                                );

/**
 * \brief 				prepare query preparation to mimic odbc api
 */
int    					prestoclient_prepare                    (PRESTOCLIENT *prestoclient
																, PRESTOCLIENT_RESULT** result
                                                                , const char *in_sql_statement                                                                                                                            
                                                                );


int					    prestoclient_execute                    (PRESTOCLIENT *prestoclient
                                                                ,PRESTOCLIENT_RESULT *prepared_result                                                                                                                              
                                                                , void (*in_write_callback_function)(void*, void*)																
																, void *in_client_object);

void                    prestoclient_deleteresult               (PRESTOCLIENT *prestoclient
                                                                ,PRESTOCLIENT_RESULT *prepared_result                                                                                                                      
																);


/**
 * \brief               Return the status of the query as determined by prestoclient
 *                      Note this is not the same as the state reported by the Presto server!
 *
 * \param result        A handle to a PRESTOCLIENT_RESULT object
 *
 * \return              Numeric value corresponding to enum E_CLIENTSTATUS
 */
unsigned int            prestoclient_getstatus                  (PRESTOCLIENT_RESULT *result);


/**
 * \brief               Return state of the request as reported by the Presto server
 *
 * \param result        A handle to a PRESTOCLIENT_RESULT object
 *
 * \return              Null terminated string
 */
char*                   prestoclient_getlastserverstate         (PRESTOCLIENT_RESULT *result);

/**
 * \brief               Returns the number of columns of the query
 *
  * \param result       A handle to a PRESTOCLIENT_RESULT object
*
 * \return              Number of columns or zero if the resultset doesn't contain columninformation (yet)
 */
size_t                  prestoclient_getcolumncount             (PRESTOCLIENT_RESULT *result);

/**
 * \brief               Return the column name of the specified column
 *
 * \param result        A handle to a PRESTOCLIENT_RESULT object
 * \param columnindex   Zero based index of column. Should be smaller than number returned by prestoclient_getcolumncount
 *
 * \return              Null terminated string
 */
char*                   prestoclient_getcolumnname              (PRESTOCLIENT_RESULT *result, const size_t columnindex);

/**
 * \brief               Return the column type of the specified column
 *
 * \param result        A handle to a PRESTOCLIENT_RESULT object
 * \param columnindex   Zero based index of column. Should be smaller than number returned by prestoclient_getcolumncount
 *
 * \return              Numeric value corresponding to enum E_FIELDTYPES
 */
unsigned int            prestoclient_getcolumntype              (PRESTOCLIENT_RESULT *result, const size_t columnindex);

/**
 * \brief               Return the column type of the specified column as string
 *
 * \param result        A handle to a PRESTOCLIENT_RESULT object
 * \param columnindex   Zero based index of column. Should be smaller than number returned by prestoclient_getcolumncount
 *
 * \return              Null terminated string
 */
const char*             prestoclient_getcolumntypedescription   (PRESTOCLIENT_RESULT *result, const size_t columnindex);

/**
 * \brief               Return the content of the specified column for the current row as string
 *
 * \param result        A handle to a PRESTOCLIENT_RESULT object
 * \param columnindex   Zero based index of column. Should be smaller than number returned by prestoclient_getcolumncount
 *
 * \return              Null terminated string
 */
char*                   prestoclient_getcolumndata              (PRESTOCLIENT_RESULT *result, const size_t columnindex);

/**
 * \brief               Returns true if the content of the specified column is NULL according to the database
 *
 * \param result        A handle to a PRESTOCLIENT_RESULT object
 * \param columnindex   Zero based index of column. Should be smaller than number returned by prestoclient_getcolumncount
 *
 * \return              Return true (1) if the content of the specified column is NULL, otherwise false (0)
 */
int                     prestoclient_getnullcolumnvalue         (PRESTOCLIENT_RESULT *result, const size_t columnindex);

/**
 * \brief               Inform prestoclient to cancel the running query
 *                      Prestoclient should cancel the running query. As soon as prestoclient detects this signal and is not
 *                      in the middle of handling a curl response, it will send a cancel query request to the Presto server
 *                      and return from the prestoclient_query function.
 *
 * \param result        A handle to a PRESTOCLIENT_RESULT object
 */
void                    prestoclient_cancelquery                (PRESTOCLIENT_RESULT *result);

/**
 * \brief               Return error message of last executed request generated by the prestoserver
 *
 * \param result        A handle to a PRESTOCLIENT_RESULT object
 *
 * \return              Error message generated by the prestoserver or NULL if there is no error
 */
char*                   prestoclient_getlastservererror         (PRESTOCLIENT_RESULT *result);

/**
 * \brief               Returns description of last error of determined by prestoclient
 *
 * \param result        A handle to a PRESTOCLIENT_RESULT object
 *
 * \return              Null terminated string or NULL if no errors occurred
 */
char*                   prestoclient_getlastclienterror         (PRESTOCLIENT_RESULT *result);

/**
 * \brief               Returns additional error messages produced by curl
 *
 * \param result        A handle to a PRESTOCLIENT_RESULT object
 *
 * \return              Null terminated string, may be empty
 */
char*                   prestoclient_getlastcurlerror           (PRESTOCLIENT_RESULT *result);

#ifdef __cplusplus
}
#endif

#endif // EASYPTORA_PRESTOCLIENT_HH