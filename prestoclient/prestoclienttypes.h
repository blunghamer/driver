/*
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

// Do not include this file in your project
// These are private defines and type declarations for prestoclient
// Use "prestoclient.h" instead

#ifndef EASYPTORA_PRESTOCLIENTTYPES_HH
#define EASYPTORA_PRESTOCLIENTTYPES_HH

#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <curl/curl.h>
#include "jsonparser.h"

/* --- Defines -------------------------------------------------------------------------------------------------------- */
#define PRESTOCLIENT_QUERY_URL "v1/statement" 				// URL added to servername to start a query
#define PRESTOCLIENT_INFO_URL "v1/info"					    // URL added to get info from server
#define PRESTOCLIENT_CURL_BUFFERSIZE CURL_MAX_WRITE_SIZE	// (Preferred) Buffersize for communication with curl
#define PRESTOCLIENT_CURL_EXPECT_HTTP_GET_POST 200			// Expected http response code for get and post requests
#define PRESTOCLIENT_CURL_EXPECT_HTTP_DELETE   204			// Expected http response code for delete requests
#define PRESTOCLIENT_CURL_EXPECT_HTTP_BUSY     503			// Expected http response code when presto server is busy

/* --- Enums ---------------------------------------------------------------------------------------------------------- */
enum E_RESULTCODES
{
	PRESTOCLIENT_RESULT_OK = 0,
	PRESTOCLIENT_RESULT_BAD_REQUEST_DATA,
	PRESTOCLIENT_RESULT_SERVER_ERROR,
	PRESTOCLIENT_RESULT_MAX_RETRIES_REACHED,
	PRESTOCLIENT_RESULT_CURL_ERROR,
	PRESTOCLIENT_RESULT_PARSE_JSON_ERROR
};

enum E_HTTP_REQUEST_TYPES
{
	PRESTOCLIENT_HTTP_REQUEST_TYPE_GET,
	PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
	PRESTOCLIENT_HTTP_REQUEST_TYPE_DELETE
};

typedef struct ST_PRESTOCLIENT_COLUMN
{
	char						 *name;							//!< Name of column
	char                         *catalog;   					//!< catalog name or null
	char						 *schema;						//!< schema name or null
	char                         *table;						//!< table name or null
	enum E_FIELDTYPES			  type;							//!< Type of field	
	size_t                        bytesize;                     //!< max length of the datatype
	size_t                        precision;                    //!< precision of floars
	size_t                        scale;                        //!< scale of floats after decimal sign
	char						 *data;							//!< Buffer for fielddata
	size_t				          datasize;						//!< Size of data buffer can be less then 
	bool						  dataisnull;					//!< Set to true if content of data is null
	bool                          alias;						//!< Set to true if is an alias
} PRESTOCLIENT_COLUMN;

typedef struct ST_PRESTOCLIENT_TABLEBUFFER
{
	char              		    **rowbuff;		//!< result array	
	size_t 						  nalloc;		//!< alloc'ed size of result array
	size_t 						  nrow;			//!< number of rows in result array
	size_t 						  ncol;			//!< number of columns in result array
	ptrdiff_t 					  ndata;		//!< index into result array
	int                           rowidx;       //!< row index pointer can be negative -1 for not started to iterate
} PRESTOCLIENT_TABLEBUFFER;

typedef struct ST_PRESTOCLIENT PRESTOCLIENT;

typedef struct ST_PRESTOCLIENT_RESULT
{
	PRESTOCLIENT				 *client;						//!< Pointer to PRESTOCLIENT
	CURL						 *hcurl;						//!< Handle to libCurl
	char						 *curl_error_buffer;			//!< Buffer for storing curl error messages
	void (*write_callback_function)(void*, void*);				//!< Functionpointer to client function handling queryoutput
	void (*describe_callback_function)(void*, void*);			//!< Functionpointer to client function handling output description
	void						 *client_object;				//!< Pointer to object to pass to client function
	char						 *lastinfouri;					//!< Uri to query information on the Presto server
	char						 *lastnexturi;					//!< Uri to next dataframe on the Presto server
	char						 *lastcanceluri;				//!< Uri to cancel query on the Presto server
	char						 *laststate;					//!< State returned by last request to Presto server
	char						 *lasterrormessage;				//!< Last error message returned by Presto server
	char                         *query;						//!< query
	char                         *prepared_stmt_name;           //!< prepared statement name
	char                         *prepared_stmt_hdr;            //!< prepared statement header 
	enum E_CLIENTSTATUS			  clientstatus;					//!< Status defined by PrestoClient: NONE, RUNNING, SUCCEEDED, FAILED
	bool						  cancelquery;					//!< Boolean, when set to true signals that query should be cancelled
	char						 *lastresponse;					//!< Buffer for curl response
	size_t						  lastresponsebuffersize;		//!< Maximum size of the curl buffer
	size_t						  lastresponseactualsize;		//!< Actual size of the curl buffer
	PRESTOCLIENT_TABLEBUFFER     *tablebuff;                    //!< Buffer for result rows of the http fetch (should not be more than 16 MB of json in one go)
	PRESTOCLIENT_COLUMN			**columns;						//!< Buffer for the column information returned by the query	
	size_t      				  columncount;					//!< Number of columns in output or 0 if unknown	
	PRESTOCLIENT_COLUMN         **parameters;					//!< Buffer for the parameters returned by the query	
	size_t                        parametercount;				//!< Number of parameters in output or 0 if unknown
	bool						  columninfoavailable;			//!< Flag set to true if columninfo is available and complete (also used for json lexer)
	bool						  columninfoprinted;			//!< Flag set to true if columninfo has been printed to output	
	int							  currentdatacolumn;			//!< Index to datafield (columns array) currently handled or -1 when not parsing field data
	bool						  dataavailable;				//!< Flag set to true if a row of data is available
	enum E_RESULTCODES			  errorcode;					//!< Errorcode, set when terminating a request
	JSONPARSER					 *json;							//!< Pointer to the json parser
	JSONLEXER					 *lexer;						//!< Pointer to the json lexer
} PRESTOCLIENT_RESULT;

typedef struct ST_PRESTOCLIENT
{
	char                         *baseurl;                      //!< baseurl protocol, server port 
	char						 *useragent;					//!< Useragent name sent to Presto server
	char						 *server;						//!< IP address or DNS name of Presto server
	char						 *protocol;						//!< http or https
	unsigned int				  port;							//!< TCP port of Presto server
	char						 *catalog;						//!< Catalog name to be used by Presto server
	char                         *schema;                       //!< Schema name to be used by Presto server
	char						 *user;							//!< Username to pass to Presto server
	char						 *timezone;						//!< Timezone to pass to Presto server
	char						 *language;						//!< Language to pass to Presto server
	PRESTOCLIENT_RESULT			**results;						//!< Array containing query status and data
	size_t				         active_results;				//!< Number of queries issued
	bool                         trace_http;					//!< trace http / verbose curl stuff
} PRESTOCLIENT;

/* --- Functions ------------------------------------------------------------------------------------------------------ */

// Utility functions
extern char* get_username();
extern void util_sleep(const int sleeptime_msec);

// Memory handling functions
extern void alloc_copy(char **var, const char *newvalue);
extern void alloc_add(char **var, const char *addedvalue);

// this is ugly and should not be part of the external contract
extern PRESTOCLIENT_COLUMN* new_prestocolumn();
extern PRESTOCLIENT_TABLEBUFFER* new_tablebuffer();

// JSON Functions
extern bool json_reader(PRESTOCLIENT_RESULT* result);


#endif // EASYPTORA_PRESTOCLIENTTYPES_HH