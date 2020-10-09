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

#include "prestoclient.h"
#include "prestoclienttypes.h"
#include <curl/curl.h>
#include <assert.h>

// forward declarations
static void remove_result(PRESTOCLIENT_RESULT *result);

/* --- Private functions ---------------------------------------------------------------------------------------------- */

// malloc/realloc memory for the variable and copy the newvalue to the variable. Exit on failure
void alloc_copy(char **var, const char *newvalue)
{
	unsigned int newlength, currlength = 0;

	assert(var);
	assert(newvalue);

	newlength = (strlen(newvalue) + 1) * sizeof(char);

	if (*var)
	{
		// realloc
		currlength = (strlen(*var) + 1) * sizeof(char);

		if (currlength < newlength)
			*var = (char *)realloc((char *)*var, newlength);
	}
	else
	{
		// malloc
		*var = (char *)malloc(newlength);
	}

	// Not doing rigorous checking and handling of all malloc's because:
	// - On the intended platforms for this code (Linux, windows boxes with lots of (virtual)memory) malloc failures are very rare
	// - Because such failures are rare it's very difficult to test proper malloc handling code
	// - Handling failures will likely also fail
	// Whenever an alloc fails we're doing an: exit(1)
	if (!var)
		exit(1);

	strcpy(*var, newvalue);
}

void alloc_add(char **var, const char *addedvalue)
{
	unsigned int newlength, currlength = 0;

	assert(var);
	assert(addedvalue);

	newlength = (strlen(addedvalue) + 1) * sizeof(char);

	if (*var)
	{
		currlength = (strlen(*var) + 2) * sizeof(char);
		*var = (char *)realloc((char *)*var, currlength + newlength);
	}
	else
	{
		*var = (char *)malloc(newlength);
		(*var)[0] = 0;
	}

	if (!var)
		exit(1);

	if (strlen(*var) > 0)
		strcat(*var, "\n");

	strcat(*var, addedvalue);
}

PRESTOCLIENT_FIELD *new_prestofield()
{
	PRESTOCLIENT_FIELD *field = (PRESTOCLIENT_FIELD *)malloc(sizeof(PRESTOCLIENT_FIELD));

	if (!field)
		exit(1);

	field->name = NULL;
	field->type = PRESTOCLIENT_TYPE_UNDEFINED;
	field->datasize = 1024 * sizeof(char);
	field->data = (char *)malloc(field->datasize + 1);
	field->dataisnull = false;

	if (!field->data)
		exit(1);

	return field;
}

static void delete_prestofield(PRESTOCLIENT_FIELD *field)
{
	if (!field)
		return;

	if (field->name)
		free(field->name);

	if (field->data)
		free(field->data);

	free(field);
}

PRESTOCLIENT_TABLEBUFFER* new_tablebuffer(size_t initialsize) {
	PRESTOCLIENT_TABLEBUFFER* tab = (PRESTOCLIENT_TABLEBUFFER*)malloc(sizeof(PRESTOCLIENT_TABLEBUFFER));
	tab->nalloc = initialsize;	
	tab->rowbuff = (unsigned char**)malloc(sizeof(unsigned char*) * tab->nalloc);
	tab->nrow = 0;
	tab->ncol = 0;
	tab->ndata = 0;
	tab->rowidx = -1;
	return tab;
}

void grow_tablebuffer(PRESTOCLIENT_TABLEBUFFER* tab, size_t addsize) {
	size_t newsz = tab->nalloc + addsize;	
	tab->rowbuff = realloc(tab->rowbuff, sizeof(unsigned char*) * newsz );
	tab->nalloc = newsz;			
}

static void delete_tablebuffer(PRESTOCLIENT_TABLEBUFFER *tab) {
	if (!tab) 
		return;
	
	if (tab->rowbuff) {
		for (size_t zz = 0 ; zz < tab->ndata ; zz++) {
			if (tab->rowbuff[zz]) {
				free(tab->rowbuff[zz]);
			}
		}
		free(tab->rowbuff);
	}

	free(tab);
}

static PRESTOCLIENT_RESULT *new_prestoresult()
{
	PRESTOCLIENT_RESULT *result = (PRESTOCLIENT_RESULT *)malloc(sizeof(PRESTOCLIENT_RESULT));

	if (!result)
		exit(1);

	result->client = NULL;
	result->hcurl = NULL;
	result->curl_error_buffer = NULL;
	result->lastinfouri = NULL;
	result->lastnexturi = NULL;
	result->lastcanceluri = NULL;
	result->laststate = NULL;
	result->lasterrormessage = NULL;
	result->clientstatus = PRESTOCLIENT_STATUS_NONE;
	result->cancelquery = false;
	result->lastresponse = NULL;
	result->lastresponsebuffersize = 0;
	result->lastresponseactualsize = 0;
	result->prepared_stmt_hdr = NULL;
	result->prepared_stmt_name = NULL;
	result->columns = NULL;
	result->columncount = 0;
	result->tablebuff = NULL;
	result->columninfoavailable = false;
	result->columninfoprinted = false;
	result->currentdatacolumn = -1;
	result->dataavailable = false;
	result->errorcode = PRESTOCLIENT_RESULT_OK;
	result->json = NULL;
	result->lexer = NULL;

	// we should set the function pointers to null, right?
	result->describe_callback_function = NULL;
	result->write_callback_function = NULL;

	return result;
}


// Delete this result set from memory and remove from PRESTOCLIENT
static void delete_prestoresult(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return;

	// disassociate result from PRESTOCLIENT buffer
	remove_result(result);

	if (result->hcurl)
	{
		curl_easy_cleanup(result->hcurl);
		if (result->curl_error_buffer)
			free(result->curl_error_buffer);
	}

	json_delete_parser(result->json);

	json_delete_lexer(result->lexer);

	if (result->lastinfouri)
		free(result->lastinfouri);

	if (result->lastnexturi)
		free(result->lastnexturi);

	if (result->lastcanceluri)
		free(result->lastcanceluri);

	if (result->laststate)
		free(result->laststate);

	if (result->lasterrormessage)
		free(result->lasterrormessage);

	if (result->lastresponse)
		free(result->lastresponse);

	if (result->prepared_stmt_hdr)
		free(result->prepared_stmt_hdr);

	if (result->prepared_stmt_name)
		free(result->prepared_stmt_name);

	if (result->columns) {
		for (size_t i = 0; i < result->columncount; i++) {
			if (result->columns[i])
				delete_prestofield(result->columns[i]);
		}
		free(result->columns);
		result->columns = NULL;
	}
		
	if (result->tablebuff) {
		delete_tablebuffer(result->tablebuff);
		result->tablebuff = NULL;		
	}

	free(result);
}

static void reset_prestoresult(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return;

	json_delete_parser(result->json);
	result->json = NULL;
	json_delete_lexer(result->lexer);
	result->lexer = NULL;

	if (result->columns) {
		for (size_t i = 0; i < result->columncount; i++) {
			if (result->columns[i])
				delete_prestofield(result->columns[i]);
		}
		free(result->columns);
		result->columns = NULL;
	}

	if (result->tablebuff) {
		delete_tablebuffer(result->tablebuff);
		result->tablebuff = NULL;		
	}

	result->columninfoavailable = false;
	result->columninfoprinted = false;
	result->dataavailable = false;
	result->currentdatacolumn = -1;
	result->columncount = 0;
}



static PRESTOCLIENT *new_prestoclient(bool trace_http)
{
	PRESTOCLIENT *client = (PRESTOCLIENT *)malloc(sizeof(PRESTOCLIENT));

	if (!client)
		exit(1);

	client->baseurl = NULL;
	client->useragent = NULL;
	client->protocol = NULL;
	client->server = NULL;
	client->port = PRESTOCLIENT_DEFAULT_PORT;
	client->catalog = NULL;
	client->user = NULL;
	client->timezone = NULL;
	client->language = NULL;
	client->results = NULL;
	client->active_results = 0;
	client->trace_http = trace_http;

	return client;
}


/*
 * The write callback function. This function will be called for every row of
 * query data and append to internal row buffer
 */
static void write_callback_buffer(void *in_userdata, void *in_result)
{
	// userdata is null in this call...
	PRESTOCLIENT_RESULT *result = (PRESTOCLIENT_RESULT *)in_result;	
	size_t columncount = prestoclient_getcolumncount(result);

	size_t growby = 10;

	if (!result->tablebuff) {
		result->tablebuff = new_tablebuffer(columncount * growby);
		result->tablebuff->ncol = columncount;
	}

	if (result->tablebuff->nalloc <= (columncount + result->tablebuff->ndata) ) {		
		grow_tablebuffer(result->tablebuff, columncount * growby);
	}

	result->tablebuff->nrow++;

	for (size_t idx = 0; idx < columncount; idx++)
	{		
		char * flddata = prestoclient_getcolumndata(result, idx);		
		result->tablebuff->rowbuff[result->tablebuff->ndata] = (unsigned char*)malloc(strlen(flddata) + 1);
		strcpy(result->tablebuff->rowbuff[result->tablebuff->ndata], (unsigned char*)flddata);
		result->tablebuff->ndata++;		
	}
}


// Add this result set to the PRESTOCLIENT
static void add_result(PRESTOCLIENT_RESULT *result)
{
	PRESTOCLIENT *client;

	if (!result)
		return;

	if (!result->client)
		return;

	client = result->client;

	client->active_results++;

	if (client->active_results == 1)
		client->results = (PRESTOCLIENT_RESULT **)malloc(sizeof(PRESTOCLIENT_RESULT *));
	else
		client->results = (PRESTOCLIENT_RESULT **)realloc((PRESTOCLIENT_RESULT **)client->results, client->active_results * sizeof(PRESTOCLIENT_RESULT *));

	if (!client->results)
		exit(1);

	client->results[client->active_results - 1] = result;
}

static void remove_result(PRESTOCLIENT_RESULT *result) {
	PRESTOCLIENT *client;

	if (!result)
		return;

	if (!result->client)
		return;

	client = result->client;

	if (client->active_results == 0)
		return;

	bool found = false;
	for (size_t idx=0; idx < client->active_results ; idx++) {
		if (client->results[idx] == result ) {
			found = true;
		}
		if (found && (idx+1) < client->active_results){
			client->results[idx] = client->results[idx+1];
		}
	}
	client->active_results--;

	if (client->active_results == 0) {
		free(client->results);
		client->results = NULL;
	} else {
		client->results = (PRESTOCLIENT_RESULT **)realloc((PRESTOCLIENT_RESULT **)client->results, client->active_results * sizeof(PRESTOCLIENT_RESULT *));
	}
}


// Add a key/value to curl header list
static void add_headerline(struct curl_slist **header, char *name, char *value)
{
	int length = (strlen(name) + strlen(value) + 3) * sizeof(char);
	char *line = (char *)malloc(length);

	if (!line)
		exit(1);

	strcpy(line, name);
	strcat(line, ": ");
	strcat(line, value);
	strcat(line, "\0");

	*header = curl_slist_append(*header, line);

	free(line);
}

// Callback function for CURL data. Data is added to the resultset databuffer
static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
	size_t contentsize = size * nmemb;
	PRESTOCLIENT_RESULT *result = (PRESTOCLIENT_RESULT *)userp;

	// Do we need a bigger buffer ? Should not happen
	if (result->lastresponseactualsize + contentsize > result->lastresponsebuffersize)
	{
		result->lastresponse = (char *)realloc(result->lastresponse, result->lastresponseactualsize + contentsize + 1);

		if (!result->lastresponse)
			exit(1);

		result->lastresponsebuffersize = result->lastresponseactualsize + contentsize + 1;
	}

	// Add contents to buffer
	memcpy(&(result->lastresponse[result->lastresponseactualsize]), contents, contentsize);

	// Update actual size
	result->lastresponseactualsize += contentsize;

	// Add terminating zero
	result->lastresponse[result->lastresponseactualsize] = 0;

	printf("%s\n", result->lastresponse);
	// Start/continue parsing json. Stop on errors
	if (!json_reader(result))
		return 0;

	// Return number of bytes processed or zero if the query should be cancelled
	return (result->cancelquery ? 0 : contentsize);
}

typedef void (*split_fn)(const char *, size_t, void *);

void split(const char *str, char sep, split_fn fun, void *data)
{
	unsigned int start = 0, stop;
	for (stop = 0; str[stop]; stop++)
	{
		if (str[stop] == sep)
		{
			fun(str + start, stop - start, data);
			start = stop + 1;
		}
	}
	fun(str + start, stop - start, data);
}


size_t findinstring(const char *str, char sep)
{
	for (size_t idx = 0; idx < strlen(str); idx++)
	{
		if (str[idx] == sep)
		{
			return idx;
		}
	}
	return 0;
}

static size_t header_callback(char *buffer, size_t size,
							  size_t nitems, void *userdata)
{
	size_t length = nitems * size;
	// header set is: "X-Presto-Added-Prepare: qryname=select+*+from*...
	if (length > 26 && strncmp("X-Presto-Added-Prepare", buffer, 22) == 0)
	{
		printf("can work with prepared statement: >%.*s< items: %li size: %li \n", (int)length, buffer, nitems, size);
		PRESTOCLIENT_RESULT *result = (PRESTOCLIENT_RESULT *)userdata;
		size_t cidx = findinstring(buffer, '=');
		if (cidx > 25)
		{
			size_t len = cidx - 24;
			result->prepared_stmt_name = (char *)malloc(len + 1);
			memcpy(result->prepared_stmt_name, &buffer[24], len);
			result->prepared_stmt_name[len] = '\0';
		}
		result->prepared_stmt_hdr = (char *)malloc(length - 25);
		memcpy(result->prepared_stmt_hdr, &buffer[24], length - 26);
		result->prepared_stmt_hdr[length - 26] = '\0';
	}
	return length;
}

// Send a http request to the Presto server. in_uri is emptied
static unsigned int openuri(enum E_HTTP_REQUEST_TYPES in_request_type,
							CURL *hcurl,
							const char *in_baseurl,
							char **in_uri,
							const char *in_body,
							const char *in_catalog,
							const char *in_schema,
							const char *in_useragent,
							const char *in_user,
							const char *in_timezone,
							const char *in_language,
							const unsigned long *in_buffersize,
							PRESTOCLIENT_RESULT *result)
{
	CURLcode curlstatus;
	char *uasource, *query_url, *full_url, port[32];
	struct curl_slist *headers;
	bool retry;
	unsigned int retrycount, length;
	long http_code, expected_http_code, expected_http_code_busy;

	uasource = PRESTOCLIENT_SOURCE;
	query_url = PRESTOCLIENT_QUERY_URL;
	headers = NULL;
	expected_http_code_busy = PRESTOCLIENT_CURL_EXPECT_HTTP_BUSY;

	// Check parameters
	if (!hcurl ||
		!in_useragent ||
		!in_user ||
		(in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_POST && (!in_baseurl || !in_body || !in_catalog || !in_schema || !in_buffersize || !result)) ||
		(in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_GET && (!in_uri || !*in_uri || !result)) ||
		(in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_DELETE && (!in_uri || !*in_uri)))
	{
		result->errorcode = PRESTOCLIENT_RESULT_BAD_REQUEST_DATA;
		return result->errorcode;
	}

	// Set up curl error buffer
	if (!result->curl_error_buffer)
	{
		result->curl_error_buffer = (char *)calloc(CURL_ERROR_SIZE, sizeof(char));
		// Not a fatal error if alloc didn't succeed
	}

	if (result->curl_error_buffer)
	{
		result->curl_error_buffer[0] = 0;
		curl_easy_setopt(hcurl, CURLOPT_ERRORBUFFER, result->curl_error_buffer);
	}

	// set verbose output on curl
	if (result->client->trace_http)
		curl_easy_setopt(hcurl, CURLOPT_VERBOSE, 1L);

	// URL
	if (in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_POST)
	{
		// Url
		length = (strlen(query_url) + strlen(in_baseurl)) * sizeof(char);
		full_url = (char *)malloc(length + 1);
		if (!full_url)
			exit(1);

		strcpy(full_url, in_baseurl);
		strcat(full_url, query_url);
		curl_easy_setopt(hcurl, CURLOPT_URL, full_url);
		free(full_url);
	}
	else
	{
		curl_easy_setopt(hcurl, CURLOPT_URL, *in_uri);
		*in_uri[0] = 0;
	}

	// CURL options
	curl_easy_setopt(hcurl, CURLOPT_CONNECTTIMEOUT_MS, (long)PRESTOCLIENT_URLTIMEOUT);

	switch (in_request_type)
	{
	case PRESTOCLIENT_HTTP_REQUEST_TYPE_POST:
	{
		expected_http_code = PRESTOCLIENT_CURL_EXPECT_HTTP_GET_POST;
		curl_easy_setopt(hcurl, CURLOPT_POST, (long)1);
		curl_easy_setopt(hcurl, CURLOPT_BUFFERSIZE, (long)(in_buffersize - 1));
		break;
	}

	case PRESTOCLIENT_HTTP_REQUEST_TYPE_GET:
	{
		expected_http_code = PRESTOCLIENT_CURL_EXPECT_HTTP_GET_POST;
		curl_easy_setopt(hcurl, CURLOPT_HTTPGET, (long)1);
		curl_easy_setopt(hcurl, CURLOPT_BUFFERSIZE, (long)(in_buffersize - 1));
		break;
	}

	case PRESTOCLIENT_HTTP_REQUEST_TYPE_DELETE:
	{
		expected_http_code = PRESTOCLIENT_CURL_EXPECT_HTTP_DELETE;
		curl_easy_setopt(hcurl, CURLOPT_HTTPGET, (long)1);
		curl_easy_setopt(hcurl, CURLOPT_CUSTOMREQUEST, "DELETE");
		break;
	}
	}

	// HTTP Headers
	if (in_user)
		add_headerline(&headers, "X-Presto-User", (char *)in_user);
	if (in_catalog)
		add_headerline(&headers, "X-Presto-Catalog", (char *)in_catalog);
	add_headerline(&headers, "X-Presto-Source", uasource);
	if (in_schema)
		add_headerline(&headers, "X-Presto-Schema", (char *)in_schema);
	if (in_timezone)
		add_headerline(&headers, "X-Presto-Time-Zone", (char *)in_timezone);
	if (in_language)
		add_headerline(&headers, "X-Presto-Language", (char *)in_language);

	if (in_useragent)
		add_headerline(&headers, "User-Agent", (char *)in_useragent);

	if (result->prepared_stmt_hdr && strlen(result->prepared_stmt_hdr) > 0)
	{
		add_headerline(&headers, "X-Presto-Prepared-Statement", result->prepared_stmt_hdr);
	}

	// but wait there is more...
	/*
	 public static final String PRESTO_TRACE_TOKEN = "X-Presto-Trace-Token";
    public static final String PRESTO_SESSION = "X-Presto-Session";
    public static final String PRESTO_SET_CATALOG = "X-Presto-Set-Catalog";
    public static final String PRESTO_SET_SCHEMA = "X-Presto-Set-Schema";
    public static final String PRESTO_SET_SESSION = "X-Presto-Set-Session";
    public static final String PRESTO_CLEAR_SESSION = "X-Presto-Clear-Session";
    public static final String PRESTO_SET_ROLE = "X-Presto-Set-Role";
    public static final String PRESTO_ROLE = "X-Presto-Role";
    public static final String PRESTO_PREPARED_STATEMENT = "X-Presto-Prepared-Statement";
    public static final String PRESTO_ADDED_PREPARE = "X-Presto-Added-Prepare";
    public static final String PRESTO_DEALLOCATED_PREPARE = "X-Presto-Deallocated-Prepare";
    public static final String PRESTO_TRANSACTION_ID = "X-Presto-Transaction-Id";
    public static final String PRESTO_STARTED_TRANSACTION_ID = "X-Presto-Started-Transaction-Id";
    public static final String PRESTO_CLEAR_TRANSACTION_ID = "X-Presto-Clear-Transaction-Id";
    public static final String PRESTO_CLIENT_INFO = "X-Presto-Client-Info";
    public static final String PRESTO_CLIENT_TAGS = "X-Presto-Client-Tags";
    public static final String PRESTO_RESOURCE_ESTIMATE = "X-Presto-Resource-Estimate";
    public static final String PRESTO_EXTRA_CREDENTIAL = "X-Presto-Extra-Credential";

    public static final String PRESTO_CURRENT_STATE = "X-Presto-Current-State";
    public static final String PRESTO_MAX_WAIT = "X-Presto-Max-Wait";
    public static final String PRESTO_MAX_SIZE = "X-Presto-Max-Size";
    public static final String PRESTO_TASK_INSTANCE_ID = "X-Presto-Task-Instance-Id";
    public static final String PRESTO_PAGE_TOKEN = "X-Presto-Page-Sequence-Id";
    public static final String PRESTO_PAGE_NEXT_TOKEN = "X-Presto-Page-End-Sequence-Id";
    public static final String PRESTO_BUFFER_COMPLETE = "X-Presto-Buffer-Complete";
	*/

	// Set Writeback function and request body
	if (in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_POST || in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_GET)
	{
		curl_easy_setopt(hcurl, CURLOPT_WRITEFUNCTION, WriteCallback);
		curl_easy_setopt(hcurl, CURLOPT_WRITEDATA, (void *)result);
	}

	// Set request body
	if (in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_POST)
	{
		printf("query sent is: %s\n", in_body);
		curl_easy_setopt(hcurl, CURLOPT_POSTFIELDS, in_body);
		curl_easy_setopt(hcurl, CURLOPT_POSTFIELDSIZE, (long)strlen(in_body));
	}

	// Set header
	curl_easy_setopt(hcurl, CURLOPT_HTTPHEADER, headers);

	// receive headers via callback to dispatch header actions
	curl_easy_setopt(hcurl, CURLOPT_HEADERFUNCTION, header_callback);
	// give header callback access to the result statement handle
	curl_easy_setopt(hcurl, CURLOPT_HEADERDATA, result);

	// Execute CURL request, retry when server is busy
	result->errorcode = PRESTOCLIENT_RESULT_OK;
	retry = true;
	retrycount = 0;

	while (retry)
	{
		retrycount++;

		// Execute request
		curlstatus = curl_easy_perform(hcurl);

		if (curlstatus == CURLE_OK)
		{
			// Get return code
			http_code = 0;
			curl_easy_getinfo(hcurl, CURLINFO_RESPONSE_CODE, &http_code);

			if (http_code == expected_http_code)
			{
				retry = false;
			}
			else if (http_code == expected_http_code_busy)
			{
				// Server is busy
				util_sleep(PRESTOCLIENT_RETRYWAITTIMEMSEC * retrycount);
			}
			else
			{
				result->errorcode = PRESTOCLIENT_RESULT_SERVER_ERROR;
				// Re-using port buffer
				sprintf(port, "Http-code: %d", (unsigned int)http_code);
				alloc_copy(&result->curl_error_buffer, port);
				retry = false;
			}

			if (retry && retrycount > PRESTOCLIENT_MAXIMUMRETRIES)
			{
				result->errorcode = PRESTOCLIENT_RESULT_MAX_RETRIES_REACHED;
				retry = false;
			}
		}
		else
		{
			result->errorcode = PRESTOCLIENT_RESULT_CURL_ERROR;
			retry = false;
		}
	}

	// Cleanup
	curl_slist_free_all(headers);

	return result->errorcode;
}

// Send a cancel request to the Prestoserver
static void cancel(PRESTOCLIENT_RESULT *result)
{
	if (result->lastcanceluri)
	{
		// Not checking returncode since we're cancelling the request and don't care if it succeeded or not
		openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_DELETE,
				result->hcurl,
				NULL,
				&result->lastcanceluri,
				NULL,
				NULL,
				NULL,
				result->client->useragent,
				result->client->user,
				NULL,
				NULL,
				NULL,
				result);
	}
}

// Fetch the next uri from the prestoserver, handle the response and determine if we're done or not
static bool prestoclient_queryisrunning(PRESTOCLIENT_RESULT *result)
{
	PRESTOCLIENT *prestoclient;

	if (!result)
		return false;

	if (result->cancelquery)
	{
		cancel(result);
		return false;
	}

	// Do we have a url ?
	if (!result->lastnexturi || strlen(result->lastnexturi) == 0)
		return false;

	prestoclient = result->client;
	if (!prestoclient)
		return false;

	// Start request. This will execute callbackfunction when data is recieved
	if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_GET,
				result->hcurl,
				NULL,
				&result->lastnexturi,
				NULL,
				NULL,
				NULL,
				prestoclient->useragent,
				prestoclient->user,
				NULL,
				NULL,
				NULL,
				result) == PRESTOCLIENT_RESULT_OK)
	{
		// Determine client state
		if (result->lastnexturi && strlen(result->lastnexturi) > 0)
			result->clientstatus = PRESTOCLIENT_STATUS_RUNNING;
		else
		{
			if (result->lasterrormessage && strlen(result->lasterrormessage) > 0)
				result->clientstatus = PRESTOCLIENT_STATUS_FAILED;
			else
				result->clientstatus = PRESTOCLIENT_STATUS_SUCCEEDED;
		}

		// Update columninfoavailable flag
		if (result->columncount > 0 && !result->columninfoavailable)
			result->columninfoavailable = true;

		// Call print header callback function
		if (!result->columninfoprinted && result->columninfoavailable)
		{
			result->columninfoprinted = true;

			if (result->describe_callback_function)
				result->describe_callback_function(result->client_object, (void *)result);
		}

		// Clear lexer data for next run
		json_reset_lexer(result->lexer);
	}
	else
	{
		return false;
	}

	if (!result->lastnexturi || strlen(result->lastnexturi) == 0)
		return false;

	return true;
}

// Start fetching packets until we're done. Wait for a specified interval between requests
static void prestoclient_waituntilfinished(PRESTOCLIENT_RESULT *result)
{
	while (prestoclient_queryisrunning(result))
	{
		// Once there is data use the short wait interval
		if (result->dataavailable)
		{
			util_sleep(PRESTOCLIENT_RETRIEVEWAITTIMEMSEC);
		}
		else
		{
			util_sleep(PRESTOCLIENT_UPDATEWAITTIMEMSEC);
		}
	}
}

/* --- Public functions ----------------------------------------------------------------------------------------------- */
char *prestoclient_getversion()
{
	return PRESTOCLIENT_VERSION;
}

PRESTOCLIENT *prestoclient_init(const char *in_protocol, const char *in_server, const unsigned int *in_port,
								const char *in_catalog, const char *in_user, const char *in_pwd,
								const char *in_timezone, const char *in_language, bool trace_http)
{
	PRESTOCLIENT *client = NULL;
	char *uasource, *uaversion, *defaultcatalog;
	unsigned int length;

	uasource = PRESTOCLIENT_SOURCE;
	uaversion = PRESTOCLIENT_VERSION;
	defaultcatalog = PRESTOCLIENT_DEFAULT_CATALOG;

	(void)in_pwd; // Get rid of compiler warning

	if (in_server && strlen(in_server) > 0)
	{
		client = new_prestoclient(trace_http);
		length = (strlen(uasource) + strlen(uaversion) + 2) * sizeof(char);
		client->useragent = (char *)malloc(length);
		if (!client->useragent)
			exit(1);

		strcpy(client->useragent, uasource);
		strcat(client->useragent, "/");
		strcat(client->useragent, uaversion);

		// TODO: check if in_server contains a port ?
		alloc_copy(&client->server, in_server);

		if (in_protocol && strlen(in_protocol) > 0)
		{
			alloc_copy(&client->protocol, in_protocol);
		}
		else
		{
			client->protocol = "http";
		}

		if (in_port && *in_port > 0 && *in_port <= 65535)
			client->port = *in_port;

		// assemble base url...
		char *port = (char *)malloc(32);
		sprintf(port, "%i", client->port);

		size_t length = (strlen(in_server) + strlen(in_protocol) + strlen(port) + 5) * sizeof(char);
		char *base_url = (char *)malloc(length + 1);

		if (!base_url)
			exit(1);

		strcpy(base_url, client->protocol);
		strcat(base_url, "://");
		strcat(base_url, client->server);
		strcat(base_url, ":");
		strcat(base_url, port);
		strcat(base_url, "/");
		base_url[length] = '\0';

		client->baseurl = base_url;
		free(port);

		alloc_copy(&client->catalog, in_catalog ? in_catalog : defaultcatalog);

		if (in_user && strlen(in_user) > 0)
		{
			alloc_copy(&client->user, in_user);
		}
		else
		{
			client->user = get_username();
		}

		if (in_timezone)
			alloc_copy(&client->timezone, in_timezone);

		if (in_language)
			alloc_copy(&client->language, in_language);
	}

	return client;
}

void prestoclient_close(PRESTOCLIENT *prestoclient)
{
	if (!prestoclient)
		return;

	if (prestoclient->baseurl)
		free(prestoclient->baseurl);

	if (prestoclient->useragent)
		free(prestoclient->useragent);

	if (prestoclient->protocol)
		free(prestoclient->protocol);

	if (prestoclient->server)
		free(prestoclient->server);

	if (prestoclient->catalog)
		free(prestoclient->catalog);

	if (prestoclient->user)
		free(prestoclient->user);

	if (prestoclient->timezone)
		free(prestoclient->timezone);

	if (prestoclient->language)
		free(prestoclient->language);

	if (prestoclient->results)
	{
		for (size_t i = 0; i < prestoclient->active_results; i++)
			if (prestoclient->results[i])
				delete_prestoresult(prestoclient->results[i]);

		free(prestoclient->results);
	}

	free(prestoclient);
	prestoclient = NULL;
}

struct MemoryStruct {
  char *memory;
  size_t size;
};

static size_t read_to_mem(void *contents, size_t size, size_t nmemb, void *userp)
{
  size_t realsize = size * nmemb;
  struct MemoryStruct *mem = (struct MemoryStruct *)userp;
 
  char *ptr = (char*)realloc(mem->memory, mem->size + realsize + 1);
  if(ptr == NULL) {
    /* out of memory! */ 
    printf("not enough memory (realloc returned NULL)\n");
    return 0;
  }
 
  mem->memory = ptr;
  memcpy(&(mem->memory[mem->size]), contents, realsize);
  mem->size += realsize;
  mem->memory[mem->size] = 0;
 
  return realsize;
}
 

char *make_url(const char *base_url, const char *url_part)
{
	size_t lng = strlen(base_url) + strlen(url_part);
	char *url = (char *)malloc(lng + 1);
	strcpy(url, base_url);
	strcat(url, url_part);
	url[lng] = '\0';
	return url;
}

char* prestoclient_serverinfo(PRESTOCLIENT *prestoclient)
{
	if (!prestoclient)
	{
		return NULL;
	}

	CURLcode res;
	struct MemoryStruct ret; 
  	ret.memory = (char*)malloc(1); 
	ret.memory[0]='\0';
  	ret.size = 0; 

	CURL *curl = curl_easy_init();
	if (!curl)
	{
		return NULL;
	}

	/* from here, always cleanup do not return early */
	char *url = make_url(prestoclient->baseurl, PRESTOCLIENT_INFO_URL);
	curl_easy_setopt(curl, CURLOPT_URL, url);
	free(url);

	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&ret);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, read_to_mem);

	res = curl_easy_perform(curl);
	if (res != CURLE_OK)
	{
		fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
		free(ret.memory);
		ret.memory = NULL;
	}
	
	curl_easy_cleanup(curl);
	return ret.memory;	
}

PRESTOCLIENT_RESULT *prestoclient_query(PRESTOCLIENT *prestoclient, const char *in_sql_statement, const char *in_schema,
										void (*in_write_callback_function)(void *, void *),
										void (*in_describe_callback_function)(void *, void *),
										void *in_client_object)
{
	if (!prestoclient)
	{
		return NULL;
	}

	PRESTOCLIENT_RESULT *result = NULL;
	char *defschema;
	unsigned long buffersize;

	defschema = PRESTOCLIENT_DEFAULT_SCHEMA;
	buffersize = PRESTOCLIENT_CURL_BUFFERSIZE;

	if (prestoclient && in_sql_statement && strlen(in_sql_statement) > 0)
	{
		// Prepare the result set
		result = new_prestoresult();

		result->client = prestoclient;

		result->write_callback_function = in_write_callback_function;

		result->describe_callback_function = in_describe_callback_function;

		result->client_object = in_client_object;

		result->hcurl = curl_easy_init();

		if (!result->hcurl)
		{
			delete_prestoresult(result);
			return NULL;
		}

		// Reserve memory for curl data buffer
		result->lastresponse = (char *)malloc(buffersize + 1); //  * sizeof(char) ?
		if (!result->lastresponse)
			exit(1);
		memset(result->lastresponse, 0, buffersize); //  * sizeof(char) ?
		result->lastresponsebuffersize = buffersize;

		// Add resultset to the client
		add_result(result);

		// Create request
		if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
					result->hcurl,
					prestoclient->baseurl,
					NULL,
					in_sql_statement,
					prestoclient->catalog,
					in_schema ? in_schema : defschema,
					prestoclient->useragent,
					prestoclient->user,
					prestoclient->timezone,
					prestoclient->language,
					&buffersize,
					result) == PRESTOCLIENT_RESULT_OK)
		{
			// Start polling server for data
			prestoclient_waituntilfinished(result);
		}
	}

	return result;
}

PRESTOCLIENT_RESULT *prestoclient_prepare(PRESTOCLIENT *prestoclient, const char *in_sql_statement, const char *in_schema)
{
	PRESTOCLIENT_RESULT *result = NULL;
	char *defschema;
	unsigned long buffersize;

	defschema = PRESTOCLIENT_DEFAULT_SCHEMA;
	buffersize = PRESTOCLIENT_CURL_BUFFERSIZE;

	size_t sql_len = strlen(in_sql_statement);

	if (prestoclient && in_sql_statement && sql_len > 0)
	{
		// Prepare the result set
		result = new_prestoresult();

		result->client = prestoclient;
		result->describe_callback_function = NULL;
		result->write_callback_function = NULL;

		result->hcurl = curl_easy_init();

		if (!result->hcurl)
		{
			delete_prestoresult(result);
			return 0;
		}

		// Add resultset to the client 
		add_result(result);
		char *prep_name = (char *)malloc(sizeof(long));
		sprintf(prep_name, "qry%li", result->client->active_results);

		char *prepqry = (char *)malloc(sql_len + 40);
		sprintf(prepqry, "PREPARE %s FROM %s", prep_name, in_sql_statement);
		free(prep_name);

		// Reserve memory for curl data buffer
		result->lastresponse = (char *)malloc(buffersize + 1); //  * sizeof(char) ?
		if (!result->lastresponse)
			exit(1);

		memset(result->lastresponse, 0, buffersize); //  * sizeof(char) ?
		result->lastresponsebuffersize = buffersize;

		// Create request
		if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
					result->hcurl,
					prestoclient->baseurl,
					NULL,
					prepqry,
					prestoclient->catalog,
					in_schema ? in_schema : defschema,
					prestoclient->useragent,
					prestoclient->user,
					prestoclient->timezone,
					prestoclient->language,
					&buffersize,
					result) == PRESTOCLIENT_RESULT_OK)
		{
			// Start polling server for data
			prestoclient_waituntilfinished(result);
		}
		else
		{
			remove_result(result);
			delete_prestoresult(result);
			result = NULL;
		}
		free(prepqry);
	}
	return result;
}

PRESTOCLIENT_RESULT *prestoclient_execute(PRESTOCLIENT *prestoclient, PRESTOCLIENT_RESULT *prepared_result,
										  void (*in_write_callback_function)(void *, void *),
										  void (*in_describe_callback_function)(void *, void *),
										  void *in_client_object)
{
	// PRESTOCLIENT_RESULT* result;
	char *defschema;
	unsigned long buffersize;

	defschema = PRESTOCLIENT_DEFAULT_SCHEMA;
	buffersize = PRESTOCLIENT_CURL_BUFFERSIZE;

	if (prestoclient && prepared_result && prepared_result->prepared_stmt_name && strlen(prepared_result->prepared_stmt_name) > 0)
	{
		// handle was used before so reset and reuse
		reset_prestoresult(prepared_result);

		if (in_write_callback_function){
			prepared_result->write_callback_function = in_write_callback_function; 
		} else {
			prepared_result->write_callback_function = &write_callback_buffer; 
		}
		prepared_result->describe_callback_function = in_describe_callback_function;
		prepared_result->client_object = in_client_object;

		char *in_sql_statement = (char *)malloc(strlen(prepared_result->prepared_stmt_name) + 15);
		sprintf(in_sql_statement, "EXECUTE %s ", prepared_result->prepared_stmt_name);

		// Create request
		if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
					prepared_result->hcurl,
					prestoclient->baseurl,
					NULL,
					in_sql_statement,
					prestoclient->catalog,
					defschema,
					prestoclient->useragent,
					prestoclient->user,
					prestoclient->timezone,
					prestoclient->language,
					&buffersize,
					prepared_result) == PRESTOCLIENT_RESULT_OK)
		{
			// Start polling server for data
			prestoclient_waituntilfinished(prepared_result);
		} else {
			remove_result(prepared_result);
			delete_prestoresult(prepared_result);
			prepared_result = NULL;
		}
		free(in_sql_statement);
	} else {
		remove_result(prepared_result);
		delete_prestoresult(prepared_result);
		prepared_result = NULL;
	}
	return prepared_result;
}


void prestoclient_deleteresult(PRESTOCLIENT *prestoclient, PRESTOCLIENT_RESULT *prepared_result)
{
	if (prestoclient && prepared_result)
	{
		delete_prestoresult(prepared_result);
		prepared_result = NULL;
	}
}

unsigned int prestoclient_getstatus(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return PRESTOCLIENT_STATUS_NONE;

	return result->clientstatus;
}

char *prestoclient_getlastserverstate(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return NULL;

	return (result->laststate ? result->laststate : (char*)"");
}

char *prestoclient_getlastservererror(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return NULL;

	return result->lasterrormessage;
}

size_t prestoclient_getcolumncount(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return false;

	return result->columncount;
}

char *prestoclient_getcolumnname(PRESTOCLIENT_RESULT *result, const size_t columnindex)
{
	if (!result)
		return NULL;

	if (columnindex >= result->columncount)
		return NULL;

	return result->columns[columnindex]->name;
}

unsigned int prestoclient_getcolumntype(PRESTOCLIENT_RESULT *result, const size_t columnindex)
{
	if (!result)
		return PRESTOCLIENT_TYPE_UNDEFINED;

	if (columnindex >= result->columncount)
		return PRESTOCLIENT_TYPE_UNDEFINED;

	return result->columns[columnindex]->type;
}

const char *prestoclient_getcolumntypedescription(PRESTOCLIENT_RESULT *result, const size_t columnindex)
{
	if (!result)
		return NULL;

	if (columnindex >= result->columncount)
		return NULL;


	// Please maintain typenames in lockstep to the enum above
	switch (result->columns[columnindex]->type) {
	case PRESTOCLIENT_TYPE_UNDEFINED: return "PRESTO_TYPE_UNDEFINED";
	case PRESTOCLIENT_TYPE_VARCHAR: return "PRESTO_TYPE_VARCHAR";
	case PRESTOCLIENT_TYPE_CHAR: return "PRESTO_TYPE_CHAR";
	case PRESTOCLIENT_TYPE_VARBINARY: return "PRESTO_TYPE_VARBINARY";
	case PRESTOCLIENT_TYPE_TINYINT: return "PRESTO_TYPE_TINYINT";
	case PRESTOCLIENT_TYPE_SMALLINT: return "PRESTO_TYPE_SMALLINT";
	case PRESTOCLIENT_TYPE_INTEGER: return "PRESTO_TYPE_INTEGER";
	case PRESTOCLIENT_TYPE_BIGINT: return "PRESTO_TYPE_BIGINT";
	case PRESTOCLIENT_TYPE_BOOLEAN: return "PRESTO_TYPE_BOOLEAN";
	case PRESTOCLIENT_TYPE_REAL: return "PRESTO_TYPE_REAL";
	case PRESTOCLIENT_TYPE_DOUBLE: return "PRESTO_TYPE_DOUBLE";
	case PRESTOCLIENT_TYPE_DECIMAL: return "PRESTO_TYPE_DECIMAL";
	case PRESTOCLIENT_TYPE_DATE: return "PRESTO_TYPE_DATE";
	case PRESTOCLIENT_TYPE_TIME: return "PRESTO_TYPE_TIME";
	case PRESTOCLIENT_TYPE_TIME_WITH_TIME_ZONE: return "PRESTO_TYPE_TIME_WITH_TIME_ZONE";
	case PRESTOCLIENT_TYPE_TIMESTAMP: return "PRESTO_TYPE_TIMESTAMP";
	case PRESTOCLIENT_TYPE_TIMESTAMP_WITH_TIME_ZONE: return "PRESTO_TYPE_TIMESTAMP_WITH_TIME_ZONE";
	case PRESTOCLIENT_TYPE_INTERVAL_YEAR_TO_MONTH: return "PRESTO_TYPE_INTERVAL_YEAR_TO_MONTH";
	case PRESTOCLIENT_TYPE_INTERVAL_DAY_TO_SECOND: return "PRESTO_TYPE_INTERVAL_DAY_TO_SECOND";
	case PRESTOCLIENT_TYPE_ARRAY: return "PRESTO_TYPE_ARRAY";
	case PRESTOCLIENT_TYPE_MAP: return "PRESTO_TYPE_MAP";
	case PRESTOCLIENT_TYPE_JSON: return "PRESTO_TYPE_JSON";
	default: return "PRESTO_TYPE_UNDEFINED";
	}	
}

char *prestoclient_getcolumndata(PRESTOCLIENT_RESULT *result, const size_t columnindex)
{
	if (!result || !result->columns)
		return NULL;

	if (columnindex >= result->columncount)
		return NULL;

	return result->columns[columnindex]->data;
}

int prestoclient_getnullcolumnvalue(PRESTOCLIENT_RESULT *result, const size_t columnindex)
{
	if (!result || !result->columns)
		return true;

	if (columnindex >= result->columncount)
		return true;

	return result->columns[columnindex]->dataisnull ? true : false;
}

void prestoclient_cancelquery(PRESTOCLIENT_RESULT *result)
{
	if (result)
		result->cancelquery = true;
}

char *prestoclient_getlastclienterror(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return NULL;

	switch (result->errorcode)
	{
	case PRESTOCLIENT_RESULT_OK:
		return NULL;
	case PRESTOCLIENT_RESULT_BAD_REQUEST_DATA:
		return "Not all parameters to start request are available";
	case PRESTOCLIENT_RESULT_SERVER_ERROR:
		return "Server returned error";
	case PRESTOCLIENT_RESULT_MAX_RETRIES_REACHED:
		return "Server is busy";
	case PRESTOCLIENT_RESULT_CURL_ERROR:
		return "CURL error occurred";
	case PRESTOCLIENT_RESULT_PARSE_JSON_ERROR:
		return "Error parsing returned json object";
	default:
		return "Invalid errorcode";
	}
}

char *prestoclient_getlastcurlerror(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return NULL;

	if (!result->curl_error_buffer)
		return NULL;

	if (strlen(result->curl_error_buffer) == 0)
		return NULL;

	return (result->curl_error_buffer ? result->curl_error_buffer : (char*)"");
}