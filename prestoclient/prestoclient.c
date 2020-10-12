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
static void write_callback_buffer(void *in_userdata, void *in_result);

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

PRESTOCLIENT_COLUMN *new_prestocolumn()
{
	PRESTOCLIENT_COLUMN *field = (PRESTOCLIENT_COLUMN *)malloc(sizeof(PRESTOCLIENT_COLUMN));

	if (!field)
		exit(1);

	field->name = NULL;
	field->catalog = NULL;
	field->schema = NULL;
	field->table = NULL;
	field->type = PRESTOCLIENT_TYPE_UNDEFINED;
	field->datasize = 1024 * sizeof(char);
	field->data = (char *)malloc(field->datasize + 1);
	field->dataisnull = false;

	if (!field->data)
		exit(1);

	return field;
}

static void delete_prestocolumn(PRESTOCLIENT_COLUMN *field)
{
	if (!field)
		return;

	if (field->name)
		free(field->name);

	if (field->catalog)
		free(field->catalog);

	if (field->schema)
		free(field->schema);

	if (field->table)
		free(field->table);

	if (field->data)
		free(field->data);

	free(field);
}

PRESTOCLIENT_TABLEBUFFER *new_tablebuffer(size_t initialsize)
{
	PRESTOCLIENT_TABLEBUFFER *tab = (PRESTOCLIENT_TABLEBUFFER *)malloc(sizeof(PRESTOCLIENT_TABLEBUFFER));
	tab->nalloc = initialsize;
	tab->rowbuff = (char **)malloc(tab->nalloc * sizeof(char*));
	tab->nrow = 0;
	tab->ncol = 0;
	tab->ndata = 0;
	tab->rowidx = -1;
	return tab;
}

void grow_tablebuffer(PRESTOCLIENT_TABLEBUFFER *tab, size_t addsize)
{
	size_t newsz = tab->nalloc + addsize;	
	// printf("%li reallocate old %li\n", newsz, tab->nalloc);
	tab->rowbuff = (char **)realloc(tab->rowbuff,newsz* sizeof(char*));
	// printf("success reallocate %li\n", newsz);
	tab->nalloc = newsz;
}

static void delete_tablebuffer(PRESTOCLIENT_TABLEBUFFER *tab)
{
	if (!tab)
		return;

	if (tab->rowbuff)
	{
		for (size_t zz = 0; zz < (size_t)tab->ndata; zz++)
		{
			if (tab->rowbuff[zz])
			{
				free(tab->rowbuff[zz]);
			}
		}
		free(tab->rowbuff);
	}

	free(tab);
}

static void tablebuffer_print(PRESTOCLIENT_TABLEBUFFER *tab)
{
	if (!tab)
		return;

	if (tab->ndata <= 0 )
		return;

	for (size_t zz = 0; zz < (size_t)tab->ndata; zz++)
	{
		if (tab->rowbuff[zz])
			printf("%s\t", tab->rowbuff[zz]);

		if ((zz + 1) % tab->ncol == 0)
		{
			printf("\n");
		}
	}
}

static PRESTOCLIENT_RESULT *new_prestoresult()
{
	PRESTOCLIENT_RESULT *result = (PRESTOCLIENT_RESULT *)malloc(sizeof(PRESTOCLIENT_RESULT));

	if (!result)
		return NULL;

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
	result->query = NULL;
	result->prepared_stmt_hdr = NULL;
	result->prepared_stmt_name = NULL;
	result->columns = NULL;
	result->columncount = 0;
	result->parameters = NULL;
	result->parametercount = 0;
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

	if (result->query)
		free(result->query);

	if (result->prepared_stmt_hdr)
		free(result->prepared_stmt_hdr);

	if (result->prepared_stmt_name)
		free(result->prepared_stmt_name);

	if (result->columns)
	{
		for (size_t i = 0; i < result->columncount; i++)
		{
			if (result->columns[i])
				delete_prestocolumn(result->columns[i]);
		}
		free(result->columns);
		result->columns = NULL;
	}

	if (result->parameters)
	{
		for (size_t i = 0; i < result->parametercount; i++)
		{
			if (result->parameters[i])
				delete_prestocolumn(result->parameters[i]);
		}
		free(result->parameters);
		result->parameters = NULL;
	}

	if (result->tablebuff)
	{
		delete_tablebuffer(result->tablebuff);
		result->tablebuff = NULL;
	}

	free(result);
}


static PRESTOCLIENT_RESULT *new_prestoresult_readied(PRESTOCLIENT *prestoclient, size_t buffersize) {
	int rc = PRESTO_OK;
	PRESTOCLIENT_RESULT * res = new_prestoresult();
	if (!res) 
		return NULL;


	res->client = prestoclient;
	res->describe_callback_function = NULL;	
	res->write_callback_function = &write_callback_buffer;
	res->client_object = NULL;
	res->hcurl = curl_easy_init();
	if (!res->hcurl)
	{		
		rc = PRESTO_NO_MEMORY;
		goto exit;
	}

	// Reserve memory for curl data buffer
	res->lastresponse = (char *)malloc(sizeof(char) * (buffersize + 1)); 
	if (!res->lastresponse) {	
		rc = PRESTO_NO_MEMORY;	
		goto exit;
	}

	// init buffer
	memset(res->lastresponse, 0, buffersize);
	res->lastresponsebuffersize = buffersize;

exit:
	if (rc != PRESTO_OK) {
		if (res) {
			delete_prestoresult(res);
		}
		res = NULL;
	}
	return res;
}

static void reset_prestoresult(PRESTOCLIENT_RESULT *result)
{
	if (!result)
		return;

	json_delete_parser(result->json);
	result->json = NULL;
	json_delete_lexer(result->lexer);
	result->lexer = NULL;

	if (result->columns)
	{
		for (size_t i = 0; i < result->columncount; i++)
		{
			if (result->columns[i])
				delete_prestocolumn(result->columns[i]);
		}
		free(result->columns);
		result->columns = NULL;
	}

	if (result->tablebuff)
	{
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
	client->schema = NULL;
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
	// userdata is null in this call so we work exclusively with the result
	PRESTOCLIENT_RESULT *result = (PRESTOCLIENT_RESULT *)in_result;
	size_t columncount = prestoclient_getcolumncount(result);

	size_t growby = 10;

	if (!result->tablebuff)
	{
		result->tablebuff = new_tablebuffer(columncount * growby);
		result->tablebuff->ncol = columncount;
	}

	if (result->tablebuff->nalloc <= (columncount + result->tablebuff->ndata))
	{
		grow_tablebuffer(result->tablebuff, columncount * growby);
	}

	result->tablebuff->nrow++;

	for (size_t idx = 0; idx < columncount; idx++)
	{
		char *flddata = prestoclient_getcolumndata(result, idx);
		result->tablebuff->rowbuff[result->tablebuff->ndata] = (char *)malloc(sizeof(char)* (strlen(flddata) + 1) );
		strcpy((char *)result->tablebuff->rowbuff[result->tablebuff->ndata], (char *)flddata);
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

static void remove_result(PRESTOCLIENT_RESULT *result)
{
	PRESTOCLIENT *client;

	if (!result)
		return;

	if (!result->client)
		return;

	client = result->client;

	if (client->active_results == 0)
		return;

	bool found = false;
	for (size_t idx = 0; idx < client->active_results; idx++)
	{
		if (client->results[idx] == result)
		{
			found = true;
		}
		if (found && (idx + 1) < client->active_results)
		{
			client->results[idx] = client->results[idx + 1];
		}
	}
	client->active_results--;

	if (client->active_results == 0)
	{
		free(client->results);
		client->results = NULL;
	}
	else
	{
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

	if (result->client->trace_http)
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

static void freenil(char *ptr)
{
	if (ptr)
	{
		free(ptr);
		ptr = NULL;
	}
}

static size_t header_callback(char *buffer, size_t size,
							  size_t nitems, void *userdata)
{
	size_t length = nitems * size;
	PRESTOCLIENT_RESULT *result = (PRESTOCLIENT_RESULT *)userdata;
	// header set is: "<headername>: <headervalue>crlf
	// headers are separated by crlf and curl returns that suffix to us so we always subtract two chars at the end
	
	// 26 = strlen(header) + 4 (separator and suffix)
	if (length > 26 && strncmp("X-Presto-Added-Prepare", buffer, 22) == 0)
	{		
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
	else if (length > 28 && strncmp("X-Presto-Deallocated-Prepare", buffer, 28) == 0)
	{
		if (result->prepared_stmt_hdr)
		{
			free(result->prepared_stmt_hdr);
			result->prepared_stmt_hdr = NULL;
		}
		if (result->prepared_stmt_name)
		{
			free(result->prepared_stmt_name);
			result->prepared_stmt_name = NULL;
		}
	} else if (length > 20 && strncmp("X-Presto-Set-Catalog",buffer,20) == 0 ) {
		if (result->client->catalog) 
			free(result->client->catalog);
		result->client->catalog = (char*)malloc(length - 24 + 1);
		memcpy(result->client->catalog, &buffer[22], length - 24);
		result->client->catalog[length - 24] = '\0';
	} else if (length > 19 && strncmp("X-Presto-Set-Schema",buffer,19) == 0 ) {
		if (result->client->schema) 
			free(result->client->schema);
		result->client->schema = (char*)malloc(length - 23 + 1);
		memcpy(result->client->schema, &buffer[21], length - 23);
		result->client->schema[length - 23] = '\0';
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
		(in_request_type == PRESTOCLIENT_HTTP_REQUEST_TYPE_POST && (!in_baseurl || !in_body || !in_buffersize || !result)) ||
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
		if (result->client->trace_http)
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
				result->client->catalog,
				result->client->schema,
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
				prestoclient->catalog,
				prestoclient->schema,
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
								const char *in_catalog, const char* in_schema, const char *in_user, const char *in_pwd,
								const char *in_timezone, const char *in_language, bool trace_http)
{
	PRESTOCLIENT *client = NULL;
	char *uasource, *uaversion;
	unsigned int length;

	uasource = PRESTOCLIENT_SOURCE;
	uaversion = PRESTOCLIENT_VERSION;

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

		if (in_catalog)
			alloc_copy(&client->catalog, in_catalog);

		if (in_schema)
			alloc_copy(&client->schema, in_schema);

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

	if (prestoclient->schema)
		free(prestoclient->schema);

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

struct MemBuffer
{
	char *memory;
	size_t size;
};

static size_t read_to_mem(void *contents, size_t size, size_t nmemb, void *userp)
{
	size_t realsize = size * nmemb;
	struct MemBuffer *mem = (struct MemBuffer *)userp;

	char *ptr = (char *)realloc(mem->memory, mem->size + realsize + 1);
	if (ptr == NULL)
	{
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

char *prestoclient_serverinfo(PRESTOCLIENT *prestoclient)
{
	if (!prestoclient)
	{
		return NULL;
	}

	CURLcode res;
	struct MemBuffer ret;
	ret.memory = (char *)malloc(1);
	ret.memory[0] = '\0';
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

int prestoclient_query(PRESTOCLIENT *prestoclient, 
						PRESTOCLIENT_RESULT **result,
						const char *in_sql_statement,
						void (*in_write_callback_function)(void *, void *),
						void (*in_describe_callback_function)(void *, void *),
						void *in_client_object)
	{
	int rc = PRESTOCLIENT_RESULT_OK;
	PRESTOCLIENT_RESULT *ret = NULL;
	unsigned long buffersize;

	if (!prestoclient)
	{
		return PRESTO_BAD_REQUEST;
	}

	buffersize = PRESTOCLIENT_CURL_BUFFERSIZE;

	if (prestoclient && in_sql_statement && strlen(in_sql_statement) > 0)
	{
		// Prepare the result set
		ret = new_prestoresult();
		if (!ret) {
			rc = PRESTO_NO_MEMORY;
			goto exit;
		}

		ret->client = prestoclient;

		if (in_write_callback_function)
		{
			ret->write_callback_function = in_write_callback_function;
		}
		else
		{
			ret->write_callback_function = &write_callback_buffer;
		}

		ret->describe_callback_function = in_describe_callback_function;

		ret->client_object = in_client_object;

		ret->hcurl = curl_easy_init();
		if (!ret->hcurl) {			
			rc = PRESTO_NO_MEMORY;
			goto exit;
		}

		// Reserve memory for curl data buffer
		ret->lastresponse = (char *)malloc(buffersize + 1); //  * sizeof(char) ?
		if (!ret->lastresponse) {
			rc = PRESTO_NO_MEMORY;
			goto exit;
		}			
		memset(ret->lastresponse, 0, buffersize); //  * sizeof(char) ?
		ret->lastresponsebuffersize = buffersize;

		// Add resultset to the client
		add_result(ret);

		// Create request
		if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
					ret->hcurl,
					prestoclient->baseurl,
					NULL,
					in_sql_statement,
					prestoclient->catalog,
					prestoclient->schema,
					prestoclient->useragent,
					prestoclient->user,
					prestoclient->timezone,
					prestoclient->language,
					&buffersize,
					ret) == PRESTOCLIENT_RESULT_OK)
		{
			// Start polling server for data
			prestoclient_waituntilfinished(ret);
			tablebuffer_print(ret->tablebuff);

			// nevertheless we have to check for presto errors in the body of the result (not header code only)
			// Query succeeded ?
			if (prestoclient_getstatus(ret) != PRESTOCLIENT_STATUS_SUCCEEDED) {
				rc = PRESTO_BACKEND_ERROR;
				goto exit;
			}

			// Messages from presto server
			if (prestoclient_getlastservererror(ret))
			{
				printf("%s\n", prestoclient_getlastservererror(ret));
				printf("Serverstate = %s\n", prestoclient_getlastserverstate(ret));
				rc = PRESTO_BACKEND_ERROR;
				goto exit;
			}

			// Messages from prestoclient
			if (prestoclient_getlastclienterror(ret))
			{
				printf("%s\n", prestoclient_getlastclienterror(ret));
				rc = PRESTO_BACKEND_ERROR;
				goto exit;
			}

			// Messages from curl
			if (prestoclient_getlastcurlerror(ret))
			{
				printf("%s\n", prestoclient_getlastcurlerror(ret));
				rc = PRESTO_BACKEND_ERROR;
				goto exit;
			}
		} else {
			rc = PRESTO_BAD_REQUEST;
		}
	}
exit:
	// this should no deallocated, the client has to we should return the statement handle
	*result = ret;	
	if (rc != PRESTOCLIENT_RESULT_OK) {
		if (ret) {
			remove_result(ret);
			delete_prestoresult(ret);			
			ret = NULL;
		}
	} 
	*result = ret;	
	return rc;
}

int prestoclient_prepare(PRESTOCLIENT *prestoclient, 
						PRESTOCLIENT_RESULT **result,
						const char *in_sql_statement)
{
	int rc = PRESTO_OK;
	size_t sql_len;
	char *prep_name;
	char *prepqry;
	PRESTOCLIENT_RESULT *ret = NULL;
	PRESTOCLIENT_RESULT *res_output = NULL;
	PRESTOCLIENT_RESULT *res_input = NULL;
	unsigned long buffersize;
	
	if (!prestoclient || !in_sql_statement)
		return PRESTO_BAD_REQUEST;
	
	sql_len = strlen(in_sql_statement);
	if (sql_len < 1)
		return PRESTO_BAD_REQUEST;

	buffersize = PRESTOCLIENT_CURL_BUFFERSIZE;

	// Go for cleanup when exiting from this point

	ret = new_prestoresult();
	if (!result) {
		rc = PRESTO_NO_MEMORY;
		goto exit;
	}

	ret->client = prestoclient;
	ret->describe_callback_function = NULL;	
	ret->write_callback_function = &write_callback_buffer;
	ret->client_object = NULL;

	ret->hcurl = curl_easy_init();
	if (!ret->hcurl)
	{		
		rc = PRESTO_NO_MEMORY;
		goto exit;
	}

	// Reserve memory for curl data buffer
	ret->lastresponse = (char *)malloc(sizeof(char) * (buffersize + 1)); 
	if (!ret->lastresponse) {
		result = NULL;
		goto exit;
	}

	// init buffer
	memset(ret->lastresponse, 0, buffersize);
	ret->lastresponsebuffersize = buffersize;

	// Add resultset to the client
	add_result(ret);
	prep_name = (char *)malloc(sizeof(char) * 10);
	sprintf(prep_name, "qry%li", ret->client->active_results);

	prepqry = (char *)malloc(sizeof(char) * (sql_len + 40));
	sprintf(prepqry, "PREPARE %s FROM %s", prep_name, in_sql_statement);

	// Create actual request
	if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
				ret->hcurl,
				prestoclient->baseurl,
				NULL,
				prepqry,
				prestoclient->catalog,
				prestoclient->schema,
				prestoclient->useragent,
				prestoclient->user,
				prestoclient->timezone,
				prestoclient->language,
				&buffersize,
				ret) == PRESTOCLIENT_RESULT_OK)
	{
		// Start polling server for data
		prestoclient_waituntilfinished(ret);
		tablebuffer_print(ret->tablebuff);

		// nevertheless we have to check for presto errors in the body of the result (header are not sufficient only)
		// Query succeeded ?
		if (prestoclient_getstatus(ret) != PRESTOCLIENT_STATUS_SUCCEEDED) {
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}

		// Messages from presto server
		if (prestoclient_getlastservererror(ret))
		{
			printf("%s\n", prestoclient_getlastservererror(ret));
			printf("Serverstate = %s\n", prestoclient_getlastserverstate(ret));
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}

		// Messages from prestoclient
		if (prestoclient_getlastclienterror(ret))
		{
			printf("%s\n", prestoclient_getlastclienterror(ret));
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}

		// Messages from curl
		if (prestoclient_getlastcurlerror(ret))
		{
			printf("%s\n", prestoclient_getlastcurlerror(ret));
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}
	}
	else
	{
		rc = PRESTO_BACKEND_ERROR;
		goto exit;
	}		


	res_output = new_prestoresult_readied(prestoclient, buffersize);
	if (!res_output) {
		rc = PRESTO_NO_MEMORY;
		goto exit;
	}

	// deep clone from first prepared query, do not directly do enable freeing
	alloc_copy(&res_output->prepared_stmt_hdr, ret->prepared_stmt_hdr);
	alloc_copy(&res_output->prepared_stmt_name, ret->prepared_stmt_name);
	
	// Get columns of prepared statement
	sprintf(prepqry, "DESCRIBE OUTPUT %s ", prep_name);
	if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
				res_output->hcurl,
				prestoclient->baseurl,
				NULL,
				prepqry,
				prestoclient->catalog,
				prestoclient->schema,
				prestoclient->useragent,
				prestoclient->user,
				prestoclient->timezone,
				prestoclient->language,
				&buffersize,
				res_output) == PRESTOCLIENT_RESULT_OK)
	{
		// Start polling server for data
		prestoclient_waituntilfinished(res_output);
		tablebuffer_print(res_output->tablebuff);

		// nevertheless we have to check for presto errors in the body of the result (not header code only)
		// Query succeeded ?
		if (prestoclient_getstatus(res_output) != PRESTOCLIENT_STATUS_SUCCEEDED) {
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}

		// Messages from presto server
		if (prestoclient_getlastservererror(res_output))
		{
			printf("%s\n", prestoclient_getlastservererror(res_output));
			printf("Serverstate = %s\n", prestoclient_getlastserverstate(res_output));
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}

		// Messages from prestoclient
		if (prestoclient_getlastclienterror(res_output))
		{
			printf("%s\n", prestoclient_getlastclienterror(res_output));
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}

		// Messages from curl
		if (prestoclient_getlastcurlerror(res_output))
		{
			printf("%s\n", prestoclient_getlastcurlerror(res_output));
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}
	}
	else
	{
		rc = PRESTO_BACKEND_ERROR;
		goto exit;
	}

	// now we have the real columns of the query, so we delete them and reset them with the real stuff
	if (ret->columns)
	{
		for (size_t i = 0; i < ret->columncount; i++)
		{
			if (ret->columns[i])
				delete_prestocolumn(ret->columns[i]);
		}
		free(ret->columns);
		ret->columns = NULL;
	}

	// we have as much columns in the final result set as rows in this records set
	ret->columncount = res_output->tablebuff->nrow;
	ret->columns = (PRESTOCLIENT_COLUMN**)malloc(ret->columncount * sizeof(PRESTOCLIENT_COLUMN*) );	
	for (size_t ridx = 0 ; ridx < ret->columncount; ridx++){
		PRESTOCLIENT_COLUMN* tmp = new_prestocolumn();
		alloc_copy(&(tmp->name), res_output->tablebuff->rowbuff[ridx * res_output->tablebuff->ncol + 0]);
		alloc_copy(&(tmp->catalog), res_output->tablebuff->rowbuff[ridx * res_output->tablebuff->ncol + 1]);
		alloc_copy(&(tmp->schema), res_output->tablebuff->rowbuff[ridx * res_output->tablebuff->ncol + 2]);
		alloc_copy(&(tmp->table), res_output->tablebuff->rowbuff[ridx * res_output->tablebuff->ncol + 3]);
		// Type Conversions
		//alloc_copy(&(tmp->type), res_output->tablebuff->rowbuff[ridx * res_output->tablebuff->ncol + 4]);
		//alloc_copy(&(tmp->bytesize), res_output->tablebuff->rowbuff[ridx * res_output->tablebuff->ncol + 5]);
		//alloc_copy(&(tmp->alias), res_output->tablebuff->rowbuff[ridx * res_output->tablebuff->ncol + 6]);
		ret->columns[ridx] = tmp;		
	}

	res_input = new_prestoresult_readied(prestoclient, buffersize);
	if (!res_output) {
		rc = PRESTO_NO_MEMORY;
		goto exit;
	}

	// deep clone from first prepared query, do not directly do enable freeing
	alloc_copy(&res_input->prepared_stmt_hdr, ret->prepared_stmt_hdr);
	alloc_copy(&res_input->prepared_stmt_name, ret->prepared_stmt_name);
	
	sprintf(prepqry, "DESCRIBE INPUT %s ", prep_name);
	if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
				res_input->hcurl,
				prestoclient->baseurl,
				NULL,
				prepqry,
				prestoclient->catalog,
				prestoclient->schema,
				prestoclient->useragent,
				prestoclient->user,
				prestoclient->timezone,
				prestoclient->language,
				&buffersize,
				res_input) == PRESTOCLIENT_RESULT_OK)
	{
		// Start polling server for data
		prestoclient_waituntilfinished(res_input);	
		tablebuffer_print(res_input->tablebuff);

		// nevertheless we have to check for presto errors in the body of the result (not header code only)
		// Query succeeded ?
		if (prestoclient_getstatus(res_input) != PRESTOCLIENT_STATUS_SUCCEEDED) {
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}

		// Messages from presto server
		if (prestoclient_getlastservererror(res_input))
		{
			printf("%s\n", prestoclient_getlastservererror(res_input));
			printf("Serverstate = %s\n", prestoclient_getlastserverstate(res_input));
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}

		// Messages from prestoclient
		if (prestoclient_getlastclienterror(res_input))
		{
			printf("%s\n", prestoclient_getlastclienterror(res_input));
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}

		// Messages from curl
		if (prestoclient_getlastcurlerror(res_input))
		{
			printf("%s\n", prestoclient_getlastcurlerror(res_input));
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}	
	}
	else
	{
		rc = PRESTO_BACKEND_ERROR;
		goto exit;
	}
exit:	
	if (prepqry)
		free(prepqry);
	if (prep_name)
		free(prep_name);
	if (res_input)
		delete_prestoresult(res_input);
	if (res_output)
		delete_prestoresult(res_output);

	if (rc != PRESTO_OK) { 
		if (ret) {
			remove_result(ret);
			delete_prestoresult(ret);
			ret = NULL;
		}
	}	
	*result = ret;	
	return rc;
}

int prestoclient_execute(PRESTOCLIENT *prestoclient
						, PRESTOCLIENT_RESULT *prepared_result,
						void (*in_write_callback_function)(void *, void *),
						void (*in_describe_callback_function)(void *, void *),
						void *in_client_object)
{
	int rc = PRESTO_OK;
	unsigned long buffersize = PRESTOCLIENT_CURL_BUFFERSIZE;
	char *in_sql_statement = NULL;	

	if (prestoclient && prepared_result && prepared_result->prepared_stmt_name && strlen(prepared_result->prepared_stmt_name) > 0)
	{
		// handle was used before so reset and reuse
		reset_prestoresult(prepared_result);

		if (in_write_callback_function)
			prepared_result->write_callback_function = in_write_callback_function;
		else
			prepared_result->write_callback_function = &write_callback_buffer;

		prepared_result->describe_callback_function = in_describe_callback_function;
		prepared_result->client_object = in_client_object;

		in_sql_statement = (char *)malloc(strlen(prepared_result->prepared_stmt_name) + 15);
		sprintf(in_sql_statement, "EXECUTE %s ", prepared_result->prepared_stmt_name);

		// Create request
		if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
					prepared_result->hcurl,
					prestoclient->baseurl,
					NULL,
					in_sql_statement,
					prestoclient->catalog,
					prestoclient->schema,
					prestoclient->useragent,
					prestoclient->user,
					prestoclient->timezone,
					prestoclient->language,
					&buffersize,
					prepared_result) == PRESTOCLIENT_RESULT_OK)
		{
			// Start polling server for data
			prestoclient_waituntilfinished(prepared_result);
			tablebuffer_print(prepared_result->tablebuff);
		}
		else
		{
			rc = PRESTO_BACKEND_ERROR;
			goto exit;
		}
		
	}
	else
	{
		// if we got a fully executed query already...
		if (prepared_result->query && strlen(prepared_result->query ) > 0) {
			printf("No need to execute, this is a special case\n");
			rc = PRESTO_OK;
			goto exit;
		} else {
			printf("Unable to work with prepared query\n");
			rc = PRESTO_BAD_REQUEST;		
			goto exit;
		}
	}
exit:
	if (in_sql_statement)
		free(in_sql_statement);

	if (rc != PRESTO_OK) {
		if (prepared_result) {
			remove_result(prepared_result);
			delete_prestoresult(prepared_result);
			prepared_result = NULL;
		}
	} 
	return rc;
}

static void prestoclient_unprepare(PRESTOCLIENT *prestoclient, PRESTOCLIENT_RESULT *prepared_result)
{
	unsigned long buffersize;
	buffersize = PRESTOCLIENT_CURL_BUFFERSIZE;

	if (prestoclient && prepared_result && prepared_result->prepared_stmt_name && strlen(prepared_result->prepared_stmt_name))
	{
		reset_prestoresult(prepared_result);
		prepared_result->describe_callback_function = NULL;
		prepared_result->write_callback_function = NULL;

		char *deallocqry = (char *)malloc(sizeof(char *) * (strlen(prepared_result->prepared_stmt_name) + 20));
		sprintf(deallocqry, "DEALLOCATE PREPARE %s", prepared_result->prepared_stmt_name);

		// Create request
		if (openuri(PRESTOCLIENT_HTTP_REQUEST_TYPE_POST,
					prepared_result->hcurl,
					prestoclient->baseurl,
					NULL,
					deallocqry,
					prestoclient->catalog,
					prestoclient->schema,
					prestoclient->useragent,
					prestoclient->user,
					prestoclient->timezone,
					prestoclient->language,
					&buffersize,
					prepared_result) == PRESTOCLIENT_RESULT_OK)
		{
			// Start polling server for data
			prestoclient_waituntilfinished(prepared_result);
			tablebuffer_print(prepared_result->tablebuff);

			if (prepared_result->prepared_stmt_hdr) {
				free(prepared_result->prepared_stmt_hdr);
			}
			prepared_result->prepared_stmt_hdr = NULL;
			if (prepared_result->prepared_stmt_name) {
				free(prepared_result->prepared_stmt_name);
			}
			prepared_result->prepared_stmt_name = NULL;
		}
		/*		
		else
		{
			remove_result(prepared_result);
			delete_prestoresult(prepared_result);
			prepared_result = NULL;
		}
		*/
		free(deallocqry);
	}	
}

void prestoclient_deleteresult(PRESTOCLIENT *prestoclient, PRESTOCLIENT_RESULT *result)
{
	if (prestoclient && result)
	{
		prestoclient_unprepare(prestoclient, result);
		if (result)
			prestoclient_cancelquery(result);
		if (result)
			delete_prestoresult(result);
		result = NULL;
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

	return (result->laststate ? result->laststate : (char *)"");
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
	switch (result->columns[columnindex]->type)
	{
	case PRESTOCLIENT_TYPE_UNDEFINED:
		return "PRESTO_TYPE_UNDEFINED";
	case PRESTOCLIENT_TYPE_VARCHAR:
		return "PRESTO_TYPE_VARCHAR";
	case PRESTOCLIENT_TYPE_CHAR:
		return "PRESTO_TYPE_CHAR";
	case PRESTOCLIENT_TYPE_VARBINARY:
		return "PRESTO_TYPE_VARBINARY";
	case PRESTOCLIENT_TYPE_TINYINT:
		return "PRESTO_TYPE_TINYINT";
	case PRESTOCLIENT_TYPE_SMALLINT:
		return "PRESTO_TYPE_SMALLINT";
	case PRESTOCLIENT_TYPE_INTEGER:
		return "PRESTO_TYPE_INTEGER";
	case PRESTOCLIENT_TYPE_BIGINT:
		return "PRESTO_TYPE_BIGINT";
	case PRESTOCLIENT_TYPE_BOOLEAN:
		return "PRESTO_TYPE_BOOLEAN";
	case PRESTOCLIENT_TYPE_REAL:
		return "PRESTO_TYPE_REAL";
	case PRESTOCLIENT_TYPE_DOUBLE:
		return "PRESTO_TYPE_DOUBLE";
	case PRESTOCLIENT_TYPE_DECIMAL:
		return "PRESTO_TYPE_DECIMAL";
	case PRESTOCLIENT_TYPE_DATE:
		return "PRESTO_TYPE_DATE";
	case PRESTOCLIENT_TYPE_TIME:
		return "PRESTO_TYPE_TIME";
	case PRESTOCLIENT_TYPE_TIME_WITH_TIME_ZONE:
		return "PRESTO_TYPE_TIME_WITH_TIME_ZONE";
	case PRESTOCLIENT_TYPE_TIMESTAMP:
		return "PRESTO_TYPE_TIMESTAMP";
	case PRESTOCLIENT_TYPE_TIMESTAMP_WITH_TIME_ZONE:
		return "PRESTO_TYPE_TIMESTAMP_WITH_TIME_ZONE";
	case PRESTOCLIENT_TYPE_INTERVAL_YEAR_TO_MONTH:
		return "PRESTO_TYPE_INTERVAL_YEAR_TO_MONTH";
	case PRESTOCLIENT_TYPE_INTERVAL_DAY_TO_SECOND:
		return "PRESTO_TYPE_INTERVAL_DAY_TO_SECOND";
	case PRESTOCLIENT_TYPE_ARRAY:
		return "PRESTO_TYPE_ARRAY";
	case PRESTOCLIENT_TYPE_MAP:
		return "PRESTO_TYPE_MAP";
	case PRESTOCLIENT_TYPE_JSON:
		return "PRESTO_TYPE_JSON";
	default:
		return "PRESTO_TYPE_UNDEFINED";
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

	return (result->curl_error_buffer ? result->curl_error_buffer : (char *)"");
}