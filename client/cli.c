/* This file is part of Easy to Oracle - Free Open Source Data Integration
*
* Copyright (C) 2014 Ivo Herweijer
*
* Easy to Oracle is free software: you can redistribute it and/or modify
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

#include "../prestoclient/prestoclient.h"
#include "../prestoclient/prestoclienttypes.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <check.h>

PRESTOCLIENT *pc = NULL;

/*
 * Define a struct to hold the data for a client session. A pointer
 * to this struct will be passed in all callback functions. This
 * enables you to handle multiple queries simultaneously.
 */
typedef struct ST_QUERYDATA
{
	bool hdr_printed;
	char *cache;
	size_t cache_size;
} QUERYDATA;

void querydata_delete(QUERYDATA* qdata) {
	if (qdata && qdata->cache)
		free(qdata->cache);

	if (qdata)
		free(qdata);
}

QUERYDATA* querydata_new() {
	QUERYDATA* qdata = (QUERYDATA *)malloc(sizeof(QUERYDATA));
	qdata->hdr_printed = false;
	qdata->cache_size = 1;
	qdata->cache = (char *)malloc(qdata->cache_size);
	qdata->cache[0] = 0;
	return qdata;
} 

/*
 * The write callback function. This function will be called for every row of
 * query data.
 */
static void write_callback_function(void *in_querydata, void *in_result)
{
	QUERYDATA *qdata = (QUERYDATA *)in_querydata;
	PRESTOCLIENT_RESULT *result = (PRESTOCLIENT_RESULT *)in_result;
	size_t newdatalen; 
	size_t columncount = prestoclient_getcolumncount(result);

	/*
	 * Output one data row
	 */
	for (size_t idx = 0; idx < columncount; idx++)
	{
		/*
		 * Check cache size first
		 */
		newdatalen = strlen(prestoclient_getcolumndata(result, idx));

		if (qdata->cache_size < strlen(qdata->cache) + newdatalen + 3)
		{
			/*
			 * Add memory block of 1 Kb to cache
			 */
			qdata->cache_size = ((qdata->cache_size + newdatalen + 1027) / 1024) * 1024;
			qdata->cache = (char *)realloc((char *)qdata->cache, qdata->cache_size);
			if (!qdata->cache)
				exit(1);
		}

		/*
		 * Add field value as string, prestoclient doesn't do any type conversions (yet)
		 */
		strcat(qdata->cache, prestoclient_getcolumndata(result, idx));

		/*
		 * You can use prestoclient_getnullcolumnvalue here
		 * to test if value is NULL in the database
		 */

		/*
		 * Add a field separator
		 */
		if (idx < columncount - 1)
			strcat(qdata->cache, ";");
	}

	/*
	 * Print rowdata and a row separator
	 */
	printf("%s\n", qdata->cache);

	/*
	 * Clear cache
	 */
	qdata->cache[0] = 0;
}

/*
 * Min function for a simple commandline application.
 */
int main(int argc, char** argv)
{
	int prc = 0;
	int rc = 0;	
	PRESTOCLIENT* pc = NULL;
	PRESTOCLIENT_RESULT* result = NULL;

	if (argc < 3)
	{
		printf("Usage: cprestoclient <servername> <catalog> <sql-statement>\n");
		printf("Example:\ncprestoclient localhost \"select * from system.runtime.queries\"\n");
		exit(1);
	}
	
	/* from here we do have to cleanup results */
	QUERYDATA *qdata = querydata_new();	

	/*
	 * Initialize prestoclient. We're using default values for everything but the servername
	 */
	pc = prestoclient_init("http", argv[1], NULL , NULL , NULL , NULL, NULL, NULL, NULL, 1);
	if (!pc)
	{
		printf("Could not initialize prestoclient\n");
		rc = 1;
		goto exit;		
	}
	
	prc = prestoclient_query(pc, &result, argv[2], &write_callback_function, (void *)qdata);
	if (prc != PRESTO_OK)
	{
		printf("Could not start query '%s' on server '%s'\n", argv[2], argv[1]);
		rc = 5;
		goto exit;
	}
	else
	{
		// Query succeeded ?
		if (prestoclient_getstatus(result) != PRESTOCLIENT_STATUS_SUCCEEDED) {
			printf("Query failed\n");
		}

		// Messages from presto server
		if (prestoclient_getlastservererror(result))
		{
			printf("%s\n", prestoclient_getlastservererror(result));
			printf("Serverstate = %s\n", prestoclient_getlastserverstate(result));
		}

		// Messages from prestoclient
		if (prestoclient_getlastclienterror(result))
		{
			printf("%s\n", prestoclient_getlastclienterror(result));
		}

		// Messages from curl
		if (prestoclient_getlastcurlerror(result))
		{
			printf("%s\n", prestoclient_getlastcurlerror(result));
		}
	}
	prestoclient_deleteresult(pc, result);

exit:
	prestoclient_close(pc);
	querydata_delete(qdata);
	return rc;
}
