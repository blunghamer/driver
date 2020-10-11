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
 * The descibe callback function. This function will be called when the
 * column description data becomes available. You can use it to print header
 * information or examine column type info.
 */
static void describe_callback_function(void *in_querydata, void *in_result)
{
	QUERYDATA *qdata = (QUERYDATA *)in_querydata;
	PRESTOCLIENT_RESULT *result = (PRESTOCLIENT_RESULT *)in_result;
	size_t columncount = prestoclient_getcolumncount(result);

	if (!qdata->hdr_printed && columncount > 0)
	{
		/*
		 * Print header row
		 */
		for (size_t i = 0; i < columncount; i++)
			printf("%s%s", i > 0 ? ";" : "", prestoclient_getcolumnname(result, i));

		printf("\n");

		/*
		 * Print datatype of each column
		 */
		for (size_t i = 0; i < columncount; i++)
			printf("%s%s", i > 0 ? ";" : "", prestoclient_getcolumntypedescription(result, i));

		printf("\n");

		/*
		 * Mark header as printed
		 */
		qdata->hdr_printed = true;
	}
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


void setup(void)
{
    pc = prestoclient_init("http", "localhost", NULL , NULL , NULL , NULL, NULL, NULL, NULL, 1);
	if (!pc)
	{
		printf("Could not initialize prestoclient\n");
		return;
	} 

	printf("Setup done\n");
}

void teardown(void)
{
    prestoclient_close(pc);
}



START_TEST (test_can_serverinfo)
{	
	char * info = NULL;
	info = prestoclient_serverinfo(pc);
	if (!info) {
		printf("unable to connect to server, no server info queryable\n");
		goto exit;
	}
	ck_assert_ptr_nonnull(info);
exit:
	if (info)
		free(info);
}
END_TEST


/*
	"select * from system.jdbc.types"
	"show catalogs"
*/
START_TEST (test_can_query)
{
	int prc;
	PRESTOCLIENT_RESULT* result;	
	char *qry = "use system.runtime";
	prc = prestoclient_query(pc, &result, qry, NULL, NULL, NULL);
	if (prc != PRESTO_OK)
	{
		printf("Could not start query '%s'\n", qry);	
		goto exit;
	}	
	ck_assert_int_eq(PRESTO_OK, prc);
	ck_assert_str_eq("system", pc->catalog);
	ck_assert_str_eq("runtime", pc->schema);
exit:
	if (result)
		prestoclient_deleteresult(pc, result);		
}
END_TEST


START_TEST (test_can_prepare)
{
	int prc;
	PRESTOCLIENT_RESULT* result;	
	char *qry = "select * from system.runtime.queries";
	prc = prestoclient_prepare(pc, &result, qry);
	if (prc != PRESTO_OK)
	{
		printf("Could not start query '%s'\n", qry);	
		goto exit;
	}	

	ck_assert_ptr_nonnull(result->columns);
	ck_assert_int_eq(result->columncount, 15);

	prc = prestoclient_execute(pc, result, NULL , NULL, NULL );
	if (prc != PRESTO_OK)
	{
		printf("Could not execute prepared query '%s'\n", qry);		
		goto exit;
	}
	
	ck_assert_int_eq(PRESTO_OK, prc);
	ck_assert_ptr_nonnull(result->tablebuff);
	ck_assert_ptr_nonnull(result->tablebuff->rowbuff);
	
exit:
	if (result)
		prestoclient_deleteresult(pc, result);		
}
END_TEST

Suite * prestoclient_suite(void)
{
    Suite *s;
    TCase *tc_core;

    s = suite_create("prestoclient");

    /* Core test case */
    tc_core = tcase_create("Core");

	tcase_add_checked_fixture(tc_core, setup, teardown);
    tcase_add_test(tc_core, test_can_serverinfo);
	tcase_add_test(tc_core, test_can_query);
	tcase_add_test(tc_core, test_can_prepare);
    suite_add_tcase(s, tc_core);

    return s;
}


int main(void)
{
	int number_failed;
	Suite *s;
	SRunner *sr;

	s = prestoclient_suite();
	sr = srunner_create(s);

	srunner_run_all(sr, CK_NORMAL);
	number_failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}

/*
 * Min function for a simple commandline application.
 */
int main0(int argc, char** argv)
{
	int prc = 0;
	int rc = 0;	
	PRESTOCLIENT* pc = NULL;
	PRESTOCLIENT_RESULT* result = NULL;

	if (argc < 3)
	{
		printf("Usage: cprestoclient <servername> <catalog> <sql-statement>\n");
		printf("Example:\ncprestoclient localhost \"select * from system.queries\"\n");
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
	
	prc = prestoclient_query(pc, &result, argv[2], &write_callback_function, &describe_callback_function, (void *)qdata);
	if (prc != PRESTO_OK)
	{
		printf("Could not start query '%s' on server '%s'\n", argv[2], argv[1]);
		rc = 5;
		goto exit;
	}
	else
	{
		// Query succeeded ?
		bool status = prestoclient_getstatus(result) == PRESTOCLIENT_STATUS_SUCCEEDED;

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

	/*
	* Cleanup
	*/
exit:
	prestoclient_close(pc);
	querydata_delete(qdata);
	return rc;
}
