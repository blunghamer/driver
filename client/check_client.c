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
START_TEST (test_can_use_schema)
{
	int prc;
	PRESTOCLIENT_RESULT* result;	
	char *qry = "use system.runtime";
	prc = prestoclient_query(pc, &result, qry, NULL, NULL);
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

START_TEST (test_can_query_information_schema)
{
	int prc;
	PRESTOCLIENT_RESULT* result;	
	char *qry = "select * from information_schema.tables";
	prc = prestoclient_query(pc, &result, qry, NULL, NULL);
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

START_TEST (test_bad_query_fails_with_errorcode)
{
	int prc;
	PRESTOCLIENT_RESULT* result;	
	char *qry = "select * from information_schema.tables;";
	prc = prestoclient_query(pc, &result, qry, NULL, NULL);
	if (prc != PRESTO_OK)
	{
		printf("Could not execute query '%s'\n", qry);	
		goto exit;
	}	
	ck_assert_int_ne(PRESTO_OK, prc);	
exit:
	if (result)
		prestoclient_deleteresult(pc, result);		
}
END_TEST


START_TEST (test_bad_prepare_fails_with_errorcode)
{
	int prc;
	PRESTOCLIENT_RESULT* result;	
	char *qry = "select * from information_schema.tables;";
	prc = prestoclient_prepare(pc, &result, qry);
	if (prc != PRESTO_OK)
	{
		printf("Could not prepare query '%s'\n", qry);	
		goto exit;
	}	
	ck_assert_int_ne(PRESTO_OK, prc);	
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

	prc = prestoclient_execute(pc, result, NULL , NULL);
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

START_TEST (test_can_query_mass_test)
{
	int prc;
	size_t idx;

	for (idx = 0; idx < 10; idx++) {		
		PRESTOCLIENT_RESULT* result;	
		char *qry = "select * from system.runtime.queries order by created desc ";
		prc = prestoclient_query(pc, &result, qry, NULL, NULL);
		if (prc != PRESTO_OK)
		{
			printf("Could not start query '%s'\n", qry);		
		}	
		prestoclient_deleteresult(pc, result);	
	}	
}
END_TEST

/*
	"select * from system.jdbc.types"
	"show catalogs"
*/
START_TEST (test_can_use_and_then_query)
{
	int prc;
	PRESTOCLIENT_RESULT* result;	
	char *qry = "use system.runtime";
	prc = prestoclient_query(pc, &result, qry, NULL, NULL);
	if (prc != PRESTO_OK)
	{
		printf("Could not start query '%s'\n", qry);	
		goto exit;
	}	
	prestoclient_deleteresult(pc, result);
	result = NULL;

	ck_assert_int_eq(PRESTO_OK, prc);
	ck_assert_str_eq("system", pc->catalog);
	ck_assert_str_eq("runtime", pc->schema);

	
	char *qry3 = "select * from system.runtime.nodes";
	prc = prestoclient_query(pc, &result, qry3, NULL, NULL);
	if (prc != PRESTO_OK)
	{
		printf("Could not execute query '%s'\n", qry);	
		goto exit;
	}
	prestoclient_deleteresult(pc, result);	
	result = NULL;
	ck_assert_int_eq(PRESTO_OK, prc);	

	char *qry4 = "select * from system.runtime.transactions";
	prc = prestoclient_query(pc, &result, qry4, NULL, NULL);
	if (prc != PRESTO_OK)
	{
		printf("Could not execute query '%s'\n", qry);	
		goto exit;
	}
	prestoclient_deleteresult(pc, result);	
	result = NULL;
	ck_assert_int_eq(PRESTO_OK, prc);	


	char *qry5 = "select * from system.runtime.optimizer_rule_stats";
	prc = prestoclient_query(pc, &result, qry5, NULL, NULL);
	if (prc != PRESTO_OK)
	{
		printf("Could not execute query '%s'\n", qry);	
		goto exit;
	}
	prestoclient_deleteresult(pc, result);	
	result = NULL;
	ck_assert_int_eq(PRESTO_OK, prc);	

	char *qry2 = "select * from system.information_schema.tables";
	prc = prestoclient_query(pc, &result, qry2, NULL, NULL);
	if (prc != PRESTO_OK)
	{
		printf("Could not execute query '%s'\n", qry);	
		goto exit;
	}
	prestoclient_deleteresult(pc, result);	
	result = NULL;
	ck_assert_int_eq(PRESTO_OK, prc);	

	char *qry6 = "select * from system.runtime.queries";
	prc = prestoclient_query(pc, &result, qry6, NULL, NULL);
	if (prc != PRESTO_OK)
	{
		printf("Could not execute query '%s'\n", qry);	
		goto exit;
	}
	prestoclient_deleteresult(pc, result);	
	result = NULL;
	ck_assert_int_eq(PRESTO_OK, prc);	


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
	tcase_add_test(tc_core, test_can_query_mass_test);
	tcase_add_test(tc_core, test_can_query_information_schema);		
	tcase_add_test(tc_core, test_bad_query_fails_with_errorcode);	
	tcase_add_test(tc_core, test_bad_prepare_fails_with_errorcode);			
	tcase_add_test(tc_core, test_can_use_schema);
	tcase_add_test(tc_core, test_can_prepare);
	tcase_add_test(tc_core, test_can_use_and_then_query);
	
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

	// forking is fine to speed up tests but debugging is a nightmare
	srunner_set_fork_status (sr, CK_NOFORK);
	srunner_run_all(sr, CK_NORMAL);
	number_failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
