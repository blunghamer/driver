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


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <check.h>
#include "str2odbc.h"

size_t num_iter = 1000000;

void setup(void)
{
	//
}

void teardown(void)
{
    //
}

START_TEST (convert_prestojson_timestamp)
{	
	
	clock_t begin = clock();
	for (size_t zz = 0 ; zz < num_iter ; zz++) {
		TIMESTAMP_MS res = timestamp_to_long("1970-01-19 13:12:51.895");
		ck_assert_int_eq(1599171000, res);			
	}
	clock_t end = clock();
	double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
	printf("Time spent in timestamp_to_long %f\n", time_spent);

}
END_TEST

START_TEST (convert_prestojson_date)
{	
	DATE_STRUCT ds;
	dt_to_odbc("1970-01-19", &ds);
	ck_assert_int_eq(1970, ds.year);
}
END_TEST

START_TEST (convert_prestojson_to_odbcts)
{	
	clock_t begin = clock();
	for (size_t zz = 0 ; zz < num_iter ; zz++) {
		TIMESTAMP_STRUCT ts;
		ts_to_odbc("1970-01-19 13:12:51.895", &ts);
		if (ts.year != 1970) return; 
		//ck_assert_int_eq(1970, ts.year );
		//ck_assert_int_eq(1, ts.month );
		//ck_assert_int_eq(19, ts.day );
	}
	clock_t end = clock();
	double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
	printf("Time spent with buffer copy %f\n", time_spent);
}
END_TEST

START_TEST (convert_prestojson_to_odbcts_old)
{	
	clock_t begin = clock();
	for (size_t zz = 0 ; zz < num_iter ; zz++) {
		TIMESTAMP_STRUCT ts;
		str2timestamp("1970-01-19 13:12:51.895", &ts);
		if (ts.year != 1970) return;
		//ck_assert_int_eq(1970, ts.year );
		//ck_assert_int_eq(1, ts.month );
		//ck_assert_int_eq(19, ts.day );
	}
	clock_t end = clock();
	double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
	printf("Time spent with addtional logic %f\n", time_spent);
}
END_TEST



Suite * prestojson_suite(void)
{
    Suite *s;
    TCase *tc_core;

    s = suite_create("prestoclient");

    /* Core test case */
    tc_core = tcase_create("Core");

	tcase_add_checked_fixture(tc_core, setup, teardown);
    tcase_add_test(tc_core, convert_prestojson_timestamp);
	tcase_add_test(tc_core, convert_prestojson_to_odbcts);
	tcase_add_test(tc_core, convert_prestojson_to_odbcts_old);
    suite_add_tcase(s, tc_core);

    return s;
}


int main(void)
{
	int number_failed;
	Suite *s;
	SRunner *sr;

	s = prestojson_suite();
	sr = srunner_create(s);

	// forking is fine to speed up tests but debugging is a nightmare
	srunner_set_fork_status (sr, CK_NOFORK);
	srunner_run_all(sr, CK_NORMAL);
	number_failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
