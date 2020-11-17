add_library(PrestoODBC SHARED str2odbc.c wcutils.c driver.c driver.h)

target_link_libraries (PrestoODBC PUBLIC prestoclient)
target_link_libraries (PrestoODBC PUBLIC sqlparser)

#target_link_libraries (PrestoODBC LINK_PUBLIC prestoclient)
add_library(str2odbc str2odbc.c)

#add_executable(str2odbctest str2odbc_tests.c)
#target_link_libraries (str2odbctest PUBLIC check)
#target_link_libraries (str2odbctest PUBLIC str2odbc)
