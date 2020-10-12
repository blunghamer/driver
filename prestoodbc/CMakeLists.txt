add_library(PrestoODBC SHARED str2odbc.c wcutils.c driver.c driver.h)

target_link_libraries (PrestoODBC PUBLIC prestoclient)
target_link_libraries (PrestoODBC PUBLIC sqlparser)

#target_link_libraries (PrestoODBC LINK_PUBLIC prestoclient)