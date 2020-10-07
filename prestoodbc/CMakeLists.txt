add_library(PrestoODBC SHARED wcutils.c simple_driver.c simple_driver.h)
target_link_libraries (PrestoODBC LINK_PUBLIC prestoclient)