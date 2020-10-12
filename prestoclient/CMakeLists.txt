add_library(prestoclient json.c prestoclient.c prestoclient.h prestoclientjsonstream.c prestoclienttypes.h prestoclientutils.c)
add_library(sqlparser sqlparser.c sqlparser.h)

#jsonparser.c jsonparser.h 
target_link_libraries (prestoclient LINK_PUBLIC curl)
target_include_directories (prestoclient PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})