add_library(prestoclient prestoclient.c prestoclient.h prestoclientjsonstream.c prestoclientutils.c)
add_library(sqlparser sqlparser.c sqlparser.h)
add_library(jsonparser json.c json.h)
add_executable(jsont jsont.c)

#jsonparser.c jsonparser.h 
target_link_libraries (prestoclient LINK_PUBLIC curl)
target_link_libraries (jsont LINK_PUBLIC jsonparser)
target_include_directories (prestoclient PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})