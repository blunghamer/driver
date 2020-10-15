add_library(prestoclient prestoclient.c prestoclient.h prestoclientutils.c prestojson.c)
#prestoclientjsonstream.c 
add_library(sqlparser sqlparser.c sqlparser.h)
add_library(jsonparser json.c json.h)
#add_library(prestojson prestojson.c prestojson.h)

#jsonparser.c jsonparser.h 
target_link_libraries (prestoclient LINK_PUBLIC curl jsonparser)

add_executable(jsont jsont.c)
target_link_libraries (jsont LINK_PUBLIC jsonparser)

target_include_directories (prestoclient PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})