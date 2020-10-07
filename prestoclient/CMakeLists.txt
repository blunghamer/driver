add_library(prestoclient prestoclient.c prestoclient.h prestoclientjsonstream.c prestoclienttypes.h prestoclientutils.c)
target_link_libraries (prestoclient LINK_PUBLIC curl)
target_include_directories (prestoclient PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})