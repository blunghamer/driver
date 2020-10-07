add_executable(prestocli main.c)

target_link_libraries (prestocli PUBLIC prestoclient)

#include (CTest)
#add_test(client_test client)