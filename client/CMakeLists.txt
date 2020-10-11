add_executable(prestotests check_client.c)

target_link_libraries (prestotests PUBLIC prestoclient)
target_link_libraries (prestotests PUBLIC check)


#include (CTest)
#add_test(client_test client)