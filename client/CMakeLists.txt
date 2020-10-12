add_executable(prestotests check_client.c)

target_link_libraries (prestotests PUBLIC prestoclient)
target_link_libraries (prestotests PUBLIC check)

add_executable(cli cli.c)
target_link_libraries (cli PUBLIC prestoclient)
