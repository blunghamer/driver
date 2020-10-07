cmake_minimum_required(VERSION 3.1...3.18)

project(
  MyODBCDriver 
  VERSION 0.1
  LANGUAGES C)

add_subdirectory(prestoclient)
# add_subdirectory(prestoodbc)
add_subdirectory(client)
# enable_testing()