cmake_minimum_required(VERSION 3.1...3.18)

project(
  PrestoODBCDriver 
  VERSION 0.1
  LANGUAGES C)

if (MSVC)
    # warning level 4 and all warnings as errors
    add_compile_options(/W4 /WX)
else()
    # lots of warnings and all warnings as errors -pedantic
    #add_compile_options(-Wall -Wextra  -Werror -fPIC)
    add_compile_options(-g -Wall -fPIC)
endif()

add_subdirectory(prestoclient)
add_subdirectory(prestoodbc)
add_subdirectory(client)
# enable_testing()