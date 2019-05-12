cmake_minimum_required(VERSION 3.9)
project(leveldbApp)

set(CMAKE_CXX_STANDARD 11)

add_executable(leveldbApp main.cpp db_handle.cpp db_handle.h proj_service.h proj_service.cpp bigdata_proj.cpp)

target_link_libraries(leveldbApp leveldb pthread)