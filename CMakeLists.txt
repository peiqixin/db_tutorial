cmake_minimum_required(VERSION 3.16)
project(db_tutorial)

set(CMAKE_CXX_STANDARD 20)

add_executable(db main.cpp db.cpp db.h)
add_executable(test test.cpp db.cpp db.h)