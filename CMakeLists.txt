# CMakeLists.txt
cmake_minimum_required(VERSION 3.10)
set (CMAKE_TOOLCHAIN_FILE "$ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake")
set (CMAKE_CXX_STANDARD 11)

project(project4)

find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(Threads REQUIRED)

add_subdirectory(external)
add_subdirectory(src)
add_subdirectory(test)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test/input/testdata_1.txt ${CMAKE_BINARY_DIR}/bin/input/testdata_1.txt COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test/input/testdata_2.txt ${CMAKE_BINARY_DIR}/bin/input/testdata_2.txt COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test/input/testdata_3.txt ${CMAKE_BINARY_DIR}/bin/input/testdata_3.txt COPYONLY)

