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

add_custom_target(build-time-make-directory ALL
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/bin/tmp)
