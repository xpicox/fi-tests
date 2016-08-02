cmake_minimum_required(VERSION 2.8)
project(fi-test CXX)

#Add internal search path for scripts
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR})

message(STATUS "Using ${CMAKE_CXX_COMPILER_ID} compiler")

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # using GCC
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unused-local-typedefs -Wno-unused-parameter -fno-exceptions -fno-rtti")
    set(CMAKE_CXX_FLAGS_RELEASE "-O2")
    set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3")
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # using Clang or AppleClang
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unused-parameter -fno-exceptions -fno-rtti")
    set(CMAKE_CXX_FLAGS_RELEASE "-O2")
    set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3")
else ()
    message(FATAL_ERROR "${CMAKE_CXX_COMPILER_ID} is not a supported compiler")
    # exit due to fatal error
endif ()

# Check for c++11 support
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
CHECK_CXX_COMPILER_FLAG("-std=c++0x" COMPILER_SUPPORTS_CXX0X)
if (COMPILER_SUPPORTS_CXX11)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
elseif (COMPILER_SUPPORTS_CXX0X)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
else ()
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
    # exit due to fatal error
endif ()

find_package(LibFabric REQUIRED)

include_directories(
  ${fi-test_SOURCE_DIR}
  ${LIBFABRIC_INCLUDE_DIRS}
)

add_executable(
        fi-test
        main.cpp
)

target_link_libraries(
        fi-test
        ${LIBFABRIC_LIBRARIES}
)

add_executable(
        print_services
        print_services.cpp
)

target_link_libraries(
        print_services
        ${LIBFABRIC_LIBRARIES}
)

add_subdirectory(shared)

add_executable(
        rc_pingpong
        rc_pingpong.cpp
)

target_link_libraries(
        rc_pingpong
        reduced
        ${LIBFABRIC_LIBRARIES}
)

add_subdirectory(Ping)

add_executable(
    fi_pingpong
    pingpong.cpp
)

target_link_libraries(
    fi_pingpong
    ${LIBFABRIC_LIBRARIES}
)
