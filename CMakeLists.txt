cmake_minimum_required(VERSION 3.13)
project(RedisPubSub VERSION 1.0
        DESCRIPTION "Redis on network"
        LANGUAGES C CXX)

# Set module path for Tester Catch2 cmake modules
#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")

# set everything up for c++ 20 features - require coroutines and BOOST_ASIO_HAS_CO_AWAIT
set(CMAKE_CXX_STANDARD 20) 
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#add_compile_definitions(USE_EXP_FILESYSTEM)

# FetchContent added in CMake 3.11, downloads during the configure step
#include(FetchContent)

add_compile_definitions(HAVE_ASIO)

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_DEBUG_LIBS OFF)
set(Boost_USE_RELEASE_LIBS ON) 
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME ON) 
set(Boost_NO_BOOST_CMAKE ON)
set(BOOST_ROOT /usr/local)
find_package(Boost 1.86 REQUIRED)

option(BUILD_PUBSUB_TESTS "Build tests and include clientProducer and clientSubscriber" OFF)

if (BUILD_PUBSUB_TESTS)
  add_subdirectory(clientSubscriber)
  add_subdirectory(clientPublisher)
endif()

add_subdirectory(pubsub/publish)

add_subdirectory(pubsub/subscribe)



