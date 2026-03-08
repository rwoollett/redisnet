cmake_minimum_required(VERSION 3.13)
project(RedisPubSub VERSION 1.0
        DESCRIPTION "Redis on network"
        LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20) 
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

add_compile_definitions(HAVE_ASIO)

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_DEBUG_LIBS OFF)
set(Boost_USE_RELEASE_LIBS ON) 
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME ON) 
set(Boost_NO_BOOST_CMAKE ON)
set(BOOST_ROOT /usr/local)
find_package(Boost 1.86 REQUIRED)

FetchContent_Declare(
    MTLog
    GIT_REPOSITORY https://github.com/rwoollett/mtlog.git
    GIT_TAG master
)
FetchContent_MakeAvailable(MTLog)

option(BUILD_PUBSUB_TESTS "Build tests and include clientProducer and clientSubscriber" OFF)

if (BUILD_PUBSUB_TESTS)
  add_subdirectory(clientSubscriber)
  add_subdirectory(clientPublisher)
endif()

add_subdirectory(pubsub/publish)

add_subdirectory(pubsub/subscribe)



