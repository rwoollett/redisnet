
add_library(RedisSubscribe
  Subscribe.cpp
  Subscribe.h
  logsync.cpp
  logsync.h
)

add_library(RedisPubSub::RedisSubscribe ALIAS RedisSubscribe)

target_include_directories(RedisSubscribe 
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../..      # <pubsub/...>
    PRIVATE
        $<BUILD_INTERFACE:${Boost_INCLUDE_DIR}>
)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(RedisSubscribe PRIVATE Threads::Threads ${CMAKE_DL_LIBS})


