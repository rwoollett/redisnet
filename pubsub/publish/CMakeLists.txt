
add_library(RedisPublish
  Publish.cpp
  Publish.h
  logsync.cpp
  logsync.h
)

add_library(RedisPubSub::RedisPublish ALIAS RedisPublish)

target_include_directories(RedisPublish 
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../..      # <pubsub/...>
    PRIVATE
        $<BUILD_INTERFACE:${Boost_INCLUDE_DIR}>
)

target_link_libraries(RedisPublish 
  PUBLIC 
    MTLog
)


