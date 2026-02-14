
add_executable(ClientPublish main.cpp)

target_include_directories(ClientPublish PRIVATE
        $<BUILD_INTERFACE:${RedisPubSub_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${RedisPubSub_SOURCE_DIR}/ClientPublish>
        )

set(CXX_FILESYSTEM_LIBRARIES "stdc++fs")
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
target_link_libraries(ClientPublish PRIVATE Threads::Threads ${CMAKE_DL_LIBS} ${CXX_FILESYSTEM_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto RedisPublish)

