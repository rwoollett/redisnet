
add_executable(ClientSubscribe 
        AwakenerWaitable.h
        main.cpp)

target_include_directories(ClientSubscribe PRIVATE
        $<BUILD_INTERFACE:${RedisPubSub_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${RedisPubSub_SOURCE_DIR}/ClientSubscribe>
        )

set(CXX_FILESYSTEM_LIBRARIES "stdc++fs")
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
target_link_libraries(ClientSubscribe PRIVATE Threads::Threads ${CMAKE_DL_LIBS} ${CXX_FILESYSTEM_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto RedisSubscribe)

