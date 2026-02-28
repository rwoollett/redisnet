//#include "io_utility/io_utility.h"
#include <csignal>
#include <cstdlib> // For std::getenv
#include "../pubsub/subscribe/Subscribe.h"
#include "AwakenerWaitable.h"
#include <mutex>
#include <condition_variable>
#include <thread>
#include <iostream>
#include <boost/redis/connection.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/redis/src.hpp> // boost redis implementation
#include <string>
#include <stdexcept>

int main(int argc, char **argv)
{
  int result = EXIT_SUCCESS;
  const char *redis_host = std::getenv("REDIS_HOST");
  const char *redis_port = std::getenv("REDIS_PORT");
  const char *redis_channel = std::getenv("REDIS_CHANNEL");
  const char *redis_password = std::getenv("REDIS_PASSWORD");
  const char *redis_use_ssl = std::getenv("REDIS_USE_SSL");
  if (!(redis_host && redis_port && redis_password && redis_channel))
  {
    std::cerr << "Environment variables REDIS_HOST, REDIS_PORT, REDIS_CHANNEL, REDIS_PASSWORD or REDIS_USE_SSL are not set." << std::endl;
    exit(1);
  }
  
  boost::asio::io_context main_ioc;
  AwakenerWaitable awakener;
  bool m_worker_shall_stop{false}; 
  auto main_ioc_thread = std::thread([&main_ioc]()
    { main_ioc.run(); });

  try
  {
    RedisSubscribe::Subscribe redisSubscribe;
    redisSubscribe.main_redis(awakener);
    D(std::cout << "Application loop stated\n";)
    while (!m_worker_shall_stop)
    {
      awakener.wait_broadcast();
      D(std::cout << "Application loop awakened, awake count: " << awakener.awake_load() << std::endl;)

      if (redisSubscribe.is_signal_stopped())
      {
        m_worker_shall_stop = true;
      }
    }
    std::cout << "Exited normally\n";

  }
  catch (const std::exception &e)
  {
    std::cout << e.what() << "\n";
    result = EXIT_FAILURE;
  }
  catch (const std::string &e)
  {
    std::cout << e << "\n";
    result = EXIT_FAILURE;
  }

  main_ioc.stop();
  if (main_ioc_thread.joinable())
  {
    main_ioc_thread.join();
  }

  return result;
}
