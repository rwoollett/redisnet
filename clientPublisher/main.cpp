
#include <csignal>
#include <cstdlib> // For std::getenv
#include <mutex>
#include <condition_variable>
#include <thread>
#include <iostream>
#include "../pubsub/publish/Publish.h" // RedisPublish class
#include <boost/redis/src.hpp>         // boost redis implementation
#include <mtlog/mt_log.hpp>

int main(int argc, char **argv)
{

  // Check all environment variable
  const char *redis_host = std::getenv("REDIS_HOST");
  const char *redis_port = std::getenv("REDIS_PORT");
  const char *redis_channel = std::getenv("REDIS_CHANNEL");
  const char *redis_password = std::getenv("REDIS_PASSWORD");
  const char *REDIS_PUBSUB_PUBLISHER_LOGFILE = std::getenv("REDIS_PUBSUB_PUBLISHER_LOGFILE");

  if (!(redis_host && redis_port && redis_password && redis_channel))
  {
    std::cerr << "Environment variables REDIS_HOST, REDIS_PORT, REDIS_CHANNEL, REDIS_PASSWORD or REDIS_USE_SSL are not set." << std::endl;
    exit(1);
  }
  if (argc > 1)
  {
    std::cerr << "Using command line arguments as channels to publish messages." << std::endl;
  }

  mt_logging::logger().log(
      {REDIS_PUBSUB_PUBLISHER_LOGFILE,
       REDIS_PUBSUB_PUBLISHER_LOGFILE,
       std::ios::out,
       true});

  try
  {

    RedisPublish::Publish redisPublish;
    // Before running do a sanity check on connections for Redis.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    mt_logging::logger().log(
        {REDIS_PUBSUB_PUBLISHER_LOGFILE,
         fmt::format("Redis publisher connected: {}", (redisPublish.is_redis_connected() ? "true" : "false")),
         std::ios::app,
         true});

    auto doPublish = [&redisPublish](const std::string &channel,
                                     const std::string &msg = "default message")
    {
      if (redisPublish.is_redis_connected())
        redisPublish.enqueue_message(channel, msg);
    };

    mt_logging::logger().log(
        {REDIS_PUBSUB_PUBLISHER_LOGFILE,
         "Application loop stated",
         std::ios::app,
         true});
    bool m_worker_shall_stop{false}; // false
    while (!m_worker_shall_stop)
    {

      if (redisPublish.is_redis_signaled())
      {
        m_worker_shall_stop = true;
        continue;
      }

      if (argc > 1)
      {
        for (int i = 1; i < argc; ++i)
        {
          doPublish(argv[i]);
        }
      }
      else
      {
        doPublish("csToken_request");
        doPublish("csToken_acquire");
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
  }
  catch (const std::exception &e)
  {
    mt_logging::logger().log(
        {REDIS_PUBSUB_PUBLISHER_LOGFILE,
         fmt::format("Application error {}", e.what()),
         std::ios::app,
         true});
    return EXIT_FAILURE;
  }
  catch (const std::string &e)
  {
    mt_logging::logger().log(
        {REDIS_PUBSUB_PUBLISHER_LOGFILE,
         fmt::format("Application error {}", e),
         std::ios::app,
         true});
    return EXIT_FAILURE;
  }

  mt_logging::logger().log(
      {REDIS_PUBSUB_PUBLISHER_LOGFILE,
       "Exited normally",
       std::ios::app,
       true});
  return EXIT_SUCCESS;
}
