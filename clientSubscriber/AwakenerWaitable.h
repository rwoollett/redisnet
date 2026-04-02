#ifndef REDISCLIENT_AWAKENER_WAITABLE_H_
#define REDISCLIENT_AWAKENER_WAITABLE_H_

#include "../pubsub/subscribe/Subscribe.h"
#include <mtlog/mt_log.hpp>


class AwakenerWaitable : public RedisSubscribe::Awakener
{
  int awake{0};
  std::mutex m_class_lock;
  std::condition_variable m_cond_not_awake;
  bool shall_stop_awaken = false;

public:
  int awake_load() { return awake; };

  void wait_broadcast()
  {
    std::unique_lock<std::mutex> cl(m_class_lock);
    m_cond_not_awake.wait(cl, [this]
                          { return awake > 0 || shall_stop_awaken; });
    if (awake > 0)
      awake--;
  };

  virtual void broadcast_messages(std::list<std::string> broadcast_messages)
  {
    std::unique_lock<std::mutex> cl(m_class_lock);
    if (broadcast_messages.empty())
      return;
    // // The base class will print the messages.
    // DRPSSI(std::cout << "- Broadcast subscribed messages\n";
    //   for (const auto &msg : broadcast_messages) {
    //     std::cout << msg << std::endl;
    //   } std::cout
    //   << std::endl;
    //   std::cout << "******************************************************#\n\n";)

    for (const auto &msg : broadcast_messages)
    {
      mt_logging::logger().log(
          {std::getenv("REDIS_PUBSUB_SUBSCRIBER_LOGFILE"),
           fmt::format("Message: {} ", msg),
           std::ios::app,
           true});
    }

    awake++;
    m_cond_not_awake.notify_one();
  };

  virtual void stop()
  {
    {
      std::unique_lock<std::mutex> cl(m_class_lock);
      shall_stop_awaken = true;
      m_cond_not_awake.notify_one();
    }
  }
};

#endif // NETPROC_AWAKENER_WAITABLE_H_