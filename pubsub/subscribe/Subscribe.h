
#ifndef LIB_REDIS_SUBSCRIBE_H_
#define LIB_REDIS_SUBSCRIBE_H_

#ifdef NDEBUG
#define D(x)
#else
#define D(x) x
#endif

#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <mutex>
#include <condition_variable>
#include <stdexcept>

#ifdef HAVE_ASIO
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/redis/connection.hpp>
#include <boost/redis/logger.hpp>
#include <boost/asio/io_context.hpp>
#include <thread>
#include <iostream>

namespace asio = boost::asio;
namespace redis = boost::redis;

namespace RedisSubscribe
{

  class Awakener
  {

  public:
    // The base class will print the messages.
    // It is able to be overridden in a derived class if you want to handle the messages differently.
    virtual void broadcast_messages(const std::list<std::string> &broadcast_messages)
    {
      std::cout << "Awakener::broadcast_messages\n";
      if (broadcast_messages.empty())
        return; // If there are no messages, do not update.
      // The base class will print the messages.
      std::cout << "- Broadcast subscribed messages\n";
      for (const auto &msg : broadcast_messages)
      {
        std::cout << msg << std::endl;
      }
      std::cout << std::endl;
      D(std::cout << "******************************************************#\n\n";)
    };

    virtual void on_subscribe()
    {
      std::cout << "Awakener::on_subscribe\n";
      // do nothing in base class
    };

    virtual void stop()
    {
      std::cout << "Awakener::stop\n";
      // do nothing in base class
    };
  };

  class Subscribe
  {
    asio::io_context m_ioc;
    std::shared_ptr<redis::connection> m_conn;
    volatile std::sig_atomic_t m_signal_status;
    int m_subscribed_count{0};
    int m_mssage_count{0};
    volatile std::sig_atomic_t m_is_connected;
    std::thread m_receiver_thread;
    int m_reconnect_count{0};

  public:
    Subscribe();
    virtual ~Subscribe();

    asio::awaitable<void> receiver(Awakener &awakener);
    asio::awaitable<void> co_main(Awakener &awakener);
    virtual auto main_redis(Awakener &awakener) -> int;
    virtual bool is_signal_stopped() { return (m_signal_status == 1); };
    bool is_redis_connected() { return (m_is_connected == 1); };

  };

} /* namespace RedisSubscribe */
#endif // HAVE_ASIO
#endif /* LIB_REDIS_SUBSCRIBE_H_ */
