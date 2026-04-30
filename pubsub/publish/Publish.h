
#ifndef LIB_REDIS_PUBLISH_H_
#define LIB_REDIS_PUBLISH_H_

#include <fstream>
#include <iostream>
#include <string>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <atomic>

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
#include <boost/asio/strand.hpp>
#include <boost/redis/connection.hpp>
#include <boost/redis/logger.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <thread>
#include <iostream>
#include <memory>

namespace asio = boost::asio;
namespace redis = boost::redis;

namespace RedisPublish
{

  static std::atomic<std::sig_atomic_t> MESSAGE_QUEUED_COUNT = 0;
  static std::atomic<std::sig_atomic_t> MESSAGE_COUNT = 0;
  static std::atomic<std::sig_atomic_t> SUCCESS_COUNT = 0;
  static std::atomic<std::sig_atomic_t> PUBLISHED_COUNT = 0;

  constexpr int CHANNEL_LENGTH = 64;
  constexpr int MESSAGE_LENGTH = 256;

  struct PublishMessage
  {
    std::string channel;
    std::string message;
  };

  class Publish
  {
    asio::io_context m_ioc;
    asio::strand<asio::io_context::executor_type> m_strand;
    std::shared_ptr<redis::connection> m_conn;

    std::atomic<bool> m_signal_status{false};
    std::atomic<bool> m_shutting_down{false};
    std::atomic<bool> m_conn_alive{false};
    std::atomic<std::sig_atomic_t> m_reconnect_count{0};
    std::thread m_sender_thread;
    enum class ConnectionState
    {
      Idle,
      Connecting,
      Authenticating,
      Ready,
      Broken,
      Reconnecting,
      Shutdown
    };

    std::atomic<ConnectionState> m_state;

  public:
    Publish();
    virtual ~Publish();

    bool is_redis_signaled() { return m_signal_status.load(); };
    void dispatch_message(const std::string &channel, const std::string &message);

  private:
    asio::awaitable<void> co_main();
    asio::awaitable<void> publish_one(const PublishMessage msg);

    void set_state(ConnectionState new_state, std::string_view reason);
  };

  class Sender : public std::enable_shared_from_this<Sender>
  {
    RedisPublish::Publish &m_redis_publisher;

  public:
    Sender(RedisPublish::Publish &publisher) : m_redis_publisher{publisher} {};
    ~Sender() {};

    void Send(const std::string &channel, const std::string &message)
    {
      m_redis_publisher.dispatch_message(channel, message);
    };
  };

} /* namespace RedisPublish */
#endif // HAVE_ASIO
#endif /* LIB_REDIS_PUBLISH_H_ */
