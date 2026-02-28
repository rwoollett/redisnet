
#ifndef LIB_REDIS_PUBLISH_H_
#define LIB_REDIS_PUBLISH_H_

#ifdef NDEBUG
    #define D(x)
#else
    #include "logsync.h"
    #define D(x) do { std::lock_guard<std::mutex> lock(g_cout_mutex); x; } while(0);
#endif

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

  constexpr int BATCH_SIZE = 10;
  constexpr int CHANNEL_LENGTH = 64;
  constexpr int MESSAGE_LENGTH = 256;
  constexpr int QUEUE_LENGTH = 128;

  struct PublishMessage
  {
    char channel[CHANNEL_LENGTH];
    char message[MESSAGE_LENGTH];
  };

  class Publish
  {
    asio::io_context m_ioc;
    std::shared_ptr<redis::connection> m_conn;
    boost::lockfree::spsc_queue<PublishMessage, boost::lockfree::capacity<QUEUE_LENGTH>> msg_queue; // Lock-free queue
    std::atomic<bool> m_is_connected;
    std::atomic<bool> m_signal_status;
    std::atomic<std::sig_atomic_t> m_reconnect_count;
    std::thread m_sender_thread;

  public:
    Publish();
    virtual ~Publish();

    bool is_redis_signaled() { return m_signal_status.load(); };
    bool is_redis_connected() { return m_is_connected.load(); };

    void enqueue_message(const std::string &channel, const std::string &message);

  private:
    asio::awaitable<void> co_main();
    asio::awaitable<void> process_messages();
  };

  class Sender : public std::enable_shared_from_this<Sender>
  {
    RedisPublish::Publish &m_redis_publisher;

  public:
    Sender(RedisPublish::Publish &publisher) : m_redis_publisher{publisher} {};
    ~Sender() {};

    void Send(const std::string &channel, const std::string &message)
    {
      m_redis_publisher.enqueue_message(channel, message);
    };
  };

} /* namespace RedisPublish */
#endif // HAVE_ASIO
#endif /* LIB_REDIS_PUBLISH_H_ */
