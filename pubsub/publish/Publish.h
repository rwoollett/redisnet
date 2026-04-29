
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

  template <typename T, size_t Capacity>
  class BlockingSPSCQueue
  {
  public:
    BlockingSPSCQueue() : m_shutdown(false) {}

    // Non-blocking push (same as your current queue)
    bool push(const T &item)
    {
      bool ok = m_queue.push(item);
      if (ok)
      {
        std::lock_guard<std::mutex> lock(m_mtx);
        m_cv.notify_one();
      }
      return ok;
    }

    // Non-blocking pop (same as your current queue)
    bool pop(T &out)
    {
      return m_queue.pop(out);
    }

    // Blocking pop: waits until item available or shutdown
    bool blocking_pop(T &out)
    {
      std::unique_lock<std::mutex> lock(m_mtx);

      m_cv.wait(lock, [&]
                { return m_shutdown || !m_queue.empty(); });

      if (m_shutdown)
        return false;

      return m_queue.pop(out);
    }

    // Signal shutdown and wake any waiting consumer
    void shutdown()
    {
      {
        std::lock_guard<std::mutex> lock(m_mtx);
        m_shutdown = true;
      }
      m_cv.notify_all();
    }

    bool empty() const
    {
      return m_queue.empty();
    }

  private:
    boost::lockfree::spsc_queue<T, boost::lockfree::capacity<Capacity>> m_queue;
    mutable std::mutex m_mtx;
    std::condition_variable m_cv;
    bool m_shutdown;
  };

  static std::atomic<std::sig_atomic_t> MESSAGE_QUEUED_COUNT = 0;
  static std::atomic<std::sig_atomic_t> MESSAGE_COUNT = 0;
  static std::atomic<std::sig_atomic_t> SUCCESS_COUNT = 0;
  static std::atomic<std::sig_atomic_t> PUBLISHED_COUNT = 0;

  constexpr int BATCH_SIZE = 1;
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
    BlockingSPSCQueue<PublishMessage, QUEUE_LENGTH> msg_queue; // pop blocking Lock-free queue
    std::mutex m_pub_mutex;

    std::atomic<bool> m_signal_status{false};
    std::atomic<bool> m_shutting_down{false};
    std::atomic<bool> m_conn_alive{false};
    std::atomic<std::sig_atomic_t> m_reconnect_count{0};
    std::thread m_sender_thread;
    std::thread m_worker;
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
    // bool is_redis_connected() { return m_conn_alive.load(); };
    void enqueue_message(const std::string &channel, const std::string &message);

  private:
    asio::awaitable<void> co_main();
    asio::awaitable<void> publish_one(const PublishMessage &msg);
    void worker_thread_fn(boost::asio::any_io_executor ex);

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
      m_redis_publisher.enqueue_message(channel, message);
    };
  };

} /* namespace RedisPublish */
#endif // HAVE_ASIO
#endif /* LIB_REDIS_PUBLISH_H_ */
