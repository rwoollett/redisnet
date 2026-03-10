
#include "Subscribe.h"
#include <memory>
#include <fstream>
#include <sstream>
#include <string>
#include <mtlog/mt_log.hpp>

#ifdef HAVE_ASIO

#include <boost/asio/connect.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/lexical_cast.hpp>

namespace RedisSubscribe
{

  static const char *REDIS_HOST = std::getenv("REDIS_HOST");
  static const char *REDIS_PORT = std::getenv("REDIS_PORT");
  static const char *REDIS_CHANNEL = std::getenv("REDIS_CHANNEL");
  static const char *REDIS_PASSWORD = std::getenv("REDIS_PASSWORD");
  static const char *REDIS_USE_SSL = std::getenv("REDIS_USE_SSL");
  static const char *REDIS_PUBSUB_SUBSCRIBER_LOGFILE = std::getenv("REDIS_PUBSUB_SUBSCRIBER_LOGFILE");

  static const int CONNECTION_RETRY_AMOUNT = -1;
  static const int CONNECTION_RETRY_DELAY = 3;

  void Awakener::broadcast_messages(std::list<std::string> broadcast_messages)
  {
    if (broadcast_messages.empty())
      return;
    mt_logging::logger().log({REDIS_PUBSUB_SUBSCRIBER_LOGFILE,
                              fmt::format("- Broadcast subscribed messages {}", fmt::join(broadcast_messages, ", ")),
                              std::ios::app,
                              true});
  };

  std::list<std::string> split_by_comma(const char *str)
  {
    std::list<std::string> result;
    if (str == nullptr)
    {
      return result; // Return an empty list if the input is null
    }

    std::stringstream ss(str);
    std::string token;

    // Split the string by commas
    while (std::getline(ss, token, ','))
    {
      result.push_back(token);
    }

    return result;
  }

#if defined(BOOST_ASIO_HAS_CO_AWAIT)

  auto verify_certificate(bool, asio::ssl::verify_context &) -> bool
  {
    return true;
  }
  // Helper to load a file into an SSL context
  void load_certificates(asio::ssl::context &ctx,
                         const std::string &ca_file,
                         const std::string &cert_file,
                         const std::string &key_file)
  {
    try
    {
      // Load trusted CA
      ctx.load_verify_file(ca_file);

      // Load client certificate
      ctx.use_certificate_file(cert_file, asio::ssl::context::pem);

      // Load private key
      ctx.use_private_key_file(key_file, asio::ssl::context::pem);
    }
    catch (const std::exception &e)
    {
      mt_logging::logger().log({REDIS_PUBSUB_SUBSCRIBER_LOGFILE,
                                fmt::format("Subscribe::load certiciates {}", e.what()),
                                std::ios::app,
                                true});
    }
  }

  Subscribe::Subscribe(Awakener &awakener) : m_ioc{1},
                                             m_awakener(awakener),
                                             m_conn{}
  {
    m_is_connected.store(false);
    m_signal_status.store(false);
    m_reconnect_count.store(0);
    m_subscribed_count.store(0);
    m_mssage_count.store(0);

    if (REDIS_PUBSUB_SUBSCRIBER_LOGFILE == nullptr ||
        REDIS_HOST == nullptr ||
        REDIS_PORT == nullptr ||
        REDIS_CHANNEL == nullptr ||
        REDIS_PASSWORD == nullptr ||
        REDIS_USE_SSL == nullptr)
    {
      throw std::runtime_error("Environment variables REDIS_PUBSUB_SUBSCRIBER_LOGFILE, REDIS_HOST, REDIS_PORT, REDIS_CHANNEL, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }
    mt_logging::logger().log({REDIS_PUBSUB_SUBSCRIBER_LOGFILE,
                              "Subscriber created",
                              std::ios::app,
                              true});
  }

  Subscribe::~Subscribe()
  {
    request_stop();
    join();
    mt_logging::logger().log({REDIS_PUBSUB_SUBSCRIBER_LOGFILE,
                              "Subscriber destroyed",
                              std::ios::app,
                              true});
  }

  void Subscribe::request_stop()
  {
    m_signal_status.store(true);

    // Wake Redis operations
    if (m_conn)
    {
      boost::asio::post(m_ioc, [conn = m_conn]
                        { conn->cancel(); });
    }

    // Wake the awakener
    m_awakener.stop();

    // Stop the io_context on its own thread
    boost::asio::post(m_ioc, [this]
                      { m_ioc.stop(); });
  }

  void Subscribe::join()
  {
    if (m_receiver_thread.joinable())
      m_receiver_thread.join();
  }

  auto Subscribe::receiver() -> asio::awaitable<void>
  {
    std::list<std::string> channels = split_by_comma(REDIS_CHANNEL);
    mt_logging::logger().log({REDIS_PUBSUB_SUBSCRIBER_LOGFILE,
                              fmt::format("- Broadcast subscribed channels {}", fmt::join(channels, ", ")),
                              std::ios::app,
                              true});

    redis::request req;
    req.push_range("SUBSCRIBE", channels);

    redis::generic_response resp;
    m_conn->set_receive_response(resp);
    co_await m_conn->async_exec(req, redis::ignore, asio::deferred);

    // awakener.on_subscribe();
    m_is_connected.store(true);
    m_reconnect_count.store(0); // reset
    for (boost::system::error_code ec;;)
    {
      if (m_signal_status.load())
      {
        co_return;
      }
      m_conn->receive(ec);
      if (ec == redis::error::sync_receive_push_failed)
      {
        ec = {};
        co_await m_conn->async_receive(asio::redirect_error(asio::use_awaitable, ec));
      }

      if (ec)
      {
        mt_logging::logger().log({REDIS_PUBSUB_SUBSCRIBER_LOGFILE,
                                  fmt::format("- Subscribe::receiver ec  {}", ec.message()),
                                  std::ios::app,
                                  true});
        co_return; // Connection lost, break so we can reconnect to channels.
      }

      int index = 0;
      std::list<std::string> messages;
      for (const auto &node : resp.value())
      {
        auto ancestorNode = (index > 1) ? resp.value().at(index - 2) : node;
        auto prevNode = (index > 0) ? resp.value().at(index - 1) : node;
        if (node.data_type == boost::redis::resp3::type::simple_error)
        {
          mt_logging::logger().log({REDIS_PUBSUB_SUBSCRIBER_LOGFILE,
                                    fmt::format("- Subscribe::receiver error: {}", node.value),
                                    std::ios::app,
                                    true});
          continue; // Skip to the next node
        }
        if (node.data_type == boost::redis::resp3::type::blob_string ||
            node.data_type == boost::redis::resp3::type::simple_string)
        {
          auto msg = node.value;
          if (msg == "subscribe")
          {
            m_subscribed_count.fetch_add(1, std::memory_order_relaxed);
          }
          else if (msg == "message")
          {
            m_mssage_count.fetch_add(1, std::memory_order_relaxed);
          }
          else
          {
            if (ancestorNode.value == "message")
            {
              std::stringstream ss;
              ss << prevNode.value << " message: " << msg;
              messages.push_back(msg);
            }
          }
        }
        index++;
      }

      if (messages.size() > 0)
      {
        mt_logging::logger().log(
            {REDIS_PUBSUB_SUBSCRIBER_LOGFILE,
             fmt::format(
                 "{} subscribed, {} successful message received. {} messages in this response. ",
                 m_subscribed_count.load(),
                 m_mssage_count.load(),
                 messages.size()),
             std::ios::app,
             true});

        m_awakener.broadcast_messages(std::move(messages));
      }
      resp.value().clear();
      redis::consume_one(resp);
    }
  }

  auto Subscribe::co_main() -> asio::awaitable<void>
  {
    auto ex = co_await asio::this_coro::executor;
    redis::config cfg;
    cfg.addr.host = REDIS_HOST;
    cfg.addr.port = REDIS_PORT;
    cfg.password = REDIS_PASSWORD;
    if (std::string(REDIS_USE_SSL) == "on")
    {
      cfg.use_ssl = true;
    }

    boost::asio::signal_set sig_set(ex, SIGINT, SIGTERM);
#if defined(SIGQUIT)
    sig_set.add(SIGQUIT);
#endif // defined(SIGQUIT)
    sig_set.async_wait(
        [&](const boost::system::error_code &, int)
        {
          m_signal_status.store(true);
          m_awakener.stop();

          if (m_conn)
          {
            m_conn->cancel();
          }
        });

    for (;;)
    {
      if (m_signal_status.load())
      {
        co_return;
      }
      if (std::string(REDIS_USE_SSL) == "on")
      {
        asio::ssl::context ssl_ctx{asio::ssl::context::tlsv12_client};
        ssl_ctx.set_verify_mode(asio::ssl::verify_peer);
        load_certificates(ssl_ctx,
                          "tls/ca.crt",    // Your self-signed CA
                          "tls/redis.crt", // Your client certificate
                          "tls/redis.key"  // Your private key
        );
        ssl_ctx.set_verify_callback(verify_certificate);
        m_conn = std::make_shared<redis::connection>(ex, std::move(ssl_ctx));
      }
      else
      {
        m_conn = std::make_shared<redis::connection>(ex);
      }

      m_conn->async_run(cfg, redis::logger{redis::logger::level::err}, asio::consign(asio::detached, m_conn));

      try
      {
        co_await receiver();
      }
      catch (const std::exception &e)
      {
        mt_logging::logger().log(
            {REDIS_PUBSUB_SUBSCRIBER_LOGFILE,
             fmt::format("Redis subscribe error: {}", e.what()),
             std::ios::app,
             true});
      }

      // Delay before reconnecting
      m_is_connected.store(false);
      m_reconnect_count.fetch_add(1, std::memory_order_relaxed);

      mt_logging::logger().log(
          {REDIS_PUBSUB_SUBSCRIBER_LOGFILE,
           fmt::format("Receiver exited {} times, reconnecting in {} seconds...", m_reconnect_count.load(), CONNECTION_RETRY_DELAY),
           std::ios::app,
           true});

      m_conn->cancel();

      co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY)).async_wait(asio::use_awaitable);

      if (CONNECTION_RETRY_AMOUNT == -1)
        continue;
      if (m_reconnect_count.load() >= CONNECTION_RETRY_AMOUNT)
      {
        break;
      }
    }
    m_signal_status.store(true);
    m_awakener.stop();
  }

  auto Subscribe::main_redis() -> int
  {
    try
    {
      // net::io_context ioc;
      asio::co_spawn(m_ioc.get_executor(), Subscribe::co_main(),
                     [](std::exception_ptr p)
                     {
                       if (p)
                         std::rethrow_exception(p);
                     });
      m_receiver_thread = std::thread([this]()
                                      { m_ioc.run(); });
      return 0;
    }
    catch (std::exception const &e)
    {
      mt_logging::logger().log(
          {REDIS_PUBSUB_SUBSCRIBER_LOGFILE,
           fmt::format("subscribe (main_redis) {}", e.what()),
           std::ios::app,
           true});
      return 1;
    }
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace RedisSubscribe */
#endif