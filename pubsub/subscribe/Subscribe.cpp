
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
  static const char *MTLOG_LOGFILE = std::getenv("MTLOG_LOGFILE");

  static const int CONNECTION_RETRY_AMOUNT = -1;
  static const int CONNECTION_RETRY_DELAY = 3;

  void Awakener::broadcast_messages(std::list<std::string> broadcast_messages)
  {
    if (broadcast_messages.empty())
      return;
    mt_logging::logger().log({fmt::format("- Broadcast subscribed messages {}", fmt::join(broadcast_messages, ", ")),
                              mt_logging::LogLevel::Info,
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
      mt_logging::logger().log({fmt::format("Subscribe::load certiciates {}", e.what()),
                                mt_logging::LogLevel::Error,
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
    m_state.store(SubConnectionState::Idle);

    if (MTLOG_LOGFILE == nullptr ||
        REDIS_HOST == nullptr ||
        REDIS_PORT == nullptr ||
        REDIS_CHANNEL == nullptr ||
        REDIS_PASSWORD == nullptr ||
        REDIS_USE_SSL == nullptr)
    {
      throw std::runtime_error("Environment variables MTLOG_LOGFILE, REDIS_HOST, REDIS_PORT, REDIS_CHANNEL, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }
  }

  Subscribe::~Subscribe()
  {
    request_stop();
    join();
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
    redis::request req;
    req.push_range("SUBSCRIBE", channels);

    set_state(SubConnectionState::Subscribing, fmt::format("Sending SUBSCRIBE command - subscribed channels {}", fmt::join(channels, ", ")));

    redis::generic_response resp;
    m_conn->set_receive_response(resp);

    co_await m_conn->async_exec(req, redis::ignore, asio::deferred);

    set_state(SubConnectionState::Ready, "Subscribed to channels");

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
        set_state(SubConnectionState::Broken, fmt::format("Receive error: {}", ec.message()));
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
          mt_logging::logger().log({fmt::format("- Subscribe::receiver error: {}", node.value),
                                    mt_logging::LogLevel::Error,
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
        m_awakener.broadcast_messages(std::move(messages));
      }
      set_state(SubConnectionState::Ready, "Message OK");

      resp.value().clear();
      redis::consume_one(resp);
    }
  }

  auto Subscribe::co_main() -> asio::awaitable<void>
  {
    auto ex = co_await asio::this_coro::executor;
    redis::config cfg;
    cfg.clientname = "redis_subcribe";
    cfg.addr.host = REDIS_HOST;
    cfg.addr.port = REDIS_PORT;
    cfg.password = REDIS_PASSWORD;
    if (std::string(REDIS_USE_SSL) == "on")
    {
      cfg.use_ssl = true;
    }
    cfg.health_check_interval = std::chrono::minutes(1); // set 0 for tls friendly

    boost::asio::signal_set sig_set(ex, SIGINT, SIGTERM);
#if defined(SIGQUIT)
    sig_set.add(SIGQUIT);
#endif // defined(SIGQUIT)
    sig_set.async_wait(
        [&](const boost::system::error_code &, int)
        {
          set_state(SubConnectionState::Shutdown, "Signal received");

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

      set_state(SubConnectionState::Connecting, "Starting async_run/Retrying connection");
      m_conn->async_run(cfg, redis::logger{redis::logger::level::err}, asio::consign(asio::detached, m_conn));
      set_state(SubConnectionState::Authenticating, "HELLO/AUTH handshake");

      try
      {
        co_await receiver();
      }
      catch (const std::exception &e)
      {
        mt_logging::logger().log(
            {fmt::format("Redis subscribe error: {}", e.what()),
             mt_logging::LogLevel::Error,
             true});
      }
      if (m_signal_status.load())
      {
        co_return;
      }

      // Delay before reconnecting
      m_is_connected.store(false);
      m_reconnect_count.fetch_add(1, std::memory_order_relaxed);
      m_conn->cancel();

      set_state(SubConnectionState::Reconnecting, fmt::format("Receiver exited {} times, reconnecting in {} seconds...", m_reconnect_count.load(), CONNECTION_RETRY_DELAY));

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
          {fmt::format("subscribe (main_redis) {}", e.what()),
           mt_logging::LogLevel::Error,
           true});
      return 1;
    }
  }

  void Subscribe::set_state(SubConnectionState new_state, std::string_view reason)
  {
    SubConnectionState old = m_state.exchange(new_state);

    auto to_str = [](SubConnectionState s)
    {
      switch (s)
      {
      case SubConnectionState::Idle:
        return "Idle";
      case SubConnectionState::Connecting:
        return "Connecting";
      case SubConnectionState::Authenticating:
        return "Authenticating";
      case SubConnectionState::Subscribing:
        return "Subscribing";
      case SubConnectionState::Ready:
        return "Ready";
      case SubConnectionState::Receiving:
        return "Receiving";
      case SubConnectionState::Broken:
        return "Broken";
      case SubConnectionState::Reconnecting:
        return "Reconnecting";
      case SubConnectionState::Shutdown:
        return "Shutdown";
      }
      return "Unknown";
    };

    mt_logging::logger().log({fmt::format("SUB State: {} → {} ({})",
                                          to_str(old), to_str(new_state), reason),
                              mt_logging::LogLevel::Info,
                              true});
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace RedisSubscribe */
#endif