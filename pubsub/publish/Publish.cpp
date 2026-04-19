
#include "Publish.h"
#include <memory>
#include <fstream>
#include <sstream>
#include <string>
#include <tuple>
#include <mtlog/mt_log.hpp>

#ifdef HAVE_ASIO

#include <boost/asio/connect.hpp>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/redis/response.hpp>
#include <boost/redis/request.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
using namespace boost::asio::experimental::awaitable_operators;

namespace RedisPublish
{
  static const char *REDIS_HOST = std::getenv("REDIS_HOST");
  static const char *REDIS_PORT = std::getenv("REDIS_PORT");
  static const char *REDIS_PASSWORD = std::getenv("REDIS_PASSWORD");
  static const char *REDIS_USE_SSL = std::getenv("REDIS_USE_SSL");
  static const char *MTLOG_LOGFILE = std::getenv("MTLOG_LOGFILE");
  static const int CONNECTION_RETRY_AMOUNT = -1;
  static const int CONNECTION_RETRY_DELAY = 3; // ensure longer than 1 sec to avoid reconnect ssl rteradown abort
  static const int PUBLISH_TIMEOUT_DELAY = 2;  // ensure longer than 1 sec to avoid reconnect ssl rteradown abort

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
      mt_logging::logger().log(
          {fmt::format("Publish::load certiciates {}", e.what()),
           mt_logging::LogLevel::Error,
           true});
    }
  }

  Publish::Publish()
      : m_ioc{2},
        m_conn{},
        msg_queue{}
  {
    if (MTLOG_LOGFILE == nullptr ||
        REDIS_HOST == nullptr ||
        REDIS_PORT == nullptr ||
        REDIS_PASSWORD == nullptr ||
        REDIS_USE_SSL == nullptr)
    {
      throw std::runtime_error("Environment variables MTLOG_LOGFILE, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD and REDIS_USE_SSL must be set.");
    }
    MESSAGE_QUEUED_COUNT.store(0);
    MESSAGE_COUNT.store(0);
    SUCCESS_COUNT.store(0);
    PUBLISHED_COUNT.store(0);
    m_signal_status.store(false);
    m_reconnect_count.store(0);
    m_shutting_down.store(false);
    m_conn_alive.store(false);
    m_state.store(ConnectionState::Idle);

    asio::co_spawn(
        m_ioc.get_executor(),
        [this]() -> asio::awaitable<void>
        {
          co_return co_await this->co_main();
        },
        asio::detached);

    m_sender_thread = std::thread([this]()
                                  { m_ioc.run(); });
  }

  Publish::~Publish()
  {
    m_shutting_down.store(true);

    msg_queue.shutdown();
    msg_queue.push(PublishMessage{}); // dummy wake

    if (m_worker.joinable())
      m_worker.join();

    if (m_conn)
    {
      boost::asio::post(m_ioc, [conn = m_conn]
                        {
                          conn->cancel();
                          conn->reset_stream(); //
                        });
    }

    boost::asio::post(m_ioc, [this]
                      { m_ioc.stop(); });

    if (m_sender_thread.joinable())
      m_sender_thread.join();

    mt_logging::logger().log({"Redis Publisher destroyed",
                              mt_logging::LogLevel::Debug,
                              true});
  }

  void Publish::enqueue_message(const std::string &channel, const std::string &message)
  {
    if (m_signal_status.load())
      return;
    PublishMessage msg;
    std::strncpy(msg.channel, channel.c_str(), CHANNEL_LENGTH - 1);
    msg.channel[CHANNEL_LENGTH - 1] = '\0'; // Always null-terminate
    std::strncpy(msg.message, message.c_str(), MESSAGE_LENGTH - 1);
    msg.message[MESSAGE_LENGTH - 1] = '\0'; // Always null-terminate

    MESSAGE_QUEUED_COUNT.fetch_add(1, std::memory_order_relaxed);
    msg_queue.push(msg);
  }

  asio::awaitable<void> Publish::publish_one(const PublishMessage &msg)
  {
    auto ex = co_await asio::this_coro::executor;
    // Take a strong ref to whatever connection is current *now*
    auto conn = m_conn;
    if (!conn)
      co_return;

    redis::request req;
    req.push("PUBLISH", msg.channel, msg.message);

    boost::system::error_code exec_ec;
    boost::system::error_code timer_ec;
    redis::generic_response resp;

    asio::steady_timer timer{ex};
    timer.expires_after(std::chrono::seconds(PUBLISH_TIMEOUT_DELAY)); // publish timeout

    // Wait for EITHER exec OR timer
    auto result = co_await (
        conn->async_exec(
            req,
            resp,
            asio::redirect_error(asio::use_awaitable, exec_ec)) ||
        timer.async_wait(
            asio::redirect_error(asio::use_awaitable, timer_ec)));

    // Timer fired first → async_exec is considered hung
    if (result.index() == 1)
    {
      set_state(ConnectionState::Broken, "Publish timeout");
      conn->cancel();
      conn->reset_stream();
      m_conn_alive.store(false);

      // if (!m_shutting_down.load())
      // {
      //   msg_queue.push(msg);
      // }

      // mt_logging::logger().log({REDIS_PUBSUB_PUBLISHER_LOGFILE,
      //                           "Redis publish timed out. Requeueing message and forcing reconnect.",
      //                           std::ios::app,
      //                           true});

      co_return;
    }

    // Exec completed first: check error
    if (exec_ec)
    {
      set_state(ConnectionState::Broken,
                fmt::format("Publish failed: {}", exec_ec.message()));
      conn->cancel();
      conn->reset_stream();
      m_conn_alive.store(false);

      // if (!m_shutting_down.load())
      // {
      //   msg_queue.push(msg);
      // }

      // mt_logging::logger().log({REDIS_PUBSUB_PUBLISHER_LOGFILE,
      //                           fmt::format("Redis publish failed: {}. {}",
      //                                       exec_ec.message(),
      //                                       m_shutting_down.load()
      //                                           ? "Shutdown in progress, dropping message."
      //                                           : "Requeueing message."),
      //                           std::ios::app,
      //                           true});

      co_return;
    }

    // Success path
    for (const auto &node : resp.value())
    {
      if (node.data_type == redis::resp3::type::number)
      {
        if (std::atoi(std::string(node.value).c_str()) > 0)
          SUCCESS_COUNT.fetch_add(1, std::memory_order_relaxed);
      }
      PUBLISHED_COUNT.fetch_add(1, std::memory_order_relaxed);
    }

    MESSAGE_COUNT.fetch_sub(1, std::memory_order_relaxed);
    set_state(ConnectionState::Ready, fmt::format("Publish OK: {} {}", msg.channel, msg.message));
  }

  void Publish::worker_thread_fn(boost::asio::any_io_executor ex)
  {
    for (;;)
    {
      PublishMessage msg;
      // Blocking wait
      if (!msg_queue.blocking_pop(msg))
        break; // queue shutdown

      if (m_shutting_down.load())
        break;

      // Hand off to Asio thread
      asio::post(ex, [this, msg, ex]
                 { asio::co_spawn(
                       ex,
                       [this, msg]() -> asio::awaitable<void>
                       {
                         co_return co_await publish_one(msg);
                       },
                       asio::detached); });
    }

    // --- Shutdown cleanup ---
    if (m_shutting_down.load())
    {
      PublishMessage leftover;
      int dropped = 0;

      while (msg_queue.pop(leftover))
        dropped++;

      mt_logging::logger().log({.line = fmt::format("Redis publish shutdown: dropped {} pending messages", dropped),
                                .level = mt_logging::LogLevel::Info,
                                .include_thread_id = true});
    }
  }

  asio::awaitable<void> Publish::co_main()
  {
    auto ex = co_await asio::this_coro::executor;

    // --- Setup Redis config ---
    redis::config cfg;
    cfg.clientname = "redis_publish";
    cfg.addr.host = REDIS_HOST;
    cfg.addr.port = REDIS_PORT;
    cfg.password = REDIS_PASSWORD;
    cfg.health_check_interval = std::chrono::minutes(1); // keepalive

    if (std::string(REDIS_USE_SSL) == "on")
      cfg.use_ssl = true;

    // --- Setup signal handler ---
    boost::asio::signal_set sig_set(ex, SIGINT, SIGTERM);
#if defined(SIGQUIT)
    sig_set.add(SIGQUIT);
#endif

    sig_set.async_wait([this](auto, int)
                       {
                         set_state(ConnectionState::Shutdown, "Signal received");
                         m_signal_status.store(true);
                         m_shutting_down.store(true);
                         if (m_conn)
                           m_conn->cancel();
                         msg_queue.shutdown();
                         //
                       });

    // --- Start worker thread ---
    m_worker = std::thread([this, ex]
                           { this->worker_thread_fn(ex); });

    // --- Create connection ---
    if (std::string(REDIS_USE_SSL) == "on")
    {
      asio::ssl::context ssl_ctx{asio::ssl::context::tlsv12_client};
      ssl_ctx.set_verify_mode(asio::ssl::verify_peer);
      load_certificates(ssl_ctx, "tls/ca.crt", "tls/redis.crt", "tls/redis.key");
      ssl_ctx.set_verify_callback(verify_certificate);

      m_conn = std::make_shared<redis::connection>(ex, std::move(ssl_ctx));
    }
    else
    {
      m_conn = std::make_shared<redis::connection>(ex);
    }

    // --- Main reconnect loop ---
    for (;;)
    {
      if (m_shutting_down.load())
        co_return;

      set_state(ConnectionState::Connecting, "Starting async_run");
      m_conn_alive.store(true);

      m_conn->async_run(
          cfg,
          redis::logger{redis::logger::level::err},
          asio::consign(asio::detached, [this]
                        {
                set_state(ConnectionState::Broken, "async_run ended");
                m_conn_alive.store(false); }));

      // --- Confirm Redis is actually reachable ---
      {
        redis::request ping;
        ping.push("PING");

        boost::system::error_code ec;
        co_await m_conn->async_exec(
            ping,
            boost::redis::ignore,
            asio::redirect_error(asio::use_awaitable, ec));

        if (ec)
        {
          set_state(ConnectionState::Broken,
                    fmt::format("Startup PING failed: {}", ec.message()));

          m_conn_alive.store(false);
          m_conn->cancel();
          m_conn->reset_stream();

          set_state(ConnectionState::Reconnecting, "Retrying after failed PING");

          co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY))
              .async_wait(asio::use_awaitable);

          continue; // reconnect loop
        }
      }

      // --- Redis is confirmed UP ---
      set_state(ConnectionState::Ready, "Redis connection established");

      // --- Wait until connection dies or shutdown requested ---
      while (!m_shutting_down.load() && m_conn_alive.load())
      {
        co_await asio::steady_timer(ex, std::chrono::milliseconds(200))
            .async_wait(asio::use_awaitable);
      }

      if (m_shutting_down.load())
        co_return;

      set_state(ConnectionState::Broken, "Connection dropped");

      m_reconnect_count.fetch_add(1, std::memory_order_relaxed);

      set_state(ConnectionState::Reconnecting,
                fmt::format("Reconnect attempt {} in {} seconds",
                            m_reconnect_count.load(), CONNECTION_RETRY_DELAY));

      // Delay before reconnect
      co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY))
          .async_wait(asio::use_awaitable);

      if (CONNECTION_RETRY_AMOUNT != -1 &&
          m_reconnect_count >= CONNECTION_RETRY_AMOUNT)
      {
        break;
      }
    }

    set_state(ConnectionState::Shutdown, "Exiting co_main");
    m_signal_status.store(true);
  }

  void Publish::set_state(ConnectionState new_state, std::string_view reason)
  {
    ConnectionState old = m_state.exchange(new_state);

    auto to_str = [](ConnectionState s)
    {
      switch (s)
      {
      case ConnectionState::Idle:
        return "Idle";
      case ConnectionState::Connecting:
        return "Connecting";
      case ConnectionState::Authenticating:
        return "Authenticating";
      case ConnectionState::Ready:
        return "Ready";
      case ConnectionState::Broken:
        return "Broken";
      case ConnectionState::Reconnecting:
        return "Reconnecting";
      case ConnectionState::Shutdown:
        return "Shutdown";
      }
      return "Unknown";
    };

    mt_logging::logger().log({fmt::format("State: {} → {} ({})",
                                          to_str(old), to_str(new_state), reason),
                              mt_logging::LogLevel::Info,
                              true});
  }

#endif // defined(BOOST_ASIO_HAS_CO_AWAIT)

} /* namespace RedisPublish */
#endif

// asio::awaitable<void> Publish::process_messages()
// {
//   boost::system::error_code ec;
//   redis::request ping_req;
//   ping_req.push("PING");

//   co_await m_conn->async_exec(ping_req, boost::redis::ignore, asio::redirect_error(asio::deferred, ec));
//   if (ec)
//   {
//     m_is_connected.store(false);
//     mt_logging::logger().log(
//         {REDIS_PUBSUB_PUBLISHER_LOGFILE,
//          "PING unsuccessful",
//          std::ios::app,
//          true});
//     co_return; // Connection lost, break so we can exit function and try reconnect to redis.
//   }
//   else
//   {
//     mt_logging::logger().log(
//         {REDIS_PUBSUB_PUBLISHER_LOGFILE,
//          "PING successful",
//          std::ios::app,
//          true});
//   }
///////////////////////
// asio::awaitable<void> Publish::publish_one(const PublishMessage &msg)
// {
//   std::cerr << "publish one\n";
//   redis::request req;
//   req.push("PUBLISH", msg.channel, msg.message);

//   boost::system::error_code ec;
//   redis::generic_response resp;

//   co_await m_conn->async_exec(req, resp, asio::redirect_error(asio::use_awaitable, ec));

//   std::cerr << "publish one after async exec\n";

//   if (ec)
//   {
//     std::cerr << "ec found after async exec\n";
//     // Requeue ONLY the failed message
//     if (!m_shutting_down.load())
//     {
//       msg_queue.push(msg);
//     }

//     mt_logging::logger().log({REDIS_PUBSUB_PUBLISHER_LOGFILE,
//                               fmt::format("Redis publish failed: {}. {}",
//                                           ec.message(),
//                                           m_shutting_down.load()
//                                               ? "Shutdown in progress, dropping message."
//                                               : "Requeueing message."),
//                               std::ios::app,
//                               true});

//     co_return; // trigger reconnect
//   }

//   // Count success
//   for (const auto &node : resp.value())
//   {
//     if (node.data_type == redis::resp3::type::number)
//     {
//       if (std::atoi(std::string(node.value).c_str()) > 0)
//         SUCCESS_COUNT.fetch_add(1, std::memory_order_relaxed);
//     }
//     PUBLISHED_COUNT.fetch_add(1, std::memory_order_relaxed);
//   }

//   MESSAGE_COUNT.fetch_sub(1, std::memory_order_relaxed);

//   mt_logging::logger().log({REDIS_PUBSUB_PUBLISHER_LOGFILE,
//                             fmt::format("Redis publish OK: {} {}",
//                                         msg.channel, msg.message),
//                             std::ios::app,
//                             true});
// }
