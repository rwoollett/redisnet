
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
        m_strand(asio::make_strand(m_ioc)),
        m_conn{}
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
        m_strand,
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

    if (m_conn)
    {
      boost::asio::post(m_strand, [conn = m_conn]
                        {
                          conn->cancel();
                          conn->reset_stream(); //
                        });
    }

    boost::asio::post(m_strand, [this]
                      { m_ioc.stop(); });

    if (m_sender_thread.joinable())
      m_sender_thread.join();

    mt_logging::logger().log({"Redis Publisher destroyed",
                              mt_logging::LogLevel::Debug,
                              true});
  }

  void Publish::dispatch_message(const std::string &channel, const std::string &message)
  {
    if (m_signal_status.load())
      return;

    if (m_state.load() != ConnectionState::Ready)
    {
      // Either drop, log, or buffer for later.
      // For now, just log and return.
      mt_logging::logger().log({fmt::format("Dropping publish while not Ready"), //: state={}", to_str(m_state.load()) ),
                                mt_logging::LogLevel::Info,
                                true});
      return;
    }

    PublishMessage msg{
        .channel = channel,
        .message = message};

    MESSAGE_QUEUED_COUNT.fetch_add(1, std::memory_order_relaxed);

    asio::dispatch(
        m_strand,
        [this, msg = std::move(msg)]() mutable
        {
          asio::co_spawn(
              m_strand,
              publish_one(std::move(msg)),
              asio::detached);
        });
  }

  asio::awaitable<void> Publish::publish_one(const PublishMessage msg)
  {
    auto ex = co_await asio::this_coro::executor;
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
      if (m_state.load() == ConnectionState::Ready)
      {
        set_state(ConnectionState::Broken, "Publish timeout");
        conn->cancel();
        conn->reset_stream();
        m_conn_alive.store(false);
      }
      co_return;
    }

    // Exec completed first: check error
    if (exec_ec)
    {
      if (m_shutting_down.load() &&
          exec_ec == boost::asio::error::operation_aborted)
        co_return;

      if (m_state.load() != ConnectionState::Ready)
        co_return; // don't fight startup / reconnect

      set_state(ConnectionState::Broken,
                fmt::format("Publish failed: {}", exec_ec.message()));
      conn->cancel();
      conn->reset_stream();
      m_conn_alive.store(false);
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

                         // Prevent reconnect loop from continuing
                         m_conn_alive.store(false);
                         if (m_conn)
                           m_conn->cancel();
                         //
                       });

    // --- Main reconnect loop ---
    for (;;)
    {
      if (m_shutting_down.load())
        co_return;

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

      set_state(ConnectionState::Connecting, "Starting async_run");
      m_conn_alive.store(true);

      m_conn->async_run(
          cfg,
          redis::logger{redis::logger::level::err},
          asio::consign(asio::detached, [this]
                        {
                          if (m_shutting_down.load())
                            return; // ignore during shutdown
                          set_state(ConnectionState::Broken, "async_run ended");
                          m_conn_alive.store(false);
                          //
                        }));

      // --- Confirm Redis is actually reachable ---
      // --- Startup PING with timeout ---
      {
        redis::request ping;
        ping.push("PING");

        boost::system::error_code ping_ec;
        boost::system::error_code ping_timer_ec;

        asio::steady_timer ping_timer{ex};
        ping_timer.expires_after(std::chrono::seconds(PUBLISH_TIMEOUT_DELAY));

        auto ping_result = co_await (
            m_conn->async_exec(
                ping,
                boost::redis::ignore,
                asio::redirect_error(asio::use_awaitable, ping_ec)) ||
            ping_timer.async_wait(
                asio::redirect_error(asio::use_awaitable, ping_timer_ec)));

        // Timer fired
        if (ping_result.index() == 1)
        {
          set_state(ConnectionState::Broken, "Startup PING timeout");
          m_conn_alive.store(false);
          m_conn->cancel();
          m_conn->reset_stream();

          set_state(ConnectionState::Reconnecting,
                    fmt::format("Retrying after failed PING in {} seconds",
                                CONNECTION_RETRY_DELAY));

          co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY))
              .async_wait(asio::use_awaitable);
          continue; // next attempt
        }

        // Exec error
        if (ping_ec)
        {
          if (m_shutting_down.load() &&
              ping_ec == boost::asio::error::operation_aborted)
            co_return;

          set_state(ConnectionState::Broken,
                    fmt::format("Startup PING failed: {}", ping_ec.message()));

          m_conn_alive.store(false);
          m_conn->cancel();
          m_conn->reset_stream();

          set_state(ConnectionState::Reconnecting,
                    fmt::format("Retrying after failed PING in {} seconds",
                                CONNECTION_RETRY_DELAY));

          co_await asio::steady_timer(ex, std::chrono::seconds(CONNECTION_RETRY_DELAY))
              .async_wait(asio::use_awaitable);
          continue; // next attempt
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
