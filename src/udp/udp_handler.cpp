#include "udp_handler.h"

#include <print>

UdpHandler::UdpHandler(std::shared_ptr<spdlog::logger> logger, uv_loop_t *loop, GlobalSettings *settings) : m_logger(logger), m_loop(loop), m_settings(settings)
{
    m_message.reserve(m_settings->udp_payload_size);
    for (std::size_t i = 0; i < m_settings->udp_payload_size; ++i)
    {
        m_message.push_back(static_cast<char>(i % 10) + '0');
    }
}

UdpHandler::~UdpHandler()
{
    PrintStats();
}

void UdpHandler::CreateSocket()
{
    CheckThread();
    if (uv_udp_init(m_loop, &m_udp_socket) != 0)
    {
        m_logger->error("Failed to initialize UDP socket");
        return;
    }

    // bind to random port
    struct sockaddr_in bind_addr;
    uv_ip4_addr("127.0.0.1", 0, &bind_addr);
    if (uv_udp_bind(&m_udp_socket, (const struct sockaddr *)&bind_addr, 0) != 0)
    {
        m_logger->error("Failed to bind UDP socket");
        uv_close((uv_handle_t *)&m_udp_socket, nullptr);
        return;
    }

    // get port
    struct sockaddr_storage addr;
    int addr_len = sizeof(addr);
    uv_udp_getsockname(&m_udp_socket, (struct sockaddr *)&addr, &addr_len);
    char ip[INET_ADDRSTRLEN];
    int port = 0;

    if (addr.ss_family == AF_INET)
    {
        struct sockaddr_in *sin = (struct sockaddr_in *)&addr;
        uv_ip4_name(sin, ip, sizeof(ip));
        port = ntohs(sin->sin_port);
    }

    m_settings->udp_local_addr.ip = ip;
    m_settings->udp_local_addr.port = port;

    m_logger->info("UDP socket bound to {}:{}", m_settings->udp_local_addr.ip, m_settings->udp_local_addr.port);

    // set incoming udp packets handler
    uv_handle_set_data(reinterpret_cast<uv_handle_t *>(&m_udp_socket), this);
    uv_udp_recv_start(&m_udp_socket, uv_alloc_cb, uv_recv_cb);
}

void UdpHandler::SendPacket()
{
    // make destination address
    struct sockaddr_in dest_addr;
    if (uv_ip4_addr(m_settings->udp_remote_addr.ip.c_str(), m_settings->udp_remote_addr.port, &dest_addr) != 0)
    {
        m_logger->error("Invalid UDP destination address: {}:{}", m_settings->udp_remote_addr.ip.c_str(), m_settings->udp_remote_addr.port);
        return;
    }

    const auto now = std::chrono::steady_clock::now();
    m_stat_outgoing.add_packet(now, m_message.size());

    uv_buf_t buf = uv_buf_init(m_message.data(), m_message.size());

    uv_udp_send_t *send_req = new uv_udp_send_t;
    uv_req_set_data(reinterpret_cast<uv_req_t *>(send_req), this);
    uv_udp_send(send_req, &m_udp_socket, &buf, 1,
                reinterpret_cast<const sockaddr *>(&dest_addr), uv_send_cb);
}

void UdpHandler::OnTimer()
{
    m_logger->trace("{}", __FUNCTION__);

    if (m_settings->stop)
    {
        Stop();
        return;
    }

    CheckThread();

    const auto now = std::chrono::steady_clock::now();

    if (m_settings->udp_local_addr.port == 0)
    {
        CreateSocket();
    }
    else if (m_settings->commutated)
    {
        SendPacket();

        const auto work_duration_s = std::chrono::duration_cast<std::chrono::seconds>(now - m_settings->work_start).count();
        if (work_duration_s >= m_settings->work_time)
            Stop();
    }

    m_timer_last_tp += std::chrono::milliseconds{m_settings->udp_repeat_timeout};
    uint64_t delta = std::chrono::duration_cast<std::chrono::microseconds>(m_timer_last_tp - now).count();
    uv_timer_start(&m_timer, timer_callback, (delta + (1000 / 2)) / 1000, 0);
}

void UdpHandler::timer_callback(uv_timer_t *handle)
{
    auto *self = reinterpret_cast<UdpHandler *>(uv_handle_get_data(reinterpret_cast<uv_handle_t *>(handle)));
    self->OnTimer();
}

bool UdpHandler::Init()
{
    m_logger->debug("UDP handler init");
    uv_timer_init(m_loop, &m_timer);
    uv_handle_set_data(reinterpret_cast<uv_handle_t *>(&m_timer), this);
    uv_timer_start(&m_timer, timer_callback, m_settings->udp_repeat_timeout, 0);
    m_timer_last_tp = std::chrono::steady_clock::now();

    uv_async_init(m_loop, &m_async_stop, UdpHandler::async_stop_callback);
    uv_handle_set_data(reinterpret_cast<uv_handle_t *>(&m_async_stop), this);

    return true;
}

void UdpHandler::Stop()
{
    m_settings->stop = true;
    uv_async_send(&m_async_stop);
}

void UdpHandler::uv_close_cb(uv_handle_t *handle)
{
    auto *self = reinterpret_cast<UdpHandler *>(handle);
    if (handle == (uv_handle_t *)&(self->m_udp_socket))
    {
        self->m_logger->debug("UDP socket closed");
    }
}

void UdpHandler::uv_alloc_cb([[maybe_unused]] uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf)
{
    auto *self = reinterpret_cast<UdpHandler *>(uv_handle_get_data(reinterpret_cast<uv_handle_t *>(handle)));
    self->CheckThread();
    buf->base = new char[suggested_size];
    buf->len = suggested_size;
}

void UdpHandler::uv_recv_cb([[maybe_unused]] uv_udp_t *handle, ssize_t nread, const uv_buf_t *buf,
                            const struct sockaddr *addr, [[maybe_unused]] unsigned flags)
{
    auto *self = reinterpret_cast<UdpHandler *>(uv_handle_get_data(reinterpret_cast<uv_handle_t *>(handle)));
    self->CheckThread();

    if (nread > 0)
    {
        char sender[INET_ADDRSTRLEN] = {'\0'};
        if (addr->sa_family == AF_INET)
        {
            struct sockaddr_in *addr4 = (struct sockaddr_in *)addr;
            auto port = ntohs(addr4->sin_port);
            uv_ip4_name(addr4, sender, sizeof(sender));
            self->m_logger->debug("Received UDP packet from {}:{} - {}",
                                  sender, port, std::string_view(buf->base, nread));
        }
        const auto now = std::chrono::steady_clock::now();
        self->m_stat_incoming.add_packet(now, nread);
    }
    else if (nread < 0)
    {
        self->m_logger->error("UDP receive error: {}", uv_strerror(nread));
    }

    if (buf->base)
        delete[] buf->base;
}

void UdpHandler::uv_send_cb(uv_udp_send_t *req, int status)
{
    auto *self = reinterpret_cast<UdpHandler *>(uv_req_get_data(reinterpret_cast<uv_req_t *>(req)));
    if (status == 0)
    {
        self->m_logger->trace("UDP packet sent successfully");
    }
    else
    {
        self->m_logger->error("UDP send error: {}", uv_strerror(status));
    }
    delete req;
}

void UdpHandler::CheckThread()
{
    static std::thread::id thread_id;
    const auto this_thread_id = std::this_thread::get_id();
    // m_logger->debug("UDP thread ID: {}", std::format("{}", this_thread_id));
    if (thread_id == std::thread::id{})
    {
        thread_id = this_thread_id;
        return;
    }
    ASSERT_MSG(thread_id == this_thread_id, "Another thread detected!");
}

void UdpHandler::async_stop_callback(uv_async_t *handle)
{
    auto *self = reinterpret_cast<UdpHandler *>(uv_handle_get_data(reinterpret_cast<uv_handle_t *>(handle)));
    self->OnStop();
}

void UdpHandler::OnStop()
{
    m_logger->debug("Stop UDP handle");

    uv_timer_stop(&m_timer);
    uv_close((uv_handle_t *)&m_timer, nullptr);
    uv_close((uv_handle_t *)&m_async_stop, nullptr);
    uv_close((uv_handle_t *)&m_udp_socket, nullptr);
}

void UdpHandler::PrintStats() const
{
    std::println("UDP INCOMING:\n{}", m_stat_incoming.to_string());
    std::println("UDP OUTGOING:\n{}", m_stat_outgoing.to_string());
}