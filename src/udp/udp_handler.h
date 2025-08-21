#pragma once

#include <chrono>

#include <uv.h>
#include <spdlog/spdlog.h>

#include "../common.h"
#include "../statistic.h"

class UdpHandler
{
public:
    static constexpr std::string Name = "UDP";

    UdpHandler(std::shared_ptr<spdlog::logger> logger, uv_loop_t *loop, GlobalSettings *settings);
    ~UdpHandler();

    bool Init();
    void Stop();

    void PrintStats() const;

private:
    void CheckThread();
    void OnTimer();
    void OnStop();
    void CreateSocket();
    void SendPacket();

    std::shared_ptr<spdlog::logger> m_logger;
    uv_loop_t *m_loop = nullptr;
    GlobalSettings *m_settings = nullptr;

    std::string m_message;
    std::chrono::time_point<std::chrono::steady_clock> m_timer_last_tp;

    uv_udp_t m_udp_socket;
    uv_timer_t m_timer;
    uv_async_t m_async_stop;

    static void uv_close_cb(uv_handle_t *handle);
    static void uv_alloc_cb([[maybe_unused]] uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
    static void uv_recv_cb([[maybe_unused]] uv_udp_t *handle, ssize_t nread, const uv_buf_t *buf,
                           const struct sockaddr *addr, [[maybe_unused]] unsigned flags);
    static void uv_send_cb(uv_udp_send_t *req, int status);
    static void timer_callback([[maybe_unused]] uv_timer_t *handle);

    static void async_stop_callback(uv_async_t *handle);

private:
    TrafficStats m_stat_incoming;
    TrafficStats m_stat_outgoing;
};
