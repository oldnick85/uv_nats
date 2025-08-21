#pragma once

#include <unordered_map>
#include <chrono>

#include <spdlog/spdlog.h>
#include <nats/nats.h>
#include <uv.h>
#include <nlohmann/json.hpp>

#include "msg_queue.h"
#include "../common.h"
#include "../statistic.h"

class NatsHandler
{
public:
    static constexpr std::string Name = "NATS";

    NatsHandler(std::shared_ptr<spdlog::logger> logger, uv_loop_t *loop, GlobalSettings *settings);
    ~NatsHandler();

    bool Init();
    void Stop();

    void PrintStats() const;

public:
    void CheckThread();
    void OnTimer();
    void OnStop();

    std::shared_ptr<spdlog::logger> m_logger;
    uv_loop_t *m_loop = nullptr;
    GlobalSettings *m_settings = nullptr;

    natsConnection *m_nats_conn = nullptr;
    natsSubscription *m_nats_sub = nullptr;

    uv_timer_t m_timer;
    uv_async_t m_async_stop;
    uv_async_t m_async_msg;

    uint64_t m_messages_counter = 0;
    std::unordered_map<uint64_t, std::chrono::steady_clock::time_point> m_pings;
    std::string m_payload;
    std::chrono::time_point<std::chrono::steady_clock> m_timer_last_tp;
    MsgQueue m_messages;

    void ParseMessageAddress(nlohmann::json::iterator addr_it);
    void ParseMessagePing(nlohmann::json::iterator ping_it);
    void ParseMessagePong(nlohmann::json::iterator pong_it);
    void ParseMessage(const std::string &message);
    void SendAddress();
    void SendPing();
    void SendPong(uint64_t messages_counter);
    void SendMessage(const std::string &message);

    void OnMessage();

    void OnNatsMessage(natsMsg *msg);

    static void nats_closed_cb([[maybe_unused]] natsConnection *nc, void *closure);
    static void nats_message_cb([[maybe_unused]] natsConnection *nc, [[maybe_unused]] natsSubscription *sub, natsMsg *msg, [[maybe_unused]] void *closure);

    static void timer_callback(uv_timer_t *handle);
    static void async_stop_callback(uv_async_t *handle);
    static void async_msg_callback(uv_async_t *handle);

private:
    TrafficStats m_stat_incoming;
    TrafficStats m_stat_outgoing;
    PingStats m_stat_ping;
};
