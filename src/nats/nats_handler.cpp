#include "nats_handler.h"

#include <print>

#include <nlohmann/json.hpp>
#include <nats/adapters/libuv.h>

NatsHandler::NatsHandler(std::shared_ptr<spdlog::logger> logger, uv_loop_t *loop, GlobalSettings *settings) : m_logger(logger), m_loop(loop), m_settings(settings)
{
    m_payload.reserve(m_settings->nats_payload_size);
    for (std::size_t i = 0; i < m_settings->nats_payload_size; ++i)
    {
        m_payload.push_back(static_cast<char>(i % 10) + '0');
    }
}

NatsHandler::~NatsHandler()
{
    PrintStats();
}

void NatsHandler::SendAddress()
{
    if (m_settings->udp_local_addr.ip.empty())
        return;

    nlohmann::json j;
    j["addr"] = {
        {"local_ip", m_settings->udp_local_addr.ip},
        {"local_port", m_settings->udp_local_addr.port},
        {"remote_ip", m_settings->udp_remote_addr.ip},
        {"remote_port", m_settings->udp_remote_addr.port}};

    const auto message = j.dump();

    m_logger->debug("{}: {}", __FUNCTION__, message);
    SendMessage(message);
}

void NatsHandler::SendPing()
{
    nlohmann::json j;
    j["ping"] = {
        {"count", m_messages_counter},
        {"payload", m_payload}};

    const auto message = j.dump();

    SendMessage(message);

    m_pings.emplace(m_messages_counter, std::chrono::steady_clock::now());

    m_messages_counter += 1;
}

void NatsHandler::SendPong(uint64_t messages_counter)
{
    nlohmann::json j;
    j["pong"] = {
        {"count", messages_counter},
        {"payload", m_payload}};

    const auto message = j.dump();

    SendMessage(message);
}

void NatsHandler::SendMessage(const std::string &message)
{
    natsStatus status = natsConnection_PublishString(m_nats_conn, m_settings->nats_remote_topic.c_str(), message.c_str());

    if (status == NATS_OK)
    {
        m_stat_outgoing.add_packet(message.size());
        m_logger->debug("Published NATS to {} message: {}", m_settings->nats_remote_topic, message);
    }
    else
    {
        m_logger->error("Publish failed: {}", natsStatus_GetText(status));
    }
}

void NatsHandler::ParseMessageAddress(nlohmann::json::iterator addr_it)
{
    if (!addr_it->is_object())
    {
        m_logger->error("Invalid 'addr' object");
        return;
    }

    auto &addr = *addr_it;
    auto local_ip_it = addr.find("local_ip");
    auto local_port_it = addr.find("local_port");
    auto remote_ip_it = addr.find("remote_ip");
    auto remote_port_it = addr.find("remote_port");

    if (local_ip_it == addr.end() || !local_ip_it->is_string())
    {
        m_logger->error("Invalid local IP");
        return;
    }
    if (local_port_it == addr.end() || !local_port_it->is_number_integer())
    {
        m_logger->error("Invalid local port");
        return;
    }
    if (remote_ip_it == addr.end() || !remote_ip_it->is_string())
    {
        m_logger->error("Invalid remote IP");
        return;
    }
    if (remote_port_it == addr.end() || !remote_port_it->is_number_integer())
    {
        m_logger->error("Invalid remote port");
        return;
    }

    UdpAddr local_addr;
    local_addr.ip = *local_ip_it;
    local_addr.port = *local_port_it;

    if (m_settings->udp_remote_addr != local_addr)
    {
        m_settings->udp_remote_addr = local_addr;
        m_logger->info("received remote addr {}", m_settings->udp_remote_addr.Str());
    }

    UdpAddr remote_addr;
    remote_addr.ip = *remote_ip_it;
    remote_addr.port = *remote_port_it;

    if ((m_settings->udp_local_addr == remote_addr) && (!m_settings->commutated))
    {
        SendAddress();
        if (m_settings->udp_local_addr.Valid() && m_settings->udp_remote_addr.Valid())
        {
            m_settings->work_start = std::chrono::steady_clock::now();
            m_settings->commutated = true;
        }
    }
}

void NatsHandler::ParseMessagePing(nlohmann::json::iterator ping_it)
{
    auto &ping = *ping_it;
    auto count_it = ping.find("count");
    if (count_it == ping.end() || !count_it->is_number_integer())
    {
        m_logger->error("Invalid ping count");
        return;
    }
    uint64_t count = *count_it;
    SendPong(count);
}

void NatsHandler::ParseMessagePong(nlohmann::json::iterator pong_it)
{
    auto &ping = *pong_it;
    auto count_it = ping.find("count");
    if (count_it == ping.end() || !count_it->is_number_integer())
    {
        m_logger->error("Invalid pong count");
        return;
    }
    uint64_t count = *count_it;
    const auto ping_it = m_pings.find(count);
    ASSERT_MSG(ping_it != m_pings.end(), "No ping found");

    const auto now = std::chrono::steady_clock::now();
    const auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(now - ping_it->second).count();
    m_stat_ping.add(duration_us);

    m_pings.erase(ping_it);
}

void NatsHandler::ParseMessage(const std::string &message)
{
    m_logger->trace("{}: {}", __FUNCTION__, message);

    auto j_root = nlohmann::json::parse(message, nullptr, false, true);

    if (j_root.is_discarded())
    {
        m_logger->error("JSON parse error");
        return;
    }

    auto addr_it = j_root.find("addr");
    if (addr_it != j_root.end())
        ParseMessageAddress(addr_it);

    auto ping_it = j_root.find("ping");
    if (ping_it != j_root.end())
        ParseMessagePing(ping_it);

    auto pong_it = j_root.find("pong");
    if (pong_it != j_root.end())
        ParseMessagePong(pong_it);
}

void NatsHandler::nats_closed_cb([[maybe_unused]] natsConnection *nc, void *closure)
{
    auto *self = static_cast<NatsHandler *>(closure);
    self->m_logger->debug("NATS connection closed");
    self->Stop();
}

void NatsHandler::OnMessage()
{
    CheckThread();

    MsgQueue::Message message;
    const auto now = std::chrono::steady_clock::now();
    while (m_messages.pop(message))
    {
        m_stat_incoming.add_packet(now, message.payload.size());
        m_logger->debug("Received NATS message on [{}]: {}", message.subject, message.payload);
        ParseMessage(message.payload);
    }
}

void NatsHandler::OnNatsMessage(natsMsg *msg)
{
    m_logger->trace("NATS {}", __FUNCTION__);

    const char *subject = natsMsg_GetSubject(msg);
    const char *payload = natsMsg_GetData(msg);
    int payload_len = natsMsg_GetDataLength(msg);

    MsgQueue::Message message;
    message.subject = std::string{subject};
    message.payload = std::string{payload, static_cast<std::size_t>(payload_len)};
    m_messages.push(std::move(message));
    uv_async_send(&m_async_msg);

    natsMsg_Destroy(msg);
}

void NatsHandler::nats_message_cb([[maybe_unused]] natsConnection *nc,
                                  [[maybe_unused]] natsSubscription *sub,
                                  natsMsg *msg,
                                  void *closure)
{
    auto *self = static_cast<NatsHandler *>(closure);
    self->OnNatsMessage(msg);
}

void NatsHandler::OnStop()
{
    m_logger->debug("Stop NATS handle");

    uv_timer_stop(&m_timer);
    uv_close((uv_handle_t *)&m_timer, nullptr);
    uv_close((uv_handle_t *)&m_async_stop, nullptr);
    uv_close((uv_handle_t *)&m_async_msg, nullptr);

    if (m_nats_sub)
    {
        natsSubscription_Unsubscribe(m_nats_sub);
        natsSubscription_Destroy(m_nats_sub);
        m_nats_sub = nullptr;
        m_logger->debug("NATS subscription destroyed");
    }

    if (m_nats_conn)
    {
        natsConnection_Close(m_nats_conn);
        natsConnection_Destroy(m_nats_conn);
        m_nats_conn = nullptr;
        m_logger->debug("NATS connection closed");
    }

    nats_Close();
}

void NatsHandler::OnTimer()
{
    m_logger->trace("NATS {}", __FUNCTION__);

    if (m_settings->stop)
    {
        Stop();
        return;
    }

    CheckThread();

    const auto now = std::chrono::steady_clock::now();

    if (!m_settings->commutated)
    {
        SendAddress();
    }
    else
    {
        SendPing();
        const auto work_duration_s = std::chrono::duration_cast<std::chrono::seconds>(now - m_settings->work_start).count();
        if (work_duration_s >= m_settings->work_time)
            Stop();
    }

    m_timer_last_tp += std::chrono::milliseconds{m_settings->nats_repeat_timeout};
    uint64_t delta = std::chrono::duration_cast<std::chrono::microseconds>(m_timer_last_tp - now).count();
    uv_timer_start(&m_timer, timer_callback, (delta + (1000 / 2)) / 1000, 0);
}

void NatsHandler::timer_callback(uv_timer_t *handle)
{
    auto *self = reinterpret_cast<NatsHandler *>(uv_handle_get_data(reinterpret_cast<uv_handle_t *>(handle)));
    self->OnTimer();
}

void NatsHandler::async_stop_callback(uv_async_t *handle)
{
    auto *self = reinterpret_cast<NatsHandler *>(uv_handle_get_data(reinterpret_cast<uv_handle_t *>(handle)));
    self->OnStop();
}

void NatsHandler::async_msg_callback(uv_async_t *handle)
{
    auto *self = reinterpret_cast<NatsHandler *>(uv_handle_get_data(reinterpret_cast<uv_handle_t *>(handle)));
    self->OnMessage();
}

bool NatsHandler::Init()
{
    m_logger->debug("NATS handler init");

    natsStatus status = NATS_OK;

    natsOptions *nats_opts = nullptr;
    status = natsOptions_Create(&nats_opts);
    if (status != NATS_OK)
    {
        m_logger->critical("Failed to create NATS options: {}", natsStatus_GetText(status));
        return false;
    }

    status = natsOptions_SetURL(nats_opts, m_settings->nats_url.c_str());
    if (status != NATS_OK)
    {
        m_logger->critical("Failed to set NATS URL: {}", natsStatus_GetText(status));
        natsOptions_Destroy(nats_opts);
        return false;
    }

    natsLibuv_Init();

    natsLibuv_SetThreadLocalLoop(m_loop);

    status = natsOptions_SetEventLoop(nats_opts, (void *)m_loop, natsLibuv_Attach, natsLibuv_Read, natsLibuv_Write, natsLibuv_Detach);

    if (status != NATS_OK)
    {
        m_logger->critical("Failed to set libuv event loop: {}", natsStatus_GetText(status));
        natsOptions_Destroy(nats_opts);
        return false;
    }
    natsOptions_UseGlobalMessageDelivery(nats_opts, true);

    status = natsOptions_SetClosedCB(nats_opts, nats_closed_cb, this);
    if (status != NATS_OK)
    {
        m_logger->critical("Set closed callback failed: {}", natsStatus_GetText(status));
        return false;
    }

    status = natsConnection_Connect(&m_nats_conn, nats_opts);
    natsOptions_Destroy(nats_opts);

    if (status != NATS_OK)
    {
        m_logger->critical("NATS connection failed: {}", natsStatus_GetText(status));
        return false;
    }
    m_logger->info("Connected to NATS server");

    status = natsConnection_Subscribe(&m_nats_sub, m_nats_conn, m_settings->nats_local_topic.c_str(), nats_message_cb, this);

    if (status != NATS_OK)
    {
        m_logger->critical("Subscription failed: {}", natsStatus_GetText(status));
        return false;
    }

    m_logger->info("Subscribed to {}", m_settings->nats_local_topic);

    status = natsSubscription_SetPendingLimits(m_nats_sub, -1, -1);

    uv_timer_init(m_loop, &m_timer);
    uv_handle_set_data(reinterpret_cast<uv_handle_t *>(&m_timer), this);
    uv_timer_start(&m_timer, timer_callback, m_settings->nats_repeat_timeout, 0);
    m_timer_last_tp = std::chrono::steady_clock::now();

    uv_async_init(m_loop, &m_async_stop, NatsHandler::async_stop_callback);
    uv_handle_set_data(reinterpret_cast<uv_handle_t *>(&m_async_stop), this);

    uv_async_init(m_loop, &m_async_msg, NatsHandler::async_msg_callback);
    uv_handle_set_data(reinterpret_cast<uv_handle_t *>(&m_async_msg), this);

    return true;
}

void NatsHandler::Stop()
{
    m_settings->stop = true;
    uv_async_send(&m_async_stop);
}

void NatsHandler::CheckThread()
{
    static std::thread::id thread_id;
    const auto this_thread_id = std::this_thread::get_id();
    m_logger->trace("NATS thread ID: {}", std::format("{}", this_thread_id));
    if (thread_id == std::thread::id{})
    {
        thread_id = this_thread_id;
        return;
    }
    ASSERT_MSG(thread_id == this_thread_id, "Another thread detected!");
}

void NatsHandler::PrintStats() const
{
    std::println("NATS INCOMING:\n{}", m_stat_incoming.to_string());
    std::println("NATS OUTGOING:\n{}", m_stat_outgoing.to_string());
    std::println("NATS PINGPONG:\n{}", m_stat_ping.to_string("us"));
}