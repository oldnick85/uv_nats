#pragma once

#include <string>
#include <format>

#define ASSERT_MSG(exp, msg) assert((void(msg), exp))

struct UdpAddr
{
    bool operator==(const UdpAddr &other) const
    {
        return ((ip == other.ip) && (port == other.port));
    }

    std::string Str() const
    {
        return std::format("{}:{}", ip, port);
    }

    bool Valid() const { return ((!ip.empty()) && port != 0); }

    std::string ip;
    int port = 0;
};

struct GlobalSettings
{
    std::string nats_url = "nats://localhost:4222";
    std::string nats_local_topic;
    std::string nats_remote_topic;

    UdpAddr udp_local_addr;
    UdpAddr udp_remote_addr;

    uint64_t udp_repeat_timeout = 100;  // ms
    uint64_t nats_repeat_timeout = 100; // ms

    uint64_t udp_payload_size = 100;  // bytes
    uint64_t nats_payload_size = 100; // bytes

    int64_t work_time = 100; // s

    std::atomic_bool commutated = false;
    std::atomic_bool stop = false;
    std::chrono::steady_clock::time_point work_start;

    std::string Str() const
    {
        return std::format("Settings:\n"
                           "    nats_url={}\n"
                           "    nats_local_topic={}\n"
                           "    nats_remote_topic={}\n"
                           "    udp_repeat_timeout={}ms\n"
                           "    nats_repeat_timeout={}ms\n"
                           "    udp_payload_size={}b\n"
                           "    nats_payload_size={}b\n"
                           "    work_time={}s",
                           nats_url,
                           nats_local_topic,
                           nats_remote_topic,
                           udp_repeat_timeout,
                           nats_repeat_timeout,
                           udp_payload_size,
                           nats_payload_size,
                           work_time);
    }
};