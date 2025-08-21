#pragma once

#include <string>
#include <print>

#include <argh.h>
#include <spdlog/spdlog.h>

#include "common.h"

bool ParseArguments(int argc, char *argv[], GlobalSettings &settings, std::shared_ptr<spdlog::logger> logger)
{
    argh::parser cmdl(argc, argv);

    cmdl({"-nats-url", "--nats-url"}) >> settings.nats_url;
    cmdl({"-udp-repeat", "--udp-repeat"}) >> settings.udp_repeat_timeout;
    cmdl({"-nats-repeat", "--nats-repeat"}) >> settings.nats_repeat_timeout;
    cmdl({"-udp-payload", "--udp-payload"}) >> settings.udp_payload_size;
    cmdl({"-nats-payload", "--nats-payload"}) >> settings.nats_payload_size;
    cmdl({"-work-time", "--work-time"}) >> settings.work_time;

    if (cmdl["debug"])
    {
        logger->set_level(spdlog::level::debug);
        logger->debug("Debug logging enabled");
    }

    if (cmdl["master"])
    {
        settings.nats_local_topic = "uv_nats.master";
        settings.nats_remote_topic = "uv_nats.slave";
    }
    else
    {
        settings.nats_local_topic = "uv_nats.slave";
        settings.nats_remote_topic = "uv_nats.master";
    }

    if (cmdl["help"])
    {
        std::printf("UV_NATS usage:\n"
                    "  --help           Display this information.\n"
                    "  --master         Master mode (set master topic for NATS).\n"
                    "  --nats-url       NATS server URL (default nats://localhost:4222).\n"
                    "  --udp-repeat     UDP packets send timeout in milliseconds (default 100).\n"
                    "  --nats-repeat    NATS packets send timeout in milliseconds (default 100).\n"
                    "  --udp-payload    UDP packets payload size in bytes (default 100).\n"
                    "  --nats-payload   NATS packets payload size in bytes (default 100).\n"
                    "  --work-time      Time to send packets in seconds (default 100).\n");
        return false;
    }
    return true;
}