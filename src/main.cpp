#include <uv.h>
#include <argh.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <atomic>
#include <csignal>
#include <cstring>
#include <memory>
#include <thread>
#include <string>
#include <string_view>

#include "common.h"
#include "arguments.h"
#include "nats/nats_handler.h"
#include "udp/udp_handler.h"

GlobalSettings g_settings;
std::shared_ptr<spdlog::logger> g_logger;

void InitLogger()
{
    g_logger = spdlog::stdout_color_mt("main");
    g_logger->set_level(spdlog::level::info);
    g_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%n] %v");
}

void signal_handler(int signum)
{
    g_logger->info("Received signal {}, shutting down...", signum);
    g_settings.stop = true;
}

void RegisterSignals()
{
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
}

template <typename THandler>
void run_uv_thread([[maybe_unused]] void *arg)
{
    g_logger->info("{} loop started", THandler::Name);

    uv_loop_t *loop = new uv_loop_t;
    uv_loop_init(loop);

    if (!loop)
    {
        g_logger->critical("Failed to initialize {} loop", THandler::Name);
        return;
    }

    THandler *handler = new THandler{g_logger, loop, &g_settings};

    if (!handler->Init())
    {
        g_settings.stop = true;
        return;
    }

    uv_run(loop, UV_RUN_DEFAULT);

    if (loop)
    {
        auto ret = uv_loop_close(loop);
        if (ret != 0)
            g_logger->warn("{} loop close error: {}\n", THandler::Name, uv_strerror(ret));
        else
            g_logger->debug("{} loop closed", THandler::Name);
    }

    delete loop;
    delete handler;
}

int main(int argc, char *argv[])
{
    if (!ParseArguments(argc, argv, g_settings, g_logger))
        return 0;

    InitLogger();
    RegisterSignals();
    g_logger->info("Starting application");
    g_logger->info(g_settings.Str());

    uv_thread_t thread_nats;
    uv_thread_create(&thread_nats, run_uv_thread<NatsHandler>, nullptr);

    uv_thread_t thread_udp;
    uv_thread_create(&thread_udp, run_uv_thread<UdpHandler>, nullptr);

    uv_thread_join(&thread_nats);
    uv_thread_join(&thread_udp);

    g_logger->info("Clean shutdown completed");
    spdlog::shutdown();

    return 0;
}