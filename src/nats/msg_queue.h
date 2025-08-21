#pragma once

#include <mutex>
#include <queue>
#include <string>

#include <uv.h>

class MsgQueue
{
public:

    struct Message
    {
        std::string subject;
        std::string payload;
    };

    void push(Message &&msg)
    {
        std::lock_guard lock(m_mutex);
        m_queue.push(std::move(msg));
    }

    bool pop(Message &out)
    {
        std::lock_guard lock(m_mutex);
        if (m_queue.empty())
            return false;
        out = std::move(m_queue.front());
        m_queue.pop();
        return true;
    }

private:
    std::queue<Message> m_queue;
    std::mutex m_mutex;
};