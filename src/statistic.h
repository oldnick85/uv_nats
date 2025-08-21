#pragma once

#include <chrono>
#include <queue>
#include <optional>
#include <cstdint>
#include <cmath>
#include <string>
#include <format>

class PingStats
{
public:
    PingStats() = default;

    void add(double value)
    {
        if (n_ == 0)
        {
            min_ = value;
            max_ = value;
            mean_ = value;
            n_ = 1;
            return;
        }

        min_ = std::min(min_, value);
        max_ = std::max(max_, value);
        ++n_;

        const double delta = value - mean_;
        mean_ += delta / n_;
        M2_ += delta * (value - mean_);
    }

    void reset()
    {
        n_ = 0;
        mean_ = 0.0;
        M2_ = 0.0;
        min_ = std::numeric_limits<double>::infinity();
        max_ = -std::numeric_limits<double>::infinity();
    }

    size_t count() const noexcept { return n_; }
    double min() const noexcept { return min_; }
    double max() const noexcept { return max_; }

    double mean() const
    {
        if (n_ == 0)
            return nan();
        return mean_;
    }

    double variance() const
    {
        if (n_ < 2)
            return nan();
        return M2_ / (n_ - 1);
    }

    double stddev() const
    {
        return std::sqrt(variance());
    }

    std::string to_string(const std::string &value_units) const
    {
        return std::format("    count:                         {}\n"
                           "    min/max:                       {} {} / {} {}\n"
                           "    mean/stddev:                   {:.2f} {} / {:.2f} {}",
                           count(),
                           min(), value_units,
                           max(), value_units,
                           mean(), value_units,
                           stddev(), value_units);
    }

private:
    size_t n_{0};
    double mean_{0.0};
    double M2_{0.0};
    double min_{std::numeric_limits<double>::infinity()};
    double max_{-std::numeric_limits<double>::infinity()};

    static constexpr double nan()
    {
        return std::numeric_limits<double>::quiet_NaN();
    }
};

class TrafficStats
{
public:
    void add_packet(size_t size)
    {
        const auto now = std::chrono::steady_clock::now();
        add_packet(now, size);
    }

    void add_packet(const std::chrono::steady_clock::time_point &now, size_t size)
    {
        m_total_packets++;
        m_total_bytes += size;

        if (!m_first_packet)
            m_first_packet = now;

        if (m_last_packet)
        {
            const double delay = std::chrono::duration<double>(now - *m_last_packet).count();
            m_delay_sum += delay;
            m_delay_sq_sum += delay * delay;
            m_delay_count++;
        }
        m_last_packet = now;
    }

    uint64_t total_packets() const noexcept { return m_total_packets; }

    uint64_t total_bytes() const noexcept { return m_total_bytes; }

    double avg_packet_rate() const noexcept
    {
        if (!m_first_packet || !m_last_packet || m_first_packet == m_last_packet)
            return 0.0;

        const double duration = std::chrono::duration<double>(
                                    *m_last_packet - *m_first_packet)
                                    .count();
        return m_total_packets / duration;
    }

    double avg_byte_rate() const noexcept
    {
        if (!m_first_packet || !m_last_packet || m_first_packet == m_last_packet)
            return 0.0;

        const double duration = std::chrono::duration<double>(
                                    *m_last_packet - *m_first_packet)
                                    .count();
        return m_total_bytes / duration;
    }

    double avg_delay() const noexcept
    {
        return m_delay_count > 0 ? m_delay_sum / m_delay_count : 0.0;
    }

    double delay_variance() const noexcept
    {
        if (m_delay_count == 0)
            return 0.0;
        const double mean = m_delay_sum / m_delay_count;
        return (m_delay_sq_sum / m_delay_count) - (mean * mean);
    }

    double delay_stddev() const noexcept
    {
        return std::sqrt(delay_variance());
    }

    std::string to_string() const
    {
        return std::format(
            "    total packets/bytes:           {} / {}\n"
            "    average rate packet/byte:      {:.2f} pps / {:.2f} B/s\n"
            "    delay average/stddev:          {:.6f} / {:.6f} s",
            total_packets(),
            total_bytes(),
            avg_packet_rate(),
            avg_byte_rate(),
            avg_delay(),
            delay_stddev());
    }

private:
    uint64_t m_total_packets = 0;
    uint64_t m_total_bytes = 0;

    std::optional<std::chrono::steady_clock::time_point> m_first_packet;
    std::optional<std::chrono::steady_clock::time_point> m_last_packet;

    double m_delay_sum = 0.0;
    double m_delay_sq_sum = 0.0;
    uint64_t m_delay_count = 0;
};
