#pragma once

#include <mutex>
#include <unordered_map>



/* Threadsafe hashmap */
template <typename K, typename V>
class ThreadsafeMap {
public:
    void insert_or_assign(K qpn, V qnc)
    {
        auto locked = std::scoped_lock{m_lock};
        m_map.insert_or_assign(qpn, std::move(qnc));
    }

    V & at(K qpn)
    {
        auto locked = std::scoped_lock{m_lock};
        return m_map.at(qpn);
    }

    size_t erase(K qpn)
    {
        auto locked = std::scoped_lock{m_lock};
        return m_map.erase(qpn);
    }

private:
    std::unordered_map<K, V> m_map;
    std::mutex m_lock;
};
