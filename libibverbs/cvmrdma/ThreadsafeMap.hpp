#pragma once

#include <mutex>
#include <unordered_map>

#include <stdio.h>



/* Threadsafe hashmap */
template <typename K, typename V>
class ThreadsafeMap {
public:
    void insert_or_assign(K key, V value)
    {
        auto locked = std::scoped_lock{m_lock};
        m_map.insert_or_assign(key, std::move(value));
    }

    V & at(K key)
    {
        auto locked = std::scoped_lock{m_lock};
        return m_map.at(key);
    }

    bool exists(K key) {
        std::lock_guard<std::mutex> lock(m_lock);
        return m_map.find(key) != m_map.end();
    }

    std::unordered_map<K, V> get_map()
    {
        return m_map;
    }

    size_t erase(K key)
    {
        auto locked = std::scoped_lock{m_lock};
        return m_map.erase(key);
    }

private:
    std::unordered_map<K, V> m_map;
    std::mutex m_lock;
};
