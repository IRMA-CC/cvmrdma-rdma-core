#pragma once

#include <deque>
#include <mutex>
#include <condition_variable>


/* Single consumer, multi producer queue */
template <typename T> 
class ThreadsafeQueue
{
public:
    /* Ma*/
    template <typename TT>
    void push(TT && item)
    {
        {
            auto locked = std::scoped_lock{m_lock};
            m_queue.push_back(std::forward<TT>(item));
            // unlocking before notifing to avoid waking up
            // the waiting thread only to block again (see notify_one for details)
        }

        // m_queue_has_data_cv.notify_one();
    }

    T & front()
    {
        auto locked = std::unique_lock{m_lock};
        /*while(m_queue.empty())
        {
            m_queue_has_data_cv.wait(locked);
        }*/

        if (m_queue.empty())
        {
            return *(T *)nullptr;
        }

        /* Reference stays valid even without lock because there is a
            single consumer */
        return m_queue.front();
    }

    int pop_n_into(T * dest, int n)
    {
        auto locked = std::unique_lock{m_lock};

        int i = 0;
        while((i < n) && (! m_queue.empty()) )
        {
            dest[i] = m_queue.front();
            m_queue.pop_front();
            i++;
        }

        return i;
    }

    T & pop()
    {
        auto locked = std::scoped_lock{m_lock};

        T& item = m_queue.front();
        m_queue.pop_front();


        /* No need to notify_one after pop, there is a single consummer, no one
            else is waiting. */
        return item;
    }

    size_t size()
    {
        auto locked = std::scoped_lock{m_lock};
        return m_queue.size();
    }


private:
    std::deque<T> m_queue;
    std::condition_variable m_queue_has_data_cv;
    std::mutex m_lock;
};