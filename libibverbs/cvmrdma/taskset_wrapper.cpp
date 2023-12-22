#include "taskset_wrapper.h"

#include "debug.h"

class TrivialHandler : public taskset_error_handler_t
{
public:
    void taskFailed(kj::Exception&& exception) override
    {
        DEBUG("Task failed: %s", exception.getDescription().cStr());
    }
};


taskset_error_handler_t * taskset_error_handler_alloc()
{
    return new TrivialHandler{};
}


taskset_t * taskset_alloc(taskset_error_handler_t * handler)
{
    return new taskset_t{*handler};
}

void taskset_free(taskset_t * tk)
{
    delete tk;
}

void taskset_wait(ClientSession * session, taskset_t * tk)
{
    tk->onEmpty().wait(session->getWaitScope());
}