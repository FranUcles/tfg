#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <vector>
#include <queue>
#include <thread>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <atomic>
using namespace std;

class ThreadPool {
public:
    ThreadPool(size_t num_threads);
    ~ThreadPool();
    void enqueue(function<void()> task);

private:
    vector<thread> workers;
    queue<function<void()>> tasks;

    mutex queue_mutex;
    condition_variable cv;
    atomic<bool> stop;

    void worker();
};

#endif // THREADPOOL_H
