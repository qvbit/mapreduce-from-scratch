#pragma once
#include <vector>
#include <thread>
#include <future>
#include <queue>
#include <memory>
#include <condition_variable>
#include <grpcpp/grpcpp.h>
#include <functional>
#include <stdexcept>

// Bad practice but fine for school projects.
using namespace std;

// Reference: "A simple C++11 Thread Pool implementation." by progschj
class ThreadPool {
public:
    ThreadPool(int);
    ~ThreadPool();
    /*Enqueue function takes a templated type F argument + additional arbitrary arguments via Variadic template
    / for the thread function F and returns a future of type return argument of F(Args...) */
    template<class F, class... Args>
    auto queueTask(F&& f, Args&&... args) 
        -> std::future<typename std::result_of<F(Args...)>::type>;

private:
    // Our pool of workers
    vector<thread> workers_;
    // Queue of tasks
    queue<function<void()>> tasks_;
    // Synchronization variables
    mutex queue_mutex_;
    condition_variable cond_;
};

// Queue up the threadpool with ready workers waiting for tasks to do.
inline ThreadPool::ThreadPool(int nthreads) {
    for (int i=0; i<nthreads; i++) {
        workers_.emplace_back([this] {
            while(true) {
                // Task to be run by thread (rpc call)
                function<void()> task;
                // Lock queue mutex
                unique_lock<mutex> lock(this->queue_mutex_);
                // Wait until queue is not empty signal.
                this->cond_.wait(lock, [this] {return !this->tasks_.empty(); });
                // Move (rather than copy) to efficiently assign task from queue.
                task = move(this->tasks_.front());
                // Destroy the task since we no longer need it (thus justfying use of move above).
                this->tasks_.pop();
                // Finally, run the task
                task();
            }            
        });
    }
}

inline ThreadPool::~ThreadPool() {
    unique_lock<std::mutex> lock(queue_mutex_);
    cond_.notify_all();
    for(thread &worker: workers_)
        worker.join();
}

template<class F, class... Args>
auto ThreadPool::queueTask(F&& f, Args&&... args) 
    -> std::future<typename std::result_of<F(Args...)>::type> {
    
    // Boilerplate future code to wrap the task so the result can be made available 
    // later via future's get().
    using return_type = typename result_of<F(Args...)>::type;
    auto task = make_shared< packaged_task<return_type()> >(
            bind(std::forward<F>(f), forward<Args>(args)...)
        );  
    future<return_type> res = task->get_future();

    // Place task on queue for worker to grab.
    unique_lock<mutex> lock(queue_mutex_);
    tasks_.emplace([task](){ (*task)(); });

    // Notify one waiting worker to grab this task.
    cond_.notify_one();
    
    // Return future so caller can grab result when it's ready.
    return res;
}

