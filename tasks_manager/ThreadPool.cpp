#include "ThreadPool.hpp"
using namespace std;


ThreadPool::ThreadPool(size_t num_threads) : stop(false) {
    for (size_t i = 0; i < num_threads; ++i) {
        // Vamos añadiendo hilos cuya función a ejecutar es worker
        workers.emplace_back([this]() { this->worker(); });
    }
}

ThreadPool::~ThreadPool() {
    {
        // Evitamos que otro hilo lea stop al mismo tiempo que nosotros los cambiamos
        // podemos reutilizar el mismo mutex que la queue
        unique_lock<mutex> lock(queue_mutex);
        stop = true;
    }
    // Informamos a todos de que hay un cambio
    cv.notify_all();
    // Vamos esperando a que todos los hilos terminen
    for (auto& t : workers)
        t.join();
}

void ThreadPool::enqueue(std::function<void()> task) {
    {
        // Evitamos que más hilos accedan a la cola de tareas
        // para poder añadir una tarea sin problemas
        unique_lock<std::mutex> lock(queue_mutex);
        tasks.push(std::move(task));
    }
    cv.notify_one();
}

void ThreadPool::worker() {
    while (true) {
        function<void()> task;
        {
            unique_lock<mutex> lock(queue_mutex);
            cv.wait(lock, [this]() { return stop || !tasks.empty(); });
            // En caso de que se nos haya mandado destruirnos y que no queden
            // más tareas, terminamos. En caso de que queden tareas pendientes, estas deben ser
            // atendidas primero y no terminamos
            if (stop && tasks.empty()) return;
            // Obtenemos cuál es la tarea que vamos a ejecutar
            task = move(tasks.front());
            tasks.pop();
        }
        // Ejecutamos la tarea
        task();
    }
}
