#include <cstring>
#include <iostream>
#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads),
	_num_threads(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}


// Helper function to allow std::thread execution
static void IRunnable_mtx_run(IRunnable* runnable, int task_id, int num_threads, int num_total_tasks, std::atomic<int> * const curr_task_id) {
	while (true) {
		if (*curr_task_id >= num_total_tasks) return;
		runnable->runTask(curr_task_id->fetch_add(1, std::memory_order_relaxed), num_total_tasks);
	}
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
	std::mutex mtx;
	std::atomic<int> curr_task_id(0);
	
	//static std::thread workers[_num_threads];
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
	std::thread workers[_num_threads - 1];
	for (int i = 1; i < _num_threads; ++i)
		workers[i-1] = std::thread(IRunnable_mtx_run, runnable, i, _num_threads, num_total_tasks, &curr_task_id);
	IRunnable_mtx_run(runnable, 0, _num_threads, num_total_tasks, &curr_task_id);
	for (int i = 1; i < _num_threads; ++i)
		workers[i-1].join();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

static void IRunnable_rq_spin(IRunnable** const run_ptr, int * const nextTaskId, int * const maxTaskId,
							  int * const completed, bool * const signalQuit, std::mutex *qlock, const int threadId) { 
	while (true) {
		qlock->lock();
		IRunnable *runnable = *run_ptr;
		if (runnable != nullptr && *nextTaskId < *maxTaskId) {
			int taskId = (*nextTaskId)++;
			qlock->unlock();
			runnable->runTask(taskId, *maxTaskId);
			qlock->lock();
			++(*completed);
			qlock->unlock();
		} else {
			qlock->unlock();
		}
		if (*signalQuit) break;	// Janky way of forcing all threads to terminate nicely in destructor
	}
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads),
	_num_threads(num_threads), _runnable(nullptr), _nextTaskId(0), _maxTaskId(0), _completed(0), _workers(new std::thread[num_threads]), _mtx(), _quit(false) {
	_mtx.lock();
	for (int i = 0; i < num_threads; ++i) {
		_workers[i] = std::thread(IRunnable_rq_spin, &_runnable, &_nextTaskId, &_maxTaskId, &_completed, &_quit, &_mtx, i);
	}
	_mtx.unlock();
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
	_quit = true;
	for (int i = 0; i < _num_threads; ++i)
		_workers[i].join();
	//std::cout << "Deallocating in destructor\n" << std::endl;
	delete[] _workers;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
	_mtx.lock();
	_runnable = runnable;
	_completed = _nextTaskId = 0;
	_maxTaskId = num_total_tasks;
	_mtx.unlock();
	
	while (true) {
		_mtx.lock();
		if (_completed == _maxTaskId) {
			_runnable = nullptr;
			_mtx.unlock();
			_completed = _nextTaskId = _maxTaskId = 0;
			return;
		}
		_mtx.unlock();
	}
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
	return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

static void IRunnable_sleep(IRunnable** const run_ptr, int * const nextTaskId, int * const maxTaskId,
							  std::atomic<int> * const completed, //std::atomic_flag * const compLock,
							  bool * const signalQuit, std::condition_variable *worker_cv,
							  std::condition_variable *master_cv, std::mutex *mtex, const int threadId) {
	std::unique_lock<std::mutex> qlock(*mtex, std::defer_lock);
	while (true) {
		qlock.lock();
		if (*signalQuit) {
			qlock.unlock();
			return;	// Janky way of forcing all threads to terminate nicely in destructor
		}
		while (*nextTaskId >= *maxTaskId) {
			//std::cout << "Thread #" << threadId << " sleeping: runnable = " << *run_ptr << " and NTID = " << *nextTaskId << " and MTID = " << *maxTaskId << std::endl;
			worker_cv->wait(qlock);
			if (*signalQuit) {
				qlock.unlock();
				return;
			}
			//std::cout << "Thread #" << threadId << " woken" << std::endl;
		}
		
		int taskId = (*nextTaskId)++;
		//std::cout << "Thread #" << threadId << " running task = " << taskId << std::endl;
		qlock.unlock();
		
		(*run_ptr)->runTask(taskId, *maxTaskId);
		
		if (++(*completed) == *maxTaskId) {
			//std::cout << "Thread #" << threadId << " waking on master_cv" << std::endl;
			master_cv->notify_one();
		}
	}
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads),
	_num_threads(num_threads), _runnable(nullptr), _nextTaskId(0), _maxTaskId(0), _completed(0), //_compLock(ATOMIC_FLAG_INIT),
	_worker_cv(new std::condition_variable), _mtex(), _ulock(_mtex, std::defer_lock), _master_cv(), _workers(new std::thread[num_threads]),
	_quit(new bool(false)) {
	for (int i = 0; i < num_threads; ++i) {
		_workers[i] = std::thread(IRunnable_sleep, &_runnable, &_nextTaskId, &_maxTaskId, &_completed, _quit, _worker_cv, &_master_cv, &_mtex, i);
	}
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
	_ulock.lock();
	*_quit = true;
	_worker_cv->notify_all();
	_ulock.unlock();
	for (int i = 0; i < _num_threads; ++i)
		_workers[i].join();
	//std::cout << "Deallocating in destructor\n" << std::endl;
	delete[] _workers;
	delete _worker_cv;
	//delete _master_cv;
	//delete _mtex;
	delete _quit;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

	_ulock.lock();
	//std::cout << "Run called with runnable = " << runnable << " and tasks = " << num_total_tasks << std::endl;
	_runnable = runnable;
	_completed = _nextTaskId = 0;
	_maxTaskId = num_total_tasks;
	_worker_cv->notify_all();
	
	while (_completed != _maxTaskId) {
		//std::cout << "Scheduler waking on worker_cv" << std::endl;
		//std::cout << "Scheduler sleeping" << std::endl;
		
		_master_cv.wait(_ulock);
		//std::cout << "Scheduler woken" << std::endl;
	}
	_nextTaskId = _maxTaskId = 0;
	_ulock.unlock();
	
	//std::cout << "Run returning" << std::endl;
	return;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
