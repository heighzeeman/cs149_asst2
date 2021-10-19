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
static void IRunnable_mod_run(IRunnable* runnable, int task_id, int num_threads, int num_total_tasks) { 
	for (int i = task_id; i < num_total_tasks; i += num_threads)
		runnable->runTask(i, num_total_tasks);
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

	//static std::thread workers[_num_threads];
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
	std::thread workers[_num_threads - 1];
	for (int i = 1; i < _num_threads; ++i)
		workers[i-1] = std::thread(IRunnable_mod_run, runnable, i, _num_threads, num_total_tasks);
	IRunnable_mod_run(runnable, 0, _num_threads, num_total_tasks);
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

static void IRunnable_rq_spin(std::queue<IRunnableContext> *readyQ, bool *idle, std::mutex *qLock, bool *signalQuit, int threadId) { 
	IRunnableContext toRun;
	while (true) {
		qLock->lock();
		if (!readyQ->empty()) {
			memcpy(&toRun, &readyQ->front(), sizeof(toRun));
			readyQ->pop();
			*idle = false;
			std::cout << "Thread #" << threadId <<  " running runnable: " << toRun.runnable << "w/ id= " << toRun.taskId << std::endl;
			qLock->unlock();
			
			toRun.runnable->runTask(toRun.taskId, toRun.num_total_tasks);
			
			qLock->lock();
			*idle = true;
			qLock->unlock();
		}
		else qLock->unlock();
		if (*signalQuit) break;
	}
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads),
	_num_threads(num_threads), _readyCtxs(), _idle(new bool[num_threads]), _workers(new std::thread[num_threads]), _mtx(), _quit(false) {
	_mtx.lock();
	for (int i = 0; i < num_threads; ++i) {
		_idle[i] = true;
		_workers[i] = std::thread(IRunnable_rq_spin, &_readyCtxs, &_idle[i], &_mtx, &_quit, i);
	}
	_mtx.unlock();
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
	_quit = true;
	for (int i = 0; i < _num_threads; ++i)
		_workers[i].join();
	_mtx.lock();
	std::cout << "Deallocating in destructor\n" << std::endl;
	delete[] _idle;
	delete[] _workers;
	_mtx.unlock();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
	
	IRunnableContext ctx;
    for (int i = 0; i < num_total_tasks; ++i) {
		ctx.runnable = runnable;
		ctx.taskId = i;
		ctx.num_total_tasks = num_total_tasks;
		_mtx.lock();
        _readyCtxs.push(ctx);
		_mtx.unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
	bool synced = true;
    while (true) {
		_mtx.lock();
		if (_readyCtxs.empty()) {
			for (int i = 0; i < _num_threads; ++i) {
				if (!_idle[i]) {
					synced = false;
					break;
				}
			}
		} else synced = false;
		_mtx.unlock();
		if (synced) break;
	}
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
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
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
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
