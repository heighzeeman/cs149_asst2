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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

static void IRunnable_sleep(std::queue<TaskID> * const rdyQ, std::unordered_set<TaskID> * const completed,
                            std::unordered_map<TaskID, TaskContext*> * const taskdb, std::unordered_map<TaskID, std::vector<TaskID>> * const depchildren,
							bool * const signalQuit, std::condition_variable *worker_cv, std::condition_variable *master_cv, std::mutex *mtex,
							std::atomic<unsigned> * const num_runs, const int threadId) {
	std::unique_lock<std::mutex> qlock(*mtex, std::defer_lock);
	while (true) {
		qlock.lock();
		if (*signalQuit) {
			printf("Thread #%d received termination signal, quitting...\n", threadId);
			qlock.unlock();
			return;	// Janky way of forcing all threads to terminate nicely in destructor
		}
		while (rdyQ->empty()) {
			//std::cout << "Thread #" << threadId << " sleeping: runnable = " << *run_ptr << " and NTID = " << *nextTaskId << " and MTID = " << *maxTaskId << std::endl;
			printf("Thread #%d sleeping on worker_cv...\n", threadId);
			worker_cv->wait(qlock);
			if (*signalQuit) {
				qlock.unlock();
				return;
			}
			//std::cout << "Thread #" << threadId << " woken" << std::endl;
		}
		int taskId = rdyQ->front();
		TaskContext *curr = (*taskdb)[taskId];
		int int_taskId = curr->curr_task_id++;
		printf("Thread #%d running on task ID = %d, internal task ID = %d of %d, runnable = %d\n", threadId, int_taskId, curr->num_total_tasks, curr->runnable);
		if (curr->curr_task_id == curr->num_total_tasks) {
			rdyQ->pop();
		}
		qlock.unlock();
		curr->runnable->runTask(int_taskId, curr->num_total_tasks);
		
		qlock.lock();
		if (++curr->num_completed == curr->num_total_tasks) {
			printf("Thread #%d finished running all %d internal tasks of task %d\n", threadId, curr->num_completed.load(), taskId);
			//std::cout << "Thread #" << threadId << " waking on master_cv" << std::endl;
			
			completed->insert(taskId);
			taskdb->erase(taskId);
			for (TaskID dependent : (*depchildren)[taskId]) {
				if (--(*taskdb)[dependent]->num_deps == 0)
					rdyQ->emplace(dependent);
			}
			depchildren->erase(taskId);
			if (num_runs->load() == completed->size()) {
				printf("Thread #%d waking master_cv. Finished %d tasks overall\n", threadId, completed->size());
				master_cv->notify_one();
			}
			qlock.unlock();
			delete curr;
		} else qlock.unlock();
	}
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads),
	_num_threads(num_threads), _nextID(0), _worker_cv(new std::condition_variable), _mtex(), _ulock(_mtex, std::defer_lock),
	_master_cv(), _readyTasks(), _taskDB(), _depChildren(), _workers(new std::thread[num_threads]), _completed(),
	_quit(new bool(false)) {
	for (int i = 0; i < num_threads; ++i) {
		_workers[i] = std::thread(IRunnable_sleep, &_readyTasks, &_completed, &_taskDB, &_depChildren, _quit, _worker_cv, &_master_cv, &_mtex, &_numRuns, i);
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
	runAsyncWithDeps(runnable, num_total_tasks, {});
	sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //
	++_numRuns;
	int taskId = _nextID++;
	TaskContext *curr = new TaskContext(runnable, num_total_tasks, deps);
	_ulock.lock(); // maybe can shift down 
	_taskDB[taskId] = curr;
	for (TaskID id : deps) {
		if (_completed.count(id)) {
			--curr->num_deps;
		} else {
			//vec = depChildren[id]
			_depChildren[id].emplace(_depChildren[id].end(), taskId);
		}
	}
	if (curr->num_deps == 0) {
		_readyTasks.push(taskId);
	}
	_ulock.unlock();
	
    return taskId;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
	while (true) {
		_ulock.lock();
		while (_completed.size() != _numRuns.load())
			_master_cv.wait(_ulock);
		_ulock.unlock();
		return;
	}
}
