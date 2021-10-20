#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <atomic>
#include <condition_variable>
#include "itasksys.h"
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <unordered_set>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};


struct TaskContext {
	IRunnable* 	      runnable;
	int 	   		  num_total_tasks;
	int				  num_deps;
	int 			  curr_task_id;
	std::atomic<int>  num_completed;
	TaskContext(IRunnable* runnable, int num_total_tasks, const std::vector<int>& dep)
	: runnable(runnable), num_total_tasks(num_total_tasks), num_deps(dep.size()), curr_task_id(0), num_completed(0) { }
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
	private:
		int _num_threads;
		std::atomic<unsigned> _numRuns;
		std::atomic<int> _nextID;
		
		std::condition_variable *_worker_cv;
		std::mutex _mtex;
		std::unique_lock<std::mutex> _ulock;	
		std::condition_variable _master_cv;
		
		std::queue<TaskID> _readyTasks;
		std::unordered_map<TaskID, TaskContext*> _taskDB;
		std::unordered_map<TaskID, std::vector<TaskID>> _depChildren;
		std::thread *_workers;
		std::unordered_set<TaskID> _completed;
		bool *_quit;
};
#endif
