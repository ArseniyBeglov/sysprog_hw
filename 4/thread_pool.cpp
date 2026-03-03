#include "thread_pool.h"

#include <deque>
#include <cmath>
#include <cerrno>
#include <pthread.h>
#include <time.h>
#include <vector>

enum task_state {
	TASK_STATE_NEW = 0,
	TASK_STATE_QUEUED,
	TASK_STATE_RUNNING,
	TASK_STATE_FINISHED,
};

struct thread_task {
	thread_task_f function;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	task_state state = TASK_STATE_NEW;
	bool is_pushed = false;
	bool is_detached = false;
	bool is_joined = false;
	int join_waiters = 0;
	struct thread_pool *pool = nullptr;
};

struct thread_pool {
	int max_threads = 0;
	std::vector<pthread_t> threads;
	std::deque<thread_task*> queue;
	size_t waiting_threads = 0;
	size_t tasks_in_pool = 0;
	bool is_stopping = false;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
};

static void
thread_pool_task_done(struct thread_pool *pool)
{
	pthread_mutex_lock(&pool->mutex);
	--pool->tasks_in_pool;
	pthread_cond_broadcast(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);
}

static void *
thread_pool_worker(void *arg)
{
	auto *pool = static_cast<thread_pool*>(arg);
	pthread_mutex_lock(&pool->mutex);
	while (true) {
		while (pool->queue.empty() && !pool->is_stopping) {
			++pool->waiting_threads;
			pthread_cond_wait(&pool->cond, &pool->mutex);
			--pool->waiting_threads;
		}
		if (pool->is_stopping && pool->queue.empty()) {
			pthread_mutex_unlock(&pool->mutex);
			return nullptr;
		}
		thread_task *task = pool->queue.front();
		pool->queue.pop_front();
		pthread_mutex_unlock(&pool->mutex);

		pthread_mutex_lock(&task->mutex);
		task->state = TASK_STATE_RUNNING;
		pthread_mutex_unlock(&task->mutex);

		task->function();

		bool should_auto_delete = false;
		pthread_mutex_lock(&task->mutex);
		task->state = TASK_STATE_FINISHED;
		if (task->is_detached && task->is_pushed &&
		    task->join_waiters == 0 && !task->is_joined) {
			task->is_pushed = false;
			task->pool = nullptr;
			should_auto_delete = true;
		}
		pthread_cond_broadcast(&task->cond);
		pthread_mutex_unlock(&task->mutex);

		if (should_auto_delete) {
			thread_pool_task_done(pool);
			pthread_cond_destroy(&task->cond);
			pthread_mutex_destroy(&task->mutex);
			delete task;
		}

		pthread_mutex_lock(&pool->mutex);
	}
}

static int
thread_pool_maybe_start_worker_locked(struct thread_pool *pool)
{
	if (pool->threads.size() >= static_cast<size_t>(pool->max_threads))
		return 0;
	if (pool->waiting_threads > 0)
		return 0;
	pthread_t tid;
	int rc = pthread_create(&tid, nullptr, thread_pool_worker, pool);
	if (rc != 0)
		return rc;
	pool->threads.push_back(tid);
	return 0;
}

static void
timespec_add_seconds(struct timespec *ts, double seconds)
{
	if (seconds <= 0)
		return;
	double integral = 0;
	double fractional = std::modf(seconds, &integral);
	ts->tv_sec += static_cast<time_t>(integral);
	long add_nsec = static_cast<long>(fractional * 1000000000.0);
	ts->tv_nsec += add_nsec;
	if (ts->tv_nsec >= 1000000000L) {
		++ts->tv_sec;
		ts->tv_nsec -= 1000000000L;
	}
}

int
thread_pool_new(int thread_count, struct thread_pool **pool)
{
	if (pool == nullptr || thread_count <= 0 ||
	    thread_count > TPOOL_MAX_THREADS)
		return TPOOL_ERR_INVALID_ARGUMENT;

	auto *result = new thread_pool();
	result->max_threads = thread_count;
	pthread_mutex_init(&result->mutex, nullptr);
	pthread_cond_init(&result->cond, nullptr);
	*pool = result;
	return 0;
}

int
thread_pool_delete(struct thread_pool *pool)
{
	if (pool == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	pthread_mutex_lock(&pool->mutex);
	if (pool->tasks_in_pool != 0) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_HAS_TASKS;
	}
	pool->is_stopping = true;
	pthread_cond_broadcast(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);

	for (pthread_t tid : pool->threads)
		pthread_join(tid, nullptr);

	pthread_cond_destroy(&pool->cond);
	pthread_mutex_destroy(&pool->mutex);
	delete pool;
	return 0;
}

int
thread_pool_push_task(struct thread_pool *pool, struct thread_task *task)
{
	if (pool == nullptr || task == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	pthread_mutex_lock(&pool->mutex);
	if (pool->is_stopping) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_INVALID_ARGUMENT;
	}
	if (pool->tasks_in_pool >= TPOOL_MAX_TASKS) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_TOO_MANY_TASKS;
	}

	pthread_mutex_lock(&task->mutex);
	task->pool = pool;
	task->state = TASK_STATE_QUEUED;
	task->is_pushed = true;
	task->is_detached = false;
	task->is_joined = false;
	task->join_waiters = 0;
	pthread_mutex_unlock(&task->mutex);

	pool->queue.push_back(task);
	++pool->tasks_in_pool;
	thread_pool_maybe_start_worker_locked(pool);
	pthread_cond_signal(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);
	return 0;
}

int
thread_task_new(struct thread_task **task, const thread_task_f &function)
{
	auto *result = new thread_task();
	result->function = function;
	pthread_mutex_init(&result->mutex, nullptr);
	pthread_cond_init(&result->cond, nullptr);
	*task = result;
	return 0;
}

bool
thread_task_is_finished(const struct thread_task *task)
{
	if (task == nullptr)
		return false;
	auto *mutable_task = const_cast<thread_task*>(task);
	pthread_mutex_lock(&mutable_task->mutex);
	bool is_finished = mutable_task->is_joined;
	pthread_mutex_unlock(&mutable_task->mutex);
	return is_finished;
}

bool
thread_task_is_running(const struct thread_task *task)
{
	if (task == nullptr)
		return false;
	auto *mutable_task = const_cast<thread_task*>(task);
	pthread_mutex_lock(&mutable_task->mutex);
	bool is_running = mutable_task->state == TASK_STATE_RUNNING;
	pthread_mutex_unlock(&mutable_task->mutex);
	return is_running;
}

int
thread_task_join(struct thread_task *task)
{
	if (task == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	thread_pool *pool = nullptr;
	pthread_mutex_lock(&task->mutex);
	if (task->is_joined) {
		pthread_mutex_unlock(&task->mutex);
		return 0;
	}
	if (!task->is_pushed) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	++task->join_waiters;
	while (task->is_pushed && task->state != TASK_STATE_FINISHED)
		pthread_cond_wait(&task->cond, &task->mutex);
	--task->join_waiters;
	if (task->is_joined) {
		pthread_mutex_unlock(&task->mutex);
		return 0;
	}
	if (!task->is_pushed) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	pool = task->pool;
	task->is_pushed = false;
	task->pool = nullptr;
	task->is_detached = false;
	task->is_joined = true;
	pthread_mutex_unlock(&task->mutex);

	thread_pool_task_done(pool);
	return 0;
}

#if NEED_TIMED_JOIN

int
thread_task_timed_join(struct thread_task *task, double timeout)
{
	if (task == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	if (!std::isfinite(timeout) || timeout > 100000000.0)
		return thread_task_join(task);

	thread_pool *pool = nullptr;
	pthread_mutex_lock(&task->mutex);
	if (task->is_joined) {
		pthread_mutex_unlock(&task->mutex);
		return 0;
	}
	if (!task->is_pushed) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	if (timeout <= 0 && task->state != TASK_STATE_FINISHED) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TIMEOUT;
	}
	++task->join_waiters;
	if (task->state != TASK_STATE_FINISHED) {
		struct timespec deadline;
		clock_gettime(CLOCK_REALTIME, &deadline);
		timespec_add_seconds(&deadline, timeout);
		while (task->is_pushed && task->state != TASK_STATE_FINISHED) {
			int rc = pthread_cond_timedwait(&task->cond, &task->mutex,
							&deadline);
			if (rc == ETIMEDOUT && task->state != TASK_STATE_FINISHED) {
				--task->join_waiters;
				pthread_mutex_unlock(&task->mutex);
				return TPOOL_ERR_TIMEOUT;
			}
		}
	}
	--task->join_waiters;
	if (task->is_joined) {
		pthread_mutex_unlock(&task->mutex);
		return 0;
	}
	if (!task->is_pushed) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	pool = task->pool;
	task->is_pushed = false;
	task->pool = nullptr;
	task->is_detached = false;
	task->is_joined = true;
	pthread_mutex_unlock(&task->mutex);

	thread_pool_task_done(pool);
	return 0;
}

#endif

int
thread_task_delete(struct thread_task *task)
{
	if (task == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	pthread_mutex_lock(&task->mutex);
	bool is_pushed = task->is_pushed;
	pthread_mutex_unlock(&task->mutex);
	if (is_pushed)
		return TPOOL_ERR_TASK_IN_POOL;
	pthread_cond_destroy(&task->cond);
	pthread_mutex_destroy(&task->mutex);
	delete task;
	return 0;
}

#if NEED_DETACH

int
thread_task_detach(struct thread_task *task)
{
	if (task == nullptr)
		return TPOOL_ERR_INVALID_ARGUMENT;

	thread_pool *pool = nullptr;
	bool should_delete_now = false;

	pthread_mutex_lock(&task->mutex);
	if (task->is_joined) {
		pthread_mutex_unlock(&task->mutex);
		return 0;
	}
	if (!task->is_pushed) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	task->is_detached = true;
	if (task->state == TASK_STATE_FINISHED &&
	    task->join_waiters == 0 && !task->is_joined) {
		pool = task->pool;
		task->is_pushed = false;
		task->pool = nullptr;
		should_delete_now = true;
	}
	pthread_mutex_unlock(&task->mutex);

	if (should_delete_now) {
		thread_pool_task_done(pool);
		pthread_cond_destroy(&task->cond);
		pthread_mutex_destroy(&task->mutex);
		delete task;
	}
	return 0;
}

#endif
