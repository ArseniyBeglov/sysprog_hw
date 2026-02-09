#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <deque>

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
	struct rlist base;
	struct coro *coro;
};

/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
	struct rlist coros;
};

static void
wakeup_queue_create(struct wakeup_queue *queue)
{
	rlist_create(&queue->coros);
}

/** Suspend the current coroutine until it is woken up. */
static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
	struct wakeup_entry entry;
	rlist_create(&entry.base);
	entry.coro = coro_this();
	rlist_add_tail_entry(&queue->coros, &entry, base);
	coro_suspend();
	/* Safe even if already removed by a waker. */
	rlist_del_entry(&entry, base);
}

/** Wakeup the first coroutine in the queue (if any). */
static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
	if (rlist_empty(&queue->coros))
		return;
	struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
		struct wakeup_entry, base);
	/* Detach before wakeup so the queue owner may be destroyed safely. */
	rlist_del_entry(entry, base);
	coro_wakeup(entry->coro);
}

static void
wakeup_queue_wakeup_all(struct wakeup_queue *queue)
{
	while (!rlist_empty(&queue->coros)) {
		struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
			struct wakeup_entry, base);
		rlist_del_entry(entry, base);
		coro_wakeup(entry->coro);
	}
}

struct coro_bus_channel {
	/** Channel max capacity. */
	size_t size_limit;
	/** Coroutines waiting until the channel is not full. */
	struct wakeup_queue send_queue;
	/** Coroutines waiting until the channel is not empty. */
	struct wakeup_queue recv_queue;
	/** Message queue. */
	std::deque<unsigned> data;
};

struct coro_bus {
	struct coro_bus_channel **channels;
	int channel_count;
#if NEED_BROADCAST
	struct wakeup_queue broadcast_queue;
#endif
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
	return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

static struct coro_bus_channel *
coro_bus_get_channel(struct coro_bus *bus, int channel)
{
	if (bus == NULL || channel < 0 || channel >= bus->channel_count ||
	    bus->channels == NULL || bus->channels[channel] == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return NULL;
	}
	return bus->channels[channel];
}

struct coro_bus *
coro_bus_new(void)
{
	struct coro_bus *bus = new coro_bus;
	bus->channels = NULL;
	bus->channel_count = 0;
#if NEED_BROADCAST
	wakeup_queue_create(&bus->broadcast_queue);
#endif
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
	if (bus == NULL)
		return;
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels == NULL || bus->channels[i] == NULL)
			continue;
		/* Per spec, no suspended coroutines should exist here. */
		assert(rlist_empty(&bus->channels[i]->send_queue.coros));
		assert(rlist_empty(&bus->channels[i]->recv_queue.coros));
		delete bus->channels[i];
		bus->channels[i] = NULL;
	}
	delete[] bus->channels;
	bus->channels = NULL;
	bus->channel_count = 0;
	delete bus;
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
	if (bus == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	int free_idx = -1;
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] == NULL) {
			free_idx = i;
			break;
		}
	}
	if (free_idx < 0) {
		const int new_count = bus->channel_count + 1;
		auto **new_channels = new coro_bus_channel *[new_count];
		for (int i = 0; i < new_count; ++i)
			new_channels[i] = NULL;
		for (int i = 0; i < bus->channel_count; ++i)
			new_channels[i] = bus->channels[i];
		delete[] bus->channels;
		bus->channels = new_channels;
		free_idx = bus->channel_count;
		bus->channel_count = new_count;
	}

	struct coro_bus_channel *ch = new coro_bus_channel;
	ch->size_limit = size_limit;
	wakeup_queue_create(&ch->send_queue);
	wakeup_queue_create(&ch->recv_queue);
	ch->data.clear();
	bus->channels[free_idx] = ch;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	/*
	 * One of the tests will force you to reuse the channel
	 * descriptors. It means, that if your maximal channel
	 * descriptor is N, and you have any free descriptor in
	 * the range 0-N, then you should open the new channel on
	 * that old descriptor.
	 *
	 * A more precise instruction - check if any of the
	 * bus->channels[i] with i = 0 -> bus->channel_count is
	 * free (== NULL). If yes - reuse the slot. Don't grow the
	 * bus->channels array, when have space in it.
	 */
	return free_idx;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
	if (bus == NULL || bus->channels == NULL || channel < 0 ||
	    channel >= bus->channel_count || bus->channels[channel] == NULL)
		return;

	struct coro_bus_channel *ch = bus->channels[channel];
	/* Make it disappear first so waiters observe NO_CHANNEL. */
	bus->channels[channel] = NULL;

	/* Detach all waiters before destroying the channel. */
	wakeup_queue_wakeup_all(&ch->send_queue);
	wakeup_queue_wakeup_all(&ch->recv_queue);

#if NEED_BROADCAST
	/* Channel set changed - may unblock broadcasters. */
	wakeup_queue_wakeup_all(&bus->broadcast_queue);
#endif

	delete ch;
	/*
	 * Be very attentive here. What happens, if the channel is
	 * closed while there are coroutines waiting on it? For
	 * example, the channel was empty, and some coros were
	 * waiting on its recv_queue.
	 *
	 * If you wakeup those coroutines and just delete the
	 * channel right away, then those waiting coroutines might
	 * on wakeup try to reference invalid memory.
	 *
	 * Can happen, for example, if you use an intrusive list
	 * (rlist), delete the list itself (by deleting the
	 * channel), and then the coroutines on wakeup would try
	 * to remove themselves from the already destroyed list.
	 *
	 * Think how you could address that. Remove all the
	 * waiters from the list before freeing it? Yield this
	 * coroutine after waking up the waiters but before
	 * freeing the channel, so the waiters could safely leave?
	 */
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
	while (true) {
		struct coro_bus_channel *ch = coro_bus_get_channel(bus, channel);
		if (ch == NULL)
			return -1;
		if (ch->data.size() < ch->size_limit) {
			ch->data.push_back(data);
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			wakeup_queue_wakeup_first(&ch->recv_queue);
			return 0;
		}
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		wakeup_queue_suspend_this(&ch->send_queue);
	}
	/*
	 * Try sending in a loop, until success. If error, then
	 * check which one is that. If 'wouldblock', then suspend
	 * this coroutine and try again when woken up.
	 *
	 * If see the channel has space, then wakeup the first
	 * coro in the send-queue. That is needed so when there is
	 * enough space for many messages, and many coroutines are
	 * waiting, they would then wake each other up one by one
	 * as lone as there is still space.
	 */
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
	struct coro_bus_channel *ch = coro_bus_get_channel(bus, channel);
	if (ch == NULL)
		return -1;
	if (ch->data.size() >= ch->size_limit) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	ch->data.push_back(data);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	wakeup_queue_wakeup_first(&ch->recv_queue);
	return 0;
	/*
	 * Append data if has space. Otherwise 'wouldblock' error.
	 * Wakeup the first coro in the recv-queue! To let it know
	 * there is data.
	 */
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	while (true) {
		struct coro_bus_channel *ch = coro_bus_get_channel(bus, channel);
		if (ch == NULL)
			return -1;
		if (!ch->data.empty()) {
			*data = ch->data.front();
			ch->data.pop_front();
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			wakeup_queue_wakeup_first(&ch->send_queue);
#if NEED_BROADCAST
			wakeup_queue_wakeup_first(&bus->broadcast_queue);
#endif
			return 0;
		}
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		wakeup_queue_suspend_this(&ch->recv_queue);
	}
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	struct coro_bus_channel *ch = coro_bus_get_channel(bus, channel);
	if (ch == NULL)
		return -1;
	if (ch->data.empty()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	*data = ch->data.front();
	ch->data.pop_front();
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	wakeup_queue_wakeup_first(&ch->send_queue);
#if NEED_BROADCAST
	wakeup_queue_wakeup_first(&bus->broadcast_queue);
#endif
	return 0;
}


#if NEED_BROADCAST

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	while (true) {
		if (bus == NULL || bus->channels == NULL || bus->channel_count == 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		bool has_any = false;
		bool all_have_space = true;
		for (int i = 0; i < bus->channel_count; ++i) {
			struct coro_bus_channel *ch = bus->channels[i];
			if (ch == NULL)
				continue;
			has_any = true;
			if (ch->data.size() >= ch->size_limit) {
				all_have_space = false;
				break;
			}
		}
		if (!has_any) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		if (all_have_space) {
			for (int i = 0; i < bus->channel_count; ++i) {
				struct coro_bus_channel *ch = bus->channels[i];
				if (ch == NULL)
					continue;
				ch->data.push_back(data);
				wakeup_queue_wakeup_first(&ch->recv_queue);
			}
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return 0;
		}
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		wakeup_queue_suspend_this(&bus->broadcast_queue);
	}
}

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
	if (bus == NULL || bus->channels == NULL || bus->channel_count == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	bool has_any = false;
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch == NULL)
			continue;
		has_any = true;
		if (ch->data.size() >= ch->size_limit) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			return -1;
		}
	}
	if (!has_any) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch == NULL)
			continue;
		ch->data.push_back(data);
		wakeup_queue_wakeup_first(&ch->recv_queue);
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

#endif

#if NEED_BATCH

int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	if (count == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return 0;
	}
	while (true) {
		struct coro_bus_channel *ch = coro_bus_get_channel(bus, channel);
		if (ch == NULL)
			return -1;
		if (ch->data.size() >= ch->size_limit) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			wakeup_queue_suspend_this(&ch->send_queue);
			continue;
		}
		const size_t space = ch->size_limit - ch->data.size();
		const unsigned to_send = std::min<unsigned>((unsigned)space, count);
		for (unsigned i = 0; i < to_send; ++i)
			ch->data.push_back(data[i]);
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		for (unsigned i = 0; i < to_send; ++i)
			wakeup_queue_wakeup_first(&ch->recv_queue);
		return (int)to_send;
	}
}

int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	if (count == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return 0;
	}
	struct coro_bus_channel *ch = coro_bus_get_channel(bus, channel);
	if (ch == NULL)
		return -1;
	if (ch->data.size() >= ch->size_limit) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	const size_t space = ch->size_limit - ch->data.size();
	const unsigned to_send = std::min<unsigned>((unsigned)space, count);
	for (unsigned i = 0; i < to_send; ++i)
		ch->data.push_back(data[i]);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	for (unsigned i = 0; i < to_send; ++i)
		wakeup_queue_wakeup_first(&ch->recv_queue);
	return (int)to_send;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	if (capacity == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return 0;
	}
	while (true) {
		struct coro_bus_channel *ch = coro_bus_get_channel(bus, channel);
		if (ch == NULL)
			return -1;
		if (ch->data.empty()) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			wakeup_queue_suspend_this(&ch->recv_queue);
			continue;
		}
		const unsigned to_recv = std::min<unsigned>((unsigned)ch->data.size(), capacity);
		for (unsigned i = 0; i < to_recv; ++i) {
			data[i] = ch->data.front();
			ch->data.pop_front();
		}
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		for (unsigned i = 0; i < to_recv; ++i)
			wakeup_queue_wakeup_first(&ch->send_queue);
#if NEED_BROADCAST
		wakeup_queue_wakeup_first(&bus->broadcast_queue);
#endif
		return (int)to_recv;
	}
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	if (capacity == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return 0;
	}
	struct coro_bus_channel *ch = coro_bus_get_channel(bus, channel);
	if (ch == NULL)
		return -1;
	if (ch->data.empty()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	const unsigned to_recv = std::min<unsigned>((unsigned)ch->data.size(), capacity);
	for (unsigned i = 0; i < to_recv; ++i) {
		data[i] = ch->data.front();
		ch->data.pop_front();
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	for (unsigned i = 0; i < to_recv; ++i)
		wakeup_queue_wakeup_first(&ch->send_queue);
#if NEED_BROADCAST
	wakeup_queue_wakeup_first(&bus->broadcast_queue);
#endif
	return (int)to_recv;
}

#endif
