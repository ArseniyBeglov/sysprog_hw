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
	return free_idx;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
	if (bus == NULL || bus->channels == NULL || channel < 0 ||
	    channel >= bus->channel_count || bus->channels[channel] == NULL)
		return;

	struct coro_bus_channel *ch = bus->channels[channel];
	bus->channels[channel] = NULL;

	wakeup_queue_wakeup_all(&ch->send_queue);
	wakeup_queue_wakeup_all(&ch->recv_queue);

#if NEED_BROADCAST
	wakeup_queue_wakeup_all(&bus->broadcast_queue);
#endif

	delete ch;
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
