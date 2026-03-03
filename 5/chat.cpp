#include "chat.h"

#include <cstring>
#include <new>
#include <poll.h>

void *
operator new[](std::size_t size)
{
	void *ptr = ::operator new(size);
	memset(ptr, 0, size);
	return ptr;
}

void
operator delete[](void *ptr) noexcept
{
	::operator delete(ptr);
}

void
operator delete[](void *ptr, std::size_t) noexcept
{
	::operator delete(ptr);
}

int
chat_events_to_poll_events(int mask)
{
	int res = 0;
	if ((mask & CHAT_EVENT_INPUT) != 0)
		res |= POLLIN;
	if ((mask & CHAT_EVENT_OUTPUT) != 0)
		res |= POLLOUT;
	return res;
}
