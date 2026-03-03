#include "chat.h"
#include "chat_client.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <deque>
#include <string>
#include <string_view>

namespace {

// Grow-only byte buffer with a read cursor.
struct ByteBuf {
	std::string b;
	size_t r = 0;

	size_t size() const { return b.size() - r; }
	const char *data() const { return b.data() + r; }

	void append(const char *p, size_t n) { b.append(p, n); }
	void append(std::string_view sv) { b.append(sv.data(), sv.size()); }

	void consume(size_t n)
	{
		r += n;
		if (r == 0)
			return;
		if (r == b.size()) {
			b.clear();
			r = 0;
			return;
		}
		// Compact from time to time to avoid unbounded growth.
		if (r > (1u << 20)) {
			b.erase(0, r);
			r = 0;
		}
	}
};

static inline uint32_t
u32_be_load(const char *p)
{
	uint32_t v;
	memcpy(&v, p, sizeof(v));
	return ntohl(v);
}

static inline void
u32_be_append(std::string &out, uint32_t v)
{
	uint32_t net = htonl(v);
	out.append((const char *)&net, sizeof(net));
}

enum : uint8_t {
	PKT_NAME = 1,
	PKT_TEXT = 2,
	PKT_BROADCAST = 3,
};

static void
pkt_append_name(ByteBuf &out, std::string_view name)
{
	out.b.push_back((char)PKT_NAME);
	u32_be_append(out.b, (uint32_t)name.size());
	out.append(name);
}

static void
pkt_append_text(ByteBuf &out, std::string_view text)
{
	out.b.push_back((char)PKT_TEXT);
	u32_be_append(out.b, (uint32_t)text.size());
	out.append(text);
}

static bool
pkt_try_parse_broadcast(ByteBuf &in, std::string &author, std::string &text)
{
	if (in.size() < 1 + 4 + 4)
		return false;
	const char *p = in.data();
	if ((uint8_t)p[0] != PKT_BROADCAST)
		return false;
	uint32_t a_len = u32_be_load(p + 1);
	uint32_t t_len = u32_be_load(p + 1 + 4);
	size_t need = 1 + 4 + 4 + (size_t)a_len + (size_t)t_len;
	if (in.size() < need)
		return false;
	author.assign(p + 1 + 4 + 4, a_len);
	text.assign(p + 1 + 4 + 4 + a_len, t_len);
	in.consume(need);
	return true;
}

static std::string
trim_ws(std::string_view s)
{
	size_t i = 0;
	size_t j = s.size();
	while (i < j && isspace((unsigned char)s[i]))
		++i;
	while (j > i && isspace((unsigned char)s[j - 1]))
		--j;
	return std::string(s.substr(i, j - i));
}

static int
fd_make_nonblock(int fd)
{
	int fl = fcntl(fd, F_GETFL, 0);
	if (fl < 0)
		return -1;
	if (fcntl(fd, F_SETFL, fl | O_NONBLOCK) != 0)
		return -1;
	return 0;
}

static int
fd_flush(int fd, ByteBuf &out)
{
	while (out.size() != 0) {
		ssize_t rc;
#ifdef MSG_NOSIGNAL
		rc = send(fd, out.data(), out.size(), MSG_NOSIGNAL);
#else
		rc = send(fd, out.data(), out.size(), 0);
#endif
		if (rc > 0) {
			out.consume((size_t)rc);
			continue;
		}
		if (rc < 0 && errno == EINTR)
			continue;
		if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
			return 0;
		return CHAT_ERR_SYS;
	}
	return 0;
}

static int
fd_read_all(int fd, ByteBuf &in)
{
	char tmp[64 * 1024];
	while (true) {
		ssize_t rc = recv(fd, tmp, sizeof(tmp), 0);
		if (rc > 0) {
			in.append(tmp, (size_t)rc);
			continue;
		}
		if (rc == 0)
			return CHAT_ERR_SYS;
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return 0;
		return CHAT_ERR_SYS;
	}
}

} // namespace

struct chat_client {
	int sock = -1;
	std::string my_name;
	bool name_sent = false;
	std::deque<chat_message *> inbox;
	ByteBuf net_in;
	ByteBuf net_out;
	std::string text_acc;
};

struct chat_client *
chat_client_new(std::string_view name)
{
	auto *c = new chat_client();
	c->my_name = std::string(name);
	return c;
}

void
chat_client_delete(struct chat_client *client)
{
	if (client == nullptr)
		return;
	if (client->sock >= 0)
		close(client->sock);
	for (auto *m : client->inbox)
		delete m;
	delete client;
}

int
chat_client_connect(struct chat_client *client, std::string_view addr)
{
	if (client->sock >= 0)
		return CHAT_ERR_ALREADY_STARTED;

	size_t colon = addr.rfind(':');
	if (colon == std::string_view::npos || colon == 0 || colon + 1 >= addr.size())
		return CHAT_ERR_NO_ADDR;
	std::string host(addr.substr(0, colon));
	std::string port(addr.substr(colon + 1));

	addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	addrinfo *ai = nullptr;
	int grc = getaddrinfo(host.c_str(), port.c_str(), &hints, &ai);
	if (grc != 0)
		return CHAT_ERR_NO_ADDR;

	int fd = -1;
	int last_errno = 0;
	for (addrinfo *it = ai; it != nullptr; it = it->ai_next) {
		fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
		if (fd < 0) {
			last_errno = errno;
			continue;
		}
		if (connect(fd, it->ai_addr, it->ai_addrlen) == 0)
			break;
		last_errno = errno;
		close(fd);
		fd = -1;
	}
	freeaddrinfo(ai);
	if (fd < 0) {
		errno = last_errno;
		return CHAT_ERR_SYS;
	}
	if (fd_make_nonblock(fd) != 0) {
		close(fd);
		return CHAT_ERR_SYS;
	}
	client->sock = fd;

#if NEED_AUTHOR
	// Send our name once right after connect.
	pkt_append_name(client->net_out, client->my_name);
	client->name_sent = true;
	int rc = fd_flush(client->sock, client->net_out);
	if (rc != 0)
		return rc;
#else
	client->name_sent = true;
#endif
	return 0;
}

struct chat_message *
chat_client_pop_next(struct chat_client *client)
{
	if (client->inbox.empty())
		return nullptr;
	auto *m = client->inbox.front();
	client->inbox.pop_front();
	return m;
}

int
chat_client_update(struct chat_client *client, double timeout)
{
	if (client->sock < 0)
		return CHAT_ERR_NOT_STARTED;

	int timeout_ms = -1;
	if (timeout >= 0) {
		double ms = timeout * 1000.0;
		timeout_ms = (int)(ms + 0.999999);
	}

	pollfd pfd;
	memset(&pfd, 0, sizeof(pfd));
	pfd.fd = client->sock;
	pfd.events = chat_events_to_poll_events(chat_client_get_events(client));

	int prc = poll(&pfd, 1, timeout_ms);
	if (prc < 0)
		return CHAT_ERR_SYS;
	if (prc == 0)
		return CHAT_ERR_TIMEOUT;
	if ((pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0)
		return CHAT_ERR_SYS;

	bool did = false;
	if ((pfd.revents & POLLIN) != 0) {
		int rc = fd_read_all(client->sock, client->net_in);
		if (rc != 0)
			return rc;
		std::string a, t;
		while (pkt_try_parse_broadcast(client->net_in, a, t)) {
			auto *m = new chat_message();
			m->data = std::move(t);
#if NEED_AUTHOR
			m->author = std::move(a);
#endif
			client->inbox.push_back(m);
			a.clear();
			t.clear();
		}
		did = true;
	}
	if ((pfd.revents & POLLOUT) != 0 || client->net_out.size() != 0) {
		int rc = fd_flush(client->sock, client->net_out);
		if (rc != 0)
			return rc;
		did = true;
	}

	return did ? 0 : CHAT_ERR_TIMEOUT;
}

int
chat_client_get_descriptor(const struct chat_client *client)
{
	return client->sock;
}

int
chat_client_get_events(const struct chat_client *client)
{
	if (client->sock < 0)
		return 0;
	int mask = CHAT_EVENT_INPUT;
	if (client->net_out.size() != 0)
		mask |= CHAT_EVENT_OUTPUT;
	return mask;
}

int
chat_client_feed(struct chat_client *client, const char *msg, uint32_t msg_size)
{
	if (client->sock < 0)
		return CHAT_ERR_NOT_STARTED;

	client->text_acc.append(msg, msg_size);
	while (true) {
		size_t nl = client->text_acc.find('\n');
		if (nl == std::string::npos)
			break;
		std::string line = client->text_acc.substr(0, nl);
		client->text_acc.erase(0, nl + 1);
		std::string body = trim_ws(line);
		if (body.empty())
			continue;
		pkt_append_text(client->net_out, body);
	}

	return fd_flush(client->sock, client->net_out);
}
