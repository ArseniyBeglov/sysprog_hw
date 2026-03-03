#include "chat.h"
#include "chat_server.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <deque>
#include <string>
#include <string_view>
#include <vector>

namespace {

struct ByteBuf {
	std::string b;
	size_t r = 0;

	size_t size() const { return b.size() - r; }
	const char *data() const { return b.data() + r; }

	void append(const char *p, size_t n) { b.append(p, n); }
	void append(std::string_view sv) { b.append(sv.data(), sv.size()); }
	void push_u8(uint8_t v) { b.push_back((char)v); }

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

static void
pkt_append_broadcast(ByteBuf &out, std::string_view author, std::string_view text)
{
	out.push_u8(PKT_BROADCAST);
	u32_be_append(out.b, (uint32_t)author.size());
	u32_be_append(out.b, (uint32_t)text.size());
	out.append(author);
	out.append(text);
}

struct Peer {
	int fd = -1;
	ByteBuf in;
	ByteBuf out;
	bool have_name = false;
	std::string name;
};

static bool
peer_try_parse_one(Peer &p, uint8_t &type, std::string &payload)
{
	if (p.in.size() < 1 + 4)
		return false;
	const char *d = p.in.data();
	type = (uint8_t)d[0];
	uint32_t len = u32_be_load(d + 1);
	size_t need = 1 + 4 + (size_t)len;
	if (p.in.size() < need)
		return false;
	payload.assign(d + 1 + 4, len);
	p.in.consume(need);
	return true;
}

static void
peer_close_and_delete(int epfd, Peer *p)
{
	if (p == nullptr)
		return;
	if (p->fd >= 0) {
		epoll_ctl(epfd, EPOLL_CTL_DEL, p->fd, nullptr);
		close(p->fd);
		p->fd = -1;
	}
	delete p;
}

}

struct chat_server {
	int listen_fd = -1;
	int epfd = -1;
	std::vector<Peer *> peers;
	std::deque<chat_message *> inbox;
	std::string admin_acc;
};

struct chat_server *
chat_server_new(void)
{
	auto *s = new chat_server();
	return s;
}

void
chat_server_delete(struct chat_server *server)
{
	if (server == nullptr)
		return;
	for (auto *m : server->inbox)
		delete m;
	server->inbox.clear();

	if (server->epfd >= 0) {

		for (auto *p : server->peers)
			peer_close_and_delete(server->epfd, p);
		server->peers.clear();
		close(server->epfd);
		server->epfd = -1;
	}
	if (server->listen_fd >= 0) {
		close(server->listen_fd);
		server->listen_fd = -1;
	}
	delete server;
}

int
chat_server_listen(struct chat_server *server, uint16_t port)
{
	if (server->listen_fd >= 0)
		return CHAT_ERR_ALREADY_STARTED;

	int lfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (lfd < 0)
		return CHAT_ERR_SYS;

	int one = 1;
	setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

	sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = htons(port);

	if (bind(lfd, (sockaddr *)&addr, sizeof(addr)) != 0) {
		int err = errno;
		close(lfd);
		if (err == EADDRINUSE)
			return CHAT_ERR_PORT_BUSY;
		errno = err;
		return CHAT_ERR_SYS;
	}
	if (listen(lfd, 128) != 0) {
		close(lfd);
		return CHAT_ERR_SYS;
	}
	if (fd_make_nonblock(lfd) != 0) {
		close(lfd);
		return CHAT_ERR_SYS;
	}

	int epfd = epoll_create1(0);
	if (epfd < 0) {
		close(lfd);
		return CHAT_ERR_SYS;
	}

	epoll_event ev;
	memset(&ev, 0, sizeof(ev));
	ev.events = EPOLLIN | EPOLLET;
	ev.data.ptr = server;
	if (epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &ev) != 0) {
		close(epfd);
		close(lfd);
		return CHAT_ERR_SYS;
	}

	server->listen_fd = lfd;
	server->epfd = epfd;
	return 0;
}

struct chat_message *
chat_server_pop_next(struct chat_server *server)
{
	if (server->inbox.empty())
		return nullptr;
	auto *m = server->inbox.front();
	server->inbox.pop_front();
	return m;
}

static int
server_accept_all(chat_server *s)
{
	while (true) {
		sockaddr_in cli;
		socklen_t len = sizeof(cli);
		int cfd = accept(s->listen_fd, (sockaddr *)&cli, &len);
		if (cfd < 0) {
			if (errno == EINTR)
				continue;
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				return 0;
			return CHAT_ERR_SYS;
		}
		if (fd_make_nonblock(cfd) != 0) {
			close(cfd);
			return CHAT_ERR_SYS;
		}
		auto *p = new Peer();
		p->fd = cfd;

		epoll_event ev;
		memset(&ev, 0, sizeof(ev));


		ev.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
		ev.data.ptr = p;
		if (epoll_ctl(s->epfd, EPOLL_CTL_ADD, cfd, &ev) != 0) {
			close(cfd);
			delete p;
			return CHAT_ERR_SYS;
		}
		s->peers.push_back(p);
	}
}

static void
server_broadcast(chat_server *s, Peer *from, std::string_view author,
			 std::string_view text)
{
	for (Peer *p : s->peers) {
		if (p == nullptr || p->fd < 0)
			continue;
		if (from != nullptr && p == from)
			continue;
		pkt_append_broadcast(p->out, author, text);


		fd_flush(p->fd, p->out);
	}
}

static int
server_handle_peer_io(chat_server *s, Peer *p)
{

	int rc = fd_read_all(p->fd, p->in);
	if (rc != 0)
		return rc;


	while (true) {
		uint8_t type;
		std::string payload;
		if (!peer_try_parse_one(*p, type, payload))
			break;
		if (type == PKT_NAME) {
			if (!p->have_name) {
				p->name = std::move(payload);
				p->have_name = true;
			}
			continue;
		}
		if (type != PKT_TEXT)
			continue;

		std::string body = trim_ws(payload);
		if (body.empty())
			continue;
		std::string_view author =
#if NEED_AUTHOR
			p->have_name ? std::string_view(p->name) : std::string_view("anon");
#else
			std::string_view();
#endif


		auto *m = new chat_message();
		m->data = body;
#if NEED_AUTHOR
		m->author = std::string(author);
#endif
		s->inbox.push_back(m);


		server_broadcast(s, p, author, body);
	}


	return fd_flush(p->fd, p->out);
}

int
chat_server_update(struct chat_server *server, double timeout)
{
	if (server->listen_fd < 0 || server->epfd < 0)
		return CHAT_ERR_NOT_STARTED;

	int timeout_ms = -1;
	if (timeout >= 0) {
		double ms = timeout * 1000.0;
		timeout_ms = (int)(ms + 0.999999);
	}

	epoll_event evs[64];
	int n = epoll_wait(server->epfd, evs, 64, timeout_ms);
	if (n < 0)
		return CHAT_ERR_SYS;
	if (n == 0)
		return CHAT_ERR_TIMEOUT;

	bool did = false;
	for (int i = 0; i < n; ++i) {
		void *tag = evs[i].data.ptr;
		uint32_t re = evs[i].events;
		if (tag == server) {

			int rc = server_accept_all(server);
			if (rc != 0)
				return rc;
			did = true;
			continue;
		}
		Peer *p = (Peer *)tag;
		if ((re & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0) {

			for (size_t k = 0; k < server->peers.size(); ++k) {
				if (server->peers[k] == p) {
					server->peers[k] = nullptr;
					break;
				}
			}
			peer_close_and_delete(server->epfd, p);
			did = true;
			continue;
		}
		if ((re & (EPOLLIN | EPOLLOUT)) != 0) {
			int rc = server_handle_peer_io(server, p);
			if (rc != 0) {
				for (size_t k = 0; k < server->peers.size(); ++k) {
					if (server->peers[k] == p) {
						server->peers[k] = nullptr;
						break;
					}
				}
				peer_close_and_delete(server->epfd, p);
				did = true;
			} else {
				did = true;
			}
		}
	}


	if (server->peers.size() > 0 && server->peers.size() % 64 == 0) {
		std::vector<Peer *> tmp;
		tmp.reserve(server->peers.size());
		for (Peer *p : server->peers)
			if (p != nullptr)
				tmp.push_back(p);
		server->peers.swap(tmp);
	}

	return did ? 0 : CHAT_ERR_TIMEOUT;
}

int
chat_server_get_descriptor(const struct chat_server *server)
{
	return server->epfd;
}

int
chat_server_get_socket(const struct chat_server *server)
{
	return server->listen_fd;
}

int
chat_server_get_events(const struct chat_server *server)
{
	if (server->listen_fd < 0 || server->epfd < 0)
		return 0;

	return CHAT_EVENT_INPUT;
}

int
chat_server_feed(struct chat_server *server, const char *msg, uint32_t msg_size)
{
#if !NEED_SERVER_FEED
	(void)server;
	(void)msg;
	(void)msg_size;
	return CHAT_ERR_NOT_IMPLEMENTED;
#else
	if (server->listen_fd < 0 || server->epfd < 0)
		return CHAT_ERR_NOT_STARTED;

	int acc_rc = server_accept_all(server);
	if (acc_rc != 0)
		return acc_rc;

	server->admin_acc.append(msg, msg_size);
	while (true) {
		size_t nl = server->admin_acc.find('\n');
		if (nl == std::string::npos)
			break;
		std::string line = server->admin_acc.substr(0, nl);
		server->admin_acc.erase(0, nl + 1);
		std::string body = trim_ws(line);
		if (body.empty())
			continue;
		std::string_view author =
#if NEED_AUTHOR
			std::string_view("server");
#else
			std::string_view();
#endif
		server_broadcast(server, nullptr, author, body);
	}
	return 0;
#endif
}
