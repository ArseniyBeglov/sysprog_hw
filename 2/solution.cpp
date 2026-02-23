#include "parser.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <string>
#include <vector>

namespace {

static void
print_exec_error_and_exit(const std::string &exe)
{
	(void)exe;
	_exit(127);
}

static std::vector<char *>
build_argv(const command &cmd)
{
	std::vector<char *> argv;
	argv.reserve(2 + cmd.args.size());
	argv.push_back(const_cast<char *>(cmd.exe.c_str()));
	for (const std::string &a : cmd.args)
		argv.push_back(const_cast<char *>(a.c_str()));
	argv.push_back(nullptr);
	return argv;
}

static int
wait_one(pid_t pid)
{
	int st = 0;
	while (waitpid(pid, &st, 0) < 0) {
		if (errno == EINTR)
			continue;
		return 1;
	}
	if (WIFEXITED(st))
		return WEXITSTATUS(st);
	if (WIFSIGNALED(st))
		return 128 + WTERMSIG(st);
	return 1;
}

static int
wait_all(const std::vector<pid_t> &pids)
{
	int last = 0;
	for (pid_t p : pids)
		last = wait_one(p);
	return last;
}

static int
parse_exit_code(const command &c)
{
	long v = 0;
	if (!c.args.empty()) {
		char *end = nullptr;
		long tmp = strtol(c.args[0].c_str(), &end, 10);
		if (end != nullptr && *end == '\0')
			v = tmp;
	}
	return (int)(unsigned char)v;
}

struct SegmentResult {
	int status = 0;
	bool should_exit_shell = false;
};

static SegmentResult
run_pipeline_segment(const std::vector<command> &cmds,
		     enum output_type out_type,
		     const std::string &out_file)
{
	SegmentResult res;
	if (cmds.empty())
		return res;

	if (cmds.size() == 1) {
		const command &c = cmds[0];
		if (c.exe == "cd") {
			const char *path = nullptr;
			if (!c.args.empty())
				path = c.args[0].c_str();
			else
				path = getenv("HOME");
			if (path == nullptr)
				path = "/";
			if (chdir(path) != 0)
				res.status = 1;
			else
				res.status = 0;
			return res;
		}
		if (c.exe == "exit") {
			int code = parse_exit_code(c);
			if (out_type == OUTPUT_TYPE_STDOUT)
				res.should_exit_shell = true;
			res.status = code;
			return res;
		}
	}

	int out_fd = -1;
	if (out_type == OUTPUT_TYPE_FILE_NEW || out_type == OUTPUT_TYPE_FILE_APPEND) {
		int flags = O_WRONLY | O_CREAT;
		flags |= (out_type == OUTPUT_TYPE_FILE_NEW) ? O_TRUNC : O_APPEND;
		out_fd = open(out_file.c_str(), flags, 0644);
		if (out_fd < 0) {
			res.status = 1;
			return res;
		}
	}

	std::vector<pid_t> pids;
	pids.reserve(cmds.size());

	int prev_read = -1;
	for (size_t i = 0; i < cmds.size(); ++i) {
		int pipefd[2] = {-1, -1};
		bool need_pipe = (i + 1 < cmds.size());
		if (need_pipe) {
			if (pipe(pipefd) != 0) {
				if (prev_read != -1)
					close(prev_read);
				if (out_fd != -1)
					close(out_fd);
				res.status = 1;
				return res;
			}
		}

		pid_t pid = fork();
		if (pid == 0) {
			if (prev_read != -1)
				dup2(prev_read, STDIN_FILENO);

			if (need_pipe) {
				dup2(pipefd[1], STDOUT_FILENO);
			} else {
				if (out_fd != -1)
					dup2(out_fd, STDOUT_FILENO);
			}

			if (prev_read != -1)
				close(prev_read);
			if (need_pipe) {
				close(pipefd[0]);
				close(pipefd[1]);
			}
			if (out_fd != -1)
				close(out_fd);

			const command &c = cmds[i];
			if (c.exe == "cd") {
				const char *path = nullptr;
				if (!c.args.empty())
					path = c.args[0].c_str();
				else
					path = getenv("HOME");
				if (path == nullptr)
					path = "/";
				if (chdir(path) != 0)
					_exit(1);
				_exit(0);
			}
			if (c.exe == "exit") {
				_exit(parse_exit_code(c));
			}

			auto argv = build_argv(c);
			execvp(c.exe.c_str(), argv.data());
			print_exec_error_and_exit(c.exe);
		}
		if (pid < 0) {
			if (prev_read != -1)
				close(prev_read);
			if (need_pipe) {
				close(pipefd[0]);
				close(pipefd[1]);
			}
			if (out_fd != -1)
				close(out_fd);
			res.status = 1;
			return res;
		}

		pids.push_back(pid);

		if (prev_read != -1)
			close(prev_read);
		if (need_pipe) {
			close(pipefd[1]);
			prev_read = pipefd[0];
		} else {
			if (pipefd[0] != -1)
				close(pipefd[0]);
			prev_read = -1;
		}
	}

	if (prev_read != -1)
		close(prev_read);
	if (out_fd != -1)
		close(out_fd);

	res.status = wait_all(pids);
	return res;
}

static SegmentResult
execute_command_line_impl(const struct command_line *line)
{
	SegmentResult final_res;
	final_res.status = 0;

	std::vector<command> cur_pipeline;
	cur_pipeline.reserve(4);

	enum { OP_NONE, OP_AND, OP_OR } pending_op = OP_NONE;
	bool have_prev = false;
	int last_status = 0;
	bool should_exit = false;

	auto flush_pipeline = [&](bool is_last) {
		if (cur_pipeline.empty())
			return;

		bool run = true;
		if (have_prev) {
			if (pending_op == OP_AND)
				run = (last_status == 0);
			else if (pending_op == OP_OR)
				run = (last_status != 0);
		}
		if (!run) {
			cur_pipeline.clear();
			have_prev = true;
			return;
		}

		enum output_type seg_out_type = OUTPUT_TYPE_STDOUT;
		std::string seg_out_file;
		if (is_last) {
			seg_out_type = line->out_type;
			seg_out_file = line->out_file;
		}

		SegmentResult r = run_pipeline_segment(cur_pipeline, seg_out_type, seg_out_file);
		last_status = r.status;
		have_prev = true;
		if (r.should_exit_shell)
			should_exit = true;
		cur_pipeline.clear();
	};

	for (const expr &e : line->exprs) {
		switch (e.type) {
		case EXPR_TYPE_COMMAND:
			assert(e.cmd.has_value());
			cur_pipeline.push_back(*e.cmd);
			break;
		case EXPR_TYPE_PIPE:
			break;
		case EXPR_TYPE_AND:
			flush_pipeline(false);
			if (should_exit)
				goto done;
			pending_op = OP_AND;
			break;
		case EXPR_TYPE_OR:
			flush_pipeline(false);
			if (should_exit)
				goto done;
			pending_op = OP_OR;
			break;
		default:
			assert(false);
		}
	}
	flush_pipeline(true);

done:
	final_res.status = last_status;
	final_res.should_exit_shell = should_exit;
	return final_res;
}

static void
reap_zombies()
{
	int st = 0;
	while (waitpid(-1, &st, WNOHANG) > 0) {
	}
}

static SegmentResult
execute_command_line(const struct command_line *line)
{
	assert(line != nullptr);

	if (!line->is_background)
		return execute_command_line_impl(line);

	pid_t pid = fork();
	if (pid == 0) {
		(void)execute_command_line_impl(line);
		_exit(0);
	}
	if (pid < 0) {
		SegmentResult r;
		r.status = 1;
		return r;
	}

	SegmentResult r;
	r.status = 0;
	return r;
}

} // namespace

int
main(void)
{
	const size_t buf_size = 1024;
	char buf[buf_size];
	int rc;
	struct parser *p = parser_new();
	int last_status = 0;
	bool exit_shell = false;

	while (!exit_shell && (rc = (int)read(STDIN_FILENO, buf, buf_size)) > 0) {
		reap_zombies();
		parser_feed(p, buf, (uint32_t)rc);
		struct command_line *line = NULL;
		while (!exit_shell) {
			reap_zombies();
			enum parser_error err = parser_pop_next(p, &line);
			if (err == PARSER_ERR_NONE && line == NULL)
				break;
			if (err != PARSER_ERR_NONE) {
				printf("Error: %d\n", (int)err);
				fflush(stdout);
				continue;
			}
			SegmentResult r = execute_command_line(line);
			last_status = r.status;
			exit_shell = r.should_exit_shell;
			delete line;
		}
	}

	parser_delete(p);
	return last_status;
}