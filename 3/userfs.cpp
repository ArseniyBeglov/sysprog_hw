#include "userfs.h"

#include "rlist.h"

#include <stddef.h>
#include <algorithm>
#include <cstring>
#include <new>
#include <string>
#include <vector>

enum {
	BLOCK_SIZE = 512,
	MAX_FILE_SIZE = 1024 * 1024 * 100,
};

/** Global error code. Set from any function on any error. */
static ufs_error_code g_ufs_error_code = UFS_ERR_NO_ERR;

struct block {
	/** Block memory. */
	char memory[BLOCK_SIZE];
	/** A link in the block list of the owner-file. */
	rlist in_block_list = RLIST_LINK_INITIALIZER;
};

struct file {
	/**
	 * Doubly-linked intrusive list of file blocks. Intrusiveness of the
	 * list gives you the full control over the lifetime of the items in the
	 * list without having to use double pointers with performance penalty.
	 */
	rlist blocks = RLIST_HEAD_INITIALIZER(blocks);
	/** How many file descriptors are opened on the file. */
	int refs = 0;
	/** File name. */
	std::string name;
	/** A link in the global file list. */
	rlist in_file_list = RLIST_LINK_INITIALIZER;
	size_t size = 0;
	size_t block_count = 0;
	bool is_deleted = false;
};

/**
 * Intrusive list of all files. In this case the intrusiveness of the list also
 * grants the ability to remove items from any position in O(1) complexity
 * without having to know their iterator.
 */
static rlist file_list = RLIST_HEAD_INITIALIZER(file_list);

struct filedesc {
	file *atfile = nullptr;
	size_t pos = 0;
#if NEED_OPEN_FLAGS
	int rights = UFS_READ_WRITE;
#endif
};

/**
 * An array of file descriptors. When a file descriptor is
 * created, its pointer drops here. When a file descriptor is
 * closed, its place in this array is set to NULL and can be
 * taken by next ufs_open() call.
 */
static std::vector<filedesc*> file_descriptors;

static void
set_error(ufs_error_code code)
{
	g_ufs_error_code = code;
}

static bool
is_valid_fd(int fd)
{
	return fd >= 0 &&
		static_cast<size_t>(fd) < file_descriptors.size() &&
		file_descriptors[static_cast<size_t>(fd)] != nullptr;
}

static filedesc *
get_filedesc(int fd)
{
	if (!is_valid_fd(fd))
		return nullptr;
	return file_descriptors[static_cast<size_t>(fd)];
}

static file *
find_file(const char *filename)
{
	file *it;
	rlist_foreach_entry(it, &file_list, in_file_list) {
		if (!it->is_deleted && it->name == filename)
			return it;
	}
	return nullptr;
}

static size_t
block_count_for_size(size_t size)
{
	if (size == 0)
		return 0;
	return (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
}

static block *
get_block_by_index(file *f, size_t block_index)
{
	if (block_index >= f->block_count)
		return nullptr;
	if (block_index <= f->block_count / 2) {
		rlist *it = rlist_first(&f->blocks);
		for (size_t i = 0; i < block_index; ++i)
			it = rlist_next(it);
		return rlist_entry(it, block, in_block_list);
	}
	rlist *it = rlist_last(&f->blocks);
	for (size_t i = f->block_count - 1; i > block_index; --i)
		it = rlist_prev(it);
	return rlist_entry(it, block, in_block_list);
}

static block *
next_block_or_null(file *f, block *b)
{
	rlist *next = rlist_next(&b->in_block_list);
	if (next == &f->blocks)
		return nullptr;
	return rlist_entry(next, block, in_block_list);
}

static bool
append_block(file *f)
{
	block *b = nullptr;
	try {
		b = new block();
	} catch (const std::bad_alloc&) {
		return false;
	}
	std::memset(b->memory, 0, sizeof(b->memory));
	rlist_add_tail_entry(&f->blocks, b, in_block_list);
	++f->block_count;
	return true;
}

static bool
ensure_block_count(file *f, size_t target_block_count)
{
	while (f->block_count < target_block_count) {
		if (!append_block(f))
			return false;
	}
	return true;
}

static void
remove_tail_blocks(file *f, size_t target_block_count)
{
	while (f->block_count > target_block_count) {
		block *b = rlist_shift_tail_entry(&f->blocks, block,
						  in_block_list);
		delete b;
		--f->block_count;
	}
}

static void
zero_file_range(file *f, size_t begin, size_t end)
{
	if (begin >= end)
		return;
	size_t pos = begin;
	size_t block_index = pos / BLOCK_SIZE;
	size_t offset = pos % BLOCK_SIZE;
	block *b = get_block_by_index(f, block_index);
	while (pos < end && b != nullptr) {
		size_t chunk = std::min(end - pos, BLOCK_SIZE - offset);
		std::memset(b->memory + offset, 0, chunk);
		pos += chunk;
		offset = 0;
		if (pos < end)
			b = next_block_or_null(f, b);
	}
}

static void
file_clear_blocks(file *f)
{
	while (!rlist_empty(&f->blocks)) {
		block *b = rlist_shift_entry(&f->blocks, block, in_block_list);
		delete b;
	}
	f->block_count = 0;
	f->size = 0;
}

static void
file_destroy(file *f)
{
	file_clear_blocks(f);
	rlist_del_entry(f, in_file_list);
	delete f;
}

static void
clamp_fds_to_size(file *f, size_t size)
{
	for (filedesc *desc : file_descriptors) {
		if (desc != nullptr && desc->atfile == f && desc->pos > size)
			desc->pos = size;
	}
}

#if NEED_OPEN_FLAGS
static int
normalize_rights(int flags)
{
	int rights = flags & UFS_READ_WRITE;
	if (rights == 0)
		rights = UFS_READ_WRITE;
	return rights;
}
#endif

enum ufs_error_code
ufs_errno()
{
	return g_ufs_error_code;
}

int
ufs_open(const char *filename, int flags)
{
	if (filename == nullptr) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}

	file *f = find_file(filename);
	if (f == nullptr && (flags & UFS_CREATE) == 0) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}
	bool is_new_file = false;
	if (f == nullptr) {
		try {
			f = new file();
		} catch (const std::bad_alloc&) {
			set_error(UFS_ERR_NO_MEM);
			return -1;
		}
		f->name = filename;
		rlist_add_tail_entry(&file_list, f, in_file_list);
		is_new_file = true;
	}

	filedesc *desc = nullptr;
	try {
		desc = new filedesc();
	} catch (const std::bad_alloc&) {
		if (is_new_file)
			file_destroy(f);
		set_error(UFS_ERR_NO_MEM);
		return -1;
	}
	desc->atfile = f;
	desc->pos = 0;
#if NEED_OPEN_FLAGS
	desc->rights = normalize_rights(flags);
#endif

	size_t fd_index = 0;
	for (; fd_index < file_descriptors.size(); ++fd_index) {
		if (file_descriptors[fd_index] == nullptr) {
			file_descriptors[fd_index] = desc;
			break;
		}
	}
	if (fd_index == file_descriptors.size()) {
		try {
			file_descriptors.push_back(desc);
		} catch (const std::bad_alloc&) {
			delete desc;
			if (is_new_file)
				file_destroy(f);
			set_error(UFS_ERR_NO_MEM);
			return -1;
		}
	}
	++f->refs;
	set_error(UFS_ERR_NO_ERR);
	return static_cast<int>(fd_index);
}

ssize_t
ufs_write(int fd, const char *buf, size_t size)
{
	filedesc *desc = get_filedesc(fd);
	if (desc == nullptr) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}
#if NEED_OPEN_FLAGS
	if ((desc->rights & UFS_WRITE_ONLY) == 0) {
		set_error(UFS_ERR_NO_PERMISSION);
		return -1;
	}
#endif
	if (size == 0) {
		set_error(UFS_ERR_NO_ERR);
		return 0;
	}
	file *f = desc->atfile;
	if (desc->pos > MAX_FILE_SIZE || size > MAX_FILE_SIZE - desc->pos) {
		set_error(UFS_ERR_NO_MEM);
		return -1;
	}

	size_t end_pos = desc->pos + size;
	size_t new_size = std::max(f->size, end_pos);
	if (new_size > MAX_FILE_SIZE) {
		set_error(UFS_ERR_NO_MEM);
		return -1;
	}

	size_t required_blocks = block_count_for_size(new_size);
	if (!ensure_block_count(f, required_blocks)) {
		set_error(UFS_ERR_NO_MEM);
		return -1;
	}

	if (desc->pos > f->size)
		zero_file_range(f, f->size, desc->pos);

	size_t block_index = desc->pos / BLOCK_SIZE;
	size_t offset = desc->pos % BLOCK_SIZE;
	block *b = get_block_by_index(f, block_index);
	size_t remaining = size;
	const char *src = buf;
	while (remaining > 0) {
		size_t chunk = std::min(remaining, BLOCK_SIZE - offset);
		std::memcpy(b->memory + offset, src, chunk);
		src += chunk;
		remaining -= chunk;
		offset = 0;
		if (remaining > 0)
			b = next_block_or_null(f, b);
	}

	desc->pos = end_pos;
	if (new_size > f->size)
		f->size = new_size;
	set_error(UFS_ERR_NO_ERR);
	return static_cast<ssize_t>(size);
}

ssize_t
ufs_read(int fd, char *buf, size_t size)
{
	filedesc *desc = get_filedesc(fd);
	if (desc == nullptr) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}
#if NEED_OPEN_FLAGS
	if ((desc->rights & UFS_READ_ONLY) == 0) {
		set_error(UFS_ERR_NO_PERMISSION);
		return -1;
	}
#endif
	if (size == 0) {
		set_error(UFS_ERR_NO_ERR);
		return 0;
	}
	file *f = desc->atfile;
	if (desc->pos >= f->size) {
		set_error(UFS_ERR_NO_ERR);
		return 0;
	}

	size_t to_read = std::min(size, f->size - desc->pos);
	size_t block_index = desc->pos / BLOCK_SIZE;
	size_t offset = desc->pos % BLOCK_SIZE;
	block *b = get_block_by_index(f, block_index);
	size_t remaining = to_read;
	char *dst = buf;
	while (remaining > 0) {
		size_t chunk = std::min(remaining, BLOCK_SIZE - offset);
		std::memcpy(dst, b->memory + offset, chunk);
		dst += chunk;
		remaining -= chunk;
		offset = 0;
		if (remaining > 0)
			b = next_block_or_null(f, b);
	}

	desc->pos += to_read;
	set_error(UFS_ERR_NO_ERR);
	return static_cast<ssize_t>(to_read);
}

int
ufs_close(int fd)
{
	filedesc *desc = get_filedesc(fd);
	if (desc == nullptr) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}
	file_descriptors[static_cast<size_t>(fd)] = nullptr;
	file *f = desc->atfile;
	delete desc;
	--f->refs;
	if (f->refs == 0 && f->is_deleted)
		file_destroy(f);
	set_error(UFS_ERR_NO_ERR);
	return 0;
}

int
ufs_delete(const char *filename)
{
	if (filename == nullptr) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}

	file *f = find_file(filename);
	if (f == nullptr) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}

	f->is_deleted = true;
	if (f->refs == 0)
		file_destroy(f);
	set_error(UFS_ERR_NO_ERR);
	return 0;
}

#if NEED_RESIZE

int
ufs_resize(int fd, size_t new_size)
{
	filedesc *desc = get_filedesc(fd);
	if (desc == nullptr) {
		set_error(UFS_ERR_NO_FILE);
		return -1;
	}
#if NEED_OPEN_FLAGS
	if ((desc->rights & UFS_WRITE_ONLY) == 0) {
		set_error(UFS_ERR_NO_PERMISSION);
		return -1;
	}
#endif
	if (new_size > MAX_FILE_SIZE) {
		set_error(UFS_ERR_NO_MEM);
		return -1;
	}

	file *f = desc->atfile;
	if (new_size > f->size) {
		size_t target_blocks = block_count_for_size(new_size);
		if (!ensure_block_count(f, target_blocks)) {
			set_error(UFS_ERR_NO_MEM);
			return -1;
		}
		zero_file_range(f, f->size, new_size);
		f->size = new_size;
		set_error(UFS_ERR_NO_ERR);
		return 0;
	}
	if (new_size < f->size) {
		size_t target_blocks = block_count_for_size(new_size);
		remove_tail_blocks(f, target_blocks);
		f->size = new_size;
		clamp_fds_to_size(f, new_size);
	}
	set_error(UFS_ERR_NO_ERR);
	return 0;
}

#endif

void
ufs_destroy(void)
{
	for (filedesc *desc : file_descriptors) {
		if (desc == nullptr)
			continue;
		--desc->atfile->refs;
		delete desc;
	}
	std::vector<filedesc*> empty_fds;
	file_descriptors.swap(empty_fds);

	file *it, *tmp;
	rlist_foreach_entry_safe(it, &file_list, in_file_list, tmp)
		file_destroy(it);
	set_error(UFS_ERR_NO_ERR);
}
