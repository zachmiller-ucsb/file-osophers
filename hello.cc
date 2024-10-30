/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

/** @file
 *
 * minimal example filesystem using high-level API
 *
 * Compile with:
 *
 *     gcc -Wall hello.c `pkg-config fuse3 --cflags --libs` -o hello
 *
 * ## Source code ##
 * \include hello.c
 */


#define FUSE_USE_VERSION 31

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <assert.h>
#include <stdlib.h>

#include <iostream>
#include <string>
#include <sstream>
#include <unordered_map>

/*
 * Command line options
 *
 * We can't set default values for the char* fields here because
 * fuse_opt_parse would attempt to free() them when the user specifies
 * different values on the command line.
 */
static struct options {
	std::string filename;
	std::string contents;
	std::string recipient;
	int show_help;
} options;

static struct cmd_line_options {
	char *filename;
	char *contents;
	char *recipient;
	int show_help;
} cmd_line_options;

std::unordered_map<std::string, mode_t> addl_files;

#define OPTION(t, p)                           \
    { t, offsetof(struct cmd_line_options, p), 1 }
static const struct fuse_opt option_spec[] = {
	OPTION("--name=%s", filename),
	OPTION("--contents=%s", contents),
	OPTION("--recipient=%s", recipient),
	OPTION("-h", show_help),
	OPTION("--help", show_help),
	FUSE_OPT_END
};

static void *hello_init(struct fuse_conn_info *conn,
			struct fuse_config *cfg)
{
	(void) conn;
	cfg->kernel_cache = 1;
	return NULL;
}

static int hello_getattr(const char *path, struct stat *stbuf,
			 struct fuse_file_info *fi)
{
	(void) fi;
	int res = 0;

	std::unordered_map<std::string, mode_t>::iterator ifile;

	memset(stbuf, 0, sizeof(struct stat));
	if (strcmp(path, "/") == 0) {
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
	} else if (strcmp(path+1, options.filename.c_str()) == 0) {
		stbuf->st_mode = S_IFREG | 0666;
		stbuf->st_nlink = 1;
		stbuf->st_size = options.contents.size();
	} else if ((ifile = addl_files.find(path)) != addl_files.end()) {
		stbuf->st_mode = S_IFREG | ifile->second;
		stbuf->st_nlink = 1;
		stbuf->st_size = 0;
	} else {
		res = -ENOENT;
	}

	return res;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi,
			 enum fuse_readdir_flags flags)
{
	(void) offset;
	(void) fi;
	(void) flags;

	if (strcmp(path, "/") != 0)
		return -ENOENT;

	filler(buf, ".", NULL, 0, fuse_fill_dir_flags(0));
	filler(buf, "..", NULL, 0, fuse_fill_dir_flags(0));
	filler(buf, options.filename.c_str(), NULL, 0, fuse_fill_dir_flags(0));

	return 0;
}

static int hello_open(const char *path, struct fuse_file_info *fi)
{
	if (strcmp(path + 1, options.filename.c_str()) == 0 &&
			fi->flags & O_TRUNC)
		options.contents.clear();

	return 0;
}

static int hello_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	size_t len;
	(void) fi;

	if (addl_files.find(path) != addl_files.end())
		return 0;

	if (strcmp(path+1, options.filename.c_str()) != 0)
		return -ENOENT;

	len = options.contents.size();
	// can offset be negative?
	if ((unsigned long)offset < len) {
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, options.contents.c_str() + offset, size);
	} else
		size = 0;

	return size;
}

static int hello_write(const char *path, const char *buf, size_t size, off_t offset,
					struct fuse_file_info *fi)
{
	std::stringstream cmd_ss;
	cmd_ss << "echo ";

	if (strcmp(path+1, options.filename.c_str()) != 0) {
		cmd_ss << "\"" << buf << "\"";
	}
	else {
		if (offset + size > options.contents.size()) {
			options.contents.resize(offset + size);
		}
		strncpy((char*) (options.contents.c_str() + offset), buf, size);
		cmd_ss << "\"" << options.contents << "\"";
	}

	cmd_ss << " | mail -s 'cs270 testing' " << options.recipient;

	FILE *fp;
	fp = popen(cmd_ss.str().c_str(), "r");
	if (!fp) {
		size = 0;
	}

	return size;
}

static int hello_create(const char *path, mode_t mode,
					struct fuse_file_info *fi)
{
	addl_files.insert({path, mode});
	return 0;
}

static int hello_chown(const char *, uid_t, gid_t, struct fuse_file_info *)
{
	return 0;
}

static int hello_utimens(const char *, const struct timespec tv[2],
					struct fuse_file_info *fi)
{
	return 0;
}

static int hello_chmod(const char *, mode_t, struct fuse_file_info *)
{
	return 0;
}

static struct fuse_operations hello_oper = {
	.getattr  = hello_getattr,
	.chmod    = hello_chmod,
	.chown    = hello_chown,
	.open		  = hello_open,
	.read		  = hello_read,
	.write    = hello_write,
	.readdir  = hello_readdir,
	.init     = hello_init,
	.create   = hello_create,
	.utimens  = hello_utimens,
};

static void show_help(const char *progname)
{
	printf("usage: %s [options] <mountpoint>\n\n", progname);
	printf("File-system specific options:\n"
	       "    --name=<s>          Name of the \"hello\" file\n"
	       "                        (default: \"hello\")\n"
	       "    --contents=<s>      Contents \"hello\" file\n"
	       "                        (default \"Hello, World!\\n\")\n"
	       "\n");
}

int main(int argc, char *argv[])
{
	int ret;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	/* Set defaults -- we have to use strdup so that
	   fuse_opt_parse can free the defaults if other
	   values are specified */
	cmd_line_options.filename = strdup("hello");
	cmd_line_options.contents = strdup("Hello World!\n");
	cmd_line_options.recipient = strdup("");

	/* Parse options */
	if (fuse_opt_parse(&args, &cmd_line_options, option_spec, NULL) == -1)
		return 1;

	options = {
		.filename = cmd_line_options.filename,
		.contents = cmd_line_options.contents,
		.recipient = cmd_line_options.recipient,
		.show_help = cmd_line_options.show_help,
	};

	/* When --help is specified, first print our own file-system
	   specific help text, then signal fuse_main to show
	   additional help (by adding `--help` to the options again)
	   without usage: line (by setting argv[0] to the empty
	   string) */
	if (options.show_help) {
		show_help(argv[0]);
		assert(fuse_opt_add_arg(&args, "--help") == 0);
		args.argv[0][0] = '\0';
	}

	ret = fuse_main(args.argc, args.argv, &hello_oper, NULL);
	fuse_opt_free_args(&args);
	return ret;
}
