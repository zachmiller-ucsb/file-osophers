#include "fuse/fuse.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>

#include "fs.h"
#include "fuse/fuse_lowlevel.h"

namespace fuse_ops {

void create(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode,
            struct fuse_file_info* fi) {
  auto ctx = fuse_req_ctx(req);
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));

  int16_t flags;
  Type type;
  if (!ModeToFlags(mode, &type, &flags)) {
    CHECK_EQ(fuse_reply_err(req, EINVAL), 0);
    return;
  }

  if (type != Type::kRegular) {
    // Only use create to make regular files
    CHECK_EQ(fuse_reply_err(req, EINVAL), 0);
    return;
  }

  auto parent_dir = fs->GetINode(parent);
  auto inode = parent_dir->LookupFile(name);

  if (!inode) {
    // Create file if it doesn't exist
    auto new_inode = fs->NewINode(parent_dir->block_group_);
    fs->InitINode(new_inode->data_, type, flags, ctx->uid, ctx->gid);

    CHECK(parent_dir->AddDirectoryEntry(name, new_inode->inode_, type));
    inode = new_inode;
  }

  struct fuse_entry_param e {
    .ino = static_cast<fuse_ino_t>(inode->inode_), .generation = 1,
    .attr_timeout = 0
  };
  inode->FillStat(&e.attr);

  ++inode->lookups_;

  fi->fh = reinterpret_cast<uintptr_t>(inode);

  CHECK_EQ(fuse_reply_create(req, &e, fi), 0);
}

void destroy(void* /* userdata */) { LOG(INFO) << "Destroyed"; }

void fallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset,
               off_t length, struct fuse_file_info* /* fi */) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));

  if (mode != 0) {
    // Only support fallocate in an ftruncate-like mode
    // one such unsupported mode is pre-allocating without resizing. We don't
    // have a way to do this since we assume num blocks can be directly computed
    // from size
    CHECK_EQ(fuse_reply_err(req, EINVAL), 0);
    return;
  }

  auto inode = fs->GetINode(ino);

  if (offset + length <= inode->data_->size) {
    // Smaller or same, no change
    CHECK_EQ(fuse_reply_err(req, 0), 0);
    return;
  }

  if (!fs->GrowINode(inode, offset + length)) {
    // Failed, no space available
    CHECK_EQ(fuse_reply_err(req, ENOSPC), 0);
    return;
  }

  CHECK_EQ(fuse_reply_err(req, 0), 0);
}

void flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* /* fi */) {
  LOG(INFO) << "flush " << ino;
  CHECK_EQ(fuse_reply_err(req, 0), 0);
}

void forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));
  fs->ForgetInode(ino, nlookup);

  CHECK_EQ(fuse_reply_err(req, 0), 0);
}

void fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
           struct fuse_file_info* /* fi */) {
  LOG(INFO) << "fsync " << ino << ' ' << datasync;
  CHECK_EQ(fuse_reply_err(req, 0), 0);
}

void fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
              struct fuse_file_info* /* fi */) {
  LOG(INFO) << "fsyncdir " << ino << ' ' << datasync;
  CHECK_EQ(fuse_reply_err(req, 0), 0);
}

void getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* /* fi */) {
  if (ino < 1) {
    CHECK_EQ(fuse_reply_err(req, ENOENT), 0);
    return;
  }

  // fi is always NULL
  auto fs =
      CHECK_NOTNULL(reinterpret_cast<Fileosophy*>(fuse_req_userdata(req)));
  auto inode = fs->GetINode(ino);

  struct stat s;
  inode->FillStat(&s);

  LOG(INFO) << "getattr " << ino << " " << std::hex << s.st_mode << ' '
            << s.st_uid;

  CHECK_EQ(fuse_reply_attr(req, &s, 0.0), 0);
}

void lookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));
  auto parent_dir = fs->GetINode(parent);
  auto inode = parent_dir->LookupFile(name);

  if (!inode) {
    CHECK_EQ(fuse_reply_err(req, ENOENT), 0);
    return;
  }

  struct fuse_entry_param e {
    .ino = static_cast<fuse_ino_t>(inode->inode_), .generation = 1,
    .attr_timeout = 0
  };
  inode->FillStat(&e.attr);

  ++inode->lookups_;

  CHECK_EQ(fuse_reply_entry(req, &e), 0);
}

void open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));
  auto inode = fs->GetINode(ino);
  if (inode->data_->mode == Type::kDirectory) {
    CHECK_EQ(fuse_reply_err(req, ENOSYS), 0);
    return;
  } else {
    CHECK(inode->data_->mode == Type::kRegular);
  }

  fi->fh = reinterpret_cast<uintptr_t>(inode);

  CHECK_EQ(fuse_reply_open(req, fi), 0);
}

void opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));
  auto inode = fs->GetINode(ino);
  if (inode->data_->mode != Type::kDirectory) {
    CHECK_EQ(fuse_reply_err(req, ENOTDIR), 0);
    return;
  }

  fi->fh = reinterpret_cast<uintptr_t>(inode);

  CHECK_EQ(fuse_reply_open(req, fi), 0);
}

void poll(fuse_req_t req, fuse_ino_t /* ino */, struct fuse_file_info* /* fi */,
          struct fuse_pollhandle* /* p */) {
  CHECK_EQ(fuse_reply_err(req, ENOSYS), 0);
}

void read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
          struct fuse_file_info* fi) {
  auto inode = CHECK_NOTNULL(reinterpret_cast<CachedINode*>(fi->fh));
  CHECK_EQ(static_cast<fuse_ino_t>(inode->inode_), ino);

  CHECK_LE(static_cast<int64_t>(size), kMaxReadSize)
      << "Read size larger than expected from fuse";

  const int64_t fsize = inode->data_->size;
  if (off > fsize) {
    // Out of range, EOF (can't read any bytes)
    CHECK_EQ(fuse_reply_err(req, 0), 0);
    return;
  }

  // Can read at most requested bytes, or remaining bytes starting at off
  size = std::min<int64_t>(size, fsize - off);

  LOG(INFO) << "read(" << ino << ", " << size << ", " << off << ")";
  inode->read_iovec(size, off, [req](std::span<iovec> vec) {
    LOG(INFO) << "Vec " << vec.size();
    for (auto& v : vec) {
      LOG(INFO) << v.iov_base << ' ' << v.iov_len;
    }
    CHECK_EQ(fuse_reply_iov(req, vec.data(), vec.size()), 0);
  });
}

void write(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size,
           off_t off, struct fuse_file_info* fi) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));

  auto inode = CHECK_NOTNULL(reinterpret_cast<CachedINode*>(fi->fh));
  CHECK_EQ(static_cast<fuse_ino_t>(inode->inode_), ino);

  // Reset setuid on write
  inode->data_->flags &= ~(flags::kSetUid | flags::kSetGid);

  const int64_t old_size = inode->data_->size;
  const int64_t new_size = std::max<int64_t>(old_size, off + size);

  if (old_size != new_size && !fs->GrowINode(inode, new_size)) {
    LOG(INFO) << "write new_size " << ino << " " << new_size;
    CHECK_EQ(fuse_reply_err(req, ENOSPC), 0);
    return;
  }

  inode->write(std::span(reinterpret_cast<const uint8_t*>(buf), size), off);
  CHECK_EQ(fuse_reply_write(req, size), 0);
}

void readdir(fuse_req_t req, fuse_ino_t ino, const size_t size, const off_t off,
             struct fuse_file_info* fi) {
  auto inode = CHECK_NOTNULL(reinterpret_cast<CachedINode*>(fi->fh));
  CHECK_EQ(static_cast<fuse_ino_t>(inode->inode_), ino);

  std::array<char, 1024> fuse_direntries;
  size_t bytes_remaining = std::min(fuse_direntries.size(), size);
  size_t bytes_added = 0;
  inode->ReadDir(off, [&](const DirectoryEntry* de, off_t next_off) -> bool {
    // Fuse only cares about ino and type bits of mode
    struct stat ministat {
      .st_ino = static_cast<ino_t>(de->inode),
    };
    CHECK(TypeToMode(de->type, &ministat.st_mode));

    // Copy name to a null-terminated string
    std::string name(de->name, de->name_length);

    const size_t ent_sz =
        fuse_add_direntry(req, &fuse_direntries[bytes_added], bytes_remaining,
                          name.c_str(), &ministat, next_off);
    if (ent_sz > bytes_remaining) {
      // Entry was too large and not successfully appended
      return false;
    } else {
      bytes_added += ent_sz;
      bytes_remaining -= ent_sz;
      return true;
    }
  });

  CHECK_EQ(fuse_reply_buf(req, fuse_direntries.data(), bytes_added), 0);
}

}  // namespace fuse_ops

static const struct fuse_lowlevel_ops fileosophy_ops {
  .init = nullptr, .destroy = fuse_ops::destroy, .lookup = fuse_ops::lookup,
  .forget = fuse_ops::forget, .getattr = fuse_ops::getattr, .setattr = nullptr,
  .readlink = nullptr, .mknod = nullptr, .mkdir = nullptr, .unlink = nullptr,
  .rmdir = nullptr, .symlink = nullptr, .rename = nullptr, .link = nullptr,
  .open = fuse_ops::open, .read = fuse_ops::read, .write = fuse_ops::write,
  .flush = fuse_ops::flush, .fsync = fuse_ops::fsync,
  .opendir = fuse_ops::opendir, .readdir = fuse_ops::readdir,
  .create = fuse_ops::create,
};

DEFINE_bool(f, false, "foreground");
DEFINE_bool(mkfs, false, "Erase and re-initialize filesystem");

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  CHECK_EQ(argc, 3);

  const char* disk = argv[1];
  const char* mountpoint = argv[2];

  struct fuse_args args = FUSE_ARGS_INIT(argc - 2, argv + 2);

  BlockCache cache(disk, 8);
  Fileosophy fs(&cache);

  if (FLAGS_mkfs) {
    fs.MakeFS();
    fs.MakeRootDirectory(getuid(), getgid());
  }

  int err = -1;
  auto ch = CHECK_NOTNULL(fuse_mount(mountpoint, &args));
  auto se =
      fuse_lowlevel_new(&args, &fileosophy_ops, sizeof(fileosophy_ops), &fs);

  fuse_daemonize(FLAGS_f);
  if (se != nullptr) {
    if (fuse_set_signal_handlers(se) == 0) {
      fuse_session_add_chan(se, ch);
      err = fuse_session_loop(se);
      fuse_remove_signal_handlers(se);
      fuse_session_remove_chan(ch);
    }
    fuse_session_destroy(se);
  }
  fuse_unmount(mountpoint, ch);

  return err ? 1 : 0;
}