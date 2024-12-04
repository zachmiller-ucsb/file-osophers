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

  if (fs->super_->unallocated_inodes_count < 1 ||
      fs->super_->unallocated_blocks_count < 2) {
    CHECK_EQ(fuse_reply_err(req, ENOSPC), 0);
    return;
  }

  LOG(INFO) << "create(" << parent << ", \"" << name << "\", " << std::hex
            << mode << ")";

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

  CachedINode* inode;
  if (auto ino = parent_dir->LookupFile(name)) {
    inode = fs->GetINode(*ino);
  } else {
    // Create file if it doesn't exist
    auto new_inode = fs->NewINode(parent_dir->block_group_);
    fs->InitINode(new_inode->datam(), type, flags, ctx->uid, ctx->gid);

    CHECK(parent_dir->AddDirectoryEntry(name, new_inode->inode_, type));
    inode = new_inode;
  }

  struct fuse_entry_param e{.ino = static_cast<fuse_ino_t>(inode->inode_),
                            .generation = 1,
                            .attr_timeout = 0};
  inode->FillStat(&e.attr);

  ++inode->lookups_;
  ++inode->opens_;

  fi->fh = reinterpret_cast<uintptr_t>(inode);

  CHECK_EQ(fuse_reply_create(req, &e, fi), 0);
}

void destroy(void* userdata) {
  LOG(INFO) << "Shutdown";

  auto fs = reinterpret_cast<Fileosophy*>(userdata);

  // Fuse doesn't guarantee that all lookups will be removed on unmount
  for (auto& [i, p] : fs->opened_files_) {
    p.lookups_ = 0;
  }
}

void fallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset,
               off_t length, struct fuse_file_info* /* fi */) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));

  LOG(INFO) << "fallocate(" << ino << ", " << std::hex << mode << ", " << offset
            << ", " << length << ")";

  if (mode != 0) {
    // Only support fallocate in an ftruncate-like mode
    // one such unsupported mode is pre-allocating without resizing. We don't
    // have a way to do this since we assume num blocks can be directly computed
    // from size
    CHECK_EQ(fuse_reply_err(req, EINVAL), 0);
    return;
  }

  auto inode = fs->GetINode(ino);

  if (offset + length <= inode->data()->size) {
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

  LOG(INFO) << "Forget " << ino << ' ' << nlookup;
  fs->ForgetInode(ino, nlookup);

  fuse_reply_none(req);
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
  auto inode = fs->GetTmpINode(ino);

  struct stat s;
  inode->FillStat(&s);

  // LOG(INFO) << "getattr " << ino << " " << std::hex << s.st_mode << ' '
  //           << s.st_uid;

  CHECK_EQ(fuse_reply_attr(req, &s, 0.0), 0);
}

void link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
          const char* newname) {
  auto fs =
      CHECK_NOTNULL(reinterpret_cast<Fileosophy*>(fuse_req_userdata(req)));
  auto inode = fs->GetINode(ino);
  auto new_parent = fs->GetINode(newparent);

  if (inode->data()->mode != Type::kRegular) {
    // Can only link regular files
    CHECK_EQ(fuse_reply_err(req, EPERM), 0);
    return;
  }

  if (new_parent->data()->mode != Type::kDirectory) {
    // Parent must be a directory
    CHECK_EQ(fuse_reply_err(req, ENOTDIR), 0);
    return;
  }

  if (new_parent->LookupFile(newname).has_value()) {
    // Link already exists
    CHECK_EQ(fuse_reply_err(req, EEXIST), 0);
    return;
  }

  CHECK(new_parent->AddDirectoryEntry(newname, ino, inode->data()->mode));
  ++inode->datam()->link_count;

  struct fuse_entry_param e{.ino = static_cast<fuse_ino_t>(inode->inode_),
                            .generation = 1,
                            .attr_timeout = 0};
  inode->FillStat(&e.attr);

  ++inode->lookups_;

  CHECK_EQ(fuse_reply_entry(req, &e), 0);
}

void lookup(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));
  auto parent_dir = fs->GetTmpINode(parent);
  auto ino = parent_dir->LookupFile(name);

  if (!ino) {
    CHECK_EQ(fuse_reply_err(req, ENOENT), 0);
    return;
  }

  auto inode = fs->GetTmpINode(*ino);

  struct fuse_entry_param e{
      .ino = static_cast<fuse_ino_t>(*ino), .generation = 1, .attr_timeout = 0};
  inode->FillStat(&e.attr);

  ++inode->lookups_;

  CHECK_EQ(fuse_reply_entry(req, &e), 0);
}

void mkdir(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));

  LOG(INFO) << "mkdir(" << parent << ", \"" << name << "\", " << std::hex
            << mode << ")";

  if (fs->super_->unallocated_inodes_count < 1 ||
      fs->super_->unallocated_blocks_count < 2) {
    // Need blocks for directory entries, even if we have a free inode
    CHECK_EQ(fuse_reply_err(req, ENOSPC), 0);
    return;
  }

  int16_t flags;
  Type type;
  mode |= S_IFDIR;
  if (!ModeToFlags(mode, &type, &flags)) {
    CHECK_EQ(fuse_reply_err(req, EINVAL), 0);
    return;
  }

  if (type != Type::kDirectory) {
    // Only use create to make directories
    CHECK_EQ(fuse_reply_err(req, EINVAL), 0);
    return;
  }

  auto parent_dir = fs->GetINode(parent);
  CHECK_EQ(static_cast<fuse_ino_t>(parent_dir->inode_), parent);
  auto ino = parent_dir->LookupFile(name);
  if (ino) {
    // Already exists
    CHECK_EQ(fuse_reply_err(req, EEXIST), 0);
    return;
  }

  auto ctx = fuse_req_ctx(req);

  auto inode = fs->NewINode(parent_dir->block_group_);
  auto datam = inode->datam();
  fs->InitINode(datam, type, flags, ctx->uid, ctx->gid);

  // Two links - the link from parent, and the "." file
  datam->link_count = 2;

  CHECK(parent_dir->AddDirectoryEntry(name, inode->inode_, type));
  CHECK(inode->AddDirectoryEntry(".", inode->inode_, type));

  // Link back to parent
  CHECK(inode->AddDirectoryEntry("..", parent, type));
  ++parent_dir->datam()->link_count;

  struct fuse_entry_param e{.ino = static_cast<fuse_ino_t>(inode->inode_),
                            .generation = 1,
                            .attr_timeout = 0};
  inode->FillStat(&e.attr);

  ++inode->lookups_;

  CHECK_EQ(fuse_reply_entry(req, &e), 0);
}

void open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));

  LOG(INFO) << "open(" << ino << ", " << std::hex << fi->flags << ")";

  auto inode = fs->GetINode(ino);
  if (inode->data()->mode == Type::kDirectory) {
    CHECK_EQ(fuse_reply_err(req, ENOSYS), 0);
    return;
  } else {
    CHECK(inode->data()->mode == Type::kRegular);
  }

  fi->fh = reinterpret_cast<uintptr_t>(inode);
  ++inode->opens_;

  CHECK_EQ(fuse_reply_open(req, fi), 0);
}

void opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* fi) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));
  LOG(INFO) << "opendir(" << ino << ", " << std::hex << fi->flags << ")";

  auto inode = fs->GetINode(ino);
  if (inode->data()->mode != Type::kDirectory) {
    CHECK_EQ(fuse_reply_err(req, ENOTDIR), 0);
    return;
  }

  fi->fh = reinterpret_cast<uintptr_t>(inode);
  ++inode->opens_;

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

  const int64_t fsize = inode->data()->size;
  if (off > fsize) {
    // Out of range, EOF (can't read any bytes)
    CHECK_EQ(fuse_reply_err(req, 0), 0);
    return;
  }

  // Can read at most requested bytes, or remaining bytes starting at off
  size = std::min<int64_t>(size, fsize - off);

  // LOG(INFO) << "read(" << ino << ", " << size << ", " << off << ")";
  inode->read_iovec(size, off, [req](std::span<iovec> vec) {
    CHECK_EQ(fuse_reply_iov(req, vec.data(), vec.size()), 0);
  });
}

void setattr(fuse_req_t req, fuse_ino_t ino, struct stat* attr, int to_set,
             struct fuse_file_info* /* fi */) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));
  auto inode = fs->GetINode(ino);

  auto datam = inode->datam();
  datam->flags &= ~(flags::kSetUid | flags::kSetGid);

  if (to_set & FUSE_SET_ATTR_SIZE) {
    // This can fail, so try it first
    if (!fs->TruncateINode(inode, attr->st_size)) {
      CHECK_EQ(fuse_reply_err(req, ENOSPC), 0);
      return;
    }
  }
  if (to_set & FUSE_SET_ATTR_MODE) {
    Type type;
    int16_t flags;
    ModeToFlags(attr->st_mode, &type, &flags);
    CHECK(type == datam->mode);
    datam->flags = flags;
  }
  if (to_set & FUSE_SET_ATTR_UID) {
    datam->uid = attr->st_uid;
  }
  if (to_set & FUSE_SET_ATTR_GID) {
    datam->gid = attr->st_gid;
  }
  if (to_set & FUSE_SET_ATTR_ATIME) {
    datam->atime = attr->st_atime;
  }
  if (to_set & FUSE_SET_ATTR_MTIME) {
    datam->mtime = attr->st_mtime;
  }
  if (to_set & (FUSE_SET_ATTR_ATIME_NOW | FUSE_SET_ATTR_MTIME_NOW)) {
    auto t = time(nullptr);
    if (to_set & FUSE_SET_ATTR_ATIME_NOW) {
      datam->atime = t;
    }
    if (to_set & FUSE_SET_ATTR_MTIME_NOW) {
      datam->mtime = t;
    }
  }

  struct stat new_attr;
  inode->FillStat(&new_attr);

  CHECK_EQ(fuse_reply_attr(req, &new_attr, 0.0), 0);
}

void unlink(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));
  auto p = fs->GetINode(parent);

  LOG(INFO) << "unlink(" << parent << ", \"" << name << "\")";

  if (p->data()->mode != Type::kDirectory) {
    CHECK_EQ(fuse_reply_err(req, EINVAL), 0);
    return;
  }

  auto ino = p->RemoveDE(name);
  if (!ino.has_value()) {
    CHECK_EQ(fuse_reply_err(req, ENOENT), 0);
    return;
  }

  auto inode = fs->GetINode(*ino);
  // Sanity check, we should use rmdir otherwise
  CHECK(inode->data()->mode == Type::kRegular);

  CHECK_GE(--inode->datam()->link_count, 0) << "link_count < 0?";
  CHECK_EQ(fuse_reply_err(req, 0), 0);
}

// Returns 0 on success, positive error code otherwise
bool do_rmdir(Fileosophy* fs, CachedINode* p, const char* name) {
  if (p->data()->mode != Type::kDirectory) {
    return EINVAL;
  }

  auto ino = p->LookupFile(name);
  if (!ino) {
    return ENOENT;
  }
  auto inode = fs->GetTmpINode(*ino);

  // Sanity check, we should use unlink otherwise
  CHECK(inode->data()->mode == Type::kDirectory);

  // Check that directory is empty
  if (!inode->IsEmpty()) {
    return ENOTEMPTY;
  }

  CHECK(p->RemoveDE(name).has_value());

  // Remove link to parent
  CHECK_GE(--p->datam()->link_count, 2);

  // Should be exactly 2 links at this point, since directory is empty
  CHECK_EQ(inode->data()->link_count, 2);
  inode->datam()->link_count = 0;

  return 0;
}

void rmdir(fuse_req_t req, fuse_ino_t parent, const char* name) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));

  if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
    CHECK_EQ(fuse_reply_err(req, EINVAL), 0);
    return;
  }

  auto p = fs->GetINode(parent);
  if (int err = do_rmdir(fs, p, name)) {
    CHECK_EQ(fuse_reply_err(req, err), 0);
    return;
  }

  CHECK_EQ(fuse_reply_err(req, 0), 0);
}

void write(fuse_req_t req, fuse_ino_t ino, const char* buf, size_t size,
           off_t off, struct fuse_file_info* fi) {
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));

  auto inode = CHECK_NOTNULL(reinterpret_cast<CachedINode*>(fi->fh));
  CHECK_EQ(static_cast<fuse_ino_t>(inode->inode_), ino);

  // Reset setuid on write
  inode->datam()->flags &= ~(flags::kSetUid | flags::kSetGid);

  const int64_t old_size = inode->data()->size;
  const int64_t new_size = std::max<int64_t>(old_size, off + size);

  if (old_size != new_size && !fs->GrowINode(inode, new_size)) {
    // LOG(INFO) << "write new_size " << ino << " " << new_size;
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
    struct stat ministat{
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

void release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info* /* fi */) {
  LOG(INFO) << "release(" << ino << ")";
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));
  fs->CloseInode(ino);
  CHECK_EQ(fuse_reply_err(req, 0), 0);
}

void releasedir(fuse_req_t req, fuse_ino_t ino,
                struct fuse_file_info* /* fi */) {
  LOG(INFO) << "releasedir(" << ino << ")";
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));
  fs->CloseInode(ino);
  CHECK_EQ(fuse_reply_err(req, 0), 0);
}

void rename(fuse_req_t req, fuse_ino_t parent, const char* name,
            fuse_ino_t newparent,
            const char* newname /*, unsigned int flags */) {
  LOG(INFO) << "rename(" << parent << ", \"" << name << "\", " << newparent
            << ", \"" << newname << "\")";

  unsigned int flags = 0;
  auto fs = reinterpret_cast<Fileosophy*>(fuse_req_userdata(req));

  if ((flags & RENAME_NOREPLACE) && (flags & RENAME_EXCHANGE)) {
    // Flags are mutually exclusive
    CHECK_EQ(fuse_reply_err(req, EINVAL), 0);
    return;
  }

  auto parent_dir = fs->GetINode(parent);
  auto new_parent = fs->GetINode(newparent);
  CHECK_EQ(static_cast<fuse_ino_t>(parent_dir->inode_), parent);
  CHECK_EQ(static_cast<fuse_ino_t>(new_parent->inode_), newparent);

  auto target_ino = new_parent->LookupFile(newname);
  if ((flags & RENAME_NOREPLACE) && target_ino) {
    // Don't overwrite target if it exists
    CHECK_EQ(fuse_reply_err(req, EEXIST), 0);
    return;
  }

  if ((flags & RENAME_EXCHANGE) && !target_ino) {
    // Exchange requires that target exists
    CHECK_EQ(fuse_reply_err(req, ENOENT), 0);
    return;
  }

  auto ino = parent_dir->LookupFile(name);
  if (!ino) {
    // Doesn't exist
    CHECK_EQ(fuse_reply_err(req, ENOENT), 0);
    return;
  }

  if (target_ino) {
    auto target_inode = fs->GetTmpINode(*target_ino);
    // Unlink target
    if (target_inode->data()->mode == Type::kDirectory) {
      const int err = do_rmdir(fs, new_parent, newname);
      if (err) {
        CHECK_EQ(fuse_reply_err(req, err), 0);
        return;
      }
    } else if (target_inode->data()->mode == Type::kRegular) {
      CHECK(new_parent->RemoveDE(newname).has_value());
      CHECK_GE(--target_inode->datam()->link_count, 0) << "link_count < 0?";
    } else {
      LOG(FATAL) << "???";
    }
  }

  // Remove entry from current directory
  CHECK(parent_dir->RemoveDE(name).has_value());

  auto inode = fs->GetTmpINode(*ino);

  // Add entry to new directory
  CHECK(new_parent->AddDirectoryEntry(newname, inode->inode_,
                                      inode->data()->mode));

  if (parent != newparent && inode->data()->mode == Type::kDirectory) {
    // If this is a directory and moving under a new parent, update ".."

    // Remove link to old parent
    CHECK_GE(--parent_dir->datam()->link_count, 2);
    CHECK(inode->RemoveDE("..").has_value());

    // Add link to new parent
    ++new_parent->datam()->link_count;
    CHECK(inode->AddDirectoryEntry("..", newparent, Type::kDirectory));
  }

  CHECK_EQ(fuse_reply_err(req, 0), 0);
}

}  // namespace fuse_ops

static const struct fuse_lowlevel_ops fileosophy_ops{
    .init = nullptr,
    .destroy = fuse_ops::destroy,
    .lookup = fuse_ops::lookup,
    .forget = fuse_ops::forget,
    .getattr = fuse_ops::getattr,
    .setattr = fuse_ops::setattr,
    .readlink = nullptr,
    .mknod = nullptr,
    .mkdir = fuse_ops::mkdir,
    .unlink = fuse_ops::unlink,
    .rmdir = fuse_ops::rmdir,
    .symlink = nullptr,
    .rename = fuse_ops::rename,
    .link = fuse_ops::link,
    .open = fuse_ops::open,
    .read = fuse_ops::read,
    .write = fuse_ops::write,
    .flush = fuse_ops::flush,
    .release = fuse_ops::release,
    .fsync = fuse_ops::fsync,
    .opendir = fuse_ops::opendir,
    .readdir = fuse_ops::readdir,
    .releasedir = fuse_ops::releasedir,
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

  BlockCache cache(disk, 256);
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