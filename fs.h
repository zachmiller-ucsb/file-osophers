#ifndef FILEOSOPHY_FS_H_
#define FILEOSOPHY_FS_H_

#include <sys/stat.h>
#include <sys/uio.h>

#include <functional>
#include <optional>

#include "block.h"

template <typename T>
constexpr T divide_round_up(T a, T b) {
  static_assert(std::is_integral_v<T>);
  return (a + b - 1) / b;
}

constexpr int kNumDirectBlocks = 10;
constexpr int kNumBlocksPerIndirect = kBlockSize / sizeof(int64_t);
constexpr int kNumBlocksPerDoubleIndirect =
    kNumBlocksPerIndirect * kNumBlocksPerIndirect;
constexpr int kNumBlocksPerTripleIndirect =
    kNumBlocksPerIndirect * kNumBlocksPerIndirect * kNumBlocksPerIndirect;

constexpr int64_t kSuperblockBlock = 0;
constexpr int64_t kRootInode = 1;

constexpr int kBlockGroupDescriptorSize = 32;
constexpr int kDataBlocksPerBlockGroup = kBlockSize;
// Effective ratio of 4 blocks per inode
constexpr int kINodesPerBlockGroup = kBlockSize / 4;
constexpr int kINodeSize = 256;
constexpr int kINodesPerTableBlock = kBlockSize / kINodeSize;

constexpr int kINodeTableBlocksPerBlockGroup =
    divide_round_up(kINodesPerBlockGroup * kINodeSize, kBlockSize);
constexpr int kNonDataBlocksPerBlockGroup = 1 /* block bitmap */ +
                                            1 /* inode bitmap */ +
                                            kINodeTableBlocksPerBlockGroup;
constexpr int kTotalBlocksPerBlockGroup =
    1 /* block bitmap */ + 1 /* inode bitmap */ +
    kINodeTableBlocksPerBlockGroup + kDataBlocksPerBlockGroup;

constexpr int kMaxReadSize = 128 * 1024;  // 128K
// +1 because a read might not be block aligned
constexpr int kMaxReadBlocks = divide_round_up(kMaxReadSize, kBlockSize) + 1;

constexpr int kFilenameLen = 255;

struct Superblock {
  int64_t inodes_count;
  int64_t blocks_count;
  int64_t unallocated_blocks_count;
  int64_t unallocated_inodes_count;
  int32_t num_block_groups;

  uint8_t padding[4060];
};

struct BlockGroupDescriptor {
  int64_t block_bitmap;
  int64_t inode_bitmap;
  int64_t inode_table;
  int16_t unallocated_blocks_count;
  int16_t unallocated_inodes_count;
};

static_assert(sizeof(BlockGroupDescriptor) == kBlockGroupDescriptorSize);

enum class Type : int8_t { kUnknown, kRegular, kDirectory };

namespace flags {
constexpr uint16_t kUsrR = 0x1;
constexpr uint16_t kUsrW = 0x2;
constexpr uint16_t kUsrX = 0x4;
constexpr uint16_t kGrpR = 0x8;
constexpr uint16_t kGrpW = 0x10;
constexpr uint16_t kGrpX = 0x20;
constexpr uint16_t kOthR = 0x40;
constexpr uint16_t kOthW = 0x80;
constexpr uint16_t kOthX = 0x100;
constexpr uint16_t kSetUid = 0x200;
constexpr uint16_t kSetGid = 0x400;
constexpr uint16_t kSticky = 0x800;
}  // namespace flags

inline bool ModeToType(mode_t mode, Type* type) {
  if (S_ISREG(mode)) {
    *type = Type::kRegular;
  } else if (S_ISDIR(mode)) {
    *type = Type::kDirectory;
  } else {
    return false;
  }
  return true;
}

inline bool TypeToMode(Type type, mode_t* mode) {
  switch (type) {
    case Type::kRegular:
      *mode |= S_IFREG;
      break;
    case Type::kDirectory:
      *mode |= S_IFDIR;
      break;
    default:
      return false;
  }
  return true;
}

// Returns true on success, false otherwise
inline bool ModeToFlags(mode_t mode, Type* type, int16_t* flags) {
  *flags = 0;
  if (!ModeToType(mode, type)) {
    return false;
  }

  auto map = [mode, flags](mode_t f1, uint16_t f2) {
    if (mode & f1) *flags |= f2;
  };

  using namespace flags;
  map(S_IRUSR, kUsrR);
  map(S_IWUSR, kUsrW);
  map(S_IXUSR, kUsrX);
  map(S_IRGRP, kGrpR);
  map(S_IWGRP, kGrpW);
  map(S_IXGRP, kGrpX);
  map(S_IROTH, kOthR);
  map(S_IWOTH, kOthW);
  map(S_IXOTH, kOthX);
  map(S_ISUID, kSetUid);
  map(S_ISGID, kSetGid);
  map(S_ISVTX, kSticky);

  return true;
}

// Returns true on success, false otherwise
inline bool FlagsToMode(Type type, int16_t flags, mode_t* mode) {
  *mode = 0;
  if (!TypeToMode(type, mode)) {
    return false;
  }

  auto map = [mode, flags](mode_t f2, uint16_t f1) {
    if (flags & f1) *mode |= f2;
  };

  using namespace flags;
  map(S_IRUSR, kUsrR);
  map(S_IWUSR, kUsrW);
  map(S_IXUSR, kUsrX);
  map(S_IRGRP, kGrpR);
  map(S_IWGRP, kGrpW);
  map(S_IXGRP, kGrpX);
  map(S_IROTH, kOthR);
  map(S_IWOTH, kOthW);
  map(S_IXOTH, kOthX);
  map(S_ISUID, kSetUid);
  map(S_ISGID, kSetGid);
  map(S_ISVTX, kSticky);

  return true;
}

struct INode {
  Type mode;
  int64_t size;
  int16_t flags;
  int32_t uid;
  int32_t gid;
  int64_t ctime;
  int64_t mtime;
  int64_t atime;
  int32_t link_count;
  int64_t direct_blocks[kNumDirectBlocks];
  int64_t single_indirect;
  int64_t double_indirect;
  int64_t triple_indirect;
  uint8_t overflow[88];
};

struct INodeTable {
  INode inodes[kINodesPerTableBlock];
};

static_assert(sizeof(Superblock) == kBlockSize);
static_assert(sizeof(INodeTable) == kBlockSize);
static_assert(sizeof(INode) == kINodeSize);

struct DirectoryEntry {
  int64_t inode;
  // Number of bytes needed to advance from here to the next entry
  int16_t alloc_length;

  // Length of name
  uint8_t name_length;
  Type type;

  // filename as an FLA
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
  char name[];
#pragma GCC diagnostic pop

  std::string_view name_str() const {
    return std::string_view(name, name_length);
  }
};

inline int64_t GroupOfInode(int64_t inode) {
  return inode / kINodesPerBlockGroup;
}

inline int64_t LocalINodeToGlobal(int64_t group_i, int64_t inode) {
  return group_i * kINodesPerBlockGroup + inode;
}

class Fileosophy;

struct CachedINode {
  CachedINode(int64_t inode, INode* data, PinnedBlock block, Fileosophy* fs)
      : inode_(inode),
        data__(data),
        block_(block),
        fs(fs),
        block_group_(GroupOfInode(inode)),
        inode_table_(block.id()) {}

  ~CachedINode();

  CachedINode(CachedINode&&) = delete;

  int64_t inode_;
  INode* data__;
  PinnedBlock block_;
  Fileosophy* fs = nullptr;
  const int64_t block_group_;
  const int64_t inode_table_;
  int32_t lookups_ = 0;
  int32_t opens_ = 0;

  const INode* data() const { return data__; }
  INode* datam() {
    set_modified();
    return data__;
  }

  void OpenData();
  void CloseData();

  void set_modified() { block_.set_modified(); }

  int64_t num_blocks() const {
    return divide_round_up<int64_t>(data()->size, kBlockSize);
  }

  int64_t get_block(int64_t block_index);

  [[nodiscard]] int64_t set_block(int64_t block_index, int64_t block_no);

  std::optional<int64_t> get_last_block() {
    const int64_t blocks = num_blocks();
    if (blocks == 0) {
      return std::nullopt;
    }
    return get_block(blocks - 1);
  }

  int64_t get_hint();

  void read(std::span<uint8_t> out, int64_t offset);

  void read_iovec(int64_t size, int64_t offset,
                  std::function<void(std::span<iovec>)> outs);

  void write(std::span<const uint8_t> in, int64_t offset);

  // Returns true if added, false if filename already exists (no modification)
  bool AddDirectoryEntry(std::string_view filename, int64_t inode, Type type);

  // Returns the inode if unlinked, null if not found
  // Does NOT update link count of inode
  std::optional<int64_t> RemoveDE(std::string_view filename);

  std::optional<int64_t> LookupFile(std::string_view filename);

  // Reads entries until callback returns false. To resume, invoke with off
  // returned by previous call.
  void ReadDir(off_t off,
               std::function<bool(const DirectoryEntry*, off_t)> callback);

  bool IsEmpty();

  void FillStat(struct stat* attr);

 private:
  std::optional<int32_t> old_link_count_;
};

struct CachedDirectory {
  CachedDirectory(CachedINode* inode);

  Fileosophy* fs_;
  CachedINode* inode_;
};

struct RAIIInode {
  RAIIInode(Fileosophy* fs, CachedINode* inode) : fs_(fs), inode_(inode) {
    ++inode_->opens_;
  }
  ~RAIIInode();

  RAIIInode(const RAIIInode&) = delete;
  RAIIInode& operator=(const RAIIInode&) = delete;

  CachedINode* operator->() { return inode_; }

  operator CachedINode*() { return inode_; }

  Fileosophy* fs_;
  CachedINode* inode_;
};

class Fileosophy {
 public:
  Fileosophy(BlockCache* blocks);

  void MakeFS();

  void MakeRootDirectory(uid_t uid, gid_t gid);

  std::pair<PinnedBlock, BlockGroupDescriptor*> GetBlockGroupDescriptor(
      int descriptor_num);

  std::pair<PinnedBlock, INode*> GetINodeData(
      int64_t inode, std::optional<int64_t> inode_table_i = {});

  CachedINode* GetINode(int64_t inode);

  RAIIInode GetTmpINode(int64_t inode);

  CachedINode* NewINode(int32_t hint);

  void ForgetInode(int64_t inode, uint64_t nlookup);
  void CloseInode(int64_t inode);

  void InitINode(INode* inode, Type mode, int16_t flags, int32_t uid,
                 int32_t gid);

  // Finds a new free block relative to hint. Hint is an absolute
  // block number
  std::optional<int64_t> NewFreeBlock(int64_t hint);

  [[nodiscard]] bool TruncateINode(CachedINode* inode, int64_t new_size);

  [[nodiscard]] bool GrowINode(CachedINode* inode, int64_t new_size);

  void ShrinkINode(CachedINode* inode, int64_t new_size);

  void ReleaseBlock(int64_t block);

  void DeleteINodeAndBlocks(CachedINode* inode);

  // Finds a free block in this group
  // group_i: group no to search in
  // group: corresponding group data
  // hint: local starting block to search from (group relative), ie. [0,
  // kDataBlocksPerBlockGroup) permit_small: Whether to search for fewer than 8
  // contiguous blocks
  std::optional<int64_t> FindFreeBlockInBlockGroup(int64_t group_i,
                                                   BlockGroupDescriptor* group,
                                                   int64_t hint,
                                                   bool permit_small);

  int64_t FirstDataBlockOfGroup(int64_t group) const {
    return first_data_block_ + kDataBlocksPerBlockGroup * group;
  }

  int64_t DataBlockOfGroup(int64_t block, int64_t group) const {
    return first_data_block_ + kDataBlocksPerBlockGroup * group + block;
  }

  int64_t DataBlockToGroup(int64_t block) const {
    return (block - first_data_block_) / kDataBlocksPerBlockGroup;
  }

  int64_t DataBlockToGroupLocalBlock(int64_t block) const {
    return (block - first_data_block_) % kDataBlocksPerBlockGroup;
  }

  // Returns the block number from an indirect array
  // block is position in blocks from the start of the file, not an
  // absolute block number.
  int64_t LookupIndirect(int64_t indirect_list, int64_t block_index, int depth);

  void FreeIndirect(int64_t indirect_list, int depth);

  // Writes block_no into the indirect list at position block_index.
  // Creates indirect arrays as needed, up to depth.
  // If indirect_list is 0, creates a new indirect list.
  // Returns the root of the indirect list.
  int64_t WriteIndirect(int64_t indirect_list, int64_t hint,
                        int64_t block_index, int64_t block_no, int depth,
                        int64_t* old_block_no);

  BlockCache* blocks_;

  PinnedBlock pinned_super_;
  Superblock* super_;

  int64_t first_data_block_;

  friend struct CachedINode;

  std::unordered_map<int64_t, CachedINode> opened_files_;
};

#endif  // FILEOSOPHY_FS_H_