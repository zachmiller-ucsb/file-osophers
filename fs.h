#ifndef FS_H_
#define FS_H_

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
constexpr int64_t kRootInode = 2;

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

enum class Mode : int8_t { kUnknown, kRegular, kDirectory };

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
}  // namespace flags

struct INode {
  Mode mode;
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
  char name[];
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
        data(data),
        block(block),
        fs(fs),
        block_group(GroupOfInode(inode)) {}

  int64_t inode_;
  INode* data;
  PinnedBlock block;
  Fileosophy* fs = nullptr;
  const int64_t block_group;

  int64_t num_blocks() const {
    return divide_round_up<int64_t>(data->size, kBlockSize);
  }

  int64_t get_block(int64_t block_index);

  void set_block(int64_t block_index, int64_t block_no);

  std::optional<int64_t> get_last_block() {
    const int64_t blocks = num_blocks();
    if (blocks == 0) {
      return std::nullopt;
    }
    return get_block(blocks - 1);
  }

  int64_t get_hint();

  void read(std::span<uint8_t> out, int64_t offset);

  void write(std::span<const uint8_t> in, int64_t offset);

  // Returns true if added, false if filename already exists (no modification)
  bool AddDirectoryEntry(std::string_view filename, int64_t inode);

  // Returns true if unlinked, false if entry didn't exist
  bool Unlink(std::string_view filename);
};

struct CachedDirectory {
  CachedDirectory(CachedINode* inode);

  Fileosophy* fs_;
  CachedINode* inode_;
};

class Fileosophy {
 public:
  Fileosophy(BlockCache* blocks);

  void MakeFS();

  void MakeRootDirectory();

  std::pair<PinnedBlock, BlockGroupDescriptor*> GetBlockGroupDescriptor(
      int descriptor_num);

  CachedINode GetINode(int64_t inode);

  CachedINode NewINode(int32_t hint);

 private:
  void InitINode(INode* inode, Mode mode, int16_t flags, int32_t uid,
                 int32_t gid);

  // Finds a new free block relative to hint. Hint is an absolute
  // block number
  std::optional<int64_t> NewFreeBlock(int64_t hint);

  void GrowINode(CachedINode* inode, int64_t new_size);

  void ShrinkINode(int64_t inode);

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

  // Returns the block number from an indirect array
  // block is position in blocks from the start of the file, not an
  // absolute block number.
  int64_t LookupIndirect(int64_t indirect_list, int64_t block_index, int depth);

  // Writes block_no into the indirect list at position block_index.
  // Creates indirect arrays as needed, up to depth.
  // If indirect_list is 0, creates a new indirect list.
  // Returns the root of the indirect list.
  int64_t WriteIndirect(int64_t indirect_list, int64_t hint,
                        int64_t block_index, int64_t block_no, int depth);

  BlockCache* blocks_;

  PinnedBlock pinned_super_;
  Superblock* super_;

  int64_t first_data_block_;

  friend struct CachedINode;
};

#endif  // FS_H_