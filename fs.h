#ifndef FS_H_
#define FS_H_

#include "block.h"

constexpr int kNumDirectBlocks = 10;

constexpr int64_t kSuperblockBlock = 0;

constexpr int kBlockGroupDescriptorSize = 32;
constexpr int kDataBlocksPerBlockGroup = kBlockSize * 8;
// Effective ratio of 4 blocks per inode
constexpr int kINodesPerBlockGroup = kBlockSize * 2;
constexpr int kINodeSize = 256;

constexpr int kINodeTableBlocksPerBlockGroup =
    (kINodesPerBlockGroup * kINodeSize + kBlockSize - 1) / kBlockSize;
constexpr int kTotalBlocksPerBlockGroup =
    1 /* block bitmap */ + 1 /* inode bitmap */ +
    kINodeTableBlocksPerBlockGroup + kDataBlocksPerBlockGroup;

struct Superblock {
  int64_t inodes_count;
  int64_t blocks_count;
  int64_t unallocated_blocks_count;
  int64_t unallocated_inodes_count;

  uint8_t padding[4064];
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
constexpr uint16_t kUsrR = 0x0;
constexpr uint16_t kUsrW = 0x1;
constexpr uint16_t kUsrX = 0x2;
constexpr uint16_t kGrpR = 0x4;
constexpr uint16_t kGrpW = 0x8;
constexpr uint16_t kGrpX = 0x10;
constexpr uint16_t kOthR = 0x20;
constexpr uint16_t kOthW = 0x40;
constexpr uint16_t kOthX = 0x80;
constexpr uint16_t kSetUid = 0x100;
constexpr uint16_t kSetGid = 0x200;
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
  uint8_t overflow[96];
};

static_assert(sizeof(Superblock) == kBlockSize);
static_assert(sizeof(INode) == kINodeSize);
static_assert(sizeof(INodeTable) == kBlockSize);

struct DirectoryEntry {
  int64_t inode;
  int8_t name_length;
  char name[];
};

class Fileosophy {
 public:
  Fileosophy(BlockCache* blocks);

  void MakeFS();

 private:
  BlockCache* blocks_;
};

#endif  // FS_H_