#ifndef FS_H_
#define FS_H_

#include "block.h"

constexpr int kNumDirectBlocks = 10;

struct Superblock {
  int64_t next_block;
  uint8_t dummy[4088];
};

enum class Mode : int8_t {
  kRegular,
  kDirectory
};

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
}

struct INode {
  Mode mode;
  int16_t flags;
  int32_t uid;
  int32_t gid;
  int64_t ctime;
  int64_t mtime;
  int64_t atime;
  int64_t direct_blocks[kNumDirectBlocks];
  int64_t single_indirect;
  int64_t double_indirect;
};

static_assert(sizeof(Superblock) == kBlockSize);

#endif  // FS_H_