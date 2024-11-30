#include "fs.h"

#include <sys/fcntl.h>
#include <sys/stat.h>

#include <bit>

#include "glog/logging.h"
#include "macros.h"

constexpr int kBlockGroupDescriptorsPerTable =
    kBlockSize / kBlockGroupDescriptorSize;

constexpr std::array<uint8_t, 4096> kEmptyBlock{};

Fileosophy::Fileosophy(BlockCache* blocks) : blocks_(blocks) {
  pinned_super_ = blocks_->LockBlock(0);
  super_ = pinned_super_.data_as<Superblock>();
  first_data_block_ =
      1 + kNonDataBlocksPerBlockGroup * super_->num_block_groups;
}

void Fileosophy::MakeFS() {
  /*
   * Divide available blocks by the size of each block group plus the fractional
   * amount of the block group table. We rearrange this to calculate under
   * integer division.
   *
   * (block_count - 1) / (kTotalBlocksPerBlockGroup + kBlockGroupDescriptorSize
   * / kBlockSize)
   */

  const int num_block_groups =
      ((blocks_->block_count() - 1 /* superblock */) * kBlockSize) /
      (kTotalBlocksPerBlockGroup * kBlockSize + kBlockGroupDescriptorSize);

  if (num_block_groups == 0) {
    LOG(FATAL) << "Disk must be large enough for at least 1 block group!";
  }

  LOG(INFO) << "MakeFS blocks_per_group=" << kTotalBlocksPerBlockGroup
            << " blk_size=" << kBlockSize
            << " blk_grp_des_size=" << kBlockGroupDescriptorSize;

  // Block groups / descriptor size rounded up
  const int num_block_group_descriptor_tables =
      divide_round_up(num_block_groups, kBlockGroupDescriptorsPerTable);

  super_->blocks_count = 1 /* superblock */ +
                         num_block_group_descriptor_tables +
                         num_block_groups * kTotalBlocksPerBlockGroup;
  super_->inodes_count = num_block_groups * kINodesPerBlockGroup;

  super_->unallocated_blocks_count =
      num_block_groups * kDataBlocksPerBlockGroup;
  super_->unallocated_inodes_count = super_->inodes_count;
  super_->num_block_groups = num_block_groups;

  LOG(INFO) << "Super blk_count=" << super_->blocks_count
            << " inodes_count=" << super_->inodes_count
            << " unalloc_blk_count=" << super_->unallocated_blocks_count
            << " unalloc_inode_count=" << super_->unallocated_inodes_count;

  for (int block_group_descriptor = 0;
       block_group_descriptor < num_block_groups; ++block_group_descriptor) {
    auto [lk, block_group] = GetBlockGroupDescriptor(block_group_descriptor);
    block_group->block_bitmap =
        1 + num_block_group_descriptor_tables + block_group_descriptor;
    block_group->inode_bitmap = 1 + num_block_group_descriptor_tables +
                                num_block_groups + block_group_descriptor;
    block_group->inode_table =
        1 + num_block_group_descriptor_tables + num_block_groups * 2 +
        block_group_descriptor * kINodeTableBlocksPerBlockGroup;
    block_group->unallocated_blocks_count = kDataBlocksPerBlockGroup;
    block_group->unallocated_inodes_count = kINodesPerBlockGroup;

    blocks_->WriteBlock(block_group->block_bitmap, kEmptyBlock, 0);
    blocks_->WriteBlock(block_group->inode_bitmap, kEmptyBlock, 0);

    // Mark inode 0 occupied
    if (block_group_descriptor == 0) {
      blocks_->WriteU8(block_group->inode_bitmap, 0x1, 0);
      block_group->unallocated_inodes_count -= 1;
      super_->unallocated_inodes_count -= 1;
    }
  }

  first_data_block_ =
      1 + kNonDataBlocksPerBlockGroup * super_->num_block_groups;
}

void Fileosophy::MakeRootDirectory(uid_t uid, gid_t gid) {
  auto inode = NewINode(0);
  CHECK_EQ(inode->inode_, kRootInode)
      << "Root should have inode " << kRootInode;
  using namespace flags;
  InitINode(inode->data_, Type::kDirectory,
            kUsrR | kUsrW | kUsrX | kGrpR | kGrpX | kOthR | kOthX, uid, gid);
  CHECK(inode->AddDirectoryEntry(".", inode->inode_, Type::kDirectory));
  CHECK(inode->AddDirectoryEntry("..", inode->inode_, Type::kDirectory));
  inode->data_->link_count = 2;
}

CachedINode* Fileosophy::GetINode(int64_t inode) {
  if (auto search = opened_files_.find(inode); search != opened_files_.end()) {
    return &search->second;
  }

  const int64_t block_group_num = GroupOfInode(inode);
  auto [p, block_group] = GetBlockGroupDescriptor(block_group_num);

  const int64_t local_inode = inode % kINodesPerBlockGroup;

  auto p2 = blocks_->LockBlock(block_group->inode_table +
                               local_inode / kINodesPerTableBlock);
  auto inode_table = p2.data_as<INodeTable>();

  LOG(INFO) << "Inode " << inode << " located in inode table "
            << block_group->inode_table;

  auto* inode_ptr = &inode_table->inodes[local_inode % kINodesPerTableBlock];

  return &opened_files_.try_emplace(inode, inode, inode_ptr, p2, this)
              .first->second;
}

namespace {
void ReleaseInBitmap(std::span<uint8_t> bitmap, int32_t local_block) {
  const int64_t bitmap_byte = local_block / 8;
  const int64_t bitmap_pos = local_block % 8;

  CHECK(bitmap[bitmap_byte] & (1 << bitmap_pos)) << "Block wasn't allocated";
  bitmap[bitmap_byte] &= ~(1 << bitmap_pos);
}

std::optional<int64_t> AllocInBitmap(std::span<uint8_t> bitmap, int64_t hint,
                                     bool permit_small, bool* modified) {
  const int num_bytes = std::ssize(bitmap);
  const int num_slots = num_bytes * 8;

  auto try_pos = [&](int64_t local_block) -> bool {
    const int64_t bitmap_byte = local_block / 8;
    const int64_t bitmap_pos = local_block % 8;

    return !(bitmap[bitmap_byte] & (1 << bitmap_pos));
  };

  auto write_pos = [&](int64_t local_block) {
    const int64_t bitmap_byte = local_block / 8;
    const int64_t bitmap_pos = local_block % 8;

    bitmap[bitmap_byte] |= (1 << bitmap_pos);
    // Write back bitmap
    *modified = true;
  };

  // If there is a hint, try it first
  if (hint >= 0) {
    // Hint should be in this group
    CHECK_LT(hint, num_slots);

    // First, look at the hint and hint+1
    if (try_pos(hint)) {
      write_pos(hint);
      return hint;
    } else if (hint < num_slots - 1 && try_pos(hint)) {
      write_pos(hint + 1);
      return hint + 1;
    }
  }

  if (permit_small) {
    // Look bit-by-bit
    for (int i = 0; i < num_bytes; ++i) {
      if (bitmap[i] != 0xFF) {
        const int pos = i * 8 + std::countr_one(bitmap[i]);
        write_pos(pos);
        return pos;
      }
    }

  } else {
    // Only look for fully empty bytes
    for (int i = 0; i < num_bytes; ++i) {
      if (bitmap[i] == 0) {
        const int pos = i * 8;
        write_pos(pos);
        return pos;
      }
    }
  }

  return std::nullopt;
}
}  // namespace

CachedINode* Fileosophy::NewINode(int32_t group_hint) {
  if (super_->unallocated_inodes_count <= 0) {
    LOG(FATAL) << "No inodes left in filesystem";
  }

  CHECK(group_hint >= 0 && group_hint < super_->num_block_groups);

  auto try_group = [this](int32_t group_i) -> std::optional<int> {
    auto [p, group] = GetBlockGroupDescriptor(group_i);

    if (group->unallocated_inodes_count <= 0) {
      return std::nullopt;
    }

    constexpr int num_bytes = kINodesPerBlockGroup / 8;
    std::array<uint8_t, num_bytes> bitmap;
    blocks_->CopyBlock(group->inode_bitmap, bitmap);

    bool modified = false;
    auto res = AllocInBitmap(bitmap, -1, true, &modified);

    CHECK(res.has_value()) << "Where did the inode go?";

    if (modified) {
      // Write back bitmap
      blocks_->WriteBlock(group->inode_bitmap, bitmap);
    }

    group->unallocated_inodes_count -= 1;
    super_->unallocated_inodes_count -= 1;

    CHECK(modified == res.has_value()) << "Huh?";

    return LocalINodeToGlobal(group_i, *res);
  };

  auto res = try_group(group_hint);
  if (res) {
    LOG(INFO) << "Allocated inode " << *res << " in group " << group_hint;
    CHECK_NE(*res, 0) << "Should not allocate inode 0";
    return GetINode(*res);
  }

  for (int32_t group_i = 0; group_i < super_->num_block_groups; ++group_i) {
    if (group_i == group_hint) continue;
    res = try_group(group_i);
    if (res) {
      LOG(INFO) << "Allocated inode " << *res << " in group " << group_i;
      CHECK_NE(*res, 0) << "Should not allocate inode 0";
      return GetINode(*res);
    }
  }

  LOG(FATAL) << "Where did the inode go?";
}

void Fileosophy::ForgetInode(int64_t inode, uint64_t nlookup) {
  auto search = opened_files_.find(inode);
  CHECK(search != opened_files_.end());
  CHECK_GE(search->second.lookups_ -= nlookup, 0)
      << "More forgets than lookups?";

  if (search->second.lookups_ == 0) {
    opened_files_.erase(search);
  }
}

void Fileosophy::InitINode(INode* inode, Type mode, int16_t flags, int32_t uid,
                           int32_t gid) {
  inode->mode = mode;
  inode->size = 0;
  inode->flags = flags;
  inode->uid = uid;
  inode->gid = gid;
  // TODO: Set time
  inode->ctime = inode->mtime = inode->atime = time(nullptr);
  inode->link_count = 1;
  memset(inode->direct_blocks, 0, sizeof(inode->direct_blocks));
  inode->single_indirect = 0;
  inode->double_indirect = 0;
  inode->triple_indirect = 0;
  memset(inode->overflow, 0, sizeof(inode->overflow));
}

std::pair<PinnedBlock, BlockGroupDescriptor*>
Fileosophy::GetBlockGroupDescriptor(int descriptor_num) {
  const int block_group_descriptor_table =
      descriptor_num / kBlockGroupDescriptorsPerTable;
  auto block_group_table_blk =
      blocks_->LockBlock(1 + block_group_descriptor_table);
  auto block_group_table =
      block_group_table_blk
          .data_as<BlockGroupDescriptor[kBlockGroupDescriptorsPerTable]>();

  return {
      block_group_table_blk,
      &(*block_group_table)[descriptor_num % kBlockGroupDescriptorsPerTable]};
}

bool Fileosophy::TruncateINode(CachedINode* inode, int64_t new_size) {
  const auto old_size = inode->data_->size;
  if (new_size == old_size) {
    return true;
  } else if (new_size > old_size) {
    return GrowINode(inode, new_size);
  } else {
    ShrinkINode(inode, new_size);
    return true;
  }
}

bool Fileosophy::GrowINode(CachedINode* inode, int64_t new_size) {
  CHECK_GT(new_size, inode->data_->size);

  const int64_t num_blocks = inode->num_blocks();
  const int64_t new_num_blocks = divide_round_up<int64_t>(new_size, kBlockSize);

  if (num_blocks == new_num_blocks) {
    // Grown, but same number of blocks so no change
    inode->data_->size = new_size;
    return true;
  }

  if (new_num_blocks - num_blocks > super_->unallocated_blocks_count) {
    return false;
  }

  int64_t hint = inode->get_hint();

  for (int64_t blk = num_blocks; blk < new_num_blocks; ++blk) {
    auto new_blk = NewFreeBlock(hint);
    CHECK(new_blk.has_value());
    LOG(INFO) << "Inode " << inode->inode_ << " allocate new block " << *new_blk
              << " hint " << hint;
    blocks_->WriteBlock(*new_blk, kEmptyBlock);
    CHECK_EQ(inode->set_block(blk, *new_blk), 0);
    hint = *new_blk + 1;
  }

  inode->data_->size = new_size;

  return true;
}

void Fileosophy::ShrinkINode(CachedINode* inode, int64_t new_size) {
  CHECK_LT(new_size, inode->data_->size);
  CHECK_GE(new_size, 0);

  const int64_t num_blocks = inode->num_blocks();
  const int64_t new_num_blocks = divide_round_up<int64_t>(new_size, kBlockSize);

  // Free all unused blocks
  // TODO: Free indirect lists as well
  for (int64_t blk = new_num_blocks; blk != num_blocks; ++blk) {
    // LOG(INFO) << blk << ' ' << num_blocks;
    ReleaseBlock(inode->set_block(blk, 0));
  }

  if (new_num_blocks > 0) {
    // Zero out remaining portion of last block
    const int64_t unused_size = (new_num_blocks * kBlockSize) - new_size;
    LOG(INFO) << "Shrink " << new_num_blocks << ' ' << unused_size;
    if (unused_size != 0) {
      auto blk = inode->get_block(new_num_blocks - 1);
      auto p = blocks_->LockBlock(blk);
      auto data = p.data_mutable();
      std::fill_n(&data[kBlockSize - unused_size], unused_size, 0);
    }
  }

  inode->data_->size = new_size;
}

void Fileosophy::ReleaseBlock(int64_t block) {
  auto blk_group = DataBlockToGroup(block);
  auto [p, group] = GetBlockGroupDescriptor(blk_group);

  constexpr int num_bytes = kDataBlocksPerBlockGroup / 8;
  std::array<uint8_t, num_bytes> bitmap;
  blocks_->CopyBlock(group->block_bitmap, bitmap);

  CHECK_GE(block, first_data_block_);
  ReleaseInBitmap(bitmap, DataBlockToGroupLocalBlock(block));
  blocks_->WriteBlock(group->block_bitmap, bitmap);

  ++group->unallocated_blocks_count;
  ++super_->unallocated_blocks_count;
}

void Fileosophy::DeleteINodeAndBlocks(CachedINode* inode) {
  CHECK_EQ(inode->data_->link_count, 0);
  CHECK_EQ(inode->lookups_, 0);

  // Restore inode to group
  auto [p, g] = GetBlockGroupDescriptor(inode->block_group_);
  ++g->unallocated_inodes_count;
  ++super_->unallocated_inodes_count;

  // Cleanup bitmap
  auto bitmap = blocks_->LockBlock(g->inode_bitmap);
  ReleaseInBitmap(bitmap.data_mutable(), inode->inode_ % kINodesPerBlockGroup);
  bitmap.set_modified();

  // Cleanup inode itself
  inode->data_->mode = Type::kUnknown;
  inode->data_->size = 0;

  for (int64_t b : inode->data_->direct_blocks) {
    if (b) ReleaseBlock(b);
  }

  FreeIndirect(inode->data_->single_indirect, 1);
  FreeIndirect(inode->data_->double_indirect, 2);
  FreeIndirect(inode->data_->triple_indirect, 3);
}

std::optional<int64_t> Fileosophy::FindFreeBlockInBlockGroup(
    int64_t group_i, BlockGroupDescriptor* group, int64_t hint,
    bool permit_small) {
  if (group->unallocated_blocks_count <= 0) {
    return std::nullopt;
  }

  constexpr int num_bytes = kDataBlocksPerBlockGroup / 8;
  std::array<uint8_t, num_bytes> bitmap;
  blocks_->CopyBlock(group->block_bitmap, bitmap);

  bool modified = false;
  auto res = AllocInBitmap(bitmap, hint, permit_small, &modified);

  if (modified) {
    // Write back bitmap
    blocks_->WriteBlock(group->block_bitmap, bitmap);
  }

  if (res.has_value()) {
    group->unallocated_blocks_count -= 1;
    super_->unallocated_blocks_count -= 1;

    return DataBlockOfGroup(*res, group_i);
  } else {
    return std::nullopt;
  }
}

std::optional<int64_t> Fileosophy::NewFreeBlock(int64_t hint) {
  if (super_->unallocated_blocks_count <= 0) {
    LOG(FATAL) << "No blocks left in filesystem";
  }

  const int start_group_number = DataBlockToGroup(hint);

  std::optional<int64_t> new_block;
  {
    // First search local group
    auto [p, group] = GetBlockGroupDescriptor(start_group_number);

    const int64_t local_hint = DataBlockToGroupLocalBlock(hint);
    CHECK_GE(hint, first_data_block_);
    LOG(INFO) << start_group_number << ' ' << hint << ' ' << local_hint;
    new_block =
        FindFreeBlockInBlockGroup(start_group_number, group, local_hint, true);
  }

  if (!new_block.has_value()) {
    // Look for blocks in other groups
    for (int32_t gi = 0; gi < super_->num_block_groups; ++gi) {
      if (gi == start_group_number) continue;
      auto [p, group] = GetBlockGroupDescriptor(gi);

      new_block = FindFreeBlockInBlockGroup(gi, group, 0, true);
      if (new_block.has_value()) {
        break;
      }
    }
  }

  CHECK(new_block.has_value());

  return new_block;
}

CachedINode::~CachedINode() {
  CHECK_EQ(lookups_, 0) << "Shouldn't remove inode with lookups";
  if (data_->link_count == 0) {
    fs->DeleteINodeAndBlocks(this);
  }
}

int64_t CachedINode::get_block(int64_t block_index) {
  CHECK_GE(block_index, 0);
  CHECK_LE(block_index, num_blocks());

  if (block_index < kNumDirectBlocks) {
    return data_->direct_blocks[block_index];
  }

  block_index -= kNumDirectBlocks;
  if (block_index < kNumBlocksPerIndirect) {
    return fs->LookupIndirect(data_->single_indirect, block_index, 1);
  }

  block_index -= kNumBlocksPerIndirect;
  if (block_index < kNumBlocksPerDoubleIndirect) {
    return fs->LookupIndirect(data_->double_indirect, block_index, 2);
  }

  block_index -= kNumBlocksPerDoubleIndirect;
  if (block_index < kNumBlocksPerTripleIndirect) {
    return fs->LookupIndirect(data_->triple_indirect, block_index, 3);
  }

  LOG(FATAL) << "Block index too large";
}

int64_t CachedINode::set_block(int64_t block_index, int64_t block_no) {
  if (block_index < kNumDirectBlocks) {
    const int64_t old_block_no = data_->direct_blocks[block_index];
    data_->direct_blocks[block_index] = block_no;
    return old_block_no;
  }

  int64_t old_block_no;
  block_index -= kNumDirectBlocks;
  if (block_index < kNumBlocksPerIndirect) {
    data_->single_indirect =
        fs->WriteIndirect(data_->single_indirect, get_hint(), block_index,
                          block_no, 1, &old_block_no);
    return old_block_no;
  }

  block_index -= kNumBlocksPerIndirect;
  if (block_index < kNumBlocksPerDoubleIndirect) {
    data_->double_indirect =
        fs->WriteIndirect(data_->double_indirect, get_hint(), block_index,
                          block_no, 2, &old_block_no);
    return old_block_no;
  }

  block_index -= kNumBlocksPerDoubleIndirect;
  if (block_index < kNumBlocksPerTripleIndirect) {
    data_->triple_indirect =
        fs->WriteIndirect(data_->triple_indirect, get_hint(), block_index,
                          block_no, 3, &old_block_no);
    return old_block_no;
  }

  LOG(FATAL) << "Block index too large";
}

int64_t CachedINode::get_hint() {
  const auto cur_block = get_last_block();

  if (!cur_block.has_value()) {
    // Search in the same block as the inode
    return fs->FirstDataBlockOfGroup(block_group_);
  } else {
    // Search in the block which holds the inode's last data block
    return *cur_block;
  }
}

namespace {
void read_write_helper(const int64_t size, const int64_t offset, auto&& op) {
  CHECK_GE(size, 0);
  CHECK_GE(offset, 0);

  const auto blk_start = offset / kBlockSize;
  const auto blk_end = divide_round_up<int64_t>(offset + size, kBlockSize);

  const int64_t first_block_offset = offset % kBlockSize;

  int64_t bytes_read = 0;
  for (auto i = blk_start; i < blk_end; ++i) {
    const auto local_start = (i == blk_start) ? first_block_offset : 0;
    const auto local_end = (i == blk_end - 1)
                               ? (size - bytes_read + first_block_offset)
                               : kBlockSize;
    const auto local_size = local_end - local_start;

    op(i, bytes_read, local_size, local_start);
    bytes_read += local_size;
  }
}
}  // namespace

void CachedINode::read(std::span<uint8_t> out, int64_t offset) {
  const auto size = std::ssize(out);
  CHECK_LT(size + offset, data_->size);

  read_write_helper(size, offset,
                    [this, out](int64_t block_i, int64_t bytes_read,
                                int64_t local_size, int64_t start) {
                      auto blk = get_block(block_i);
                      CHECK_NE(blk, 0);
                      fs->blocks_->CopyBlock(
                          blk, out.subspan(bytes_read, local_size), start);
                    });
}

void CachedINode::read_iovec(int64_t size, int64_t offset,
                             std::function<void(std::span<iovec>)> outs) {
  CHECK_LE(size + offset, data_->size);

  std::vector<iovec> out;
  std::vector<PinnedBlock> pinned_blocks;
  read_write_helper(
      size, offset,
      [this, &out, &pinned_blocks](int64_t block_i, int64_t /* bytes_read */,
                                   int64_t local_size, int64_t start) {
        LOG(INFO) << block_i << ' ' << local_size << ' ' << start;
        auto blk = get_block(block_i);
        CHECK_NE(blk, 0);
        auto p = fs->blocks_->LockBlock(blk);
        // TODO: We don't actually maintain the lock here
        out.push_back(
            {.iov_base = const_cast<uint8_t*>(p.data().data() + start),
             .iov_len = static_cast<size_t>(local_size)});
        pinned_blocks.push_back(std::move(p));
      });
  CHECK_LE(std::ssize(out), kMaxReadBlocks);
  outs(out);
}

void CachedINode::write(std::span<const uint8_t> in, int64_t offset) {
  const auto size = std::ssize(in);
  CHECK_LE(size + offset, data_->size);

  read_write_helper(size, offset,
                    [this, in](int64_t block_i, int64_t bytes_read,
                               int64_t local_size, int64_t start) {
                      auto blk = get_block(block_i);
                      CHECK_NE(blk, 0);
                      LOG(INFO) << "Write to " << blk << " " << local_size;
                      fs->blocks_->WriteBlock(
                          blk, in.subspan(bytes_read, local_size), start);
                    });
}

// Add a new directory entry, first by attempting to borrow an existing entry,
// if not by appending. Directory entries cannot span blocks.
bool CachedINode::AddDirectoryEntry(std::string_view filename, int64_t inode,
                                    Type type) {
  CHECK(data_->mode == Type::kDirectory);

  int32_t blknum = 0;
  const int32_t nblocks = num_blocks();

  CHECK_LE(std::ssize(filename), kFilenameLen);
  CHECK_GT(inode, 0);

  const int16_t bytes_needed = sizeof(DirectoryEntry) + filename.size();

  // First try to find an existing block to borrow
  while (blknum != nblocks) {
    auto blki = get_block(blknum);
    auto blk = fs->blocks_->LockBlock(blki);
    auto blkdata = blk.data_mutable().data();
    auto end = blk.data_mutable().data() + kBlockSize;

    do {
      DirectoryEntry* de = reinterpret_cast<DirectoryEntry*>(blkdata);
      DirectoryEntry* new_entry = nullptr;

      if (de->inode == 0) {
        if (de->alloc_length >= bytes_needed) {
          // Take over this entry
          new_entry = de;

          // Check alignment is correct (it should be)
          CHECK_EQ(reinterpret_cast<uintptr_t>(de) % 8, 0U);
        }
      } else {
        // Try to split entry

        if (de->name_str() == filename) {
          // Filename already exists
          return false;
        }

        const auto de_effective_size = de->name_length + sizeof(DirectoryEntry);

        void* new_de_start = blkdata + de_effective_size;
        std::size_t space = de->alloc_length - de_effective_size;

        if (std::align(8, bytes_needed, new_de_start, space)) {
          // We can split this entry
          new_entry = reinterpret_cast<DirectoryEntry*>(new_de_start);

          // New entry takes all the remaining size
          new_entry->alloc_length = space;

          // Old entry is resized to its name length + space used for padding
          de->alloc_length =
              reinterpret_cast<const uint8_t*>(new_de_start) - blkdata;
        }
      }

      if (new_entry) {
        new_entry->inode = inode;
        new_entry->name_length = filename.length();
        new_entry->type = type;
        memcpy(new_entry->name, filename.data(), filename.size());
        blk.set_modified();
        return true;
      }

      blkdata += de->alloc_length;
    } while (blkdata != end);

    ++blknum;
  }

  // Couldn't allocate in an existing region, add a new block
  CHECK(fs->GrowINode(this, data_->size + kBlockSize));
  auto blk = fs->blocks_->LockBlock(get_block(nblocks));

  DirectoryEntry* new_entry =
      reinterpret_cast<DirectoryEntry*>(blk.data_mutable().data());
  new_entry->inode = inode;
  new_entry->alloc_length = kBlockSize;
  new_entry->name_length = filename.length();
  new_entry->type = type;
  memcpy(new_entry->name, filename.data(), filename.size());
  blk.set_modified();

  return true;
}

std::optional<int64_t> CachedINode::RemoveDE(std::string_view filename) {
  CHECK(data_->mode == Type::kDirectory);

  int32_t blknum = 0;
  const int32_t nblocks = num_blocks();

  CHECK_LE(std::ssize(filename), kFilenameLen);

  // First try to find an existing block to borrow
  while (blknum != nblocks) {
    auto blki = get_block(blknum);
    auto blk = fs->blocks_->LockBlock(blki);
    auto blkdata = blk.data_mutable().data();
    auto end = blk.data_mutable().data() + kBlockSize;

    DirectoryEntry* prev = nullptr;

    do {
      DirectoryEntry* de = reinterpret_cast<DirectoryEntry*>(blkdata);

      if (de->name_str() == filename) {
        // Found file

        const auto ino = de->inode;

        // Outcomes:
        if (prev) {
          // 1. This entry follows an entry, merge into previous
          prev->alloc_length += de->alloc_length;
        } else {
          // 2. This is the 1st entry in the block, mark unused by setting inode
          // to 0
          de->inode = 0;
          de->type = Type::kUnknown;
        }
        // 3. This entry is the 1st and only, delete the block (not handled yet
        // since we need a way to shuffle all following entries down)

        blk.set_modified();

        return ino;
      }

      blkdata += de->alloc_length;
      prev = de;
    } while (blkdata != end);

    ++blknum;
  }

  return std::nullopt;
}

CachedINode* CachedINode::LookupFile(std::string_view filename) {
  CHECK(data_->mode == Type::kDirectory);
  CHECK_LE(std::ssize(filename), kFilenameLen);

  CachedINode* result = nullptr;
  ReadDir(0, [this, filename, &result](const DirectoryEntry* de, off_t) {
    if (filename == de->name_str()) {
      result = fs->GetINode(de->inode);
      return false;
    } else {
      return true;
    }
  });

  return result;
}

void CachedINode::ReadDir(
    const off_t off,
    std::function<bool(const DirectoryEntry*, off_t)> callback) {
  CHECK(data_->mode == Type::kDirectory);

  // We use off as follows: first to determine the starting block.
  // Then, seek to the first directory entry with an offset at least off
  // from the start of the block. This should satisfy the modification while
  // iterating rules.

  int32_t blknum = off / kBlockSize;
  const int32_t nblocks = num_blocks();

  const auto off_local = off % kBlockSize;

  while (blknum < nblocks) {
    auto blki = get_block(blknum);
    auto blk = fs->blocks_->LockBlock(blki);
    const auto start = blk.data().data();
    const auto end = start + kBlockSize;
    auto blkdata = start;

    do {
      const DirectoryEntry* de =
          reinterpret_cast<const DirectoryEntry*>(blkdata);
      blkdata += de->alloc_length;

      if (blkdata - start < off_local) {
        // Skip blocks whose offset is less than our starting offset
        continue;
      }

      if (de->inode == 0) {
        // Skip unused entries
        continue;
      }

      const auto next_off = (blknum * kBlockSize) + (blkdata - start);
      if (!callback(de, next_off)) {
        break;
      }
    } while (blkdata != end);

    ++blknum;
  }
}

bool CachedINode::IsEmpty() {
  CHECK(data_->mode == Type::kDirectory);

  bool is_empty = true;
  ReadDir(0, [&is_empty](const DirectoryEntry* de, off_t) {
    if (de->name_str() != "." && de->name_str() != "..") {
      is_empty = false;
      return false;
    }
    return true;
  });

  return is_empty;
}

void CachedINode::FillStat(struct stat* attr) {
  memset(attr, 0, sizeof(struct stat));

  attr->st_ino = inode_;
  CHECK(FlagsToMode(data_->mode, data_->flags, &attr->st_mode));
  attr->st_nlink = data_->link_count;
  attr->st_uid = data_->uid;
  attr->st_gid = data_->gid;
#if __linux__
  attr->st_atime = data_->atime;
  attr->st_mtime = data_->mtime;
  attr->st_ctime = data_->ctime;
#elif __APPLE__
  attr->st_atimespec =
      timespec{.tv_sec = static_cast<long>(data_->atime), .tv_nsec = 0};
  attr->st_mtimespec =
      timespec{.tv_sec = static_cast<long>(data_->mtime), .tv_nsec = 0};
  attr->st_ctimespec =
      timespec{.tv_sec = static_cast<long>(data_->ctime), .tv_nsec = 0};
#endif
  attr->st_size = data_->size;
  attr->st_blocks = num_blocks();
  attr->st_blksize = kBlockSize;
}

int64_t Fileosophy::LookupIndirect(const int64_t indirect_list,
                                   const int64_t block_index, int depth) {
  int64_t the_block = indirect_list;
  for (int i = 0; i < depth; ++i) {
    const int64_t local_block = block_index >> ((depth - 1 - i) * 9) & 0x1FF;
    the_block = blocks_->ReadI64(the_block, local_block);
  }

  return the_block;
}

void Fileosophy::FreeIndirect(const int64_t indirect_list, int depth) {
  if (indirect_list == 0) {
    return;
  }

  std::array<int64_t, kNumBlocksPerIndirect> list;
  static_assert(sizeof(list) == kBlockSize);

  blocks_->CopyBlock(indirect_list, list);

  if (depth > 0) {
    // This is an indirect list
    for (int64_t b : list) {
      FreeIndirect(b, depth - 1);
    }
  }

  // Release current block (either a list, or a data block)
  ReleaseBlock(indirect_list);
}

int64_t Fileosophy::WriteIndirect(int64_t indirect_list, int64_t hint,
                                  int64_t block_index, int64_t block_no,
                                  int depth, int64_t* old_block_no) {
  if (indirect_list == 0) {
    // No indirect list, allocate a new one
    // TODO: Should this be relative to inode?
    indirect_list = CHECK_NOTNULLOPT(NewFreeBlock(hint));
    VLOG(1) << "Allocate new indirect root depth=" << depth
            << ", blk=" << indirect_list;

    // Zero out the indirect list
    blocks_->WriteBlock(indirect_list, kEmptyBlock);
  }

  const int64_t indirect_root = indirect_list;

  for (int i = 0; i < depth; ++i) {
    const int64_t local_block = block_index >> ((depth - 1 - i) * 9) & 0x1FF;

    if (i == depth - 1) {
      if (old_block_no != nullptr) {
        *old_block_no = blocks_->ReadI64(indirect_list, local_block);
      }

      // Reached the end; write the block_no
      blocks_->WriteI64(indirect_list, block_no, local_block);
    } else {
      int64_t the_block = blocks_->ReadI64(indirect_list, local_block);

      if (the_block == 0) {
        // Need to allocate this indirect block
        the_block = CHECK_NOTNULLOPT(NewFreeBlock(indirect_list));
        blocks_->WriteI64(indirect_list, the_block, local_block);
        VLOG(1) << "Allocate new indirect block depth=" << i
                << ", blk=" << indirect_list;
        // Zero out the indirect list
        blocks_->WriteBlock(the_block, kEmptyBlock);
      }

      indirect_list = the_block;
    }
  }

  CHECK_NE(indirect_root, 0);
  return indirect_root;
}
