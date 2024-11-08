#include "fs.h"

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

  const int num_block_groups = divide_round_up<int64_t>(
      (blocks_->block_count() - 1 /* superblock */) * kBlockSize,
      kTotalBlocksPerBlockGroup * kBlockSize + kBlockGroupDescriptorSize);

  LOG(INFO) << kTotalBlocksPerBlockGroup << " " << kBlockSize << " "
            << kBlockGroupDescriptorSize;

  // Block groups / descriptor size rounded up
  const int num_block_group_descriptor_tables =
      divide_round_up(num_block_groups, kBlockGroupDescriptorsPerTable);

  super_->blocks_count = 1 /* superblock */ +
                         num_block_group_descriptor_tables +
                         num_block_groups * kTotalBlocksPerBlockGroup;
  super_->inodes_count = num_block_groups * kINodesPerBlockGroup;

  super_->unallocated_blocks_count = super_->blocks_count - 1 /* superblock */;
  super_->unallocated_inodes_count = super_->inodes_count;
  super_->num_block_groups = num_block_groups;

  LOG(INFO) << "Super " << super_->blocks_count << " " << super_->inodes_count
            << " " << super_->unallocated_blocks_count << " "
            << super_->unallocated_inodes_count;

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
  }

  first_data_block_ =
      1 + kNonDataBlocksPerBlockGroup * super_->num_block_groups;
}

void Fileosophy::MakeRootDirectory() {
  auto inode = GetINode(0);
  using namespace flags;
  InitINode(inode.data, Mode::kDirectory,
            kUsrR | kUsrW | kUsrX | kGrpR | kGrpX | kOthR | kOthX, 0, 0);
  GrowINode(&inode, 4096 * 12);
}

CachedINode Fileosophy::GetINode(int64_t inode) {
  const int64_t block_group_num = GroupOfInode(inode);
  auto [p, block_group] = GetBlockGroupDescriptor(block_group_num);

  const int64_t local_inode = inode % kINodesPerBlockGroup;

  auto p2 = blocks_->LockBlock(block_group->inode_table +
                               local_inode / kINodesPerTableBlock);
  auto inode_table = p2.data_as<INodeTable>();

  LOG(INFO) << "Inode " << inode << " located in inode table "
            << block_group->inode_table;

  auto* inode_ptr = &inode_table->inodes[local_inode % kINodesPerTableBlock];

  return {inode, inode_ptr, p2, this};
}

void Fileosophy::InitINode(INode* inode, Mode mode, int16_t flags, int32_t uid,
                           int32_t gid) {
  inode->mode = mode;
  inode->size = 0;
  inode->flags = flags;
  inode->uid = uid;
  inode->gid = gid;
  // TODO: Set time
  inode->ctime = inode->mtime = inode->atime = 0;
  inode->link_count = 0;
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

  LOG(INFO) << block_group_descriptor_table << " "
            << (descriptor_num % kBlockGroupDescriptorsPerTable);
  return {
      block_group_table_blk,
      &(*block_group_table)[descriptor_num % kBlockGroupDescriptorsPerTable]};
}

void Fileosophy::GrowINode(CachedINode* inode, int64_t new_size) {
  CHECK_GT(new_size, inode->data->size);

  const int64_t num_blocks = inode->num_blocks();
  const int64_t new_num_blocks = divide_round_up<int64_t>(new_size, kBlockSize);

  if (num_blocks == new_num_blocks) {
    // Grown, but same number of blocks so no change
    return;
  }

  const auto cur_block = inode->get_last_block();

  int64_t hint;
  if (!cur_block.has_value()) {
    // Search in the same block as the inode
    hint = FirstDataBlockOfGroup(inode->block_group);
  } else {
    // Search in the block which holds the inode's last data block
    hint = *cur_block;
  }

  for (int64_t blk = num_blocks; blk < new_num_blocks; ++blk) {
    auto new_blk = NewFreeBlock(hint);
    CHECK(new_blk.has_value());
    LOG(INFO) << "Inode " << inode->inode << " allocate new block " << *new_blk
              << " hint " << hint;
    inode->set_block(blk, *new_blk);
    hint = *new_blk + 1;
  }

  inode->data->size = new_size;
}

std::optional<int64_t> Fileosophy::FindFreeBlockInBlockGroup(
    int64_t group_i, BlockGroupDescriptor* group, int64_t hint,
    bool permit_small) {
  constexpr int num_bytes = kDataBlocksPerBlockGroup / 8;
  std::array<uint8_t, num_bytes> bitmap;
  blocks_->CopyBlock(group->block_bitmap, bitmap);

  auto try_pos = [&](int64_t local_block) -> bool {
    const int64_t bitmap_byte = local_block / 8;
    const int64_t bitmap_pos = local_block % 8;

    return !(bitmap.at(bitmap_byte) & (1 << bitmap_pos));
  };

  auto write_pos = [&](int64_t local_block) {
    const int64_t bitmap_byte = local_block / 8;
    const int64_t bitmap_pos = local_block % 8;

    bitmap[bitmap_byte] |= (1 << bitmap_pos);
    // Write back bitmap
    blocks_->WriteBlock(group->block_bitmap, bitmap);
  };

  // If there is a hint, try it first
  if (hint >= 0) {
    // Hint should be in this group
    CHECK_LT(hint, kDataBlocksPerBlockGroup);

    // First, look at the hint and hint+1
    if (try_pos(hint)) {
      write_pos(hint);
      return DataBlockOfGroup(hint, group_i);
    } else if (hint < kDataBlocksPerBlockGroup - 1 && try_pos(hint)) {
      write_pos(hint + 1);
      return DataBlockOfGroup(hint + 1, group_i);
    }
  }

  if (permit_small) {
    // Look bit-by-bit
    for (int i = 0; i < num_bytes; ++i) {
      if (bitmap[i] != 0xFF) {
        const int pos = i * 8 + std::countr_one(bitmap[i]);
        write_pos(pos);
        return DataBlockOfGroup(pos, group_i);
      }
    }

  } else {
    // Only look for fully empty bytes
    for (int i = 0; i < num_bytes; ++i) {
      if (bitmap[i] == 0) {
        const int pos = i * 8;
        write_pos(pos);
        return DataBlockOfGroup(pos, group_i);
      }
    }
  }

  return std::nullopt;
}

std::optional<int64_t> Fileosophy::NewFreeBlock(int64_t hint) {
  const int start_group_number = DataBlockToGroup(hint);

  auto [p, group] = GetBlockGroupDescriptor(start_group_number);

  const int64_t local_hint =
      (hint - first_data_block_) % kDataBlocksPerBlockGroup;
  CHECK_GE(hint, first_data_block_);
  LOG(INFO) << start_group_number << ' ' << hint << ' ' << local_hint;
  const auto new_block =
      FindFreeBlockInBlockGroup(start_group_number, group, local_hint, true);
  CHECK(new_block.has_value()) << "Could not find free block";

  // TODO: Search other blocks

  return new_block;
}

int64_t CachedINode::get_block(int64_t block_index) {
  CHECK_GE(block_index, 0);
  CHECK_LE(block_index, num_blocks());

  if (block_index < kNumDirectBlocks) {
    return data->direct_blocks[block_index];
  }

  block_index -= kNumDirectBlocks;
  if (block_index < kNumBlocksPerIndirect) {
    return fs->LookupIndirect(data->single_indirect, block_index, 1);
  }

  block_index -= kNumBlocksPerIndirect;
  if (block_index < kNumBlocksPerDoubleIndirect) {
    return fs->LookupIndirect(data->double_indirect, block_index, 2);
  }

  block_index -= kNumBlocksPerDoubleIndirect;
  if (block_index < kNumBlocksPerTripleIndirect) {
    return fs->LookupIndirect(data->triple_indirect, block_index, 3);
  }

  LOG(FATAL) << "Block index too large";
}

void CachedINode::set_block(int64_t block_index, int64_t block_no) {
  if (block_index < kNumDirectBlocks) {
    data->direct_blocks[block_index] = block_no;
    return;
  }

  block_index -= kNumDirectBlocks;
  if (block_index < kNumBlocksPerIndirect) {
    data->single_indirect =
        fs->WriteIndirect(data->single_indirect, block_index, block_no, 1);
    return;
  }

  block_index -= kNumBlocksPerIndirect;
  if (block_index < kNumBlocksPerDoubleIndirect) {
    data->double_indirect =
        fs->WriteIndirect(data->double_indirect, block_index, block_no, 2);
    return;
  }

  block_index -= kNumBlocksPerDoubleIndirect;
  if (block_index < kNumBlocksPerTripleIndirect) {
    data->triple_indirect =
        fs->WriteIndirect(data->triple_indirect, block_index, block_no, 3);
    return;
  }

  LOG(FATAL) << "Block index too large";
}

int64_t Fileosophy::LookupIndirect(const int64_t indirect_list,
                                   const int64_t block_index, int depth) {
  // Check block is the correct size for the depth requested
  // This makes depth redundant, but performs a sanity check
  const int computed_depth =
      std::max(std::bit_width(static_cast<uint64_t>(block_index)) - 1, 0) / 12;
  CHECK_EQ(computed_depth + 1, depth);

  int64_t the_block = indirect_list;
  for (int i = 0; i < depth; ++i) {
    const int64_t local_block = block_index >> ((depth - 1 - i) * 12) & 0xFFF;
    the_block = blocks_->ReadI64(the_block, local_block);
  }

  return the_block;
}

int64_t Fileosophy::WriteIndirect(int64_t indirect_list, int64_t block_index,
                                  int64_t block_no, int depth) {
  const int computed_depth =
      std::max(std::bit_width(static_cast<uint64_t>(block_index)) - 1, 0) / 12;
  CHECK_EQ(computed_depth + 1, depth);

  if (indirect_list == 0) {
    // TODO: Should this be relative to inode?
    indirect_list = CHECK_NOTNULLOPT(NewFreeBlock(block_no));
    VLOG(1) << "Allocate new indirect root depth=" << depth
            << ", blk=" << indirect_list;

    // Zero out the indirect list
    blocks_->WriteBlock(indirect_list, kEmptyBlock);
  }

  const int64_t indirect_root = indirect_list;

  for (int i = 0; i < depth; ++i) {
    const int64_t local_block = block_index >> ((depth - 1 - i) * 12) & 0xFFF;

    if (i == depth - 1) {
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
      }

      indirect_list = the_block;
    }
  }

  CHECK_NE(indirect_root, 0);
  return indirect_root;
}
