#include "fs.h"

#include <bit>

#include "glog/logging.h"

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
}

void Fileosophy::MakeRootDirectory() {}

CachedINode Fileosophy::GetINode(int64_t inode) {
  const int64_t block_group_num = GroupOfInode(inode);
  auto [p, block_group] = GetBlockGroupDescriptor(block_group_num);

  const int64_t local_inode = inode % kINodesPerBlockGroup;

  auto p2 = blocks_->LockBlock(block_group->inode_table +
                               local_inode / kINodesPerTableBlock);
  auto inode_table = p2.data_as<INodeTable>();

  auto* inode_ptr = &inode_table->inodes[local_inode % kINodesPerTableBlock];

  return {inode, inode_ptr, p2, this};
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

void Fileosophy::GrowINode(int64_t inode) {}

int64_t Fileosophy::FindFreeBlockInBlockGroup(int64_t group_i,
                                              BlockGroupDescriptor* group,
                                              int64_t hint, bool permit_small) {
  constexpr int num_bytes = kDataBlocksPerBlockGroup / sizeof(uint8_t);
  std::array<uint8_t, num_bytes> bitmap;
  blocks_->CopyBlock(group->block_bitmap, bitmap);

  auto try_pos = [&](int64_t local_block) -> bool {
    const int64_t bitmap_byte = local_block / sizeof(uint8_t);
    const int64_t bitmap_pos = local_block % sizeof(uint8_t);

    return !(bitmap.at(bitmap_byte) & (1 << bitmap_pos));
  };

  // If there is a hint, try it first
  if (hint >= 0) {
    // Hint should be in this group
    CHECK_LT(hint, kDataBlocksPerBlockGroup);

    // First, look at the hint and hint+1
    if (try_pos(hint)) {
      return hint;
    } else if (hint < kDataBlocksPerBlockGroup - 1 && try_pos(hint)) {
      return hint + 1;
    }
  }

  if (permit_small) {
    // Look bit-by-bit
    for (int i = 0; i < num_bytes; ++i) {
      if (bitmap[i] != 0xFF) {
        return FirstDataBlockOfGroup(group_i) + i * sizeof(uint8_t) +
               std::countr_one(bitmap[i]);
      }
    }

  } else {
    // Only look for fully empty bytes
    for (int i = 0; i < num_bytes; ++i) {
      if (bitmap[i] == 0) {
        return FirstDataBlockOfGroup(group_i) + i * sizeof(uint8_t);
      }
    }

    return -1;
  }
}

int64_t Fileosophy::NewFreeBlock(CachedINode* inode) {
  const auto cur_block = inode->get_last_block();

  int64_t hint;
  if (!cur_block.has_value()) {
    // Search in the same block as the inode
    hint = FirstDataBlockOfGroup(inode->block_group);
  } else {
    // Search in the block which holds the inode's last data block
    hint = *cur_block;
  }

  const int start_group_number =
      (hint - first_data_block_) / kDataBlocksPerBlockGroup;

  auto [p, group] = GetBlockGroupDescriptor(start_group_number);

  FindFreeBlockInBlockGroup(start_group_number, group, hint, true);
}

int64_t CachedINode::get_block(int64_t block_index) {
  CHECK_GE(block_index, 0);
  CHECK_LE(block_index, num_blocks());

  if (block_index < kNumDirectBlocks) {
    return data->direct_blocks[block_index];
  }

  block_index -= kNumDirectBlocks;
  if (block_index < kNumBlocksPerIndirect) {
    int64_t the_block;
    fs->blocks_->CopyBlock(
        data->single_indirect,
        {reinterpret_cast<uint8_t*>(&the_block), sizeof(int64_t)},
        block_index % kNumBlocksPerIndirect);

    return the_block;
  }

  block_index -= kNumBlocksPerIndirect;
  if (block_index < kNumBlocksPerDoubleIndirect) {
    int64_t single_indirect;
    fs->blocks_->CopyBlock(
        data->double_indirect,
        {reinterpret_cast<uint8_t*>(&single_indirect), sizeof(int64_t)},
        block_index / kNumBlocksPerIndirect);

    int64_t the_block;
    fs->blocks_->CopyBlock(
        single_indirect,
        {reinterpret_cast<uint8_t*>(&the_block), sizeof(int64_t)},
        block_index % kNumBlocksPerIndirect);
  }

  block_index -= kNumBlocksPerDoubleIndirect;
  if (block_index < kNumBlocksPerTripleIndirect) {
    int64_t double_indirect;
    fs->blocks_->CopyBlock(
        data->triple_indirect,
        {reinterpret_cast<uint8_t*>(&double_indirect), sizeof(int64_t)},
        block_index / kNumBlocksPerDoubleIndirect);

    int64_t single_indirect;
    fs->blocks_->CopyBlock(
        double_indirect,
        {reinterpret_cast<uint8_t*>(&single_indirect), sizeof(int64_t)},
        block_index / kNumBlocksPerIndirect);

    int64_t the_block;
    fs->blocks_->CopyBlock(
        single_indirect,
        {reinterpret_cast<uint8_t*>(&the_block), sizeof(int64_t)},
        block_index % kNumBlocksPerIndirect);

    return the_block;
  }

  LOG(FATAL) << "Block index too large";
}
