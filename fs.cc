#include "fs.h"

void Fileosophy::MakeFS() {
  auto superblk = blocks_->LockBlock(-1);
  auto super = superblk.data_as<Superblock>();

  constexpr int block_group_descriptors_per_table =
      kBlockSize / kBlockGroupDescriptorSize;

  /*
   * Divide available blocks by the size of each block group plus the fractional
   * amount of the block group table. We rearrange this to calculate under
   * integer division.
   *
   * (block_count - 1) / (kTotalBlocksPerBlockGroup + kBlockGroupDescriptorSize
   * / kBlockSize)
   */

  const int num_block_groups =
      (blocks_->block_count() - 1 /* superblock */) * kBlockSize /
      (kTotalBlocksPerBlockGroup * kBlockSize + kBlockGroupDescriptorSize);

  // Block groups / descriptor size rounded up
  const int num_block_group_descriptor_tables =
      (num_block_groups + block_group_descriptors_per_table - 1) /
      block_group_descriptors_per_table;

  super->blocks_count = 1 /* superblock */ + num_block_group_descriptor_tables +
                        num_block_groups * kTotalBlocksPerBlockGroup;
  super->inodes_count = num_block_groups * kINodesPerBlockGroup;

  super->unallocated_blocks_count = super->blocks_count - 1 /* superblock */;
  super->unallocated_inodes_count = super->inodes_count;

  for (int block_group_descriptor_table = 0;
       block_group_descriptor_table < num_block_group_descriptor_tables;
       ++block_group_descriptor_table) {
    auto block_group_table_blk =
        blocks_->LockBlock(block_group_descriptor_table);
    auto block_group_table =
        block_group_table_blk
            .data_as<BlockGroupDescriptor[block_group_descriptors_per_table]>();

    //for (int block_group_descriptor = )
  }

  // Initialize block groups
  for (int block_group = 0; block_group < num_block_groups; ++block_group) {
    int block_group_addr = block_group * kTotalBlocksPerBlockGroup;
    auto block_group_descriptor_blk = blocks_->LockBlock(block_group_addr);
    auto block_group_descriptor =
        block_group_descriptor_blk.data_as<BlockGroupDescriptor>();

    block_group_descriptor->block_bitmap = block_group_addr + 1;
    block_group_descriptor->inode_bitmap = block_group_addr + 2;
    block_group_descriptor->inode_table = block_group_addr + 3;
  }
}