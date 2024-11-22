#include "block.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "glog/logging.h"

Block::Block(int64_t id, Block* next) : id_(id), lru_next_(next) {
  if (lru_next_) {
    CHECK_EQ(lru_next_->lru_prev_, nullptr)
        << "May only insert at head of list";
    lru_next_->lru_prev_ = this;
  }
}

Block::~Block() {
  // Should already be unlinked
  CHECK_EQ(lru_next_, nullptr);
  CHECK_EQ(lru_prev_, nullptr);
}

void Block::Unlink() {
  if (lru_prev_) {
    lru_prev_->lru_next_ = lru_next_;
  }
  if (lru_next_) {
    lru_next_->lru_prev_ = lru_prev_;
  }

  lru_prev_ = lru_next_ = nullptr;
}

void Block::RelinkInFrontOf(Block* next) {
  if (next == this) {
    return;
  }

  Unlink();
  lru_next_ = next;
  if (lru_next_) {
    lru_prev_ = lru_next_->lru_prev_;
    lru_next_->lru_prev_ = this;

    if (lru_prev_) {
      lru_prev_->lru_next_ = this;
    }
  }
}

PinnedBlock::PinnedBlock(Block* block) : block_(block) { ++block_->ref_count_; }

void PinnedBlock::reset(Block* block) {
  release();
  if (block) {
    block_ = block;
    ++block_->ref_count_;
  }
}

void PinnedBlock::release() {
  if (block_) {
    --block_->ref_count_;
    block_ = nullptr;
  }
}

BlockCache::BlockCache(const char* path, int cache_size)
    : BlockCache(open(path, O_RDWR
#if __linux__
                                | O_DIRECT
#endif
                                | O_SYNC),
                 cache_size) {
}

BlockCache::BlockCache(int fd, int cache_size)
    : fd_(fd), cache_size_(cache_size) {
  PCHECK(fd_ > 0);
  const int64_t size = lseek(fd_, 0, SEEK_END);
  PCHECK(size > 0);
  block_count_ = size / kBlockSize;

  LOG(INFO) << "Opened fs with " << block_count_ << " blocks";
}

BlockCache::~BlockCache() {
  // Copy block ids so we can mutate
  std::vector<int64_t> block_ids;
  for (const auto& it : blocks_) {
    block_ids.push_back(it.first);
  }

  for (auto id : block_ids) {
    Drop(id);
  }

  CHECK(blocks_.empty());
  CHECK(lru_head_ == nullptr);
}

PinnedBlock BlockCache::LockBlock(int64_t block) {
  auto loaded_block = LoadBlockToCache(block);
  return PinnedBlock(loaded_block);
}

void BlockCache::CopyBlock(int64_t block, std::span<uint8_t> dest,
                           int64_t offset) {
  CHECK_LE(offset + std::ssize(dest), kBlockSize);
  auto loaded_block = LoadBlockToCache(block);

  memcpy(dest.data(), &loaded_block->data()[offset], dest.size());

  loaded_block->RelinkInFrontOf(lru_head_);
  lru_head_ = loaded_block;

  // PrintLRU(LOG(INFO));
}

void BlockCache::WriteBlock(int64_t block, std::span<const uint8_t> src,
                            int64_t offset) {
  CHECK_LE(offset + std::ssize(src), kBlockSize);
  auto loaded_block = LoadBlockToCache(block);

  memcpy(&loaded_block->data_mutable()[offset], src.data(), src.size());
  loaded_block->set_modified();

  loaded_block->RelinkInFrontOf(lru_head_);
  lru_head_ = loaded_block;

  // PrintLRU(LOG(INFO));
}

Block* BlockCache::LoadBlockToCache(int64_t block) {
  auto [blk, res] = blocks_.try_emplace(block, block, lru_head_);
  if (!res) {
    // Already loaded in cache, no need to re-read.
    return &blk->second;
  }

  const off_t off = block * kBlockSize;
  PCHECK(lseek(fd_, off, SEEK_SET) != -1)
      << "Failed to seek to block " << block;

  CHECK_EQ(std::ssize(blk->second.data_mutable()), kBlockSize);
  const auto bytes_read =
      read(fd_, blk->second.data_mutable().data(), kBlockSize);
  PCHECK(bytes_read == kBlockSize)
      << "Failed to read " << kBlockSize << " bytes from block " << block
      << " at " << off << ", got " << bytes_read;

  lru_head_ = &blk->second;

  return &blk->second;
}

void BlockCache::Drop(int64_t block) {
  // PrintLRU(LOG(INFO));
  auto search = blocks_.find(block);
  CHECK(search != blocks_.end());

  auto blk = &search->second;

  if (blk == lru_head_) {
    lru_head_ = blk->lru_next();
  }

  blk->Unlink();

  if (blk->modified()) {
    LOG(INFO) << reinterpret_cast<const int64_t*>(blk->data().data())[0];
    PCHECK(lseek(fd_, block * kBlockSize, SEEK_SET) != -1);
    PCHECK(write(fd_, blk->data().data(), kBlockSize) == kBlockSize);
  }

  blocks_.erase(search);
}

void BlockCache::PrintLRU(std::ostream& os) const {
  bool first = true;
  Block* blk = lru_head_;
  while (blk != nullptr) {
    if (!first) {
      os << " -> ";
    }
    os << blk->id();
    first = false;
    blk = blk->lru_next();
  }
}
