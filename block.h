#ifndef BLOCK_H_
#define BLOCK_H_

#include <cstdint>
#include <memory>
#include <span>
#include <unordered_map>

constexpr int kBlockSize = 4096;

class Block {
 public:
  explicit Block(int64_t id, Block* next);
  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  ~Block();

  void Unlink();

  void RelinkInFrontOf(Block* next);

  std::span<const uint8_t, kBlockSize> data() const { return data_; }
  std::span<uint8_t, kBlockSize> data_mutable() { return data_; }

  bool modified() const { return modified_; }
  void set_modified() { modified_ = true; }
  void clear_modified() { modified_ = false; }

  int64_t id() const { return id_; }
  int32_t ref_count() const { return ref_count_; }
  Block* lru_next() { return lru_next_; }

 private:
  int64_t id_;
  int32_t ref_count_ = 0;
  Block* lru_prev_ = nullptr;
  Block* lru_next_ = nullptr;
  bool modified_ = false;
  alignas(kBlockSize) std::array<uint8_t, kBlockSize> data_;

  friend class PinnedBlock;
};

class PinnedBlock {
 public:
  PinnedBlock() : block_(nullptr) {}

  PinnedBlock(Block* block);

  ~PinnedBlock();

  auto data() { return block_->data(); }
  auto data_mutable() { return block_->data_mutable(); }

  template <typename T>
  T* data_as() {
    static_assert(std::is_standard_layout_v<T> && std::is_trivial_v<T>);
    auto data = data_mutable();
    static_assert(sizeof(T) == kBlockSize);
    block_->set_modified();
    return reinterpret_cast<T*>(data.data());
  }

 private:
  Block* block_;
};

class BlockCache {
 public:
  explicit BlockCache(const char* path, int cache_size);

  explicit BlockCache(int fd, int cache_size);

  ~BlockCache();

  int64_t block_count() const { return block_count_; }

  PinnedBlock LockBlock(int64_t block);

  void CopyBlock(int64_t block, std::span<uint8_t> dest, int64_t offset = 0);

  int64_t ReadI64(int64_t block, int64_t offset) {
    int64_t data;
    CopyBlock(block, {reinterpret_cast<uint8_t*>(&data), sizeof(int64_t)},
              offset * sizeof(int64_t));
    return data;
  }

  void WriteBlock(int64_t block, std::span<const uint8_t> src,
                  int64_t offset = 0);

  void WriteI64(int64_t block, int64_t data, int64_t offset) {
    WriteBlock(block,
               {reinterpret_cast<const uint8_t*>(&data), sizeof(int64_t)},
               offset * sizeof(int64_t));
  }

 private:
  BlockCache(const BlockCache&) = delete;
  BlockCache& operator=(const BlockCache&) = delete;
  BlockCache(BlockCache&&) = delete;

  Block* LoadBlockToCache(int64_t block);

  void Drop(int64_t block);

  void PrintLRU(std::ostream& os) const;

  int fd_ = -1;
  int64_t block_count_;
  int cache_size_;
  Block* lru_head_ = nullptr;
  std::unordered_map<int64_t, Block> blocks_;
};

#endif  // BLOCK_H_