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

  std::span<const uint8_t, 4096> data() const { return data_; }
  std::span<uint8_t, 4096> data_mutable() { return data_; }

  bool modified() const { return modified_; }
  void set_modified() { modified_ = true; }
  void clear_modified() { modified_ = false; }

  int64_t id() const { return id_; }
  Block* lru_next() { return lru_next_; }

 private:
  int64_t id_;
  Block* lru_prev_ = nullptr;
  Block* lru_next_ = nullptr;
  bool modified_ = false;
  std::array<uint8_t, 4096> data_;
};

class BlockCache {
 public:
  explicit BlockCache(const char* path, int cache_size);

  explicit BlockCache(int fd, int cache_size);

  ~BlockCache();

  void CopyBlock(int64_t block, std::span<uint8_t> dest, int64_t offset = 0);

  void WriteBlock(int64_t block, std::span<const uint8_t> src,
                  int64_t offset = 0);

 private:
  Block* LoadBlockToCache(int64_t block);

  void Drop(int64_t block);

  void PrintLRU(std::ostream& os) const;

  int fd_ = -1;
  int cache_size_;
  Block* lru_head_ = nullptr;
  std::unordered_map<int64_t, Block> blocks_;
};

#endif  // BLOCK_H_