#include "block.h"

#include "glog/logging.h"
#include <gtest/gtest.h>

TEST(BlockTest, Test1) {
  char fn[] = "block_test_XXXXXX";
  auto f = mkstemp(fn);
  PCHECK(f > 0);

  LOG(INFO) << "Writing to " << fn;

  PCHECK(ftruncate(f, kBlockSize * 16) == 0);

  const std::array<uint8_t, 4> block0{'b', 'l', 'k', '0'};
  const std::array<uint8_t, 4> block1{'b', 'l', 'k', '1'};

  {
    BlockCache block_cache(f, 8);

    for (int i = 0; i < kBlockSize; i += 4) {
      block_cache.WriteBlock(0, block0, i);
    }

    for (int i = 0; i < kBlockSize; i += 4) {
      block_cache.WriteBlock(1, block1, i);
    }

    // Check what we wrote
    std::array<uint8_t, 4> buf;
    for (int i = 0; i < kBlockSize; i += 4) {
      block_cache.CopyBlock(0, buf, i);
      ASSERT_EQ(buf, block0);
    }

    for (int i = 0; i < kBlockSize; i += 4) {
      block_cache.CopyBlock(1, buf, i);
      ASSERT_EQ(buf, block1);
    }
  }

  {
    // Test reloading
    BlockCache block_cache(f, 8);
    std::array<uint8_t, 4> buf;
    for (int i = 0; i < kBlockSize; i += 4) {
      block_cache.CopyBlock(0, buf, i);
      ASSERT_EQ(buf, block0);
    }

    for (int i = 0; i < kBlockSize; i += 4) {
      block_cache.CopyBlock(1, buf, i);
      ASSERT_EQ(buf, block1);
    }
  }

  // Delete test file
  PCHECK(unlink(fn) == 0);
}