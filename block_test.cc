#include "block.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

TEST(BlockTest, Test1) {
  char fn[] = "block_test_XXXXXX";
  auto f = mkstemp(fn);
  PCHECK(f > 0);

  LOG(INFO) << "Writing to " << fn;

  PCHECK(ftruncate(f, kBlockSize * 16) == 0);

  BlockCache block_cache(f, 8);

  const char* block0 = "blk0";
  const char* block1 = "blk1";

  for (int i = 0; i < kBlockSize; i += 4) {
    block_cache.WriteBlock(
        0, std::span(reinterpret_cast<const uint8_t*>(block0), 4), i);
  }

  for (int i = 0; i < kBlockSize; i += 4) {
    block_cache.WriteBlock(
        1, std::span(reinterpret_cast<const uint8_t*>(block1), 4), i);
  }

  // Check what we wrote
  //std::array buf 4];
  //uint8_t buf2[] = "blk0";
  for (int i = 0; i < kBlockSize; i += 4) {
    block_cache.CopyBlock(0, {buf, 4}, i);
    ASSERT_EQ(buf, buf2);
  }

  for (int i = 0; i < kBlockSize; i += 4) {
    block_cache.CopyBlock(1, {buf, 4}, i);
  }

  // Delete test file
  PCHECK(unlink(fn) == 0);
}