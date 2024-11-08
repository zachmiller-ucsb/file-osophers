#include <gflags/gflags.h>
#include "glog/logging.h"

#include "fs.h"

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  CHECK(argc == 2);
  BlockCache cache(argv[1], 8);
  Fileosophy fs(&cache);

  fs.MakeFS();
  fs.MakeRootDirectory();
}