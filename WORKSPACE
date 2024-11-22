new_local_repository(
    name = "fuse_linux",
    build_file_content =
        """
cc_library(
    name = "fuse",
    hdrs = glob(["include/fuse/*.h"]),
    srcs = ["lib/x86_64-linux-gnu/libfuse.so.2"],
    includes = ["include"],
    visibility = ["//visibility:public"],
)
""",
    path = "/usr",
)

new_local_repository(
    name = "fuse_darwin",
    build_file_content =
        """
cc_library(
    name = "fuse",
    hdrs = glob(["include/fuse/*.h"]),
    srcs = ["lib/libfuse.dylib"],
    includes = ["include"],
    visibility = ["//visibility:public"],
)
""",
    path = "/usr/local",
)
