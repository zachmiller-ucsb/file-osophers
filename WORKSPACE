new_local_repository(
    name = "fuse",
    build_file_content =
        """
cc_library(
    name = "fuse",
    hdrs = glob(["include/fuse/*.h"]),
    srcs = ["lib/libfuse.dylib"],
    includes = ["include"],
    #include_prefix = "libfuse",
    visibility = ["//visibility:public"],
)
""",
    path = "/usr/local",
)
