cc_library(
    name = "fs",
    srcs = ["fs.cc"],
    hdrs = ["fs.h"],
    deps = [
        ":block",
        ":macros",
    ],
)

cc_library(
    name = "block",
    srcs = ["block.cc"],
    hdrs = ["block.h"],
    deps = ["@glog"],
)

cc_test(
    name = "block_test",
    srcs = [":block_test.cc"],
    deps = [
        ":block",
        "@googletest//:gtest_main",
    ],
)

cc_binary(
    name = "main",
    srcs = ["main.cc"],
    deps = [
        ":block",
        ":fs",
    ],
)

cc_library(
    name = "macros",
    hdrs = ["macros.h"],
    deps = ["@glog"],
)
