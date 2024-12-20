Implements an [ext2](https://wiki.osdev.org/Ext2)-like user space filesystem with [FUSE](https://docs.kernel.org/filesystems/fuse.html).

```bazel run fuse -- <disk> <mountpt>```

mounts this filesystem at `<mountpt>` with `<disk>` as the backing storage (use absolute paths). If you are mounting `<disk>` with this filesystem, add the `-mkfs` flag to initialize all data structures on disk (inodes, block groups, etc.)

```bazel run fuse -- -mkfs <disk> <mountpt>```

and `-f` to run the application in the foreground (typically for debugging purposes):

```bazel run fuse -- -f <disk> <mountpt>```
