#!/bin/bash

if mount | grep -q "cs270"; then
  echo "was mounted. unmounting..."
  fusermount -u /cs270
fi

g++ -std=gnu++2a -Wall -Wpedantic -Werror hello.cc `pkg-config fuse3 --cflags --libs` -o hello

./hello -d --recipient="csirlin@ucsb.edu" /cs270 -s 2> log &
