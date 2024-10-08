#!/bin/bash

if mount | grep -q "mountpt"; then
  echo "was mounted. unmounting..."
  fusermount -u ./mountpt
fi

g++ -Wall hello.cc `pkg-config fuse3 --cflags --libs` -o hello

./hello -d --recipient="zmiller@ucsb.edu" ./mountpt 2> log &
