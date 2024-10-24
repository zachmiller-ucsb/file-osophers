.PHONY: clean

CC_FLAGS=-Wall -Wpedantic -Werror -std=c++20 -O3

clean:
	rm main block.o block_test

block.o: block.h block.cc
	g++ $(CC_FLAGS) block.cc -c

main: main.cc
	g++ $(CC_FLAGS) block.o fs.cc main.cc -o main

block_test: block_test.cc block.o
	g++ $(CC_FLAGS) block.o block_test.cc -lgtest -lgtest_main -lglog -o block_test

all: main