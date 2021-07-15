# INFR11011 Makefile
#
# Makefile for INFR11011 Minibase project.  Needs GNU make.

BASE_DIR = .
BIN_DIR = $(BASE_DIR)/bin
LIB_DIR = $(BASE_DIR)/lib
SRC_DIR = $(BASE_DIR)/src

MAIN = $(BIN_DIR)/btree

CC = g++
CFLAGS = -Wall -Wno-unused-variable -std=c++11 -pedantic -g
INCLUDES = -I$(BASE_DIR)/include
LFLAGS = -L$(BASE_DIR)/lib -lbufmgr -lspacemgr -lglobaldefs

.PHONY: all libs globaldefs spacemgr bufmgr clean

all: libs $(MAIN)

$(MAIN): $(wildcard $(SRC_DIR)/*.cpp)
	@test -d $(dir $@) || mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INCLUDES) $^ -o $@ $(LFLAGS)

libs: globaldefs spacemgr bufmgr

globaldefs: $(LIB_DIR)/libglobaldefs.a

spacemgr: $(LIB_DIR)/libspacemgr.a

bufmgr: $(LIB_DIR)/libbufmgr.a

clean: 
	rm -fr $(BIN_DIR)