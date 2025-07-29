# Host system detection
HOST_OS := $(shell uname -s)
TARGET_OS ?= native
# Cross-compilation settings for Windows target
MINGW_PREFIX = x86_64-w64-mingw32-
WINE ?= wine
# Compiler and flags setup
ifeq ($(TARGET_OS),windows)
	CC = $(MINGW_PREFIX)gcc
	TARGET = rlite.exe
	PLATFORM_FLAGS = -D_WIN32 -D_WIN -DWIN32_LEAN_AND_MEAN -D_FILE_OFFSET_BITS=64
	RM = rm -f
	# Windows specific flags - order is important!
	CFLAGS += -pthread $(PLATFORM_FLAGS)
	LDFLAGS = -static
	LIBS = -lmingw32 -lpthread -lwinpthread -static-libgcc -static-libstdc++ -lmsvcrt
else ifeq ($(TARGET_OS),freebsd)
	CC = clang # It's common to use clang on FreeBSD
	TARGET = rlite.bin
	PLATFORM_FLAGS = -D_UNIX -D__FreeBSD__ -D_FILE_OFFSET_BITS=64 # Define FreeBSD specific macro
	RM = rm -f
	CFLAGS += -pthread $(PLATFORM_FLAGS)
	LIBS = -pthread
else # Default to a Unix-like system (including your initial native)
	CC = gcc
	TARGET = rlite.bin
	PLATFORM_FLAGS = -D_UNIX -D_FILE_OFFSET_BITS=64
	RM = rm -f
	CFLAGS += -pthread $(PLATFORM_FLAGS)
	LIBS = -pthread
endif
# Common compiler flags
CFLAGS += -O3 -s
# Source files
SRC = main.c \
	advFile/advFile.c \
	advFile/fhandle.c \
	threading/yarn.c \
	qsort_mt.c \
	roaring.c \
	msort.c
# Header files
HEADERS = xxhash.h
# Object files
OBJ = $(SRC:.c=.o)
# Default target
all: platform_info $(TARGET)
# Display platform information
platform_info:
	@echo "Host OS: $(HOST_OS)"
	@echo "Target OS: $(TARGET_OS)"
	@echo "Using compiler: $(CC)"
	@echo "Using flags: $(CFLAGS)"
	@echo "Using libs: $(LIBS)"
# Linking - note the order here is important
$(TARGET): $(OBJ)
	$(CC) $(LDFLAGS) -o $@ $(OBJ) $(LIBS)
# Compiling
%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@
# Clean build files
clean:
	$(RM) $(TARGET) $(OBJ)
# Clean and rebuild
rebuild: clean all
# Cross-compile for Windows
windows:
	$(MAKE) TARGET_OS=windows
# Test Windows binary using Wine
test-windows: windows
	$(WINE) ./$(TARGET)
# Build for FreeBSD
freebsd:
	$(MAKE) TARGET_OS=freebsd
# Phony targets
.PHONY: all clean rebuild platform_info windows test-windows freebsd