
include ../config.mk

ifndef BUILD_TARGET 
$(error Please run the "configure" or "configure-mingw32" script in ..)
endif

ifeq ($(BUILD_TARGET),win32)
include Makefile-win32
else
include Makefile-linux
endif
