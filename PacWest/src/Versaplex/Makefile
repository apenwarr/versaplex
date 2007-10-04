.PHONY: server vxodbc

all: server vxodbc

server:
	$(MAKE) -C server

vxodbc: vxodbc/configure 
	$(MAKE) -C vxodbc -f Makefile-linux

vxodbc/configure:
	cd vxodbc && autoconf
