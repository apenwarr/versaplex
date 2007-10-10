.PHONY: server vxodbc clean test

ifeq ($(OS),Windows_NT)
VXODBC_MAKEFILE=Makefile-win32
else
VXODBC_MAKEFILE=Makefile-linux
endif

all: server vxodbc

server:
	$(MAKE) -C server

vxodbc: vxodbc/.stamp-config
	$(MAKE) -C vxodbc -f $(VXODBC_MAKEFILE)

vxodbc/.stamp-config: vxodbc/configure
	cd vxodbc && ./configure
	touch $@

vxodbc/configure:
	cd vxodbc && autoconf

test:
	$(MAKE) -C tests

clean: 
	$(MAKE) -C server $@
	$(MAKE) -C vxodbc $@ -f Makefile-common
