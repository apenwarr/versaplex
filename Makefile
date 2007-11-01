.PHONY: all build clean test

include config.mk

ifndef BUILD_TARGET 
$(error Please run the "configure" or "configure-mingw32" script)
endif

WVSTREAMS_MAKEFILE=
ifeq ($(BUILD_TARGET),win32)
WVSTREAMS_MAKEFILE=Makefile-win32
VXODBC_MAKEFILE=Makefile-win32
else
WVSTREAMS_MAKEFILE=Makefile
VXODBC_MAKEFILE=Makefile-linux
endif

.PHONY: dbus-sharp wvstreams wvdotnet sqlsucker versaplexd vxodbc

all: dbus-sharp wvstreams wvdotnet sqlsucker versaplexd vxodbc

dbus-sharp wvdotnet sqlsucker versaplexd:
	$(MAKE) -C $@ all

# Note: $(MAKE) -C wv doesn't work, as wv's Makefile needs an accurate $(PWD)
wvstreams:
	cd wv && $(MAKE) wvstreams

vxodbc: vxodbc/.stamp-config
	$(MAKE) -C vxodbc -f$(VXODBC_MAKEFILE)

vxodbc/.stamp-config: vxodbc/configure
	cd vxodbc && ./configure
	touch $@

vxodbc/configure:
	cd vxodbc && autoconf

test:
	$(MAKE) -C wvdotnet $@
	$(MAKE) -C sqlsucker $@
	$(MAKE) -C versaplexd $@
	$(MAKE) -C vxodbc $@
	
clean:
	$(MAKE) -C src $@
	$(MAKE) -C wv/wvstreams -f$(WVSTREAMS_MAKEFILE) $@
	$(MAKE) -C wv/wvports $@
	$(MAKE) -C wvdotnet $@
	$(MAKE) -C sqlsucker $@
	$(MAKE) -C versaplexd $@
	$(MAKE) -C vxodbc -fMakefile-common $@
	rm config.mk
