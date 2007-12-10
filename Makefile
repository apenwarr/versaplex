.PHONY: all build clean test

include config.mk

ifndef BUILD_TARGET 
$(error Please run the "configure" or "configure-mingw32" script)
endif

ifeq ($(BUILD_TARGET),win32)
WVSTREAMS_MAKEFILE=Makefile-win32
VXODBC_MAKEFILE=Makefile-win32
else
WVSTREAMS_MAKEFILE=Makefile
VXODBC_MAKEFILE=Makefile-linux
endif

.PHONY: dbus-sharp wvstreams wvdotnet versaplexd vxodbc

all: dbus-sharp wvstreams wvdotnet versaplexd vxodbc

dbus-sharp wvdotnet versaplexd:
	$(MAKE) -C $@ all

# Note: $(MAKE) -C wv doesn't work, as wv's Makefile needs an accurate $(PWD)
wvstreams:
	cd wv && $(MAKE) wvstreams

vxodbc: 
	$(MAKE) -C vxodbc -f$(VXODBC_MAKEFILE)

test: all
	$(MAKE) -C wvdotnet $@
	$(MAKE) -C versaplexd $@
	$(MAKE) -C vxodbc $@
	
clean:
	$(MAKE) -C vxodbc -fMakefile-common $@
	$(MAKE) -C versaplexd $@
	$(MAKE) -C wvdotnet $@
	$(MAKE) -C wv/wvstreams -f$(WVSTREAMS_MAKEFILE) $@
	$(MAKE) -C wv/wvports $@
	
distclean: clean
	rm config.mk
