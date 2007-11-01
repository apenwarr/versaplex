.PHONY: all build clean test

include config.mk

ifndef BUILD_TARGET 
$(error Please run the "configure" or "configure-mingw32" script)
endif

WVSTREAMS_MAKEFILE=
ifeq ($(BUILD_TARGET),win32)
WVSTREAMS_MAKEFILE=-f Makefile-win32
endif

all: build

# Note: $(MAKE) -C wv doesn't work, as wv's Makefile needs an accurate $(PWD)
build:
	$(MAKE) -C dbus-sharp
	cd wv && $(MAKE) wvstreams
	$(MAKE) -C src

clean:
	$(MAKE) -C src $@
	$(MAKE) -C wv/wvstreams $(WVSTREAMS_MAKEFILE) $@
	$(MAKE) -C wv/wvports $@
	rm config.mk

test:
	$(MAKE) -C src $@

