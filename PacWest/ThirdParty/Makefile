include ../config.mk

WVSTREAMS_MAKEFILE=
ifeq ($(BUILD_TARGET),win32)
WVSTREAMS_MAKEFILE=-f Makefile-win32
endif

# Note: $(MAKE) -C wv doesn't work, as wv's Makefile needs an accurate $(PWD)
all: 
	$(MAKE) -C dbus-sharp
	cd wv && $(MAKE) wvstreams
	#$(MAKE) -C nunit

clean:
	$(MAKE) -C wv/wvstreams clean $(WVSTREAMS_MAKEFILE)
	$(MAKE) -C wv/wvports clean
