WVDOTNET=wvdotnet
include rules.mk
include config.mk

ifndef BUILD_TARGET 
$(error Please run the "configure" or "configure-mingw32" script)
endif

ifeq ($(BUILD_TARGET),win32)
WVSTREAMS_MAKEFILE=Makefile-win32
else
WVSTREAMS_MAKEFILE=Makefile
endif

wvdotnet dbus-sharp versaplexd wvstreams vxodbc: FORCE

dbus-sharp: dbus-sharp/Makefile

nall: versaplexd

all: nall vxodbc

# Note: $(MAKE) -C wv doesn't work, as wv's Makefile needs an accurate $(PWD)
wvstreams:
	cd wv && $(MAKE) wvstreams

vxodbc: wvstreams

versaplexd: wvdotnet dbus-sharp

ntests: nall wvdotnet/tests versaplexd/tests

ntest: nall wvdotnet/test versaplexd/test

tests: nall ntests vxodbc/tests

test: all ntest vxodbc/test
	
nclean: versaplexd/clean wvdotnet/clean dbus-sharp/clean

clean: nclean
	$(MAKE) -C vxodbc -fMakefile-common clean
	
portclean: clean
	$(MAKE) -C wv/wvstreams -f$(WVSTREAMS_MAKEFILE) clean
	$(MAKE) -C wv/wvports clean
	
distclean: clean
	rm -f config.mk
