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

wvdotnet wvdbus-sharp versaplexd wvstreams vxodbc: FORCE

wvdbus-sharp: wvdbus-sharp/Makefile

nall: wvdotnet wvdbus-sharp versaplexd

all: nall vxodbc

# Note: $(MAKE) -C wv doesn't work, as wv's Makefile needs an accurate $(PWD)

# We tell the autobuilder to ignore all warnings produced in the 'wv'
# directory, since that project isn't really this project and it should
# have its own autobuilder.
wvstreams:
	@echo --START-IGNORE-WARNINGS
	cd wv && $(MAKE) wvstreams
	@echo --STOP-IGNORE-WARNINGS

vxodbc: wvstreams

versaplexd: wvdotnet wvdbus-sharp

ntests: nall wvdotnet/tests wvdbus-sharp/tests versaplexd/tests

ntest: nall wvdotnet/test wvdbus-sharp/test versaplexd/test

tests: nall ntests vxodbc/tests

test: all ntest vxodbc/test
	
nclean: versaplexd/clean wvdotnet/clean wvdbus-sharp/clean

clean: nclean
	$(MAKE) -C vxodbc -fMakefile-common clean
	
portclean: clean
	$(MAKE) -C wv/wvstreams -f$(WVSTREAMS_MAKEFILE) clean
	$(MAKE) -C wv/wvports clean
	
distclean: clean
	rm -f config.mk

dist:
	rm -rf versaplex-dist
	mkdir versaplex-dist
	cp versaplexd/*.exe versaplexd/*.dll versaplex-dist/
	cp versaplexd/README*.txt versaplex-dist/
	todos versaplex-dist/*.txt
	cp vxodbc/*.dll versaplex-dist/
	git rev-parse HEAD >versaplex-dist/version
	cp versaplexd/versaplexd.ini.tmpl versaplex-dist/versaplexd.ini
