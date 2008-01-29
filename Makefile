include config.mk

ifndef BUILD_TARGET 
$(error Please run the "configure" or "configure-mingw32" script)
endif

ifeq ($(BUILD_TARGET),win32)
WVSTREAMS_MAKEFILE=Makefile-win32
else
WVSTREAMS_MAKEFILE=Makefile
endif

.PHONY: dbus-sharp wvstreams wvdotnet versaplexd vxodbc

all: versaplexd vxodbc

%: %/Makefile
	$(MAKE) -C $@ all

# Note: $(MAKE) -C wv doesn't work, as wv's Makefile needs an accurate $(PWD)
wvstreams:
	cd wv && $(MAKE) wvstreams

vxodbc: wvstreams

versaplexd: wvdotnet dbus-sharp

dbus-sharp wvdotnet versaplexd vxodbc:
	$(MAKE) -C $@ all

test: all
	$(MAKE) -C wvdotnet $@
	$(MAKE) -C versaplexd $@
	$(MAKE) -C vxodbc $@
	
nclean:
	for d in versaplexd wvdotnet dbus-sharp; do \
		$(MAKE) -C $$d clean; \
	done

clean: nclean
	$(MAKE) -C vxodbc -fMakefile-common clean
	
portclean: clean
	$(MAKE) -C wv/wvstreams -f$(WVSTREAMS_MAKEFILE) clean
	$(MAKE) -C wv/wvports clean
	
distclean: clean
	rm -f config.mk
