# Note: $(MAKE) -C wv doesn't work, as wv's Makefile needs an accurate $(PWD)
all: wv/config.mk
	$(MAKE) -C dbus-sharp
	cd wv && make wvstreams
	#$(MAKE) -C nunit

wv/config.mk:
	cd wv && ./configure --disable-rtti
