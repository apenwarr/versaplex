# Note: $(MAKE) -C wv doesn't work, as wv's Makefile needs an accurate $(PWD)
all: 
	$(MAKE) -C dbus-sharp
	cd wv && make wvstreams
	#$(MAKE) -C nunit
