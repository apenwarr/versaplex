.PHONY: all build clean test

include config.mk

ifndef BUILD_TARGET 
$(error Please run the "configure" or "configure-mingw32" script)
endif

all: build

build:
	$(MAKE) -C ThirdParty
	$(MAKE) -C src

clean:
	$(MAKE) -C src $@
	$(MAKE) -C ThirdParty $@
	rm config.mk

test:
	$(MAKE) -C src $@
