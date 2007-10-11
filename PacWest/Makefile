include config.mk

ifndef BUILD_TARGET 
$(error Please run the "configure" or "configure-mingw32" script)
endif

all:
	$(MAKE) -C ThirdParty
	$(MAKE) -C src
