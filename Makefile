WVDOTNET=wvdotnet
include rules.mk

wvdotnet wvdbus-sharp: FORCE

wvdbus-sharp: wvdbus-sharp/Makefile

nall: wvdotnet wvdbus-sharp

all: nall

ntests: nall wvdotnet/tests wvdbus-sharp/tests

ntest: nall wvdotnet/test wvdbus-sharp/test

tests: nall ntests

test: all ntest
	
nclean: wvdotnet/clean wvdbus-sharp/clean

clean: nclean
	
