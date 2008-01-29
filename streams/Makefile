include ../../monorules.mk

all: streamtest.exe

streamtest.exe: streamtest.cs wveventer.cs wvbuf.cs wvstream.cs wvlog.cs \
		wvextensions.cs

test: all
	./streamtest.exe
