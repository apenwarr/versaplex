include ../../monorules.mk

all: streamtest.exe

wvstreams.dll: assemblyinfo.cs \
		wveventer.cs wvbuf.cs wvstream.cs wvlog.cs \
		wvextensions.cs

streamtest.exe: streamtest.cs wvstreams.dll

test: all
	./streamtest.exe
