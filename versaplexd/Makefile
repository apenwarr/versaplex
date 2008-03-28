include ../rules.mk
include ../monorules.mk

all: versaplexd

PKGS += /unsafe /r:System.Data

test: all t/test
	$(MAKE) -C t $@
	
tests: all t/all

versaplexd.exe: versaplexd.cs \
    PriorityQueue.cs vxbufferstream.cs \
    vxapi.cs vxdbus.cs vxevent.cs \
    vxeventloop.cs vxnotifysocket.cs \
    vxsocket.cs \
    vxsqlpool.cs wvdbusserver.cs \
    ../dbus-sharp/src/NDesk.DBus.dll \
    ../wvdotnet/wv.dll

nativecall.exe: nativecall.cs wvdbusserver.cs ../wvdotnet/wv.dll

nativelib.so: nativelib.c
	gcc -shared -Wall -o $@ $^

clean:: t/clean
	rm -f versaplexd
