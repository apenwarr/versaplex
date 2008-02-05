include ../rules.mk
include ../monorules.mk

all: versaplexd

PKGS += /r:System.Data

test: all t/test
	$(MAKE) -C t $@
	
tests: all t/all

versaplexd.exe: versaplexd.cs \
    PriorityQueue.cs vxbufferstream.cs \
    vxapi.cs vxdbus.cs vxevent.cs \
    vxeventloop.cs vxnotifysocket.cs \
    vxsocket.cs \
    vxsqlpool.cs \
    ../dbus-sharp/src/NDesk.DBus.dll \
    ../wvdotnet/wv.dll

clean:: t/clean
	rm -f versaplexd
