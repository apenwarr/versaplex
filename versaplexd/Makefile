include ../rules.mk
include ../monorules.mk

all: versaplexd

PKGS += /unsafe /r:System.Data

test: all t/test
	
tests: all t/all

versaplexd.exe: versaplexd.cs \
    dodgytransport.cs PriorityQueue.cs schemamatic.cs vxbufferstream.cs \
    vxapi.cs vxcolumninfo.cs vxdbus.cs vxdbusdatetime.cs \
    vxevent.cs vxeventloop.cs vxnotifysocket.cs \
    vxschema.cs vxschemachecksums.cs vxsocket.cs \
    vxsqlpool.cs wvdbusserver.cs \
    ../dbus-sharp/src/NDesk.DBus.dll \
    ../wvdotnet/wv.dll

nativecall.exe: nativecall.cs wvdbusserver.cs ../wvdotnet/wv.dll

nativelib.so: nativelib.c
	gcc -shared -Wall -o $@ $^

clean:: t/clean
	rm -f versaplexd
