include ../rules.mk
include ../monorules.mk

all: versaplexd

PKGS += /unsafe /r:System.Data

test: all t/test
	
tests: all t/all

versaplexd.exe: versaplexd.cs \
    dodgytransport.cs ischemabackend.cs PriorityQueue.cs schemamatic.cs \
    vxapi.cs vxbufferstream.cs \
    vxcolumninfo.cs vxdbus.cs vxdbusdatetime.cs vxdbschema.cs \
    vxevent.cs vxeventloop.cs vxexceptions.cs vxnotifysocket.cs \
    vxschema.cs vxschemachecksums.cs vxschemaerrors.cs vxsocket.cs \
    vxsqlpool.cs wvdbusserver.cs \
    ../dbus-sharp/src/NDesk.DBus.dll \
    ../wvdotnet/wv.dll

nativecall.exe: nativecall.cs wvdbusserver.cs ../wvdotnet/wv.dll

nativelib.so: nativelib.c
	gcc -shared -Wall -o $@ $^

clean:: t/clean
	rm -f versaplexd
