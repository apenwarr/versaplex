
include ../monorules.mk

all: versaplexd

versaplexd.exe: versaplexd.cs \
    PriorityQueue.cs VxBufferStream.cs \
    VxDbApi.cs VxDbus.cs VxEvent.cs \
    VxEventLoop.cs VxNotifySocket.cs \
    VxSocket.cs \
    VxSqlPool.cs \
    ../dbus-sharp/src/NDesk.DBus.dll ../wvdotnet/wv.dll

clean::
	rm -f versaplexd