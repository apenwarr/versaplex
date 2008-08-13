
default: all

all: all.t.exe

test: all.t.pass

include wvtest.mk

all.t.exe: basics.t.cs.E wvtest.cs NDesk.DBus.dll
	gmcs -out:$@ /r:NDesk.DBus.dll $(filter %.cs %.cs.E,$^)


NDesk.DBus.dll:
	ln -s ../NDesk.DBus.dll .
