WVDOTNET=../../wvdotnet
include ../../monorules.mk

PKGS += /r:System.Data

WVDIR=../../wv
WVLIBDIR=$(WVDIR)/lib
WVSTREAMS=$(WVDIR)/wvstreams
XPLC=$(WVDIR)/wvports/xplc/build/xplc
#BOOST=$(WVDIR)/wvports/boost/build/boost
VALGRIND=valgrind --tool=memcheck --leak-check=yes --num-callers=10 \
    --suppressions=$(WVSTREAMS)/wvstreams.supp --log-file=valgrind.log
RUNTEST:=$(WVSTREAMS)/wvtestrunner.pl

CPPFLAGS += -I../../wvdotnet
CFLAGS+=-Wall
CFLAGS+=-g
CFLAGS+=-I$(WVSTREAMS)/include -I$(XPLC)/include 
CXXFLAGS=$(CFLAGS)

ifeq ($(BUILD_TARGET),win32)
CC=i586-mingw32msvc-g++
CXX=i586-mingw32msvc-g++
CFLAGS+= -D_MSC_VER=1300 -D_WIN32_WINNT=0x0501 -DWIN_MULTITHREAD_SUPPORT
LIBS+=-L. -L.. -L$(WVSTREAMS) -L$(WVLIBDIR)
LIBS+=-lwvwin32 -lssl -lcrypto -lxplc-cxx
LIBS+=$(DBUS)/dbus/.libs/libdbus-1.a $(WVSTREAMS)/gnulib/libgnu.a
LIBS+=$(WVSTREAMS)/wvtestmain.o $(WVSTREAMS)/utils/wvcrash.o
LIBS+=-lwsock32 -lgdi32 -lrpcrt4 -lole32 -lodbc32 -limagehlp
VALGRIND=
RUNTEST:=$(RUNTEST) wine
else
CC=gcc
CXX=g++
LIBS+=$(WVSTREAMS)/libwvtest.a 
export LD_LIBRARY_PATH=..:$(WVLIBDIR):$(WVSTREAMS)
LIBS+=-L$(WVSTREAMS) -lwvdbus 
endif

VX_SOURCES = ../dodgytransport.cs ../vxnotifysocket.cs ../vxsocket.cs \
	../vxcolumninfo.cs ../vxdbusdatetime.cs ../vxdbusutils.cs \
	../vxexceptions.cs versaplextester.cs.E

SCHEMAMATIC_SOURCES = ../vxschema.cs ../vxschemachecksums.cs \
	../vxschemadiff.cs ../vxschemaerrors.cs ../ischemabackend.cs \
	../vxdiskschema.cs ../vxdbusschema.cs ../vxdbschema.cs \
	../vxexceptions.cs schemamatictester.cs.E

TESTSOURCES=$(wildcard *.t.cc)
TESTHEADERS=$(wildcard *.h)
TESTOBJS=$(patsubst %.cc,%.o,$(TESTSOURCES))

TESTS=dbisanity.t.pass \
    versaplex.t.pass vxsanity.t.pass \
    verifydata.t.pass schemamatic.t.pass putschema.t.pass \
    vxexecchunkrecordset.t.pass vxschemachecksums.t.pass vxdiskschema.t.pass \
    vxschema.t.pass
TESTEXES=$(patsubst %.pass,%.exe,$(TESTS))

default: test

all: tests

tests: $(TESTEXES)

test: $(TESTS) all.t
	$(RUNTEST) $(VALGRIND) ./all.t $(TESTNAME)

versaplex.t.exe: versaplex.t.cs.E \
	$(VX_SOURCES) \
	../wv.dll ../NDesk.DBus.dll

vxsanity.t.exe: vxsanity.t.cs.E \
	$(VX_SOURCES) \
	../wv.dll ../NDesk.DBus.dll

dbisanity.t.exe: dbisanity.t.cs.E \
	$(VX_SOURCES) \
	../wv.dll ../NDesk.DBus.dll

verifydata.t.exe: verifydata.t.cs.E \
	$(VX_SOURCES) \
	../wv.dll ../NDesk.DBus.dll

schemamatic.t.exe: schemamatic.t.cs.E \
	$(VX_SOURCES) $(SCHEMAMATIC_SOURCES) \
	../wv.dll ../NDesk.DBus.dll

putschema.t.exe: putschema.t.cs.E \
	$(VX_SOURCES) $(SCHEMAMATIC_SOURCES) \
	../wv.dll ../NDesk.DBus.dll

vxexecchunkrecordset.t.exe: vxexecchunkrecordset.t.cs.E \
	$(VX_SOURCES) \
	../wv.dll ../NDesk.DBus.dll

vxschemachecksums.t.exe: vxschemachecksums.t.cs.E \
	../vxschemachecksums.cs \
	../wv.dll ../NDesk.DBus.dll

# This test file doesn't need the whole Versaplex/Schemamatic stack, 
# so let's try to save some needless recompilations.
vxschema.t.exe: vxschema.t.cs.E \
	../vxschema.cs ../vxschemaerrors.cs ../vxschemachecksums.cs \
	../vxschemadiff.cs ../ischemabackend.cs ../vxexceptions.cs \
	../wv.dll ../NDesk.DBus.dll

vxdiskschema.t.exe: vxdiskschema.t.cs.E \
	$(VX_SOURCES) $(SCHEMAMATIC_SOURCES) \
	../wv.dll ../NDesk.DBus.dll

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c -o $@ $< 

all.t: $(TESTOBJS)
	$(CXX) -o $@ $^ $(LIBS)

clean::
	rm -f *.pass *.t.o all.t
