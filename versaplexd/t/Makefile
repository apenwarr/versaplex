include ../../monorules.mk

PKGS += /r:System.Data

WVDIR=../../wv
WVLIBDIR=$(WVDIR)/lib
WVSTREAMS=$(WVDIR)/wvstreams
XPLC=$(WVDIR)/wvports/xplc/build/xplc
#BOOST=$(WVDIR)/wvports/boost/build/boost
VALGRIND=valgrind --tool=memcheck --leak-check=yes --num-callers=10 \
    --suppressions=$(WVSTREAMS)/wvstreams.supp --log-file=valgrind.log
RUNTEST=$(WVSTREAMS)/wvtesthelper

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
RUNTEST=wine
else
CC=gcc
CXX=g++
LIBS+=$(WVSTREAMS)/libwvtest.a 
export LD_LIBRARY_PATH=..:$(WVLIBDIR):$(WVSTREAMS)
LIBS+=-L$(WVSTREAMS) -lwvdbus 
endif

TESTSOURCES=$(wildcard *.t.cc)
TESTHEADERS=$(wildcard *.h)
TESTOBJS=$(patsubst %.cc,%.o,$(TESTSOURCES))

TESTS=versaplex.t.pass vxsanity.t.pass verifydata.t.pass schemamatic.t.pass
TESTEXES=$(patsubst %.pass,%.exe,$(TESTS))

default: test

all: tests

tests: $(TESTEXES)

test: $(TESTS) all.t
	$(RUNTEST) $(VALGRIND) ./all.t $(TESTNAME)

versaplex.t.exe: versaplex.t.cs.E versaplextester.cs.E dbuserror.cs \
	../dodgytransport.cs ../vxnotifysocket.cs ../vxsocket.cs \
	../vxcolumninfo.cs ../vxdbusdatetime.cs ../vxdbusutils.cs \
	../wv.dll ../NDesk.DBus.dll

vxsanity.t.exe: vxsanity.t.cs.E versaplextester.cs.E dbuserror.cs \
	../dodgytransport.cs ../vxnotifysocket.cs ../vxsocket.cs \
	../vxcolumninfo.cs ../vxdbusdatetime.cs ../vxdbusutils.cs \
	../wv.dll ../NDesk.DBus.dll

verifydata.t.exe: verifydata.t.cs.E versaplextester.cs.E dbuserror.cs \
	../dodgytransport.cs ../vxnotifysocket.cs ../vxsocket.cs \
	../vxcolumninfo.cs ../vxdbusdatetime.cs ../vxdbusutils.cs \
	../wv.dll ../NDesk.DBus.dll

schemamatic.t.exe: schemamatic.t.cs.E versaplextester.cs.E dbuserror.cs \
	../dodgytransport.cs ../vxnotifysocket.cs ../vxsocket.cs \
	../vxcolumninfo.cs ../vxdbusdatetime.cs ../vxschemachecksums.cs \
	../vxschema.cs ../vxschemachecksums.cs ../vxschemaerrors.cs \
	../ischemabackend.cs ../vxdiskschema.cs ../vxdbusschema.cs \
	../vxdbschema.cs ../vxdbusutils.cs ../vxexceptions.cs \
	../wv.dll ../NDesk.DBus.dll

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c -o $@ $< 

all.t: $(TESTOBJS)
	$(CXX) -o $@ $^ $(LIBS)

clean::
	rm -f *.pass *.t.o all.t
