include ../../config.mk

CFLAGS=-DHAVE_CONFIG_H -I. -I. -I../../../include -I../../../include   
CFLAGS+=-D_FREETDS_LIBRARY_SOURCE -DUNIXODBC -D_REENTRANT -D_THREAD_SAFE 
CFLAGS+=-DDEBUG=1 

WVDIR=../../wv
WVLIBDIR=$(WVDIR)/lib
BOOST=$(WVDIR)/wvports/boost/build/boost
DBUS=$(WVDIR)/wvports/dbus/build/dbus
ARGP=$(WVDIR)/wvports/argp/build/argp
WVSTREAMS=$(WVDIR)/wvstreams
VALGRIND=valgrind --tool=memcheck --leak-check=yes --num-callers=10 --suppressions=$(WVSTREAMS)/wvstreams.supp --log-file=valgrind.log
RUNTEST=$(WVSTREAMS)/wvtestrunner.pl

CFLAGS+=-Wall -Wno-long-long  
CFLAGS+=-g 
CFLAGS+=-I$(WVSTREAMS)/include -I$(BOOST)
CFLAGS+=-DODBCVER=0x0351
CXXFLAGS=$(CFLAGS)

ifeq ($(BUILD_TARGET),win32)
CC=i586-mingw32msvc-g++
CXX=i586-mingw32msvc-g++
CFLAGS+= -D_MSC_VER=1300 -D_WIN32_WINNT=0x0501 -DWIN_MULTITHREAD_SUPPORT
LIBS+=-L. -L.. -L$(WVSTREAMS) -L$(WVLIBDIR)
LIBS+=-lwvstatic -lssl -lcrypto
LIBS+=../vxodbc.a $(DBUS)/dbus/.libs/libdbus-1.a
LIBS+=$(WVSTREAMS)/wvtestmain.o $(WVSTREAMS)/utils/wvcrash.o 
LIBS+=-lwsock32 -lgdi32 -lrpcrt4 -lole32 -lodbc32 -limagehlp -lcrypt32
VALGRIND=
RUNTEST=$(RUNTEST) wine
else
CC=gcc
CXX=g++
LIBS+=../vxodbc-test.so $(WVSTREAMS)/libwvtest.a /usr/lib/libodbc.a 
export LD_LIBRARY_PATH=..:$(WVLIBDIR):$(WVSTREAMS)
LIBS+=-L$(WVSTREAMS) -lwvdbus /usr/lib/libltdl.so -ldl -lpthread 
endif

TESTSOURCES=$(wildcard *.t.cc)
TESTHEADERS=$(wildcard *.h)
TESTOBJS=$(patsubst %.cc,%.o,$(TESTSOURCES))
HELPEROBJS=\
    common.o \
    column.o \
    vxodbctester.o 

all: all.t

tests: all.t

test: tests
	$(RUNTEST) $(VALGRIND) ./all.t $(TESTNAME)

# FIXME: Should be using GCC-generated dependencies here
all.t: $(HELPEROBJS) $(TESTOBJS)
	$(CXX) -o $@ $^ $(LIBS)

# FIXME: Should be using GCC-generated dependencies here
%.o: %.cc $(TESTHEADERS)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean: 
	rm -f $(HELPEROBJS) $(TESTOBJS) all.t valgrind.log.*
