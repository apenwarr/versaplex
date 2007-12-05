include ../../config.mk

CFLAGS=-DHAVE_CONFIG_H -I. -I. -I../../../include -I../../../include   
CFLAGS+=-D_FREETDS_LIBRARY_SOURCE -DUNIXODBC -D_REENTRANT -D_THREAD_SAFE 
CFLAGS+=-DDEBUG=1 

WVDIR=../../wv
WVLIBDIR=$(WVDIR)/lib
XPLC=$(WVDIR)/wvports/xplc/build/xplc
BOOST=$(WVDIR)/wvports/boost/build/boost
DBUS=$(WVDIR)/wvports/dbus/build/dbus
WVSTREAMS=$(WVDIR)/wvstreams
VALGRIND=valgrind --tool=memcheck --leak-check=yes --num-callers=10 --suppressions=$(WVSTREAMS)/wvstreams.supp --log-file=valgrind.log
RUNTEST=$(WVSTREAMS)/wvtesthelper

CFLAGS+=-Wall -Wno-long-long  
CFLAGS+=-g 
CFLAGS+=-I$(WVSTREAMS)/include -I$(XPLC)/include -I$(BOOST)
CFLAGS+=-DODBCVER=0x0351
CXXFLAGS=$(CFLAGS)

ifeq ($(BUILD_TARGET),win32)
CC=i586-mingw32msvc-g++
CXX=i586-mingw32msvc-g++
CFLAGS+= -D_MSC_VER=1300 -D_WIN32_WINNT=0x0501 -DWIN_MULTITHREAD_SUPPORT
LIBS+=../vxodbc.a $(WVSTREAMS)/libwvwin32.a $(DBUS)/dbus/.libs/libdbus-1.a 
LIBS+=$(WVLIBDIR)/libssl.a $(WVLIBDIR)/libcrypto.a $(WVLIBDIR)/libxplc-cxx.a
LIBS+=$(WVSTREAMS)/gnulib/libgnu.a
LIBS+=-lwsock32 -lodbc32 -lws2_32 -lgdi32 -lrpcrt4 -lole32 
else
CC=gcc
CXX=g++
LIBS+=../vxodbc-test.so $(WVSTREAMS)/libwvtest.a /usr/lib/libodbc.a
LIBS+=-L$(WVSTREAMS) -lwvdbus /usr/lib/libltdl.so -ldl -lpthread 
endif

TESTSOURCES=$(wildcard *.t.cc)
TESTHEADERS=$(wildcard *.h)
TESTOBJS=$(patsubst %.cc,%.o,$(TESTSOURCES))

all: all.t

test: all.t
	$(RUNTEST) $(VALGRIND) ./all.t $(TESTNAME)

# FIXME: Should be using GCC-generated dependencies here
all.t: $(TESTOBJS) common.o vxodbctester.o column.o 
	$(CXX) -o $@ $^ $(LIBS)

# FIXME: Should be using GCC-generated dependencies here
%.o: %.cc $(TESTHEADERS)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean: 
	rm -f $(TESTOBJS) all.t valgrind.log.*
