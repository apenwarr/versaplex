CFLAGS=-DHAVE_CONFIG_H -I. -I. -I../../../include -I../../../include   
CFLAGS+=-D_FREETDS_LIBRARY_SOURCE -DUNIXODBC -D_REENTRANT -D_THREAD_SAFE 
CFLAGS+=-DDEBUG=1 

WVDIR=../../wv
XPLC=$(WVDIR)/wvports/xplc/build/xplc
WVSTREAMS=$(WVDIR)/wvstreams
VALGRIND=valgrind --tool=memcheck --leak-check=yes --num-callers=10 --suppressions=$(WVSTREAMS)/wvstreams.supp --log-file=valgrind.log
RUNTEST=$(WVSTREAMS)/wvtesthelper

CFLAGS+=-Wall -Wno-long-long  
CFLAGS+=-g -O2 
CFLAGS+=-I$(WVSTREAMS)/include -I$(XPLC)/include
CXXFLAGS=$(CFLAGS)
LIBS=../vxodbc-test.so /usr/lib/libltdl.so -ldl -lpthread /usr/lib/libodbc.a
LIBS+=-L$(WVSTREAMS) -lwvdbus $(WVSTREAMS)/libwvtest.a 
CC=gcc
CXX=g++

TESTSOURCES=$(wildcard *.t.cc)
TESTOBJS=$(patsubst %.cc,%.o,$(TESTSOURCES))

all: all.t

test: all.t
	$(RUNTEST) $(VALGRIND) ./all.t $(TESTNAME)

# FIXME: Should be using GCC-generated dependencies here
all.t: $(TESTOBJS) common.o fakeversaplex.o column.o 
	$(CXX) -o $@ $^ $(LIBS)

# FIXME: Should be using GCC-generated dependencies here
%.o: %.cc %.h
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean: 
	rm -f *.o $(TESTPROGS) valgrind.log.*
