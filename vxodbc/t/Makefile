TESTPROGS=t0001 t0003

all: $(TESTPROGS)

CFLAGS=-DHAVE_CONFIG_H -I. -I. -I../../../include -I../../../include   
CFLAGS+=-D_FREETDS_LIBRARY_SOURCE -DUNIXODBC -D_REENTRANT -D_THREAD_SAFE 
CFLAGS+=-DDEBUG=1 

WVDIR=../../wv
XPLC=$(WVDIR)/wvports/xplc/build/xplc
WVSTREAMS=$(WVDIR)/wvstreams

CFLAGS+=-Wall -Wno-long-long  
CFLAGS+=-g -O2 
CFLAGS+=-I$(WVSTREAMS)/include -I$(XPLC)/include
LIBS=../vxodbc.so /usr/lib/libltdl.so -ldl -lpthread $(WVSTREAMS)/libwvdbus.a $(WVSTREAMS)/libwvstreams.a $(WVSTREAMS)/libwvtest.a
CC=gcc
CXX=g++

t0001: t0001.o common.o
	$(CXX) -o $@ $^ $(LIBS) 

t0003: t0003.o common.o
	$(CXX) -o $@ $^ $(LIBS) 
	
test:
	# nothing yet

%.o: %.cc
	$(CXX) $(CFLAGS) -c -o $@ $^

clean: 
	rm -f *.o $(TESTPROGS)
