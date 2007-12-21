
all: streamtest.exe

streamtest.exe: streamtest.cs wveventer.cs wvbuf.cs wvstream.cs wvlog.cs \
		wvextensions.cs
	gmcs -langversion:linq -debug -out:$@ $^

test: all
	./streamtest.exe

clean:
	rm -f streamtest.exe *.exe *~ *.mdb

