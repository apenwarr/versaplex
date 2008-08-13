
SHELL=/bin/bash

# cc -E tries to guess by extension what to do with the file.
# And it does other weird things. cpp seems to Just Work(tm), so use that for
# our C# (.cs) files
CSCPP=cpp

# Rule for actually preprocessing source files with headers
%.cs.E: %.cs
	@rm -f $@
	set -o pipefail; $(CSCPP) $(CPPFLAGS) -C -dI $< \
		| expand -8 \
		| sed -e 's,^#include,//#include,' \
		| grep -v '^# [0-9]' \
		>$@ || (rm -f $@ && exit 1)

%.pass: %.exe
	rm -f $@
	mono --debug ./$^
	touch $@

clean::
	rm -f *~ *.E *.d *.exe *.dll *.mdb *.pdb *.pass
