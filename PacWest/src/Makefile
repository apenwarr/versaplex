.PHONY: Wv.Net Versaplex

all: Wv.Net Versaplex

Wv.Net:
	$(MAKE) -C Wv.Net

Versaplex:
	$(MAKE) -C Versaplex

test: Wv.Net
	$(MAKE) -C Wv.Net $@
	$(MAKE) -C Versaplex $@
	$(MAKE) -C SqlSucker/tests $@

clean:
	$(MAKE) -C Wv.Net $@
	$(MAKE) -C Versaplex $@
	$(MAKE) -C SqlSucker/tests $@
