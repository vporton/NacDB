#!/usr/bin/make -f

.PHONY: stress-test

# test:
# 	moc -r $(shell mops sources) -wasi-system-api test/*Test.mo

# test2:
# 	moc $(shell mops sources) test/*Test.mo

.PHONY: stress-test
stress-test: deploy
	time dfx canister call stresser main '()'

.PHONY: deploy
deploy:
	dfx deploy stresser
	dfx ledger fabricate-cycles --amount 100000000000 --canister stresser

###########################################################################

DESTDIR = out
DFXDIR = .
NETWORK = local
IDENTITY = default
MOFLAGS =
MOFILES = $(shell find src stress-test/motoko -name "*.mo")

.PHONY: FORCE
FORCE:

# .wasm compilation is slow.
.PRECIOUS: %.wasm

.PHONY: deps
deps: $(DESTDIR)/.deps

$(DESTDIR)/.deps: $(MOFILES)
	echo -n > $(DESTDIR)/.deps
	for i in $(MOFILES); do \
	  { echo -n "$$i: "; moc --print-deps $$i | awk 'BEGIN {ORS = " "} !/^mo:/ {print $$2}'; echo; } >> $(DESTDIR)/.deps; \
	done

$(DESTDIR)/%.wasm $(DESTDIR)/%.did $(DESTDIR)/%.most: %.mo
	mkdir -p $(dir $@)
	moc $(MOFLAGS) --idl --stable-types `mops sources` -o $@ $<

$(DESTDIR)/%.ts: $(DESTDIR)/%.did
	didc bind -t ts $< > $@

%.install: %.wasm FORCE
	dfx canister create --network=$(NETWORK) --identity=$(IDENTITY) $(DFXCREATEFLAGS_$*) $(*F)
	dfx canister install --network=$(NETWORK) --identity=$(IDENTITY) -m install $(DFXINSTALLFLAGS_$*) --wasm=$< $(*F)

%.upgrade: %.wasm %.most FORCE
	mkdir -p $(DFXDIR)/.dfx/local/canisters/$(*F)
	TMPDIR=`mktemp -d` && \
	  trap 'rm -rf $$TMPDIR' EXIT && \
	  dfx canister metadata stresser motoko:stable-types > $$TMPDIR/interface.most && \
	  { echo; \
	    moc --stable-compatible $$TMPDIR/interface.most $*.most || \
	    { echo "\nWARNING!\nStable interface compatibility check failed for canister '$(*F)'.\nUpgrade will either FAIL or LOSE some stable variable data.\n"; \
		  read -r -p "Do you want to proceed? yes/No " REPLY; test "$$REPLY" = yes; }; \
	  }
	dfx canister install --network=$(NETWORK) --identity=$(IDENTITY) -m upgrade -y $(DFXINSTALLFLAGS_$*) --wasm=$< $(*F)

-include $(DESTDIR)/.deps
