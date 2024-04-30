#!/usr/bin/make -f

stress-test:

.PHONY: configure
configure:
	mops i

.PHONY: stress-test
stress-test: deploy
	time dfx canister call stresser main '()'

.PHONY: build
build: $(DESTDIR)/stress-test/stresser.wasm

.PHONY: deploy
deploy: $(DESTDIR)/stress-test/stresser.deploy
	dfx ledger fabricate-cycles --amount 100000000000 --canister stresser
