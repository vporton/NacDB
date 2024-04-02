#!/usr/bin/make -f

stress-test:

include icp.rules

MOFILES = \
  src/Cycles.mo \
  src/GUID.mo \
  src/OpsQueue.mo \
  src/NacDB.mo \
  stress-test/stresser.mo

.PHONY: configure
configure:
	mops i

.PHONY: stress-test
stress-test: deploy
	time dfx canister call stresser main '()'

.PHONY: deploy
deploy: $(DESTDIR)/stress-test/stresser.deploy
	dfx ledger fabricate-cycles --amount 100000000000 --canister stresser
