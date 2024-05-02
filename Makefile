#!/usr/bin/make -f

stress-test:

.PHONY: configure
configure:
	mops i

.PHONY: stress-test
stress-test: deploy-stress-test
	time dfx canister call stresser main '()'

.PHONY: deploy-stress-test
deploy-stress-test:
	dfx deploy stresser
	dfx ledger fabricate-cycles --amount 1000000000 --canister stresser