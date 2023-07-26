#!/usr/bin/make -f

.PHONY: stress-test
stress-test: deploy
	dfx canister call stresser main '()'

.PHONY: deploy
deploy:
	dfx deploy stresser
	dfx ledger fabricate-cycles --amount 100000000000 --canister stresser
