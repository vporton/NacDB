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
