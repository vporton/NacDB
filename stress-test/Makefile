#!/usr/bin/make -f

.PHONY: stress-test
stress-test: deploy
	dfx canister call stresser main '()'

.PHONY: deploy
deploy:
	dfx deploy stresser --with-cycles 99000000000000000
