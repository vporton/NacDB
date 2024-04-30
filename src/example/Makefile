#!/usr/bin/make -f

.PHONY: all
all: deploy

.PHONY: deploy
deploy: generate
#	dfx deploy example_backend
	dfx deploy example_frontend
	dfx canister call example_backend init '()'