#!/usr/bin/make -f

.PHONY: all
all: deploy

.PHONY: generate
generate:
	dfx generate partition
	dfx generate index
	dfx generate example_frontend

.PHONY: deploy
deploy: generate
#	dfx deploy example_backend
	dfx deploy example_frontend
	dfx canister call example_backend init '()'