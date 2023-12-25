#!/usr/bin/make -f

.PHONY: test

test:
	moc -r $(shell mops sources) -wasi-system-api test/*Test.mo

test2:
	moc $(shell mops sources) test/*Test.mo
