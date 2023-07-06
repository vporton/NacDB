#!/usr/bin/make -f

.PHONY: test

test:
	$(shell vessel bin)/moc -r $(shell vessel sources) -wasi-system-api test/*Test.mo

test2:
	$(shell vessel bin)/moc $(shell vessel sources) test/*Test.mo
