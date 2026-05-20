.PHONY: temporal-up temporal-down build test

temporal-up:
	$(MAKE) -C temporal up

temporal-down:
	$(MAKE) -C temporal down

build:
	$(MAKE) -C services/go-worker build

test:
	$(MAKE) -C services/go-worker test
	$(MAKE) -C services/python-worker test
