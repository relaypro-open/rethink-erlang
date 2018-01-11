.PHONY: all
all:
	@(rebar3 compile)

.PHONY: test
test: all
	@(rebar3 eunit)
