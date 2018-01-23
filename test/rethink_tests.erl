-module(rethink_tests).
-include_lib("eunit/include/eunit.hrl").

rethink_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     fun(_C) ->
             [
              fun() -> {error,{already_exists,<<"Test already exists">>}} =
                       rethink:parse_error({error, {runtime_error, <<"Test already exists">>}})
              end,
              fun() -> {error,{table_does_not_exist,<<"Table `testdb.testtable` does not exist">>}} =
                       rethink:parse_error({error, {runtime_error, <<"Table `testdb.testtable` does not exist">>}})
              end
             ]
     end}.

setup() ->
    rethink:start(),
    ok.

teardown(_C) ->
    rethink:stop().
