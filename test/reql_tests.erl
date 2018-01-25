-module(reql_tests).
-include_lib("eunit/include/eunit.hrl").

-define(Now, {1510,796448,975000}). % msec precision
-define(Binary, <<1,2,3,4,5,6>>).
-define(List, [1,2,3,4,5,6]).

reql_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     fun(_C) ->
             [
              fun() -> {ok, 42} = rethink:run1([{expr, 42}]) end,
              fun() -> {ok, 30} = rethink:run1(reql:do([10, 20],
                                     fun(X, Y) -> reql:add(X, Y) end)) end,
              fun() -> rethink:run1([{db_drop, temp_db}]) end,

              % Manipulating databases
              fun() -> {ok, _} = rethink:run1(reql:db_create(reql:new(), temp_db)) end,
              fun() -> {ok, _} = rethink:run1(reql:db_list(reql:new())) end,
              fun() -> {ok, _} = rethink:run1(reql:db_drop(reql:new(), temp_db)) end,
              fun() -> {ok, _} = rethink:run1(reql:db_create(reql:new(), temp_db)) end,

              % Manipulating tables
              fun() -> {ok, _} = rethink:run1(reql:table_create(reql:db(temp_db), my_table)) end,
              fun() -> {ok, _} = rethink:run1(reql:table_list(reql:db(temp_db))) end,
              fun() -> {ok, _} = rethink:run1(reql:table_drop(reql:db(temp_db), my_table)) end,
              fun() -> {ok, _} = rethink:run1(reql:table_create(reql:db(temp_db), my_table)) end,

              fun() -> {ok, _} = rethink:run1(reql:index_create(reql:table(reql:db(temp_db), my_table), my_index)) end,
              fun() -> {ok, _} = rethink:run1(reql:index_wait(reql:table(reql:db(temp_db), my_table), my_index)) end,
              fun() -> {ok, _} = rethink:run1(reql:index_wait(reql:table(reql:db(temp_db), my_table))) end,
              fun() -> {ok, _} = rethink:run1(reql:index_list(reql:table(reql:db(temp_db), my_table))) end,
              fun() -> {ok, _} = rethink:run1(reql:index_status(reql:table(reql:db(temp_db), my_table))) end,
              fun() -> {ok, _} = rethink:run1(reql:index_status(reql:table(reql:db(temp_db), my_table), my_index)) end,
              fun() -> {ok, _} = rethink:run1(reql:index_rename(reql:table(reql:db(temp_db), my_table), my_index, my_index2)) end,
              fun() -> {ok, _} = rethink:run1(reql:index_rename(reql:table(reql:db(temp_db), my_table), my_index2, my_index)) end,
              fun() -> {ok, _} = rethink:run1(reql:index_wait(reql:table(reql:db(temp_db), my_table), my_index)) end,

              % Writing data
              fun() -> {ok, _} = rethink:run1(
                                   fun(R) ->
                                       reql:db(R, temp_db),
                                       reql:table(R, my_table),
                                       reql:insert(R, #{ name => <<"jms">>,
                                                         nest => #{
                                                           timestamp => ?Now,
                                                           blob => reql:binary(?Binary),
                                                           list => ?List},
                                                         my_index => 1})
                                   end) end,

              % Selecting data
              fun() ->
                GetAll = reql:db(temp_db),
                reql:table(GetAll, my_table),
                reql:get_all(GetAll, 1, #{index => <<"my_index">>}),
                {ok, [#{<<"name">> := <<"jms">>,
                               <<"nest">> := #{
                                   <<"blob">> := {binary, ?Binary},
                                   <<"timestamp">> := ?Now,
                                   <<"list">> := ?List
                                  },
                               <<"my_index">> := 1}]} = rethink:run1(GetAll)
              end,

              % Filter function
              fun() ->
                      GetAll = reql:db(temp_db),
                      reql:table(GetAll, my_table),
                      reql:get_all(GetAll, 1, #{index => my_index}),
                      reql:filter(GetAll, fun(X) ->
                                                  reql:get_field(X, my_index),
                                                  reql:eq(X, 1)
                                          end),
                      {ok, _} = rethink:run1(GetAll)
              end,

              % Client-side timeout
              fun() ->
                      {error, timeout} =
                        rethink:run1(fun(X) ->
                                             reql:javascript(X, <<"while(true) {}">>,
                                                             #{timeout => 1.3})
                                     end, 100)
              end,

              % Closure test
              fun() ->
                      Reql = reql:db(temp_db),
                      reql:table(Reql, my_table),
                      Inserter = reql:closure(Reql, insert, #{return_changes => true}),
                      {ok, C} = gen_rethink:connect(),
                      {ok, #{<<"changes">> := [_|_]}} =
                        gen_rethink:run_closure(C, Inserter,
                               [#{name => <<"closureitem">>}], timer:minutes(5)),
                      gen_rethink:close(C)
              end,

              % Raw insert test
              fun() ->
                      {ok, C} = gen_rethink:connect(),
                      {ok, #{<<"changes">> := [_|_]}} = gen_rethink:insert_raw(C, <<"temp_db">>, <<"my_table">>,
                                <<"{\"name\":\"insertrawitem\"}">>, #{return_changes => true},
                                timer:minutes(5)),
                      gen_rethink:close(C)
              end,

              % Concurrent ReQL test
              fun() ->
                      {ok, C} = gen_rethink:connect(),
                      Self = self(),
                      WorkFunc = fun() ->
                                    Waiter = reql:new(),
                                    reql:javascript(Waiter, <<"while(true) {}">>,
                                                    #{timeout => 0.1}),
                                    R = gen_rethink:run(C, Waiter, 500),
                                    Self ! R
                                 end,
                      Num = 2,
                      [ spawn(WorkFunc) || _ <- lists:seq(1, Num) ],
                      fun Loop(0) -> ok;
                          Loop(N) ->
                            receive
                                {error,{runtime_error,<<"JavaScript query `while(true) {}` timed out after 0.100 seconds.">>}} ->
                                    % We received timeout from rethink server - expected
                                    Loop(N-1);
                                Msg ->
                                    % We received client-side timeout, unexpected
                                    throw(Msg)
                            after 10000 ->
                                      throw({error, timeout})
                            end
                      end(Num)
              end

             ]
     end}.

setup() ->
    rethink:start(),
    ok.

teardown(_C) ->
    rethink:stop().
