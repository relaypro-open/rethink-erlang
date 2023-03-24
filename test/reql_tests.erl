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
              fun() -> {ok, _} = rethink:run1(reql:table_create(reql:db(temp_db), my_table2)) end,
              fun() -> {ok, _} = rethink:run1(reql:table_list(reql:db(temp_db))) end,
              fun() -> {ok, _} = rethink:run1(reql:table_drop(reql:db(temp_db), my_table)) end,
              fun() -> {ok, _} = rethink:run1(reql:table_drop(reql:db(temp_db), my_table2)) end,
              fun() -> {ok, _} = rethink:run1(reql:table_create(reql:db(temp_db), my_table)) end,
              fun() -> {ok, _} = rethink:run1(reql:table_create(reql:db(temp_db), my_table2)) end,

              fun() -> {ok, _} = rethink:run1(reql:index_create(reql:table(reql:db(temp_db), my_table), my_index)) end,
              fun() -> {ok, _} = rethink:run1(reql:index_create(reql:table(reql:db(temp_db), my_table2), my_index)) end,
              fun() -> {ok, _} = rethink:run1(reql:index_create(reql:table(reql:db(temp_db), my_table2), compound_index, [my_index, other_index])) end,
              fun() -> {ok, _} = rethink:run1(reql:index_wait(reql:table(reql:db(temp_db), my_table), my_index)) end,
              fun() -> {ok, _} = rethink:run1(reql:index_wait(reql:table(reql:db(temp_db), my_table))) end,
              fun() -> {ok, _} = rethink:run1(reql:index_wait(reql:table(reql:db(temp_db), my_table2))) end,
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
              fun() -> {ok, _} = rethink:run1(
                                   fun(R) ->
                                       reql:db(R, temp_db),
                                       reql:table(R, my_table),
                                       reql:insert(R, #{ name => <<"jms2">>,
                                                         nest => #{
                                                           timestamp => ?Now,
                                                           blob => reql:binary(?Binary),
                                                           list => ?List},
                                                         my_index => 2})
                                   end) end,
              fun() -> {ok, _} = rethink:run1(
                                   fun(R) ->
                                       reql:db(R, temp_db),
                                       reql:table(R, my_table2),
                                       reql:insert(R, #{ name => <<"jms2">>,
                                                         nest => #{
                                                           timestamp => ?Now,
                                                           blob => reql:binary(?Binary),
                                                           list => ?List},
                                                         my_index => 2},
                                                   #{conflict => fun(_, _, New) -> New end})
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
              fun() ->
                      Union1 = reql:db(temp_db),
                      reql:table(Union1, my_table),
                      Union2 = reql:db(temp_db),
                      reql:table(Union2, my_table2),
                      reql:union(Union1, Union2),
                      {ok, Results} = rethink:run1(Union1),
                      3 = length(Results),
                      false = is_process_alive(Union1),
                      false = is_process_alive(Union2)
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
              end,

              % Convenience functions
              fun() -> {ok, _} = rethink:run1(reql:x(fun(X) -> reql:db(X, temp_db), reql:table(X, my_table) end)) end,
              fun() -> {ok, _} = rethink:run1(reql:new([{db,temp_db},{table,my_table}])) end,
              fun() -> {ok, _} = rethink:run1(reql:new(temp_db,my_table)) end,

              % Control functions
              fun() ->
                      % This test implements a compare-and-swap operation to test 'branch' and 'error' commands.
                      % Notice that the record must exist in the table before the CAS. RethinkDB docs indicate
                      % that this operation is atomic.
                      {ok, C} = gen_rethink:connect(),
                      _ = gen_rethink:run(C, fun(X) -> reql:db(X, temp_db), reql:table(X, my_table), reql:get(X, cas_test), reql:delete(X) end),
                      _ = gen_rethink:run(C, fun(X) -> reql:db(X, temp_db), reql:table(X, my_table), reql:insert(X, #{id => cas_test, val => null}) end),
                      CasFun = fun() -> gen_rethink:run(C, fun(X) ->
                                                                   reql:db(X, temp_db),
                                                                   reql:table(X, my_table),
                                                                   reql:get(X, cas_test),
                                                                   reql:update(X,
                                                                               fun(RSchemaLock) ->
                                                                                       reql:branch(reql:eq(reql:bracket(RSchemaLock, val), null),
                                                                                                   #{val => <<"held">>},
                                                                                                   reql:error(held))
                                                                               end)
                                                           end)
                               end,
                      {ok, #{<<"replaced">> := 1}} = CasFun(),
                      {ok, #{<<"errors">> := 1, <<"first_error">> := <<"held">>}} = CasFun(),
                      gen_rethink:close(C)
              end,

              % Get multiple
              fun() ->
                      GetPKs = reql:db(temp_db),
                      reql:table(GetPKs, my_table),
                      {ok, Objs} = rethink:run1(GetPKs),
                      Ids = [ Id || #{<<"id">> := Id} <- Objs ],

                      MyIndexes = [ Index || #{<<"my_index">> := Index} <- Objs ],

                      GetAllList = reql:db(temp_db),
                      reql:table(GetAllList, my_table),
                      reql:get_all(GetAllList, reql:args(Ids)),
                      {ok, Objs2} = rethink:run1(GetAllList),
                      true = length(Objs2) == length(Ids),

                      GetAllListIndex = reql:db(temp_db),
                      reql:table(GetAllListIndex, my_table),
                      reql:get_all(GetAllListIndex, reql:args(MyIndexes), #{index => <<"my_index">>}),
                      {ok, Objs3} = rethink:run1(GetAllListIndex),
                      true = length(Objs3) == length(MyIndexes)
              end

             ]
     end}.

setup() ->
    rethink:start(),
    ok.

teardown(_C) ->
    rethink:stop().
