-module(gen_rethink_tests).

-compile([export_all]).

-define(AdminUser, #{<<"id">> := <<"admin">>,<<"password">> := false}).

connect_test() ->
    {ok, Re} = gen_rethink:connect("localhost", 28015, [], 1000),
    Reql = reql:db(<<"rethinkdb">>),
    reql:hold(Reql),
    reql:table(Reql, <<"users">>),
    reql:filter(Reql, #{<<"password">> => false}),
    ok = fun() ->
        {ok, Cursor} = gen_rethink:run(Re, Reql, 1000),
        {ok,ResultsList} = rethink_cursor:all(Cursor),
        [?AdminUser] = lists:flatten(ResultsList),
        false = is_process_alive(Cursor),
        ok
     end(),
    ok = fun() ->
        {ok, Cursor} = gen_rethink:run(Re, Reql, 1000),
        ok = rethink_cursor:activate(Cursor),
        [?AdminUser] = receive
            {rethink_cursor, Result} ->
                Result
        after 1000 ->
                  []
        end,
        ok = receive
                 {rethink_cursor, done} ->
                     ok
             after 1000 ->
                       error
             end,
        false = is_process_alive(Cursor),
        ok
       end().

