-module(rethink).

-export([start/0,
         stop/0,
         run1/1,
         run1/2,
         decode/1,
         encode/1,
         counter/3,
         parse_error/1]).


%% API
start() ->
    application:ensure_all_started(rethink).

stop() ->
    application:stop(rethinkdb).

run1(Reql) ->
    run1(Reql, undefined).

run1(Reql, Timeout) ->
    run1("localhost", 28015, Reql, Timeout).

run1(Host, Port, Reql, Timeout) when is_list(Reql) ->
    R = reql:new(Reql),
    Res = run1(Host, Port, R, Timeout),
    Res;
run1(Host, Port, Reql, Timeout) ->
    % This function is provided for convenience and takes some actions that
    % would not be memory-safe for large queries. Use with caution.
    {ok, C} = gen_rethink:connect(Host, Port),
    Return = case gen_rethink:run(C, Reql, Timeout) of
        {ok, Pid} when is_pid(Pid) ->
            case rethink_cursor:all(Pid) of
                {ok, Res} when is_list(Res) ->
                    {ok, lists:flatten(Res)};
                Er ->
                    Er
            end;
        Res ->
            Res
    end,
    gen_rethink:close(C),
    Return.

decode(X) ->
    ql2:undatum(jsx:decode(X, [return_maps])).

encode(X) ->
    jsx:encode(X).

counter(Name, Incr, Max) ->
    counter(Name, Incr, Max, 1, 0).

counter(Name, Incr, Threshold, SetValue, Default) ->
    UpdateOp = {2, Incr, Threshold, SetValue},
    ets:update_counter(rethink_counters, Name, UpdateOp, {Name, Default}).

parse_error(Err={error,{runtime_error, RuntimeError}}) ->
    case re:run(RuntimeError, <<"already exists">>, [{capture, none}]) of
        match ->
            {error, {already_exists, RuntimeError}};
        nomatch ->
            Err
    end;
parse_error(Err) ->
    Err.
