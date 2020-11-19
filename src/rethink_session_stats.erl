-module(rethink_session_stats).

-export([get_connections/1, await_connections/2]).

%% @doc returns a list of connection pids, sorted by
%% their likelihood for being able to serve a new query
%% with the lowest latency. First pid in the list is
%% the least-busy
get_connections(Table) ->
    List = ets:tab2list(Table),
    List2 = lists:sort(
      % Fun(A, B) is to return true if A compares less
      % than or equal to B in the ordering, otherwise false.
      fun
          ({_APid, ok, _AWaiting, _ACompleted},
          {_BPid, BStatus, _BWaiting, _BCompleted}) when BStatus =/= ok ->
              true;
          ({_APid, AStatus, _AWaiting, _ACompleted},
          {_BPid, ok, _BWaiting, _BCompleted}) when AStatus =/= ok ->
              false;
          ({_APid, _AStatus, AWaiting, _ACompleted},
          {_BPid, _BStatus, BWaiting, _BCompleted}) when AWaiting < BWaiting ->
              true;
          ({_APid, _AStatus, _AWaiting, ACompleted},
          {_BPid, _BStatus, _BWaiting, BCompleted}) when ACompleted < BCompleted ->
              true;
          (A, B) ->
              A =< B
      end,
      List),
    Pids = [ X || {X, _, _, _} <- List2 ],
    lists:filter(fun is_process_alive/1, Pids).

%% @doc checks the ets table with get_connections/1, but if it returns
%% empty, we try again on an aggressive exponential backoff until the
%% timeout value is reached. (infinity not supported)
await_connections(Table, Timeout) ->
    await_connections(Table, Timeout*1000, 0).

await_connections(Table, TimeoutU, Depth) ->
    case get_connections(Table) of
        [] ->
            WaitU = trunc(100*math:pow(2, Depth)),
            if WaitU < 1000 ->
                   ok;
               true ->
                   timer:sleep(WaitU div 1000)
            end,
            TimeoutU2 = TimeoutU-WaitU,
            if TimeoutU2 < 0 ->
                   erlang:error(timeout);
               true ->
                   await_connections(Table, TimeoutU-WaitU, Depth+1)
            end;
        Connections ->
            Connections
    end.

