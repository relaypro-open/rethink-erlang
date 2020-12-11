-module(rethink_session_stats).

-export([get_connections/1, await_connections/2, sort/1]).

%% @doc returns a list of connection pids, sorted by
%% their likelihood for being able to serve a new query
%% with the lowest latency. First pid in the list is
%% the least-busy
get_connections(Table) ->
    List = ets:tab2list(Table),
    List2 = sort(List),
    Pids = [ X || {X, _, _, _} <- List2 ],
    lists:filter(fun is_process_alive/1, Pids).

sort(List) ->
    lists:sort(
      % Fun(A, B) is to return true if A compares less
      % than or equal to B in the ordering, otherwise false.
      fun
          ({_APid, AStatus, _AWaiting, _ACompleted},
           {_BPid, BStatus, _BWaiting, _BCompleted}) when AStatus =/= BStatus ->
             if AStatus =:= ok -> true;
                BStatus =:= ok -> false;
                true -> AStatus =< BStatus
             end;

          ({_APid, _AStatus, AWaiting, _ACompleted},
           {_BPid, _BStatus, BWaiting, _BCompleted}) when AWaiting /= BWaiting ->
              AWaiting =< BWaiting;

          ({_APid, _AStatus, _AWaiting, ACompleted},
           {_BPid, _BStatus, _BWaiting, BCompleted}) when ACompleted /= BCompleted ->
              ACompleted =< BCompleted;

          (A, B) ->
              A =< B
      end,
      List).

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

