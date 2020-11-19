-module(rethink_session_stats).

-export([get_connections/1]).

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

