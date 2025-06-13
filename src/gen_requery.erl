-module(gen_requery).
-behaviour(gen_server).

-export([start_link/3, start_link/4, start/3, start/4, call/3, cast/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(ReconnectWait, 1000).
-define(ReconnectMax, 60000).
-define(ReqlTimeout, infinity).

-type(state() :: any()).
-type(reql() :: pid() | undefined).

-callback init(Args :: any()) ->
    {ok, ConnectOptions ::  map(), State :: state()} |
    {stop, Reason :: any()} |
    ignore.
-callback handle_connection_up(Connection :: pid(), State :: state()) ->
    {noreply, Reql :: reql(), NewState :: state()} |
    {noreply, NewState :: state()} |
    {stop, Reason :: any(), NewState :: state()}.
-callback handle_connection_down(State :: state()) ->
    {noreply, NewState :: state()} |
    {stop, Reason :: any(), NewState :: state()}.
-callback handle_query_done(State :: state()) -> 
    {noreply, NewState :: state()} |
    {stop, Reason :: any(), NewState :: state()}.
-callback handle_query_result(Result :: list(), State :: state()) ->
    {noreply, NewState :: state()} |
    {stop, Reason :: any(), NewState :: state()}.
-callback handle_query_error(Error :: any(), State :: state()) ->
    {noreply, NewState :: state()} |
    {stop, Reason :: any(), NewState :: state()}.
-callback handle_call(Request :: any(), From :: pid(), State :: state()) ->
    {reply, Reply :: any(), NewState :: state()} |
    {noreply, NewState :: state()} |
    {stop, Reason :: any(), Reply :: any(), NewState :: state()} |
    {stop, Reason :: any(), NewState :: state()}.
-callback handle_cast(Msg :: any(), State :: state()) ->
    {noreply, NewState :: state()} |
    {stop, Reason :: any(), NewState :: state()}.
-callback handle_info(Info :: any(), State :: state()) ->
    {noreply, NewState :: state()} |
    {stop, Reason :: any(), NewState :: state()}.
-callback terminate(Reason :: any(), State :: state()) ->
    any().
-callback code_change(OldVsn:: any(), State :: state(), Extra :: any()) ->
    {ok, state()}.

start_link(Module, Args, Options) -> 
    gen_server:start_link(?MODULE, [Module, Args], Options).

start_link(ServerName, Module, Args, Options) ->
    gen_server:start_link(ServerName, ?MODULE, [Module, Args], Options).

start(Module, Args, Options) ->
    gen_server:start(?MODULE, [Module, Args], Options).

start(ServerName, Module, Args, Options) ->
    gen_server:start(ServerName, ?MODULE, [Module, Args], Options).

call(Ref, Call, Timeout) ->
    gen_server:call(Ref, Call, Timeout).

cast(Ref, Cast) ->
    gen_server:cast(Ref, Cast).

init([Module, Args]) ->
    case Module:init(Args) of
        {ok, ConnectOptions, CallbackState} ->
            gen_server:cast(self(), {?MODULE, connect}),
            {ok, #{module => Module,
                   connect_options => ConnectOptions,
                   monitor_ref => undefined,
                   connection => undefined,
                   callbackstate => CallbackState}};
        {stop, Reason} ->
            {stop, Reason};
        ignore ->
            ignore
    end.

handle_call(Request, From, State=#{module := Module,
                                    callbackstate := CallbackState}) ->
    case Module:handle_call(Request, From, CallbackState) of
        {reply, Reply, NewState} ->
            {reply, Reply, State#{callbackstate => NewState}};
        {noreply, NewState} ->
            {noreply, State#{callbackstate => NewState}};
        {stop, Reason, Reply, NewState} ->
            {stop, Reason, Reply, State#{callbackstate => NewState}};
        {stop, Reason, NewState} ->
            {stop, Reason, State#{callbackstate => NewState}}
    end.

handle_cast({?MODULE, connect}, State=#{connect_options := ConnectOptions}) ->
    case connect(ConnectOptions) of
        {ok, MonitorRef, Connection} ->
            State2 = State#{monitor_ref => MonitorRef,
                             connection => Connection},
            exec_handle_connection_up(State2);
        _Err ->
            gen_server:cast(self(), {?MODULE, reconnect, 0}),
            {noreply, State}
    end;
handle_cast({?MODULE, reconnect, N}, State=#{connect_options := ConnectOptions}) ->
    spawn_connect(N, ConnectOptions),
    {noreply, State};
handle_cast({?MODULE, bind_connection, Connection}, State) ->
    State2 = close_connection(State),
    MonitorRef = erlang:monitor(process, Connection),
    State3 = State2#{monitor_ref => MonitorRef,
                     connection => Connection},
    exec_handle_connection_up(State3);
handle_cast(Msg, State=#{module := Module,
                          callbackstate := CallbackState}) ->
    case Module:handle_cast(Msg, CallbackState) of
        {noreply, NewState} ->
            {noreply, State#{callbackstate => NewState}};
        {stop, Reason, NewState} ->
            {stop, Reason, State#{callbackstate => NewState}}
    end.

handle_info({'DOWN', MonitorRef, _, _, _}, State=#{monitor_ref := MonitorRef}) ->
    State2 = State#{monitor_ref => undefined,
                    connection => undefined},
    State3 = close_cursor(State2),
    case exec_handle_connection_down(State3) of
        {noreply, State4} ->
            handle_cast({?MODULE, connect}, State4);
        {stop, Reason, State4} ->
            {stop, Reason, State4}
    end;
handle_info({rethink_cursor, done}, State) ->
    exec_handle_query_done(maps:without([cursor], State));
handle_info({rethink_cursor, Result}, State) ->
    exec_handle_query_result(Result, State);
handle_info({rethink_cursor, error, Error}, State) ->
    exec_handle_query_error(Error, maps:without([cursor], State));
handle_info(Info, State=#{module := Module,
                          callbackstate := CallbackState}) ->
    case Module:handle_info(Info, CallbackState) of
        {noreply, NewState} ->
            {noreply, State#{callbackstate => NewState}};
        {stop, Reason, NewState} ->
            {stop, Reason, State#{callbackstate => NewState}}
    end.

terminate(Reason, State=#{module := Module,
                    monitor_ref := MonitorRef,
                    callbackstate := CallbackState}) ->
    if MonitorRef =:= undefined -> ok; true -> erlang:demonitor(MonitorRef) end,
    State2 = close_cursor(State),
    _State3 = close_connection(State2),
    Module:terminate(Reason, CallbackState).

close_cursor(State=#{cursor := Cursor}) ->
    rethink_cursor:close(Cursor),
    maps:without([cursor], State);
close_cursor(State) ->
    State.

close_connection(State=#{connection := Connection}) when Connection =/= undefined ->
    gen_rethink:close(Connection),
    State#{connection => undefined};
close_connection(State) ->
    State.

code_change(OldVsn, #{module := Module,
                      callbackstate := CallbackState} = State, Extra) ->
    CBS = case Module:code_change(OldVsn, CallbackState, Extra) of
        {ok, NewCallbackState} ->
            NewCallbackState;
        _ ->
            CallbackState
    end,
    {ok, State#{callbackstate => CBS}}.
    
%%
exec_handle_connection_up(State=#{module := Module,
                                  connection := Connection,
                           callbackstate := CallbackState}) ->
    case Module:handle_connection_up(Connection, CallbackState) of
        {noreply, NewState} ->
            {noreply, State#{callbackstate => NewState}};
        {noreply, {Reql, Timeout}, NewState} ->
            State2 = State#{callbackstate => NewState},
            exec_run(Connection, Reql, Timeout, State2);
        {noreply, Reql, NewState} ->
            State2 = State#{callbackstate => NewState},
            exec_run(Connection, Reql, ?ReqlTimeout, State2);
        {stop, Reason, NewState} ->
            {stop, Reason, State#{callbackstate => NewState}}
    end.

exec_handle_connection_down(State=#{module := Module,
                           callbackstate := CallbackState}) ->
    case Module:handle_connection_down(CallbackState) of
        {noreply, NewState} ->
            {noreply, State#{callbackstate => NewState}};
        {stop, Reason, NewState} ->
            {stop, Reason, State#{callbackstate => NewState}}
    end.

exec_run(Connection, Reql, Timeout, State) ->
    case gen_rethink:run(Connection, Reql, Timeout) of
        {ok, Cursor} when is_pid(Cursor) ->
            % Handle the first set of results right away. This provides fewer
            % race conditions when expecting results (see unit test)
            case rethink_cursor:flush(Cursor) of
                {ok, done} ->
                    exec_handle_query_done(State);
                {ok, Results} ->
                    case exec_handle_query_results(Results, State) of
                        {noreply, State2} ->
                            ok = rethink_cursor:activate(Cursor),
                            {noreply, State2#{cursor => Cursor}};
                        Stop ->
                            Stop
                    end;
                Error ->
                    exec_handle_query_error(Error, State)
            end;
        {ok, Result} ->
            case exec_handle_query_result(Result, State) of
                {noreply, NewState} ->
                    exec_handle_query_done(NewState);
                Stop ->
                    Stop
            end;
        Err ->
            exec_handle_query_error(Err, State)
    end.

exec_handle_query_results([], State) ->
    {noreply, State};
exec_handle_query_results([Result|Results], State=#{module := Module,
                           callbackstate := CallbackState}) ->
    case Module:handle_query_result(Result, CallbackState) of
        {noreply, NewState} ->
            exec_handle_query_results(Results, State#{callbackstate => NewState});
        {stop, Reason, NewState} ->
            {stop, Reason, State#{callbackstate => NewState}}
    end.

exec_handle_query_result(Result, State=#{module := Module,
                           callbackstate := CallbackState}) ->
    case Module:handle_query_result(Result, CallbackState) of
        {noreply, NewState} ->
            {noreply, State#{callbackstate => NewState}};
        {stop, Reason, NewState} ->
            {stop, Reason, State#{callbackstate => NewState}}
    end.

exec_handle_query_done(State=#{module := Module,
                          callbackstate := CallbackState}) ->
    case Module:handle_query_done(CallbackState) of
        {noreply, NewState} ->
            {noreply, State#{callbackstate => NewState}};
        {stop, Reason, NewState} ->
            {stop, Reason, State#{callbackstate => NewState}}
    end.

exec_handle_query_error(Error, State=#{module := Module,
                          callbackstate := CallbackState}) ->
    case Module:handle_query_error(Error, CallbackState) of
        {noreply, NewState} ->
            {noreply, State#{callbackstate => NewState}};
        {stop, Reason, NewState} ->
            {stop, Reason, State#{callbackstate => NewState}}
    end.

spawn_connect(N, Options) ->
    Pid = self(),
    spawn_link(fun() ->
                       Sleep = erlang:min(?ReconnectMax, trunc(?ReconnectWait * math:pow(2, N))),
                       timer:sleep(Sleep),
                       case gen_rethink:connect_unlinked(Options) of
                           {ok, Connection} ->
                               gen_server:cast(Pid, {?MODULE, bind_connection, Connection});
                           _Err ->
                               gen_server:cast(Pid, {?MODULE, reconnect, N+1})
                       end
               end).

connect(Options) ->
    case gen_rethink:connect_unlinked(Options) of
        {ok, Connection} ->
            MonitorRef = erlang:monitor(process, Connection),
            {ok, MonitorRef, Connection};
        Err ->
            Err
    end.
