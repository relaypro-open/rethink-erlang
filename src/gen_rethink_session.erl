-module(gen_rethink_session).
-behaviour(gen_server).

-export([start_link/0, start_link/1, get_connection/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(CallTimeout, timer:hours(1)).
-define(ReconnectWait, timer:seconds(5)).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

get_connection(Svr) ->
    gen_server:call(Svr, {get_connection}, ?CallTimeout).

init([]) ->
    process_flag(trap_exit, true),
    gen_server:cast(self(), {connect}),
    {ok, #{monitor_ref => undefined,
           connection => undefined}}.

handle_call({get_connection}, _From, State=#{connection := Connection}) when is_pid(Connection) ->
    {reply, {ok, Connection}, State};
handle_call({get_connection}, _From, State) ->
    {reply, {error, no_connection}, State}.

handle_cast({connect}, State) ->
    case connect() of
        {ok, MonitorRef, Connection} ->
            {noreply, State#{monitor_ref => MonitorRef,
                             connection => Connection}};
        _Err ->
            gen_server:cast(self(), {reconnect}),
            {noreply, State}
    end;
handle_cast({reconnect}, State) ->
    spawn_connect(),
    {noreply, State};
handle_cast({bind_connection, Connection}, State) ->
    MonitorRef = erlang:monitor(process, Connection),
    {noreply, State#{monitor_ref => MonitorRef,
                     connection => Connection}}.

handle_info({'DOWN', MonitorRef, _, _, _}, State=#{monitor_ref := MonitorRef}) ->
    handle_cast({connect}, State#{monitor_ref => undefined,
                                  connection => undefined});
handle_info({'EXIT', From, Reason}, State)  ->
    case self() of
        From ->
            {stop, Reason, State};
        _ ->
            case Reason of
                normal ->
                    {noreply, State};
                _ ->
                    io:format("reconnect process exited ~p~n", [Reason]),
                    {stop, Reason, State}
            end
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State=#{monitor_ref := undefined}) ->
    ok;
terminate(_Reason, _State=#{monitor_ref := MonitorRef, connection := Connection}) ->
    erlang:demonitor(MonitorRef),
    gen_rethink:close(Connection),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

spawn_connect() ->
    Pid = self(),
    spawn_link(fun () ->
                       timer:sleep(?ReconnectWait),
                       case gen_rethink:connect_unlinked() of
                           {ok, Connection} ->
                               gen_server:cast(Pid, {bind_connection, Connection});
                           _Err ->
                               gen_server:cast(Pid, {reconnect})
                       end
          end).

connect() ->
    case gen_rethink:connect_unlinked() of
        {ok, Connection} ->
            MonitorRef = erlang:monitor(process, Connection),
            {ok, MonitorRef, Connection};
        Err ->
            Err
    end.
