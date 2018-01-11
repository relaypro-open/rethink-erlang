-module(gen_requery).
-behaviour(gen_server).

-export([start_link/3, start_link/4, start/3, start/4, run/3, run/4]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-type(state() :: any()).
-type(reql() :: pid()).

-callback init(Args :: list()) ->
    {ok, Reql ::  reql(), State :: state()} |
    {stop, Reason :: any()} |
    ignore.
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

run(Ref, Connection, Timeout) ->
    gen_server:cast(Ref, {?MODULE, run, Connection, Timeout}).

run(Ref, Connection, Arg, Timeout) ->
    gen_server:cast(Ref, {?MODULE, run, Connection, Arg, Timeout}).

init([Module, Args]) ->
    case Module:init(Args) of
        {ok, Reql, CallbackState} when is_pid(Reql) ->
            reql:hold(Reql),
            {ok, #{module => Module,
                   reql => Reql,
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

handle_cast({?MODULE, run, Connection, Timeout}, State=#{reql := Reql}) ->
    exec_run(Connection, Reql, Timeout, State);
handle_cast({?MODULE, run, Connection, Arg, Timeout}, State=#{reql := Reql}) ->
    exec_run(Connection, Reql(Arg), Timeout, State);
handle_cast(Msg, State=#{module := Module,
                          callbackstate := CallbackState}) ->
    case Module:handle_cast(Msg, CallbackState) of
        {noreply, NewState} ->
            {noreply, State#{callbackstate => NewState}};
        {stop, Reason, NewState} ->
            {stop, Reason, State#{callbackstate => NewState}}
    end.

handle_info({rethink_cursor, done}, State) ->
    exec_handle_query_done(State);
handle_info({rethink_cursor, Result}, State) ->
    exec_handle_query_result(Result, State);
handle_info({rethink_cursor, error, Error}, State) ->
    exec_handle_query_error(Error, State);
handle_info(Info, State=#{module := Module,
                          callbackstate := CallbackState}) ->
    case Module:handle_info(Info, CallbackState) of
        {noreply, NewState} ->
            {noreply, State#{callbackstate => NewState}};
        {stop, Reason, NewState} ->
            {stop, Reason, State#{callbackstate => NewState}}
    end.

terminate(Reason, #{module := Module,
                    callbackstate := CallbackState}) ->
    Module:terminate(Reason, CallbackState).

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
exec_run(Connection, Reql, Timeout, State) ->
    case gen_rethink:run(Connection, Reql, Timeout) of
        {ok, Cursor} when is_pid(Cursor) ->
            ok = rethink_cursor:activate(Cursor),
            {noreply, State#{cursor => Cursor}};
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
