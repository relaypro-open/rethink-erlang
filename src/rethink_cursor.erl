-module(rethink_cursor).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([activate/1, deactivate/1, flush/1, all/1, close/1]).

-export([make/5, update_success/3, update_error/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
make(Connection, Token,
     LastResponseType, Result,
    Timeout) ->
    {ok, Cursor} = gen_server:start_link(?MODULE, [Connection, Token,
                                                   LastResponseType, Result,
                                                  Timeout], []),
    Cursor.

update_success(Cursor, ResponseType, Result) ->
    gen_server:call(Cursor, {update_success, ResponseType, Result}, infinity).

update_error(Cursor, Err) ->
    gen_server:call(Cursor, {update_error, Err}, infinity).

activate(Cursor) ->
    Pid = self(),
    gen_server:call(Cursor, {activate, Pid}, infinity).

deactivate(Cursor) ->
    gen_server:call(Cursor, {deactivate}, infinity).

flush(Cursor) ->
    gen_server:call(Cursor, {flush}, infinity).

all(Cursor) ->
    all(Cursor, []).

all(Cursor, Acc) ->
    case flush(Cursor) of
        {ok, done} ->
            {ok, lists:reverse(Acc)};
        {ok, Results} ->
            all(Cursor, [Results|Acc]);
        Err ->
            Err
    end.

close(_Cursor) ->
    ok.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Connection, Token, LastResponseType, Result, Timeout]) ->

    % If connection goes down, we do too
    link(Connection),

    {ok, #{connection => Connection,
           token => Token,
           last_response_type => LastResponseType,
           results => [Result],
           timeout => Timeout,
           receiver => undefined,
           waiting_feed => false}}.

handle_call({activate, Pid}, _From, State=#{receiver := undefined}) ->
    State2 = feed(State),
    case notify(State2#{receiver => Pid}) of
        {stop, State3} ->
            {stop, normal, ok, State3};
        State3 ->
            {reply, ok, State3}
    end;
handle_call({deactivate}, _From, State) ->
    {reply, ok, State#{receiver => undefined}};
handle_call({flush}, From, State=#{last_response_type := LastResponseType,
                                    results := Results}) ->
    State2 = feed(State),
    case Results of
        [] ->
            case LastResponseType of
                success_sequence ->
                    gen_server:reply(From, {ok, done}),
                    {stop, normal, State2};
                _ ->
                    case State2 of
                        #{error := Error} ->
                            gen_server:reply(From, Error),
                            {stop, normal, State2};
                        _ ->
                            {reply, {ok, []}, State2}
                    end
            end;
        _ ->
            {reply, {ok, Results}, State2#{results => []}}
    end;
handle_call({update_success, ResponseType, Result}, _From, State=#{results := Results}) ->
    State2 = State#{last_response_type => ResponseType,
                    results => Results ++ [Result],
                    waiting_feed => false},
    State3 = feed(State2),
    State4 = notify(State3),
    {reply, ok, State4};
handle_call({update_error, Error}, _From, State) ->
    State2 = State#{error => Error,
                    waiting_feed => false},
    State3 = notify(State2),
    {reply, ok, State3}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
feed(State=#{connection := Connection,
             token := Token,
             last_response_type := success_partial,
             timeout := _Timeout,
             waiting_feed := false}) ->
    % Timeout in the state was the original request's timeout, but for
    % query continuations, specificying a timeout would cause us to miss
    % data if the server sends a response after the timeout period. Instead,
    % rely on monitoring the connection process for interruptions
    Cursor = self(),
    ok = gen_rethink:feed_cursor(Connection, Cursor, Token),
    State#{waiting_feed => true};
feed(State) ->
    State.

notify({stop, State}) ->
    {stop, State};
notify(State=#{results := Results=[_|_], receiver := Receiver})
                when Receiver =/= undefined ->
    lists:foreach(fun(Result) ->
            Receiver ! result_msg(Result)
    end, Results),
    notify(State#{results => []});
notify(State=#{results := [], last_response_type := success_sequence,
              receiver := Receiver}) when Receiver =/= undefined ->
    Receiver ! done_msg(),
    {stop, State};
notify(State=#{error := Error,
               receiver := Receiver}) when Receiver =/= undefined ->
    Receiver ! error_msg(Error),
    {stop, State};
notify(State) ->
    State.

result_msg(Result) ->
    {rethink_cursor, Result}.

done_msg() ->
    {rethink_cursor, done}.

error_msg(Error) ->
    {rethink_cursor, error, Error}.
