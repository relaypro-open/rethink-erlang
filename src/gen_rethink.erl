-module(gen_rethink).
-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([connect/0,
         connect/1,
         connect/2,
         connect/4,
         connect_unlinked/0,
         connect_unlinked/1,
         run/2,
         run/3,
         insert_raw/5,
         insert_raw/6,
         run_closure/4,
         feed_cursor/3,
         close_cursor/3,
         close/1,
         server_info/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(Magic, <<16#C3, 16#BD, 16#C2, 16#34>>).
-define(CallTimeout, timer:hours(1)).
-define(RethinkTimeout, 5000).
-define(ConnectTimeout, 20000).

-record(receiver, {type, from, timeout, tref, maxlen=4294967295}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
connect(Options) ->
    Host = maps:get(host, Options, "localhost"),
    Port = maps:get(port, Options, 28015),
    Timeout = maps:get(timeout, Options, ?ConnectTimeout),
    connect(Host, Port, Options, Timeout).

connect() ->
    connect(#{}).

connect(Address, Port) ->
    connect(#{host => Address, port => Port}).

connect(Address, Port, Options, Timeout) ->
    case gen_server:start_link(?MODULE, filter_init_options(Options), []) of
        {ok, Re} ->
            connect(Re, Address, Port, Options, Timeout);
        Err ->
            Err
    end.

connect_unlinked() ->
    connect_unlinked(#{}).

connect_unlinked(Options) ->
    Host = maps:get(host, Options, "localhost"),
    Port = maps:get(port, Options, 28015),
    Timeout = maps:get(timeout, Options, ?ConnectTimeout),
    case gen_server:start(?MODULE, filter_init_options(Options), []) of
        {ok, Re} ->
            connect(Re, Host, Port, Options, Timeout);
        Err ->
            Err
    end.

connect(Re, Address, Port, Options, Timeout) ->
    case gen_server:call(Re, {connect, #{address => Address,
                                         port => Port,
                                         options => Options,
                                         timeout => Timeout}}, ?CallTimeout) of
        ok ->
            {ok, Re};
        Err ->
            gen_server:stop(Re, normal, ?CallTimeout),
            Err
    end.

server_info(Re) ->
    gen_server:call(Re, {reql_wire_query_type,
                         #{query_type => server_info,
                           timeout => ?RethinkTimeout}}, ?CallTimeout).

run(Re, Reql) ->
    run(Re, Reql, ?RethinkTimeout).

%% @doc
%% Calling run with a reql object will cause that reql to self-destruct. If
%% the caller needs to use the same reql object multiple times, you must us
%% reql:hold/1
run(Re, Reql, undefined) ->
    run(Re, Reql, ?RethinkTimeout);
run(Re, Reql, Timeout) when is_function(Reql) ->
    FunInfo = erlang:fun_info(Reql),
    Arity = proplists:get_value(arity, FunInfo),
    case Arity of
        0 ->
            run(Re, Reql(), Timeout);
        1 ->
            run(Re, reql:x(Reql), Timeout)
    end;
run(Re, Reql, Timeout) ->
    gen_server:call(Re, {run, #{reql => Reql,
                                timeout => Timeout}}, ?CallTimeout).

% {ok, C} = gen_rethink:connect().
% R = reql:new([{db, test}, {table, test}]).
% Inserter = reql:closure(RC, insert).
% gen_rethink:run_with_args(C, Inserter, #{name => an_object}, 1000).
% gen_rethink:run_with_args(C, Inserter, #{name => an_object2}, 1000).
% ...
run_closure(Re, ReqlClosure, Args, Timeout) when is_function(ReqlClosure) ->
    gen_server:call(Re, {run_closure, #{reql => ReqlClosure,
                                        args => Args,
                                        timeout => Timeout}}, ?CallTimeout).


insert_raw(Re, Db, Table, Json, Opts) when is_binary(Json) ->
    % Input json must be binary because we need to compute the size.
    % If an iolist json is required, then the function must be modified
    % to accept a size paramater as well
    insert_raw(Re, Db, Table, Json, Opts, ?RethinkTimeout).

%% @doc
%% insert_raw is provided for speed, but please take caution -- the caller is
%% responsible for passing in Json that is rethink-compatible! This means using
%% [2,] for arrays and specificying $reql_type$ manually, etc.
%%
%% If you get an error from this call like 
%% {error,{compile_error,<<"Expected a TermType as a NUMBER but found OBJECT.">>}}
%% it's likely that your input json is not rethink-compatible.
insert_raw(Re, Db, Table, Json, Opts, Timeout) ->
    gen_server:call(Re, {insert_raw, #{db => Db,
                                       table => Table,
                                       json => Json,
                                       opts => Opts,
                                       timeout => Timeout}}, ?CallTimeout).

feed_cursor(Re, Cursor, RecvToken) ->
    gen_server:cast(Re, {feed_cursor, #{cursor => Cursor,
                                        token => RecvToken}}).

close_cursor(Re, Cursor, RecvToken) ->
    gen_server:cast(Re, {close_cursor, #{cursor => Cursor,
                                         token => RecvToken}}).

close(Re) ->
    try gen_server:cast(Re, {close}),
        gen_server:stop(Re, normal, ?CallTimeout)
    catch _:_ ->
              ok
    end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(InitOptions=#{}) ->
    RecvBuffer = #{data => <<>>,  %% when token is undefined, data must be a binary.
                                  %% when token is defined, data can be an iolist or a binary.
                   token => undefined,
                   len => 0,
                   recv_size => 0},
    {ok, InitOptions#{recv_buffer => RecvBuffer,
                      socket => undefined,
                      token_idx => 1,
                      receivers => #{}}}.

handle_call({connect, #{address := Address,
                        port := Port,
                        options := Options,
                        timeout := Timeout}}, _From, State) ->
    TcpOptions = filter_tcp_options(maps:get(tcp_options, Options, [])),
    User = maps:get(user, Options, <<"admin">>),
    Password = maps:get(password, Options, <<>>),
    case gen_tcp:connect(Address, Port, TcpOptions, Timeout) of
        {ok, Socket} ->
            SocketState = State#{socket => Socket},
            Send1 = ?Magic,
            Resp = send_recv_null_term(Socket, Send1, Timeout),
            case expect_json_success(Resp) of
                {ok, _} ->
                    Method = rethink_scram:method(),
                    Nonce = rethink_scram:generate_nonce(),
                    ClientFirstMessageBare =
                            <<"n=", User/binary, ",r=", Nonce/binary>>,
                    ClientFirstMessage =
                            <<"n,,", ClientFirstMessageBare/binary>>,
                    SendCFM = [rethink:encode(#{protocol_version => 0,
                                     authentication_method => Method,
                                     authentication => ClientFirstMessage}), 0],
                    Resp2 = send_recv_null_term(Socket, SendCFM, Timeout),
                    case expect_json_success(Resp2) of
                        _Reply2={ok, #{<<"authentication">> := ServerFirstMessage}} ->
                            NonceSize = size(Nonce),
                            ServerNonce = <<Nonce:NonceSize/binary, _/binary>> =
                                rethink_scram:get_nonce(ServerFirstMessage),
                            Salt = base64:decode(rethink_scram:get_salt(ServerFirstMessage)),
                            IterationCount = rethink_scram:get_iteration_count(ServerFirstMessage),
                            ClientFinalMessageWithoutProof = <<"c=biws,r=", ServerNonce/binary>>,
                            {Proof, _, _} = rethink_scram:generate_client_proof(
                                      ClientFirstMessageBare,
                                      ServerFirstMessage,
                                      ClientFinalMessageWithoutProof,
                                      Password, Salt, IterationCount),
                            ClientFinalMessage = <<ClientFinalMessageWithoutProof/binary, ",p=", Proof/binary>>,
                            SendCFinM = [rethink:encode(#{authentication => ClientFinalMessage}), 0],
                            Resp3 = send_recv_null_term(Socket, SendCFinM, Timeout),
                            case expect_json_success(Resp3) of
                                _Reply3={ok, _} ->
                                    track_new_connection(SocketState),
                                    inet:setopts(Socket, [{active, once}]),
                                    {reply, ok, SocketState};
                                Er ->
                                    State2 = handle_call({close}, _From, SocketState),
                                    {reply, Er, State2}
                            end;
                        Er ->
                            State2 = handle_call({close}, _From, SocketState),
                            {reply, Er, State2}
                    end;
                Er ->
                    State2 = handle_call({close}, _From, SocketState),
                    {reply, Er, State2}
            end;
        Er ->
            {reply, Er, State}  
    end;
handle_call({run, #{reql := Reql,
                    timeout := Timeout}}, From, State=#{socket := Socket}) ->
    {TokenIdx, State2} = next_token_idx(State),
    Query = reql:wire(start, Reql, #{}),
    Size = iolist_size(Query),
    TokenBin = encode_unsigned(TokenIdx, 8, big),
    SizeBin = encode_unsigned(Size, 4, little),
    Packet = [TokenBin, SizeBin, Query],
    ok = send_query(Socket, Packet),
    {noreply, register_receiver(run, TokenBin, From, Timeout, State2)};
handle_call({insert_raw, #{db := Db,
                           table := Table,
                           json := Json,
                           opts := Opts,
                           timeout := Timeout}}, From, State=#{socket := Socket}) ->
    Reql = reql:db(Db),
    reql:table(Reql, Table),
    DbTableJson = reql:wire_raw(Reql),

    {TokenIdx, State2} = next_token_idx(State),

    % We need to compute the size of the query we're sending, so we're using
    % a function that will allow us to compute iolist_size on the query bytes
    % without the input Json and then add the size of the input json.
    QueryIoListFun = fun(X) -> [
             <<"[">>,
             rethink:encode(ql2:query_type(wire, start)),
             <<",">>,
                <<"[">>,
                    rethink:encode(ql2:term_type(wire, insert)),
                    <<",">>,
                        <<"[">>,
                        DbTableJson,
                        <<",">>,
                        X,
                        <<"]">>,
                    <<",">>,
                    rethink:encode(Opts), % insert opts
                <<"]">>,
             <<",">>,
             rethink:encode(#{}), % query start opts
            <<"]">>
        ]
    end,
    Size = iolist_size(QueryIoListFun(<<>>)) + size(Json),
    QueryIoList = QueryIoListFun(Json),

    TokenBin = encode_unsigned(TokenIdx, 8, big),
    SizeBin = encode_unsigned(Size, 4, little),
    Packet = [TokenBin, SizeBin, QueryIoList],
    ok = send_query(Socket, Packet),
    {noreply, register_receiver(run, TokenBin, From, Timeout, State2)};
handle_call({run_closure, #{reql := ReqlClosure,
                              args := Args,
                              timeout := Timeout}}, From, State=#{socket := Socket}) ->
    {TokenIdx, State2} = next_token_idx(State),
    Query = erlang:apply(ReqlClosure, Args),
    Size = iolist_size(Query),

    TokenBin = encode_unsigned(TokenIdx, 8, big),
    SizeBin = encode_unsigned(Size, 4, little),
    Packet = [TokenBin, SizeBin, Query],
    ok = send_query(Socket, Packet),
    {noreply, register_receiver(run, TokenBin, From, Timeout, State2)};
handle_call({reql_wire_query_type, #{query_type := QueryType,
                                     timeout := Timeout}},
                        From,
                       State=#{socket := Socket}) ->
    {TokenIdx, State2} = next_token_idx(State),
    Query = reql:wire(QueryType),
    Size = iolist_size(Query),
    TokenBin = encode_unsigned(TokenIdx, 8, big),
    SizeBin = encode_unsigned(Size, 4, little),
    Packet = [TokenBin, SizeBin, Query],
    ok = send_query(Socket, Packet),
    {noreply, register_receiver(run, TokenBin, From, Timeout, State2)};
handle_call({close}, _From, State=#{socket := Socket}) ->
    gen_tcp:close(Socket),
    {reply, ok, State#{socket => undefined}}.

handle_cast({feed_cursor, #{cursor := Cursor,
                       token := RecvToken}}, State=#{socket := Socket}) when is_binary(RecvToken) ->
    Query = reql:wire(continue),
    Size = iolist_size(Query),
    SizeBin = encode_unsigned(Size, 4, little),
    Packet = [RecvToken, SizeBin, Query],
    ok = send_query(Socket, Packet),
    {noreply, register_receiver(cursor, RecvToken, Cursor, infinity, State)};
handle_cast({close_cursor, #{cursor := Cursor,
                             token := RecvToken}}, State=#{socket := Socket}) when is_binary(RecvToken) ->
    Query = reql:wire(stop),
    Size = iolist_size(Query),
    SizeBin = encode_unsigned(Size, 4, little),
    Packet = [RecvToken, SizeBin, Query],
    ok = send_query(Socket, Packet),
    {noreply, register_receiver(cursor, RecvToken, Cursor, infinity, State)};
handle_cast({close}, State=#{socket := Socket}) ->
    gen_tcp:close(Socket),
    {noreply, State#{socket => undefined}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, State=#{socket := Socket,
                                         recv_buffer := RecvBuffer}) ->
    %io:format("recv ~p~n", [Data]),
    Notifier = fun(RecvToken, _Len, NData, StateIn) ->
                       notify_receiver(RecvToken, NData, StateIn)
               end,
    {NewRecvBuffer, State2} =
        handle_query_data(Notifier, RecvBuffer, Data, State),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State2#{recv_buffer => NewRecvBuffer}};
handle_info({tcp_closed, Socket}, State=#{socket := Socket}) ->
    {stop, closed, State};
handle_info({tcp_passsive, Socket}, State=#{socket := Socket}) ->
    {noreply, State};
handle_info({tcp_error, Socket, Reason}, State=#{socket := Socket}) ->
    {stop, Reason, State};
handle_info({receiver_timeout, RecvToken, TimeoutRef}, State) ->
    State2 = timeout_receiver(RecvToken, TimeoutRef, State),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
send_query(Socket, Packet) ->
    %io:format("send ~p~n", [Packet]),
    gen_tcp:send(Socket, Packet).

send_recv_null_term(Socket, Packet, Timeout) ->
    gen_tcp:send(Socket, Packet),
    case recv_null_term(Socket,  0, Timeout) of
        {ok, {[Response], <<>>}} ->
            {ok, rethink:decode(Response)};
        Er ->
            Er
    end.

handle_query_data(F, RecvBuffer=#{token := undefined,
                                  data := TokenData}, RecvData, State) when size(TokenData) + size(RecvData) >= 12 ->

    <<RecvToken:8/binary, Len:4/little-unsigned-integer-unit:8, Data/binary>>
               = iolist_to_binary([TokenData, RecvData]),

    %% We observed a len decoded as 578486272, which locks up the gen_rethink
    %% connection because it continuously waits for data to come in to fulfill that
    %% length. However, it appears that we received bad data from rethinkdb because
    %% at the time of observations, we didn't issue any queries that would have
    %% resulted in this much data. We suspect that there was an endianness bug. 
    %% As a workaround, we offer the maxlen env var, which can be optionally set to
    %% an upper bound expected for all queries on this instance of rethink-erlang.
    %%
    %% Note: may not be required any longer after fixing the pre-buffering of the token binary
    assert_maxlen(RecvToken, Len, State),

    %io:format("query data new token ~p ~p ~p~n", [RecvToken, Len, size(Data)]),
    handle_query_data(F, RecvBuffer#{token => RecvToken,
                                  len => Len,
                                  data => Data,
                                  recv_size => size(Data)}, <<>>,
                     State);
handle_query_data(F, RecvBuffer=#{token := RecvToken,
                               len := Len,
                               recv_size := RecvSize,
                               data := Data}, NewData, State) when Len > 0 andalso
                                                            RecvSize >= Len ->
    %io:format("query data notify ~p ~p ~p~n", [RecvToken, Len, RecvSize]),
    Data2 = iolist_to_binary(Data),
    {NotifyData, Remain} = {binary:part(Data2, {0, Len}),
                            binary:part(Data2, {Len, RecvSize-Len})},
    State2 = F(RecvToken, Len, NotifyData, State),
    NewData2 = iolist_to_binary([Remain, NewData]),
    handle_query_data(F, RecvBuffer#{token => undefined,
                                  len => 0,
                                  data => <<>>,
                                  recv_size => 0}, NewData2, State2);
handle_query_data(_F, RecvBuffer, <<>>, State) ->
    %io:format("query data no data~n", []),
    {RecvBuffer, State};
handle_query_data(F, RecvBuffer=#{token := _RecvToken,
                                  len := Len,
                               recv_size := RecvSize,
                               data := Data}, NewData, State) when Len > 0 andalso
                                                            RecvSize < Len ->
    %io:format("query data incomplete ~p ~p ~p~n", [_RecvToken, Len, RecvSize]),
    MaxData = Len - RecvSize,
    SizeNew = size(NewData),
    {SizeAdd, BufferAdd, Remain} = if SizeNew > MaxData ->
           {MaxData,
            binary:part(NewData, {0, MaxData}),
            binary:part(NewData, {MaxData, SizeNew - MaxData})};
       true ->
           {SizeNew, NewData, <<>>}
    end,
    handle_query_data(F, RecvBuffer#{recv_size => RecvSize + SizeAdd,
                                  data => [Data, BufferAdd]}, Remain, State);
handle_query_data(_F, RecvBuffer=#{token := undefined, data := TokenData}, Data, State) ->
    %% We received some new data but it's not enough bytes to tell us the token+len yet,
    %% so we have to buffer the data with an undefined token, and an unknown len
    {RecvBuffer#{data => iolist_to_binary([TokenData, Data])}, State}.

recv_null_term(Socket, Length, Timeout) ->
    case gen_tcp:recv(Socket, Length, Timeout) of
        {ok, Packet} ->
            {Split, Rem} = split_packet(Packet, [<<>>]),
            {ok, {Split, Rem}};
        Er ->
            Er
    end.

split_packet(<<>>, [<<>>|T]) ->
    {lists:reverse(T), <<>>};
split_packet(<<>>, [H|T]) ->
    {lists:reverse(T), H};
split_packet(<<0:8, Rest/binary>>, L) ->
    split_packet(Rest, [<<>>|L]);
split_packet(<<B:8, Rest/binary>>, [H|T]) ->
    split_packet(Rest, [<<H/binary, B>>|T]).

next_token_idx(State=#{token_idx := TokenIdx}) ->
    case TokenIdx of
        16#FFFFFFFFFFFFFFFF ->
            {0, State#{token_idx => 1}};
        _ ->
            {TokenIdx, State#{token_idx => TokenIdx+1}}
    end.

encode_unsigned(Int, Num, Endian) ->
    Bytes = binary:encode_unsigned(Int, Endian),
    if
        size(Bytes) > Num ->
            erlang:error(badarg);
        true ->
            bin_pad(Bytes, Num, Endian)
    end.

bin_pad(Bin, Num, little) ->
    Pad = size(Bin) rem Num,
    <<Bin/binary, 0:((Num-Pad)*8)>>;
bin_pad(Bin, Num, big) ->
    Pad = size(Bin) rem Num,
    <<0:((Num-Pad)*8), Bin/binary>>.

expect_json_success(Success={ok, #{<<"success">> := true}}) ->
    Success;
expect_json_success({ok, Fail=#{<<"success">> := _}}) ->
    {error, Fail};
expect_json_success(Error) ->
    Error.

expect_query_response(Resp) ->
    %io:format("decoding ~p~n", [Resp]),
    {ok, rethink:decode(Resp)}.

register_receiver(Type, RecvToken, From, Timeout, State) ->
    %io:format("reg  ~p ~p ~p ~p~n", [Type, RecvToken, From, Timeout]),
    TRef = case Timeout of
        infinity ->
            undefined;
        _ ->
            % We send a unique ref with the timeout event because the token
            % can be reused, and if this timer event fails to be canceled, it
            % would potentially cancel a future request.
            TimeoutRef = make_ref(),
            CancelRef = erlang:send_after(Timeout, self(),
                                          {receiver_timeout, RecvToken, TimeoutRef}),
            {TimeoutRef, CancelRef}
    end,
    MaxLen = application:get_env(rethink, maxlen, 16#FFFFFFFF),
    add_or_update_receiver(RecvToken, #receiver{type=Type, from=From,
                                            timeout=Timeout, tref=TRef, maxlen=MaxLen}, State).

timeout_receiver(RecvToken, TimeoutRef, State=#{receivers := Receivers}) ->
    TimeoutError = {error, timeout},
    case maps:find(RecvToken, Receivers) of
        {ok, #receiver{type=run, from=From, timeout=_Timeout, tref={TimeoutRef, _}}} ->
            gen_server:reply(From, TimeoutError),
            remove_receiver(RecvToken, State);
        {ok, #receiver{type=cursor, from=Cursor, timeout=_Timeout, tref={TimeoutRef, _}}} ->
            rethink_cursor:update_error(Cursor, TimeoutError),
            remove_receiver(RecvToken, State);
        _ ->
            State
    end.

add_or_update_receiver(RecvToken, Receiver, State=#{receivers := Receivers}) ->
    track_waiting_token(RecvToken, State),
    State#{receivers => Receivers#{ RecvToken => Receiver }}.

remove_receiver(RecvToken, State=#{receivers := Receivers}) ->
    track_completed_token(RecvToken, State),
    State#{receivers => maps:remove(RecvToken, Receivers)}.

track_new_connection(#{stats_table := StatsTable}) ->
    %% Updating stats with 0 initializes the pid with 'ok' status
    update_stats(waiting, 0, StatsTable);
track_new_connection(_) ->
    ok.

track_waiting_token(RecvToken, #{stats_table := StatsTable,
                             receivers := Receivers}) ->
    case maps:find(RecvToken, Receivers) of
        {ok, _} ->
            %% already tracked
            ok;
        error ->
            update_stats(waiting, 1, StatsTable)
    end;
track_waiting_token(_, _) ->
    ok.

track_completed_token(RecvToken, #{stats_table := StatsTable,
                               receivers := Receivers}) ->
    case maps:find(RecvToken, Receivers) of
        {ok, _} ->
            update_stats(waiting, -1, StatsTable),
            update_stats(completed, 1, StatsTable);
        error ->
            ok
    end;
track_completed_token(_, _) ->
    ok.

update_stats(Key, Value, Table) ->
    ets:update_counter(Table, self(), {stats_pos(Key), Value}, {self(), ok, 0, 0}).

stats_pos(waiting) -> 3;
stats_pos(completed) -> 4.

cancel_timer({_, CancelRef}) -> erlang:cancel_timer(CancelRef);
cancel_timer(_) -> ok.

notify_receiver(RecvToken, Resp, State=#{receivers := Receivers}) ->
    %io:format("notify ~p ~p ~p~n", [RecvToken, Resp, Receivers]),
    UpdatedReceiver = case maps:find(RecvToken, Receivers) of
        {ok, Receiver=#receiver{type=run, from=From, timeout=Timeout, tref=TRef}} ->
            cancel_timer(TRef),
            UpdatedReceiver_0 = Receiver#receiver{tref=undefined},
            {Reply, UpdatedReceiver_1} = case expect_query_response(Resp) of
                {ok, _FullResp=#{<<"t">> := ResponseType,
                       <<"r">> := Result}} ->
                    case ql2:response_type(human, ResponseType) of
                        success_atom ->
                            {{ok, hd(Result)},
                             undefined};
                        success_sequence ->
                            {{ok, rethink_cursor:make(self(), RecvToken,
                                                     success_sequence, Result,
                                                     Timeout)},
                             undefined};
                        success_partial ->
                            {{ok, rethink_cursor:make(self(), RecvToken,
                                                     success_partial, Result,
                                                     Timeout)},
                             UpdatedReceiver_0};
                        wait_complete ->
                            {ok,
                             undefined};
                        server_info ->
                            {{ok, hd(Result)},
                             undefined};
                        Error ->
                            {{error, {Error, hd(Result)}},
                             undefined}
                    end;
                Err ->
                    {Err, undefined}
            end,
            gen_server:reply(From, Reply),
            UpdatedReceiver_1;
        {ok, Receiver=#receiver{type=cursor, from=Cursor, tref=TRef}} ->
            cancel_timer(TRef),
            UpdatedReceiver_0 = Receiver#receiver{tref=undefined},
            case expect_query_response(Resp) of
                {ok, #{<<"t">> := ResponseType,
                       <<"r">> := Result}} ->
                    case ql2:response_type(human, ResponseType) of
                        success_sequence ->
                            rethink_cursor:update_success(Cursor,
                                                          success_sequence,
                                                          Result),
                            undefined;
                        success_partial ->
                            rethink_cursor:update_success(Cursor,
                                                          success_partial,
                                                          Result),
                            UpdatedReceiver_0;
                        client_error ->
                            rethink_cursor:update_error(Cursor, {error, Result}),
                            undefined
                    end;
                Err ->
                    rethink_cursor:update_error(Cursor, Err),
                    undefined
            end;
        _ ->
            undefined
    end,
    case UpdatedReceiver of
        undefined ->
            remove_receiver(RecvToken, State);
        _ ->
            add_or_update_receiver(RecvToken, UpdatedReceiver, State)
    end.

filter_tcp_options(TcpOptions) ->
    TcpOptions2 = proplists:delete(active, TcpOptions),
    TcpOptions3 = proplists:delete(binary, TcpOptions2),
    TcpOptions4 = [{active, false}, binary|TcpOptions3],
    TcpOptions4.

filter_init_options(ConnectionOptions) ->
    maps:with([stats_table], ConnectionOptions).

assert_maxlen(RecvToken, Len, #{receivers := Receivers}) ->
    case maps:find(RecvToken, Receivers) of
        {ok, #receiver{maxlen=MaxLen}} when Len > MaxLen ->
            erlang:error(maxlen_exceeded);
        _ ->
            %% There is no receiver registered for this token.
            %% Ideally we would want our buffer to collect this
            %% response and then discard is quietly, but we also
            %% need to protect against the maxlen bug from rethink
            MaxLen = application:get_env(rethink, maxlen, 16#FFFFFFFF),
            if Len > MaxLen ->
                   erlang:error(maxlen_exceeded);
               true ->
                   ok
            end
    end.

