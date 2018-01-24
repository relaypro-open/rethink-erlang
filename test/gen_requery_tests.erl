-module(gen_requery_tests).
-behaviour(gen_requery).

% This test module serves as a unit test fo gen_requery and a resource
% to showcase an example implementation.

% These are the API functions. You will implement your own API, depending on
% what you want your module to do.
-export([start_link/0, state/1, run_test/0]).

% These are the standard gen_server callback functions
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

% These are the callback function specific to the gen_requery behavior
-export([handle_connection_up/2,
         handle_connection_down/1,
         handle_query_result/2,
         handle_query_done/1,
         handle_query_error/2]).

%% @doc eunit function to run unit test for gen_requery
run_test() ->
    % Starting the gen_requery process will create and manage
    % a connection based on the ConnectOptions provided in the init/1
    % callback.
    {ok, Pid1} = start_link(),
    {ok, Pid2} = start_link(),

    % Originally we had a timer:sleep here, but it's not necessary. gen_requery
    % will flush initial results immediately, meaning our call to state/1 cannot
    % pre-empt the handling of the first batch of results.

    % Acquire the state of the gen_requery process and make sure
    % it has some valid data
    [#{<<"new_val">> := _}|_] = lists:flatten(state(Pid1)),
    [#{<<"new_val">> := _}|_] = lists:flatten(state(Pid2)).

start_link() ->
    gen_requery:start_link(?MODULE, [], []).

state(Ref) ->
    gen_requery:call(Ref, state, infinity).

init([]) ->
    % The ConnectOptions are provided to gen_rethink:connect_unlinked
    ConnectOptions = #{},

    % The State is up to you -- it can be any term
    State = [],

    {ok, ConnectOptions, State}.

%% @doc handle_connection_up/2 is called with a valid Connection pid whenever
%% the managed connection is newly established
handle_connection_up(_Connection, State) ->
    Reql = reql:db(<<"rethinkdb">>),
    reql:table(Reql, <<"users">>),
    reql:changes(Reql, #{<<"include_initial">> => true}),
    {noreply, Reql, State}.

%% @doc handle_connection_down/1 is called when the managed connection goes
%% down unexpectedly. The gen_requery implementation will then enter a
%% reconnect state with exponential backoffs. Your module can still process
%% requests during this time.
handle_connection_down(State) ->
    {noreply, State}.

%% @doc handle_query_result is called each time we receive a result from
%% the rethinkdb server. The contents of the result depend greatly on
%% the ReQL being executed
handle_query_result(Result, State) ->
    {noreply, [Result|State]}.

%% @doc handle_query_done is called when the ReQL query is finished. For
%% changefeeds, this should never be called
handle_query_done(State) ->
    {stop, changefeeds_should_never_be_done, State}.

%% @doc handle_query_error is called when the server encounters an error
%% with the given ReQL
handle_query_error(Error, State) ->
    {stop, Error, State}.

handle_call(state, _From, State) ->
    {reply, State, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.  
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

