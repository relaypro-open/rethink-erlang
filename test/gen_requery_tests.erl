-module(gen_requery_tests).
-behaviour(gen_requery).

-export([start_link/0, run/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([handle_query_result/2,
         handle_query_done/1,
         handle_query_error/2]).

-compile([export_all]).

start_link_test() ->
    {ok, Connection} = gen_rethink:connect("localhost", 28015, [], 1000),
    {ok, Ref} = start_link(),
    ok = run(Ref, Connection, 1000),
    timer:sleep(200),
    [#{<<"new_val">> := _}|_] = lists:flatten(lists:reverse(state(Ref))).

start_link() ->
    gen_requery:start_link(?MODULE, [], []).

run(Ref, Connection, Timeout) ->
    gen_requery:run(Ref, Connection, Timeout).

reql() ->
    Reql = reql:db(<<"rethinkdb">>),
    reql:table(Reql, <<"users">>),
    reql:changes(Reql, #{<<"include_initial">> => true}).

state(Ref) ->
    gen_server:call(Ref, state, infinity).

init([]) ->
    {ok, reql(), []}.

handle_query_result(Result, State) ->
    {noreply, [Result|State]}.

handle_query_done(State) ->
    {stop, changefeeds_shouldnt_be_done, State}.

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

