-module(gen_rethink_session).
-behaviour(gen_requery).

-export([start_link/0, start_link/1, start_link/2, get_connection/1, default_stats_table/0]).

-export([handle_connection_up/2,
         handle_connection_down/1,
         handle_query_result/2,
         handle_query_done/1,
         handle_query_error/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(CallTimeout, timer:hours(1)).

default_stats_table() ->
    rethink_session_stats.

start_link() ->
    start_link(#{}).

start_link(Options) ->
    gen_requery:start_link(?MODULE, [ensure_stats_table(Options)], []).

start_link(Name, Options) ->
    gen_requery:start_link({local, Name}, ?MODULE, [ensure_stats_table(Options)], []).

get_connection(Svr) ->
    gen_requery:call(Svr, {get_connection}, ?CallTimeout).

init([Options=#{stats_table := StatsTable}]) ->
    {ok, Options, #{connection => undefined, stats_table => StatsTable}}.

handle_connection_up(Connection, State) ->
    {noreply, State#{connection => Connection}}.

handle_connection_down(State=#{stats_table := StatsTable}) ->
    ets:delete(StatsTable, maps:get(connection, State, undefined)),
    {noreply, State#{connection => undefined}}.

handle_query_result(_, State) ->
    {noreply, State}.

handle_query_done(State) ->
    {noreply, State}.

handle_query_error(_, State) ->
    {noreply, State}.

handle_call({get_connection}, _From, State=#{connection := Connection}) when is_pid(Connection) ->
    {reply, {ok, Connection}, State};
handle_call({get_connection}, _From, State) ->
    {reply, {error, no_connection}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

ensure_stats_table(Options=#{stats_table := _}) ->
    Options;
ensure_stats_table(Options) ->
    Options#{stats_table => default_stats_table()}.
