-module(reql).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-compile({no_auto_import,[apply/2]}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

% Accessing ReQL
-export([x/1, new/0, new/1, new/2, var/1, clone/1, apply/2, closure/2, closure/3, changes/1, changes/2]).

% Manipulating databases
-export([db_create/2, db_drop/2, db_list/1]).

% Manipulating tables
-export([table_create/2, table_create/3, table_drop/2, table_list/1,
        index_create/2, index_create/3, index_drop/2, index_wait/2, index_list/1,
        index_status/1, index_status/2, index_create/4, index_rename/3, index_rename/4,
        index_wait/1]).

% Writing data
-export([insert/2, insert/3, update/2, update/3, replace/2, replace/3,
         delete/1, delete/2, sync/1]).

% Selecting data
-export([db/1, db/2, table/2, table/3, 'get'/2, get_all/2, get_all/3, between/3,
         between/4, filter/2, filter/3]).

% Joins
-export([inner_join/3, outer_join/3, eq_join/3, eq_join/4, zip/1]).

% Transformations
-export([map/2, map/3, map/4, map/5, with_fields/2, with_fields/3, with_fields/4,
         with_fields/5, concat_map/2, order_by/2, order_by/3, skip/2, limit/2,
         slice/2, slice/3, slice/4, nth/2, offsets_of/2, is_empty/1, union/2,
         union/3, sample/2, desc/1, asc/1]).

% Aggregation
-export([group/2, group/3, ungroup/1, reduce/2, reduce/3, fold/3, fold/4, count/1,
         count/2, sum/1, sum/2, sum/3, avg/1, avg/2, avg/3, 'min'/1, 'min'/2,
         'max'/1, 'max'/2, distinct/1, distinct/2, contains/2]).

% Document manipulation
-export([pluck/2, pluck/3, pluck/4, without/2, without/3, without/4,
         merge/2, merge/3, merge/4, append/2, prepend/2, difference/2,
         set_insert/2, set_union/2, set_intersection/2, set_difference/2,
         bracket/2, get_field/2, has_fields/2, has_fields/3, has_fields/4,
         insert_at/3, splice_at/3, delete_at/2, delete_at/3, change_at/3,
         keys/1, values/1, literal/2, object/2]).

% String manipulation
-export([match/2, split/1, split/2, split/3, upcase/1, downcase/1]).

% Math and logic
-export([add/2, add/3, sub/2, sub/3, mul/2, mul/3, 'div'/2, 'div'/3, mod/2,
         mod/3, 'and'/2, 'and'/3, 'or'/2, 'or'/3, eq/2, eq/3, ne/2, ne/3,
         gt/2, gt/3, ge/2, ge/3, lt/2, lt/3, le/2, le/3, 'not'/1, 'not'/2,
         random/1, random/2, random/3, random/4, 'round'/1, ceil/1, floor/1]).

% Dates and times
-export([now/1, time/5, time/8, epoch_time/2, iso8601/1,
         in_timezone/2, timezone/1, during/3, during/4, date/1, time_of_day/1,
         year/1, month/1, day/1, day_of_week/1, day_of_year/1, hours/1,
         minutes/1, seconds/1, to_iso8601/1, to_epoch_time/1]).

% Control structures
-export([args/1, binary/1, do/1, do/2, do/3, for_each/2, range/1, range/2,
         range/3, default/2, expr/1, expr/2, javascript/2, javascript/3, coerce_to/2,
         type_of/1, info/1, json/2, to_json_string/1, http/2, http/3, uuid/1,
         uuid/2, branch/3, error/1]).

% Geospatial commands
-export([circle/3, circle/4, distance/3, distance/4, fill/1, geojson/1,
         to_geojson/1, get_intersecting/3, get_nearest/3, includes/2,
         intersects/2, intersects/3, line/2, line/3, point/3, polygon/2,
         polygon_sub/2]).

% Administration
-export([grant/3, config/1, rebalance/1, reconfigure/2, status/1, wait/1, wait/2]).

-export([hold/1, release/1, wire/1, wire/3, wire_raw/1, q/1, prepare_query/1, allocate_var/1, resolve/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(reql, {cmd,
               args,
               opts}).

-define(IdleKill, 0).
-define(CallTimeout, timer:hours(1)).
-define(VarMax, 255).

% REQL API code generation
-define(REQL0(N),   N(R) when is_pid(R) -> call_cmd(R, N, [])).
-define(REQL0_O(N), N(R, O) when is_pid(R) -> call_cmd(R, N, [], O)).
-define(REQL1(N),   N(R, X) when is_pid(R) -> call_cmd(R, N, [X])).
-define(REQL1_O(N), N(R, X, O) when is_pid(R) -> call_cmd(R, N, [X], O)).
-define(REQL2(N),   N(R, X, Y) when is_pid(R) -> call_cmd(R, N, [X, Y])).
-define(REQL2_O(N), N(R, X, Y, O) when is_pid(R) -> call_cmd(R, N, [X, Y], O)).
-define(REQL3(N),   N(R, X, Y, Z) when is_pid(R) -> call_cmd(R, N, [X, Y, Z])).
-define(REQL4(N),   N(R, X, Y, Z, W) when is_pid(R) -> call_cmd(R, N, [X, Y, Z, W])).

-define(REQL1_LIST(N), N(R, L) when is_pid(R) andalso is_list(L) -> call_cmd(R, N, L);
                       N(R, X) when is_pid(R) -> call_cmd(R, N, [X])).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

% Accessing ReQL

%% @doc reql:x/1 is mostly provided as a covenience for query writing in a shell.
%% It allows the caller to do this:
%%    Reql = reql:x(fun(X) ->
%%                      reql:db(X, my_db),
%%                      reql:table(X, my_table))
%%                  end).
%% Which keeps the query nicely contained and avoids extra var bindings.
x(Fun) ->
    R = new(),
    Fun(R),
    R.

new() -> 
    {ok, R} = gen_server:start_link(?MODULE, [], []),
    R.
new(List) when is_list(List) ->
    R = new(),
    apply(R, List).

new(Db, Table) ->
    new([{db, Db}, {table, Table}]).

%% @doc due to the stateful nature of the reql process, complex queries
%% may need to clone the reql process at strategic times to snapshot the
%% object. For example, if reql:bracket must be used twice in a single
%% anonymous function, the input var must be cloned at least once, because
%% simply calling reql:bracket changes the underlying state.
clone(Src) ->
    {ok, R} = gen_server:start_link(?MODULE, [], []),
    SrcState = gen_server:call(Src, get_state, ?CallTimeout),
    gen_server:cast(R, {set_state, SrcState}),
    R.

apply(R, []) ->
    R;
apply(R, [{F, A}|List]) when is_list(A) ->
    erlang:apply(?MODULE, F, [R|A]),
    apply(R, List);
apply(R, [{F, A}|List]) ->
    erlang:apply(?MODULE, F, [R,A]),
    apply(R, List).

var(X) ->
    R = new(),
    call_cmd(R, var, [X]).

?REQL0(changes).
?REQL0_O(changes).

% Manipulating databases
?REQL1(db_create).
?REQL1(db_drop).
?REQL0(db_list).

% Manipulating tables
?REQL1(table_create).
?REQL1_O(table_create).
?REQL1(table_drop).
?REQL0(table_list).
?REQL1(index_create).
?REQL1(index_drop).
?REQL0(index_wait).
?REQL1(index_wait).
?REQL1(index_status).
?REQL0(index_list).
?REQL0(index_status).
index_create(R, Attr, O) when is_map(O) -> call_cmd(R, index_create, [Attr], O);
index_create(R, Attr, IndexFunc) -> call_cmd(R, index_create, [Attr, IndexFunc]).
?REQL2_O(index_create).
?REQL2(index_rename).
?REQL2_O(index_rename).

% Writing data
?REQL1(insert).
?REQL1_O(insert).
?REQL1(update).
?REQL1_O(update).
?REQL1(replace).
?REQL1_O(replace).
?REQL0(delete).
?REQL0_O(delete).
?REQL0(sync).

% Selecting data
db(DB) ->
    {ok, R} = gen_server:start_link(?MODULE, [], []),
    db(R, DB).
?REQL1(db).
?REQL1(table).
?REQL1_O(table).
?REQL1('get').
?REQL1(get_all).
?REQL1_O(get_all).
?REQL2(between).
?REQL2_O(between).
?REQL1(filter).
?REQL1_O(filter).

% Joins
?REQL2(inner_join).
?REQL2(outer_join).
?REQL2(eq_join).
?REQL2_O(eq_join).
?REQL0(zip).

% Transformations
?REQL1(map).
?REQL2(map).
?REQL3(map).
?REQL4(map).
?REQL1_LIST(with_fields).
?REQL2(with_fields).
?REQL3(with_fields).
?REQL4(with_fields).
?REQL1(concat_map).
?REQL1(order_by).
?REQL1_O(order_by).
?REQL1(skip).
?REQL1(limit).
?REQL1(slice).
?REQL2(slice).
?REQL2_O(slice).
?REQL1(nth).
?REQL1(offsets_of).
?REQL0(is_empty).
?REQL1(union).
?REQL1_O(union).
?REQL1(sample).
desc(X) -> {desc, X}.
asc(X) -> {asc, X}.

% Aggregation
?REQL1(group).
?REQL1_O(group).
?REQL0(ungroup).
?REQL1(reduce).
?REQL2(reduce).
?REQL2(fold).
?REQL2_O(fold).
?REQL0(count).
?REQL1(count).
?REQL0(sum).
?REQL1(sum).
?REQL2(sum).
?REQL0(avg).
?REQL1(avg).
?REQL2(avg).
?REQL0('min').
?REQL0_O('min').
?REQL0('max').
?REQL0_O('max').
?REQL0(distinct).
?REQL0_O(distinct).
?REQL1(contains).

% Document manipulation
?REQL1_LIST(pluck).
?REQL2(pluck).
?REQL3(pluck).
?REQL1_LIST(without).
?REQL2(without).
?REQL3(without).
?REQL1_LIST(merge).
?REQL2(merge).
?REQL3(merge).
?REQL1(append).
?REQL1(prepend).
?REQL1(difference).
?REQL1(set_insert).
?REQL1(set_union).
?REQL1(set_intersection).
?REQL1(set_difference).
?REQL1(bracket).
?REQL1(get_field).
?REQL1_LIST(has_fields).
?REQL2(has_fields).
?REQL3(has_fields).
?REQL2(insert_at).
?REQL2(splice_at).
?REQL1(delete_at).
?REQL2(delete_at).
?REQL2(change_at).
?REQL0(keys).
?REQL0(values).
?REQL1(literal).
?REQL1_LIST(object).

% String manipulation
?REQL1(match).
?REQL0(split).
?REQL1(split).
?REQL2(split).
?REQL0(upcase).
?REQL0(downcase).

% Math and logic
?REQL1(add).
?REQL2(add).
?REQL1(sub).
?REQL2(sub).
?REQL1(mul).
?REQL2(mul).
?REQL1('div').
?REQL2('div').
?REQL1(mod).
?REQL2(mod).
?REQL1('and').
?REQL2('and').
?REQL1('or').
?REQL2('or').
?REQL1(eq).
?REQL2(eq).
?REQL1(ne).
?REQL2(ne).
?REQL1(gt).
?REQL2(gt).
?REQL1(ge).
?REQL2(ge).
?REQL1(lt).
?REQL2(lt).
?REQL1(le).
?REQL2(le).
?REQL0('not').
?REQL1('not').
?REQL0(random).
?REQL1(random).
?REQL2(random).
?REQL2_O(random).
?REQL0(round).
?REQL0(ceil).
?REQL0(floor).

% Dates and times
?REQL0(now).
?REQL4(time).
time(R, Y, M, D, H, Mi, S, TZ) -> call_cmd(R, time, [Y, M, D, H, Mi, S, TZ]).
?REQL1(epoch_time).
?REQL1(in_timezone).
?REQL0(timezone).
?REQL2(during).
?REQL2_O(during).
?REQL0(date).
?REQL0(time_of_day).
?REQL0(year).
?REQL0(month).
?REQL0(day).
?REQL0(day_of_week).
?REQL0(day_of_year).
?REQL0(hours).
?REQL0(minutes).
?REQL0(seconds).
?REQL0(to_iso8601).
?REQL0(to_epoch_time).

% Control structures
args(X) -> X.
binary(X) -> {binary, X}.
iso8601(X) -> {iso8601, X}.
do(Fun) -> do([], Fun).
do(Args, Fun) when is_list(Args) ->
    {ok, R} = gen_server:start_link(?MODULE, [], []),
    do(R, Args, Fun);
do(R, Fun) ->
    do(R, [], Fun).
?REQL2(do).
?REQL1(for_each).
?REQL0(range).
?REQL1(range).
?REQL2(range).
?REQL1(default).
expr(X) ->
    {ok, R} = gen_server:start_link(?MODULE, [], []),
    expr(R, X).
expr(R, X) -> call_cmd(R, datum, [X]).
?REQL1(javascript).
?REQL1_O(javascript).
?REQL1(coerce_to).
?REQL0(type_of).
?REQL0(info).
?REQL1(json).
?REQL0(to_json_string).
?REQL1(http).
?REQL1_O(http).
?REQL0(uuid).
?REQL1(uuid).
branch(Test, TrueAction, FalseAction) ->
    {ok, R} = gen_server:start_link(?MODULE, [], []),
    call_cmd(R, branch, [Test, TrueAction, FalseAction]).
error(Message) ->
    {ok, R} = gen_server:start_link(?MODULE, [], []),
    call_cmd(R, error, [Message]).

% Geospatial commands
?REQL2(circle).
?REQL2_O(circle).
?REQL1_O(distance).
?REQL2_O(distance).
?REQL0(fill).
?REQL0(geojson).
?REQL0(to_geojson).
?REQL1_O(get_intersecting).
?REQL1_O(get_nearest).
?REQL1(includes).
?REQL1(intersects).
?REQL2(intersects).
?REQL1_LIST(line).
?REQL2(line).
?REQL2(point).
?REQL1_LIST(polygon).
?REQL1(polygon_sub).

% Administration
?REQL1_O(grant).
?REQL0(config).
?REQL0(rebalance).
?REQL0_O(reconfigure).
?REQL0(status).
?REQL0(wait).
?REQL0_O(wait).

%%

hold(R) -> gen_server:call(R, {refcount, 1}, ?CallTimeout).
release(R) -> gen_server:call(R, {refcount, -1}, ?CallTimeout).

%% resolve reql pid as a query into rethinkdb
resolve(R) ->
    prepare_query(q(R)).

wire(continue) ->
    rethink:encode([ql2:query_type(wire, continue)]);
wire(server_info) ->
    rethink:encode([ql2:query_type(wire, server_info)]).

wire(QueryType, R, Opts) ->
    rethink:encode([ql2:query_type(wire, QueryType),
                    resolve(R),
                    Opts]).

wire_raw(R) ->
    rethink:encode(resolve(R)).

closure(R, Cmd) ->
    closure(R, Cmd, #{}).

closure(R, Cmd, Opts) ->
    StartRaw = rethink:encode(ql2:query_type(wire, start)),
    CmdRaw = rethink:encode(ql2:term_type(wire, Cmd)),
    EmptyOptsRaw = rethink:encode(#{}),
    CmdOptsRaw = rethink:encode(prepare_opts(Opts)),
    Inner = wire_raw(R),
    FirstPart = iolist_to_binary([
             <<"[">>,
             StartRaw,
             <<",">>,
                <<"[">>,
                    CmdRaw,
                    <<",">>,
                        <<"[">>,
                        Inner,
                        <<",">>
                ]),
    LastPart = iolist_to_binary([ <<"]">>,
                    <<",">>,
                    CmdOptsRaw, % command opts
                <<"]">>,
             <<",">>,
             EmptyOptsRaw, % query start opts
            <<"]">>]),
                
    fun(X) -> [FirstPart,
               rethink:encode(ql2:datum(X)),
               LastPart]
    end.

q(R) when is_pid(R) ->
    % refcount is checked when q/1 is called
    gen_server:call(R, {q}, ?CallTimeout);
q(R) ->
    R.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    % By default this process will hang around indefinitely, or if linked, will
    % get cleaned up when the caller process dies. The caller can optionally
    % use the ref counting here to clean up manually.
    {ok, #{args => Args,
           refcount => 0,
           q => undefined}}.

handle_call({Cmd, Args, Opts}, _From, State=#{q := undefined}) ->
    {reply, self(), State#{q => #reql{ cmd = Cmd, args = Args, opts = Opts }}};
handle_call({Cmd, Args, Opts}, _From, State=#{q := Q}) ->
    {reply, self(), State#{q => #reql{ cmd = Cmd, args = [Q|Args], opts = Opts}}};
handle_call({q}, _From, State=#{q := Q}) ->
    reset_idle_timer(
      {reply, Q, State});
handle_call({refcount, C}, _From, State=#{refcount := RefCount}) ->
    reset_idle_timer(
      {reply, self(), State#{refcount => RefCount + C}});
handle_call(get_state, _From, State) ->
    reset_idle_timer({reply, State, State}).

handle_cast({set_state, State}, _State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
call_cmd(R, Cmd, Args) ->
    call_cmd(R, Cmd, Args, undefined).

call_cmd(R, Cmd, Args, Opts) ->
    Args2 = lists:map(
                  fun(X) when is_pid(X) ->
                          % Arg is itself a reql object, get its contents
                          q(X);
                     (X) ->
                          X
                  end, Args),
    gen_server:call(R, {Cmd, Args2, Opts}, ?CallTimeout).

prepare_query(#reql{cmd=datum, args=[X]}) ->
    prepare_query(X);
prepare_query(#reql{cmd=do, args=[Args, Fun], opts=Opts}) when is_function(Fun) ->
    Func = prepare_func(Fun),
    FunCall = #reql{cmd=funcall, args=[Func] ++ Args, opts=Opts},
    Result = prepare_query(FunCall),
    Result;
prepare_query(#reql{cmd=Cmd, args=Args, opts=Opts}) ->
    List = [ prepare_query(X) || X <- Args ],
    case Opts of
        undefined ->
            [ ql2:term_type(wire, Cmd), List ];
        _ ->
            [ ql2:term_type(wire, Cmd), List, prepare_opts(Opts) ]
    end;
prepare_query(Fun) when is_function(Fun) ->
    Func = prepare_func(Fun),
    prepare_query(Func);
prepare_query(X) ->
    ql2:datum(X).

prepare_opts(undefined) -> undefined;
prepare_opts(Map) when is_map(Map) ->
    maps:map(
      fun(_K, Fun) when is_function(Fun) ->
              prepare_query(prepare_func(Fun));
         (_K, V) ->
              V
      end, Map).

prepare_func(Fun) ->
    FunInfo = erlang:fun_info(Fun),
    NumVars = proplists:get_value(arity, FunInfo),
    VarIndexes = allocate_var(NumVars),
    Vars = lists:map(
             fun(X) ->
                     V = reql:var(X),
                     reql:hold(V),
                     V
             end, VarIndexes),
    FunReql = erlang:apply(Fun, Vars),
    FunQ = reql:q(FunReql),
    [ reql:release(X) || X <- Vars ],
    Func = #reql{cmd=func, args=[VarIndexes, FunQ]},
    Func.

reset_idle_timer({reply, Reply, State=#{refcount := Ref}}) when Ref > 0 ->
    {reply, Reply, State};
reset_idle_timer({reply, Reply, State}) ->
    {reply, Reply, State, ?IdleKill};
reset_idle_timer({noreply, State=#{refcount := Ref}}) when Ref > 0 ->
    {noreply, State};
reset_idle_timer({noreply, State}) ->
    {noreply, State, ?IdleKill}.

allocate_var(Num) when Num < ?VarMax ->
    % For debuggability, etc, make sure the vars are continuous and do not
    % wrap around
    CounterVal = rethink:counter(var, Num, ?VarMax),
    if CounterVal < Num ->
           allocate_var(Num);
       true ->
           lists:seq(1+CounterVal - Num, CounterVal)
    end.
