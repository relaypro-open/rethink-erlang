rethink-erlang
==============
An Erlang/OTP driver for RethinkDB using maps and supporting authentication
Version V1_0.

*Quick example:*
```
1> {ok, Connection} = gen_rethink:connect().
{ok,<0.386.0>}
2> Reql = fun(X) ->
2>          reql:db(X, rethinkdb),
2>          reql:table(X, users),
2>          reql:get(X, admin)
2>        end.
#Fun<erl_eval.6.118419387>
3> gen_rethink:run(Connection, Reql).
{ok,#{<<"id">> => <<"admin">>,<<"password">> => false}}
```

Getting Started
---------------
```
make
rebar3 shell
```

Connecting to a RethinkDB instance
----------------------------------
```
{ok, Connection} = gen_rethink:connect("localhost", 28015).
```
or
```
{ok, Connection} = gen_rethink:connect().
```

Managing a persistent connection
--------------------------------
Use the `gen_rethink_session` gen_server to manage a persistent connection with 
RethinkDB. This module will handle reconnects automatically. Just add this
worker to your supervision tree:

```
#{id => {gen_rethink_session, my_session_name},
  start => {gen_rethink_session, start_link, [my_session_name]}}
```

Once started, the connection can be accessed with
```
gen_rethink_session:get_connection(my_session_name).
```

The return value will be either `{ok, Connection}` or `{error, no_connection}`.

Creating a ReQL query
---------------------
Since Erlang doesn't support chaining function calls, ReQL queries are created
differently than in the JS and Python drivers. Each ReQL query is an Erlang
process that is linked to the calling process. Construct the query by calling
a sequence of functions on the `reql` module.
```
Reql = reql:new().
reql:db(Reql, my_db),
reql:table(Reql, my_table).
```

The function `reql:x/1` is provided as a way to contain query creation in one
place for readability, and as a way to generate a query in a single expression.

```
Reql = reql:x(fun(X) ->
            reql:db(X, my_db),
            reql:table(X, my_table),
            reql:get_all(X, <<"an_index_value">>, #{index => my_index})
    end).
```
And, as shown in the quick example above, the call to `reql:x/1` can be ommitted
when being passed into `gen_rethink:run/2`.

Note: By default, the Reql query can only be run one time; the Erlang process exits when
the query is executed by `gen_rethink:run/2`. To run a single query multiple
times, use
1. Reference counting (`reql:hold/1` and `reql:release/1`), or
2. Closures (`reql:closure/2`)

Running a ReQL query
----------------
```
gen_rethink:run(Connection, Reql).
```
or
```
gen_rethink:run(Connection, Reql, Timeout).
```
where Timeout is a client-side timeout in milliseconds.

The return value is either `{ok, Result}` or `{error, Reason}`.

Parsing the result
----------------
Depending on the ReQL query, the Result returned can either contain the data
from the query response, or it can be an Erlang pid. If it's an Erlang pid,
then the module `rethink_cursor` must be used to access the query response.

```
rethink_cursor:activate(Cursor),
fun L(C) -> receive
    {rethink_cursor, done} ->
        ok;
    {rethink_cursor, Result} ->
        % do something
        L(C);
    {rethink_cursor, error, Error} ->
        % handle error
        ok
end(Cursor).
```

Implementing changefeeds
------------------------
The `gen_requery` behaviour can be used to manage a long-living changefeed query. Please
keep in mind that you are responsible for managing the connection when using this
module. You may want to create a process monitor on the connection so that
the query can be restarted if the connection drops. See test/gen_requery_tests.erl
for an example.

RethinkDB datatypes
-------------------
The following Erlang structures can be used to store the special RethinkDB
datatypes:

| RethinkDB Data Type   | Example Erlang representation                | reql function    |
| --------------------- | -------------------------------------------- | ---------------- |
| Timestamp             | `{1515,167587,102000}`                       | none             |
| Timestamp + Time Zone | `{{1515,167587,102000}, <<"-05:00">>}`       | none             |
| ISO 8601 Timestamp    | `{iso8601, <<"2018-01-03T17:44:54+00:00">>}` | `reql:iso8601/1` |
| Binary                | `{binary, <<1,2,3,4,5>>}`                    | `reql:binary/1`  |

ReQL query closure (for speedy inserts)
-----------------------------------
For an optimization, a given ReQL query can be turned into a closure (anonymous
function) and used indefinitely. This is useful, for example, when inserting:

```
Reql = reql:x(fun(X) ->
            reql:db(X, my_db),
            reql:table(X, my_table)
        end),
Inserter = reql:closure(Reql, insert),
% Reql process is now stopped
gen_rethink:run_with_args(Connection, Inserter,
        #{a => 1, x => <<"hello">>, y => <<"world">>}),
gen_rethink:run_with_args(Connection, Inserter,
        #{a => 2, x => <<"hello">>, y => <<"rethinkdb">>}).
```

Full example
------------
Connect to RethinkDB
```
1> {ok, Connection} = gen_rethink:connect().
{ok,<0.149.0>}
```
Create a new database called `my_db`.
```
2> gen_rethink:run(Connection, fun(X) -> reql:db_create(X, my_db) end).
{ok,#{<<"config_changes">> =>
          [#{<<"new_val">> =>
                 #{<<"id">> => <<"fa21c3de-41b8-4d2a-a459-3d01f0002833">>,
                   <<"name">> => <<"my_db">>},
             <<"old_val">> => null}],
      <<"dbs_created">> => 1}}
```
Create a new table called `my_table`.
```
3> gen_rethink:run(Connection,
3>                  fun(X) ->
3>                          reql:db(X, my_db),
3>                          reql:table_create(X, my_table)
3>                  end).
{ok,#{<<"config_changes">> =>
          [#{<<"new_val">> =>
                 #{<<"db">> => <<"my_db">>,<<"durability">> => <<"hard">>,
                   <<"id">> => <<"bc39525c-aed8-480a-92c3-1f76dc5884e3">>,
                   <<"indexes">> => [],<<"name">> => <<"my_table">>,
                   <<"primary_key">> => <<"id">>,
                   <<"shards">> =>
                       [#{<<"nonvoting_replicas">> => [],
                          <<"primary_replica">> => <<"jstimpson_mbp_local_dxh">>,
                          <<"replicas">> => [<<"jstimpson_mbp_local_dxh">>]}],
                   <<"write_acks">> => <<"majority">>},
             <<"old_val">> => null}],
      <<"tables_created">> => 1}}
```
Insert a record.
```
4> gen_rethink:run(Connection,
4>                  fun(X) ->
4>                          reql:db(X, my_db),
4>                          reql:table(X, my_table),
4>                          reql:insert(X, #{timestamp => os:timestamp(),
4>                                           data => <<"Hello World!">>})
4>                  end).
{ok,#{<<"deleted">> => 0,<<"errors">> => 0,
      <<"generated_keys">> =>
          [<<"a1f00854-849f-4091-9a85-6b0fb1c77af7">>],
      <<"inserted">> => 1,<<"replaced">> => 0,<<"skipped">> => 0,
      <<"unchanged">> => 0}}
```
Read all records from the table.
```
5> {ok, Cursor} = gen_rethink:run(Connection,
5>                  fun(X) ->
5>                          reql:db(X, my_db),
5>                          reql:table(X, my_table)
5>                  end).
{ok,<0.163.0>}
```
Flush the cursor to the shell.
```
6> rethink_cursor:activate(Cursor).
ok
7> flush().
Shell got {rethink_cursor,[#{<<"data">> => <<"Hello World!">>,
                             <<"id">> =>
                                 <<"a1f00854-849f-4091-9a85-6b0fb1c77af7">>,
                             <<"timestamp">> => {1515,167587,102000}}]}
Shell got {rethink_cursor,done}
ok
```
