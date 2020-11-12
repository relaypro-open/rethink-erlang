-module(ql2).

-export([datum/1, undatum/1]).

-export([query_type/2,
         response_type/2,
         error_type/2,
         term_type/2]).

datum(List) when is_list(List) ->
    [term_type(make_array), [ datum(X) || X <- List ]];
datum(Map) when is_map(Map) ->
    maps:map(fun(_K, V) -> datum(V) end, Map);
datum({A, B, C}) when is_integer(A) andalso
                      is_integer(B) andalso
                      is_integer(C) ->
    datum({{A, B, C}, <<"+00:00">>});
datum({{A, B, C}, Timezone}) when is_integer(A) andalso
                      is_integer(B) andalso
                      is_integer(C) ->
    EpochMsec = (A * 1000*1000*1000) +
                (B * 1000) +
                (C div 1000),
    #{ '$reql_type$' => <<"TIME">>,
       epoch_time => EpochMsec / 1000.0,
       timezone => Timezone };
datum({iso8601, X}) ->
    [ql2:term_type(wire, iso8601), [X]];
datum({desc, X}) ->
    [ql2:term_type(wire, desc), [X]];
datum({asc, X}) ->
    [ql2:term_type(wire, asc), [X]];
datum(Pid) when is_pid(Pid) ->
    reql:resolve(Pid);
datum({binary, Blob}) ->
    #{ '$reql_type$' => <<"BINARY">>,
       data => base64:encode(Blob) };
datum({geometry, Geometry}) ->
    #{ '$reql_type$' => <<"GEOMETRY">>,
       data => Geometry };
datum({grouped, Data}) ->
    #{ '$reql_type$' => <<"GROUPED_DATA">>,
       data => Data };
datum(X) ->
    X.

undatum(List) when is_list(List) ->
    [ undatum(X) || X <- List ];
undatum(#{<<"$reql_type$">> := <<"GEOMETRY">>,
          <<"data">> := Data}) ->
    {geometry, Data};
undatum(#{<<"$reql_type$">> := <<"GROUPED_DATA">>,
          <<"data">> := Data}) ->
    {grouped, Data};
undatum(#{<<"$reql_type$">> := <<"BINARY">>,
          <<"data">> := Data}) ->
    {binary, base64:decode(Data)};
undatum(#{<<"$reql_type$">> := <<"TIME">>,
          <<"epoch_time">> := EpochTime,
          <<"timezone">> := Timezone}) ->
    EpochMsec = trunc(EpochTime * 1000),
    DivRem = fun(X, Y) -> {X div Y, X rem Y} end,
    {A, Rem1} = DivRem(EpochMsec, 1000*1000*1000),
    {B, Rem2} = DivRem(Rem1, 1000),
    C = Rem2 * 1000,
    case Timezone of
        <<"+00:00">> ->
            {A, B, C};
        _ ->
            {{A, B, C}, Timezone}
    end;
undatum(Map) when is_map(Map) ->
    maps:map(fun(_K, V) -> undatum(V) end, Map);
undatum(X) ->
    X.

query_type(wire, QT) when is_integer(QT) -> QT;
query_type(human, QT) when is_atom(QT) -> QT;
query_type(_, QT) -> query_type(QT).

response_type(wire, T) when is_integer(T) -> T;
response_type(human, T) when is_atom(T) -> T;
response_type(_, T) -> response_type(T).

error_type(wire, T) when is_integer(T) -> T;
error_type(human, T) when is_atom(T) -> T;
error_type(_, T) -> error_type(T).

term_type(wire, T) when is_integer(T) -> T;
term_type(human, T) when is_atom(T) -> T;
term_type(_, T) -> term_type(T).

%%%%%%%%%%%%

query_type(start) -> 1;
query_type(continue) -> 2;
query_type(stop) -> 3;
query_type(noreply_wait) -> 4;
query_type(server_info) -> 5;
query_type(1) -> start;
query_type(2) -> continue;
query_type(3) -> stop;
query_type(4) -> noreply_wait;
query_type(5) -> server_info.

response_type(success_atom) -> 1;
response_type(success_sequence) -> 2;
response_type(success_partial) -> 3;
response_type(wait_complete) -> 4;
response_type(server_info) -> 5;
response_type(client_error) -> 16;
response_type(compile_error) -> 17;
response_type(runtime_error) -> 18;
response_type(1) -> success_atom;
response_type(2) -> success_sequence;
response_type(3) -> success_partial;
response_type(4) -> wait_complete;
response_type(5) -> server_info;
response_type(16) -> client_error;
response_type(17) -> compile_error;
response_type(18) -> runtime_error.

error_type(internal) -> 1000000;
error_type(resource_limit) -> 2000000;
error_type(query_logic) -> 3000000;
error_type(non_existence) -> 3100000;
error_type(op_failed) -> 4100000;
error_type(op_indeterminate) -> 4200000;
error_type(user) -> 5000000;
error_type(permission_error) -> 6000000;
error_type(1000000) -> internal;
error_type(2000000) -> resource_limit;
error_type(3000000) -> query_logic;
error_type(3100000) -> non_existence;
error_type(4100000) -> op_failed;
error_type(4200000) -> op_indeterminate;
error_type(5000000) -> user;
error_type(600000)-> permission_error.

term_type(datum) -> 1;
term_type(make_array) -> 2;
term_type(make_obj) -> 3;
term_type(var) -> 10;
term_type(javascript) -> 11;
term_type(uuid) -> 169;
term_type(http) -> 153;
term_type(error) -> 12;
term_type(implicit_var) -> 13;
term_type(db) -> 14;
term_type(table) -> 15;
term_type(get) -> 16;
term_type(get_all) -> 78;
term_type(eq) -> 17;
term_type(ne) -> 18;
term_type(lt) -> 19;
term_type(le) -> 20;
term_type(gt) -> 21;
term_type(ge) -> 22;
term_type('not') -> 23;
term_type(add) -> 24;
term_type(sub) -> 25;
term_type(mul) -> 26;
term_type('div') -> 27;
term_type(mod) -> 28;
term_type(floor) -> 183;
term_type(ceil) -> 184;
term_type(round) -> 185;
term_type(append) -> 29;
term_type(prepend) -> 80;
term_type(difference) -> 95;
term_type(set_insert) -> 88;
term_type(set_intersection) -> 89;
term_type(set_union) -> 90;
term_type(set_difference) -> 91;
term_type(slice) -> 30;
term_type(skip) -> 70;
term_type(limit) -> 71;
term_type(offsets_of) -> 87;
term_type(contains) -> 93;
term_type(get_field) -> 31;
term_type(keys) -> 94;
term_type(values) -> 186;
term_type(object) -> 143;
term_type(has_fields) -> 32;
term_type(with_fields) -> 96;
term_type(pluck) -> 33;
term_type(without) -> 34;
term_type(merge) -> 35;
term_type(between_deprecated) -> 36;
term_type(between) -> 182;
term_type(reduce) -> 37;
term_type(map) -> 38;
term_type(fold) -> 187;
term_type(filter) -> 39;
term_type(concat_map) -> 40;
term_type(order_by) -> 41;
term_type(distinct) -> 42;
term_type(count) -> 43;
term_type(is_empty) -> 86;
term_type(union) -> 44;
term_type(nth) -> 45;
term_type(bracket) -> 170;
term_type(inner_join) -> 48;
term_type(outer_join) -> 49;
term_type(eq_join) -> 50;
term_type(zip) -> 72;
term_type(range) -> 173;
term_type(insert_at) -> 82;
term_type(delete_at) -> 83;
term_type(change_at) -> 84;
term_type(splice_at) -> 85;
term_type(coerce_to) -> 51;
term_type(type_of) -> 52;
term_type(update) -> 53;
term_type(delete) -> 54;
term_type(replace) -> 55;
term_type(insert) -> 56;
term_type(db_create) -> 57;
term_type(db_drop) -> 58;
term_type(db_list) -> 59;
term_type(table_create) -> 60;
term_type(table_drop) -> 61;
term_type(table_list) -> 62;
term_type(config) -> 174;
term_type(status) -> 175;
term_type(wait) -> 177;
term_type(reconfigure) -> 176;
term_type(rebalance) -> 179;
term_type(sync) -> 138;
term_type(grant) -> 188;
term_type(index_create) -> 75;
term_type(index_drop) -> 76;
term_type(index_list) -> 77;
term_type(index_status) -> 139;
term_type(index_wait) -> 140;
term_type(index_rename) -> 156;
term_type(set_write_hook) -> 189;
term_type(get_write_hook) -> 190;
term_type(funcall) -> 64;
term_type(branch) -> 65;
term_type('or') -> 66;
term_type('and') -> 67;
term_type(for_each) -> 68;
term_type(func) -> 69;
term_type(asc) -> 73;
term_type(desc) -> 74;
term_type(info) -> 79;
term_type(match) -> 97;
term_type(upcase) -> 141;
term_type(downcase) -> 142;
term_type(sample) -> 81;
term_type(default) -> 92;
term_type(json) -> 98;
term_type(iso8601) -> 99;
term_type(to_iso8601) -> 100;
term_type(epoch_time) -> 101;
term_type(to_epoch_time) -> 102;
term_type(now) -> 103;
term_type(in_timezone) -> 104;
term_type(during) -> 105;
term_type(date) -> 106;
term_type(time_of_day) -> 126;
term_type(timezone) -> 127;
term_type(year) -> 128;
term_type(month) -> 129;
term_type(day) -> 130;
term_type(day_of_week) -> 131;
term_type(day_of_year) -> 132;
term_type(hours) -> 133;
term_type(minutes) -> 134;
term_type(seconds) -> 135;
term_type(time) -> 136;
term_type(monday) -> 107;
term_type(tuesday) -> 108;
term_type(wednesday) -> 109;
term_type(thursday) -> 110;
term_type(friday) -> 111;
term_type(saturday) -> 112;
term_type(sunday) -> 113;
term_type(january) -> 114;
term_type(february) -> 115;
term_type(march) -> 116;
term_type(april) -> 117;
term_type(may) -> 118;
term_type(june) -> 119;
term_type(july) -> 120;
term_type(august) -> 121;
term_type(september) -> 122;
term_type(october) -> 123;
term_type(november) -> 124;
term_type(december) -> 125;
term_type(literal) -> 137;
term_type(group) -> 144;
term_type(sum) -> 145;
term_type(avg) -> 146;
term_type(min) -> 147;
term_type(max) -> 148;
term_type(split) -> 149;
term_type(ungroup) -> 150;
term_type(random) -> 151;
term_type(changes) -> 152;
term_type(args) -> 154;
term_type(binary) -> 155;
term_type(geojson) -> 157;
term_type(to_geojson) -> 158;
term_type(point) -> 159;
term_type(line) -> 160;
term_type(polygon) -> 161;
term_type(distance) -> 162;
term_type(intersects) -> 163;
term_type(includes) -> 164;
term_type(circle) -> 165;
term_type(get_intersecting) -> 166;
term_type(fill) -> 167;
term_type(get_nearest) -> 168;
term_type(polygon_sub) -> 171;
term_type(to_json_string) -> 172;
term_type(minval) -> 180;
term_type(maxval) -> 181;
term_type( 1 )   -> datum;
term_type( 2 )   -> make_array;
term_type( 3 )   -> make_obj;
term_type( 10 )  -> var;
term_type( 11 )  -> javascript;
term_type( 169 ) -> uuid;
term_type( 153 ) -> http;
term_type( 12 )  -> error;
term_type( 13 )  -> implicit_var;
term_type( 14 )  -> db;
term_type( 15 )  -> table;
term_type( 16 )  -> get;
term_type( 78 )  -> get_all;
term_type( 17 )  -> eq;
term_type( 18 )  -> ne;
term_type( 19 )  -> lt;
term_type( 20 )  -> le;
term_type( 21 )  -> gt;
term_type( 22 )  -> ge;
term_type( 23 )  -> 'not';
term_type( 24 )  -> add;
term_type( 25 )  -> sub;
term_type( 26 )  -> mul;
term_type( 27 )  -> 'div';
term_type( 28 )  -> mod;
term_type( 183 ) -> floor;
term_type( 184 ) -> ceil;
term_type( 185 ) -> round;
term_type( 29 )  -> append;
term_type( 80 )  -> prepend;
term_type( 95 )  -> difference;
term_type( 88 )  -> set_insert;
term_type( 89 )  -> set_intersection;
term_type( 90 )  -> set_union;
term_type( 91 )  -> set_difference;
term_type( 30 )  -> slice;
term_type( 70 )  -> skip;
term_type( 71 )  -> limit;
term_type( 87 )  -> offsets_of;
term_type( 93 )  -> contains;
term_type( 31 )  -> get_field;
term_type( 94 )  -> keys;
term_type( 186 ) -> values;
term_type( 143 ) -> object;
term_type( 32 )  -> has_fields;
term_type( 96 )  -> with_fields;
term_type( 33 )  -> pluck;
term_type( 34 )  -> without;
term_type( 35 )  -> merge;
term_type( 36 )  -> between_deprecated;
term_type( 182 ) -> between;
term_type( 37 )  -> reduce;
term_type( 38 )  -> map;
term_type( 187 ) -> fold;
term_type( 39 )  -> filter;
term_type( 40 )  -> concat_map;
term_type( 41 )  -> order_by;
term_type( 42 )  -> distinct;
term_type( 43 )  -> count;
term_type( 86 )  -> is_empty;
term_type( 44 )  -> union;
term_type( 45 )  -> nth;
term_type( 170 ) -> bracket;
term_type( 48 )  -> inner_join;
term_type( 49 )  -> outer_join;
term_type( 50 )  -> eq_join;
term_type( 72 )  -> zip;
term_type( 173 ) -> range;
term_type( 82 )  -> insert_at;
term_type( 83 )  -> delete_at;
term_type( 84 )  -> change_at;
term_type( 85 )  -> splice_at;
term_type( 51 )  -> coerce_to;
term_type( 52 )  -> type_of;
term_type( 53 )  -> update;
term_type( 54 )  -> delete;
term_type( 55 )  -> replace;
term_type( 56 )  -> insert;
term_type( 57 )  -> db_create;
term_type( 58 )  -> db_drop;
term_type( 59 )  -> db_list;
term_type( 60 )  -> table_create;
term_type( 61 )  -> table_drop;
term_type( 62 )  -> table_list;
term_type( 174 ) -> config;
term_type( 175 ) -> status;
term_type( 177 ) -> wait;
term_type( 176 ) -> reconfigure;
term_type( 179 ) -> rebalance;
term_type( 138 ) -> sync;
term_type( 188 ) -> grant;
term_type( 75 )  -> index_create;
term_type( 76 )  -> index_drop;
term_type( 77 )  -> index_list;
term_type( 139 ) -> index_status;
term_type( 140 ) -> index_wait;
term_type( 156 ) -> idnex_rename;
term_type( 189 ) -> set_write_hook;
term_type( 190 ) -> get_write_hook;
term_type( 64 )  -> funcall;
term_type( 65 )  -> branch;
term_type( 66 )  -> 'or';
term_type( 67 )  -> 'and';
term_type( 68 )  -> for_each;
term_type( 69 )  -> func;
term_type( 73 )  -> asc;
term_type( 74 )  -> desc;
term_type( 79 )  -> info;
term_type( 97 )  -> match;
term_type( 141 ) -> upcase;
term_type( 142 ) -> downcase;
term_type( 81 )  -> sample;
term_type( 92 )  -> default;
term_type( 98 )  -> json;
term_type( 99 )  -> iso8601;
term_type( 100 ) -> to_iso8601;
term_type( 101 ) -> epoch_time;
term_type( 102 ) -> to_epoch_time;
term_type( 103 ) -> now;
term_type( 104 ) -> in_timezone;
term_type( 105 ) -> during;
term_type( 106 ) -> date;
term_type( 126 ) -> time_of_day;
term_type( 127 ) -> timezone;
term_type( 128 ) -> year;
term_type( 129 ) -> month;
term_type( 130 ) -> day;
term_type( 131 ) -> day_of_week;
term_type( 132 ) -> day_of_year;
term_type( 133 ) -> hours;
term_type( 134 ) -> minutes;
term_type( 135 ) -> seconds;
term_type( 136 ) -> time;
term_type( 107 ) -> monday;
term_type( 108 ) -> tuesday;
term_type( 109 ) -> wednesday;
term_type( 110 ) -> thursday;
term_type( 111 ) -> friday;
term_type( 112 ) -> saturday;
term_type( 113 ) -> sunday;
term_type( 114 ) -> january;
term_type( 115 ) -> february;
term_type( 116 ) -> march;
term_type( 117 ) -> april;
term_type( 118 ) -> may;
term_type( 119 ) -> june;
term_type( 120 ) -> july;
term_type( 121 ) -> august;
term_type( 122 ) -> september;
term_type( 123 ) -> october;
term_type( 124 ) -> november;
term_type( 125 ) -> december;
term_type( 137 ) -> literal;
term_type( 144 ) -> group;
term_type( 145 ) -> sum;
term_type( 146 ) -> avg;
term_type( 147 ) -> min;
term_type( 148 ) -> max;
term_type( 149 ) -> split;
term_type( 150 ) -> ungroup;
term_type( 151 ) -> random;
term_type( 152 ) -> changes;
term_type( 154 ) -> args;
term_type( 155 ) -> binary;
term_type( 157 ) -> geojson;
term_type( 158 ) -> to_geojson;
term_type( 159 ) -> point;
term_type( 160 ) -> line;
term_type( 161 ) -> polygon;
term_type( 162 ) -> distance;
term_type( 163 ) -> intersects;
term_type( 164 ) -> includes;
term_type( 165 ) -> circle;
term_type( 166 ) -> get_intersecting;
term_type( 167 ) -> fill;
term_type( 168 ) -> get_nearest;
term_type( 171 ) -> polygon_sub;
term_type( 172 ) -> to_json_string;
term_type( 180 ) -> minval;
term_type( 181 ) -> maxval.
