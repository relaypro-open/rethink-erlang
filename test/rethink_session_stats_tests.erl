-module(rethink_session_stats_tests).
-include_lib("eunit/include/eunit.hrl").

sort_test() ->
    Sort = fun rethink_session_stats:sort/1,
    ?assertMatch([{2,ok,0,10},{1,ok,0,1000}], Sort([{1,ok,0,1000},{2,ok,0,10}])).
