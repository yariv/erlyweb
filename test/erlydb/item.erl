-module(item).
-compile(export_all).

relations() ->
    [{many_to_one, [store]}].

