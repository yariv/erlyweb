-module(store).
-compile(export_all).

relations() ->
    [{one_to_many, [item]}, {many_to_many, [customer]}].
