-module(customer).
-compile(export_all).

relations() ->
    [{many_to_many, [customer]}].
