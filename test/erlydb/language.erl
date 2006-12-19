-module(language).

-export([relations/0]).

relations() ->
    [{one_to_many, [project]}].
