-module(project).
-export([relations/0]).

relations() ->
    [{many_to_one, [language]},
     {many_to_many, [developer]}].
