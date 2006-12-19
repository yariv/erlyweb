-module(musician).
-export([table/0, fields/0, type_field/0]).

table() ->
     person.
fields() ->
     person:fields() ++ [genre, instrument].
type_field() ->
     type.
