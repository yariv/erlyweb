-module(employee).
-export([table/0, fields/0, type_field/0]).

table() ->
     person.
fields() ->
     person:fields() ++ [office, department].
type_field() ->
     type.
