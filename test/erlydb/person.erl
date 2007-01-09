-module(person).
-export([fields/0, type_field/0]).

fields() ->
     [name, age, country].

type_field() ->
     type.
