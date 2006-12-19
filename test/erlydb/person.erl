-module(person).
-export([fields/0, type_field/0]).

fields() ->
     [name, age, country, created_on].

type_field() ->
     type.
