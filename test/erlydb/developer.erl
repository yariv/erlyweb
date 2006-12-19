-module(developer).
-export([relations/0,
%	 fields/0,
	 fields/0,
	 table/0,
	 type_field/0,
	 after_fetch/1,
	 before_save/1,
	 after_save/1,
	 before_delete/1,
	 after_delete/1]).

relations() ->
    [{many_to_many, [project]}].

table() ->
    person.

fields() ->
    [name, country].

type_field() ->
    type.

%fields() ->
%    [name].

log(Msg, Rec) ->
    io:format(Msg, [Rec]),
    io:format("~n", []).

after_fetch(Developer) ->
    log("after fetch: ~p", Developer),
    Developer.
    
    
before_save(Developer) ->
    log("before save: ~p", Developer),
    Developer.

after_save(Developer) ->
    log("after save: ~p", Developer),
    Developer.

before_delete(Developer) ->
    log("before delete: ~p", Developer),
    Developer.

after_delete(Developer) ->
    log("after delete: ~p", Developer),
    Developer.
