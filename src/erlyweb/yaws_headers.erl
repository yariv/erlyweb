%% @author Roberto Saccon <rsaccon@gmail.com>
%%
%% @doc This module provides functions for getting and setting
%% values of the Yaws 'headers' record. You can use these functions
%% instead of using the record access syntax, and without
%% having to include yaws_api.hrl.
%%
%% As with {@link yaws_arg}, most functions have 2 variations: if it takes
%% 1 parameter, it returns the record's value for the field, and if it
%% takes two parameters, it returns a new record with the field having the
%% new value.

%% For license information see LICENSE.txt

-module(yaws_headers).
-author("Roberto Saccon (rsaccon@gmail.com)").

-export([new/0, connection/1, connection/2, accept/1, accept/2,
	 host/1, host/2,
	 if_modified_since/1, if_modified_since/2, if_match/1, if_match/2,
	 if_none_match/1, if_none_match/2, if_range/1, if_range/2,
	 if_unmodified_since/1, if_unmodified_since/2,
	 range/1, range/2, referer/1, referer/2, user_agent/1, user_agent/2,
	 accept_ranges/1, accept_ranges/2, cookie/1, cookie/2, keep_alive/1,
	 keep_alive/2, content_length/1, content_length/2,
	 content_type/1, content_type/2,
	 authorization/1,
	 authorization/2, other/1, other/2]).
	 

-include("yaws_api.hrl").

%% @doc Create a new 'headers' record.
new() ->
    #headers{}.

connection(Arg) ->
    (Arg#arg.headers)#headers.connection.

connection(Arg, Val) ->
    (Arg#arg.headers)#headers{connection = Val}.

accept(Arg) ->
    (Arg#arg.headers)#headers.accept.

accept(Arg, Val) ->
    (Arg#arg.headers)#headers{accept = Val}.

host(Arg) ->
    (Arg#arg.headers)#headers.host.

host(Arg, Val) ->
    (Arg#arg.headers)#headers{host=Val}.

if_modified_since(Arg) ->
    (Arg#arg.headers)#headers.if_modified_since.

if_modified_since(Arg, Val) ->
    (Arg#arg.headers)#headers{if_modified_since = Val}.

if_match(Arg) ->
    (Arg#arg.headers)#headers.if_match.

if_match(Arg, Val) ->
    (Arg#arg.headers)#headers{if_match = Val}.

if_none_match(Arg) ->
    (Arg#arg.headers)#headers.if_none_match.

if_none_match(Arg, Val) ->
    (Arg#arg.headers)#headers{if_none_match = Val}.

if_range(Arg) ->
    (Arg#arg.headers)#headers.if_range.

if_range(Arg, Val) ->
    (Arg#arg.headers)#headers{if_range = Val}.

if_unmodified_since(Arg) ->
    (Arg#arg.headers)#headers.if_unmodified_since.

if_unmodified_since(Arg, Val) ->
    (Arg#arg.headers)#headers{if_unmodified_since = Val}.

range(Arg) ->
    (Arg#arg.headers)#headers.range.

range(Arg, Val) ->
    (Arg#arg.headers)#headers{range = Val}.

referer(Arg) ->
    (Arg#arg.headers)#headers.referer.

referer(Arg, Val) ->
    (Arg#arg.headers)#headers{referer = Val}.

user_agent(Arg) ->
    (Arg#arg.headers)#headers.user_agent.

user_agent(Arg, Val) ->
    (Arg#arg.headers)#headers{user_agent = Val}.

accept_ranges(Arg) ->
    (Arg#arg.headers)#headers.accept_ranges.

accept_ranges(Arg, Val) ->
    (Arg#arg.headers)#headers{accept_ranges = Val}.

cookie(Arg) ->
    (Arg#arg.headers)#headers.cookie.

cookie(Arg, Val) ->
    (Arg#arg.headers)#headers{cookie = Val}.

keep_alive(Arg) ->
    (Arg#arg.headers)#headers.keep_alive.

keep_alive(Arg, Val) ->
    (Arg#arg.headers)#headers{keep_alive = Val}.

content_length(Arg) ->
    (Arg#arg.headers)#headers.content_length.

content_length(Arg, Val) ->
    (Arg#arg.headers)#headers{content_length = Val}.

content_type(Arg) ->
    (Arg#arg.headers)#headers.content_type.

content_type(Arg, Val) ->
    (Arg#arg.headers)#headers{content_type = Val}.

authorization(Arg) ->
    (Arg#arg.headers)#headers.authorization.

authorization(Arg, Val) ->
    (Arg#arg.headers)#headers{authorization = Val}.

other(Arg) ->
    (Arg#arg.headers)#headers.other.

other(Arg, Val) ->
    (Arg#arg.headers)#headers{other = Val}.
