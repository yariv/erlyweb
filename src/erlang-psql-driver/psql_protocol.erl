%%%-------------------------------------------------------------------
%%%    BASIC INFORMATION
%%%-------------------------------------------------------------------
%%% @copyright 2006 Erlang Training & Consulting Ltd
%%% @author  Martin Carlson <martin@erlang-consulting.com>
%%% @version 0.0.1
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(psql_protocol).

%% API
-export([decode_packet_header/1,
	 decode/1,
	 encode/1, 
	 encode/2, 
	 type/1, 
	 data_type/1,
	 error/1,
	 md5/1,
	 to_int8/1,	 
	 to_int16/1,
	 to_int32/1,
	 to_string/1]).

-export([authenticate/4,
	 md5digest/2,
	 copy_done/0,
	 q/1,
	 parse/3,
	 bind/3,
	 describe/2,
	 execute/2,
	 close/2,
	 sync/0]).

%%====================================================================
%% Encode/Decode
%%====================================================================
decode_packet_header(<<Type:8, Size:1/big-unit:32>>) ->
    {type(Type), Size}.

decode(<<Type:8, S:1/big-unit:32, R/binary>>) when S - 4 =< size(R) ->
    MsgSize = S - 4,
    <<Message:MsgSize/binary-unit:8, Tail/binary>> = R,
    {{type(Type), Message}, Tail};
decode(Acc) ->
    {next, Acc}.

encode({Type, Message}) ->
    encode(Type, Message);
encode(Message) when is_list(Message) ->
    encode(list_to_binary(Message));
encode(Message) when is_binary(Message) ->
    Size = to_int32(size(Message) + 4),
    [Size, Message].
encode(Type, Message) when is_list(Message) ->
    encode(Type, list_to_binary(Message));
encode(Type, Message) when is_binary(Message) ->
    Size = to_int32(size(Message) + 4),
    [type(Type), Size, Message].

%%====================================================================
%% Messages
%%====================================================================
authenticate(VSN, Usr, Pwd, Db) ->
    Msg = [to_int32(VSN),
	   to_string("user"),
	   to_string(Usr),
	   to_string("database"),
	   to_string(Db),
	   null()],
    Digest = psql_protocol:md5([Pwd, Usr]),
    {Msg, Digest}.

md5digest(Digest, Salt) ->
    Auth = md5([Digest, Salt]),
    {password_message, <<"md5", Auth/binary, 0:8>>}.

copy_done() ->
    {'copy_done', []}.

q(Query) ->
    {'query', to_string(Query)}.

parse(Name, Query, Types) ->
    {'parse', list_to_binary([to_string(Name), 
			      to_string(Query), 
			      to_int16(length(Types)),
			      [to_int32(T) || T <- Types]])}.

bind(Portal, Statement, Parameters) ->
    {bind, list_to_binary([to_string(Portal),
			   to_string(Statement),
			   to_int16(0), % Denotes that all prameters are in text
			   to_int16(length(Parameters)),
			   map_parameters(Parameters, []),
			   to_int16(0)])}. % Denotes that all prameters are in text

describe(Name, Type) ->
    if
	Type == statement ->
	    T = to_int8($S);
	true ->
	    T = to_int8($P)
    end,
    {describe, [T, to_string(Name)]}.

execute(Portal, ResultSetSize) ->
    {execute, list_to_binary([to_string(Portal), to_int32(ResultSetSize)])}.

close(Name, Type) ->
    if
	Type == statement ->
	    T = to_int8($S);
	true ->
	    T = to_int8($P)
    end,
    {close, [T, to_string(Name)]}.
 
sync() ->
    {sync, []}.

%%====================================================================
%% Bind Parameter Map
%%====================================================================
map_parameters([], Acc) ->
    lists:reverse(Acc);
map_parameters([nil|T], Acc) ->
    map_parameters(T, [to_int32(-1)|Acc]);
map_parameters([H|T], Acc) when is_list(H) ->
    map_parameters(T, [H, to_int32(length(H))|Acc]).

%%====================================================================
%% Message Type Map
%%====================================================================
type($R) -> authentication;
type($K) -> backend_key_data;
type($2) -> bind_complete;
type($3) -> close_complete;
type($C) -> command_complete;
type($d) -> copy_data;
type($c) -> copy_done;
type($G) -> copy_in_response;
type($H) -> copy_out_response;
type($D) -> data_row;
type($I) -> empty_query_response;
type($E) -> error;
type($V) -> function_call_response;
type($n) -> no_data;
type($N) -> notice_response;
type($A) -> notification_response;
type($t) -> parameter_description;
type($S) -> parameter_status;
type($1) -> parse_complete;
type($s) -> portal_suspended;
type($Z) -> ready_for_query;
type($T) -> row_description;

type(bind) -> $B;
type(close) -> $C;
type(copy_data) -> $d;
type(copy_done) -> $c;
type(copy_fail) -> $f;
type(describe) -> $D;
type(execute) -> $E;
type(flush) -> $H;
type(function_call) -> $F;
type(parse) -> $P;
type(password_message) -> $p;
type('query') -> $Q;
type(ssl_request) -> 8;
type(sync) -> $S;
type(terminate) -> $X.

%%====================================================================
%% Datatype Map
%%====================================================================
%% Datatypes are specified in pg_type
data_type(16) -> bool;
data_type(17) -> binary;
data_type(18) -> string;
data_type(19) -> string;
data_type(20) -> int;
data_type(21) -> int;
data_type(23) -> int;
data_type(24) -> string;
data_type(25) -> string;
data_type(26) -> binary;
data_type(896) -> ip;
data_type(1007) -> integer_array;
data_type(1014) -> char_array;
data_type(1015) -> varchar_array;
data_type(1042) -> string;
data_type(1043) -> string;
data_type(1082) -> date;
data_type(1083) -> time;
data_type(1114) -> datetime;
data_type(1184) -> datetime;
data_type(1266) -> time;
data_type(1700) -> float.

%%====================================================================
%% Errors
%%====================================================================
error(<<"SFATAL",0, "C28000", 0, Rest/binary>>) ->
    {authentication, Rest};
error(<<"SFATAL",0, "C57P01", 0, Rest/binary>>) ->
    {shutdown, Rest};
error(<<"SFATAL",0, "C57P02", 0, Rest/binary>>) ->
    {shutdown, Rest};
error(<<"SFATAL",0, "C57P03", 0, Rest/binary>>) ->
    {shutdown, Rest};
error(<<"SERROR", 0, $C, P, C, O, D, E, 0, Rest/binary>>) ->
    {_, Desc} = lists:foldl(fun(0, {Acc0, Acc}) ->
				    {[], lists:reverse(Acc0) ++ [$ |Acc]};
			       (Chr, {Acc0, Acc}) ->
				    {[Chr|Acc0], Acc}
			    end, {[], []}, binary_to_list(Rest)),			   
    {sql_error, [P,C,O,D,E], string:strip(Desc)}.

%%====================================================================
%% Utility functions
%%====================================================================
to_int8(N) ->
    <<N:1/big-signed-unit:8>>.

to_int16(N) ->
    <<N:1/big-signed-unit:16>>.

to_int32(N) ->
    <<N:1/big-signed-unit:32>>.

to_string(Str) ->
    <<(list_to_binary(Str))/binary, 0:8>>.

null() ->
    <<0:8>>.

md5(Data) ->
    Digest = erlang:md5(Data),
    list_to_binary(to_hex(Digest)).

to_hex(<<H:4, L:4, Rest/binary>>) ->
    [hex(H), hex(L) | to_hex(Rest)];
to_hex(<<>>) ->
    [].

hex(Dec) when Dec < 10 ->
    $0 + Dec;
hex(Dec) ->
    $a - 10 + Dec.
