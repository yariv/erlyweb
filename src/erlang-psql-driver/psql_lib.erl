%%%-------------------------------------------------------------------
%%%    BASIC INFORMATION
%%%-------------------------------------------------------------------
%%% @copyright 2006 Erlang Training & Consulting Ltd
%%% @author  Martin Carlson <martin@erlang-consulting.com>
%%% @version 0.0.1
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(psql_lib).

%% API
-export([type_cast/2, row_description/1, row/2, error/1, command/1]).

-include("psql.hrl").

%%====================================================================
%% API
%%====================================================================
type_cast(SqlValue, Desc) when Desc#field.type == binary ->
    SqlValue;   
type_cast(SqlValue, Desc) when Desc#field.type == list ->
    binary_to_list(SqlValue);
type_cast(SqlValue, Desc) when Desc#field.type == string ->
    binary_to_list(SqlValue);
type_cast(SqlValue, Desc) when Desc#field.type == int ->
    List = binary_to_list(SqlValue),
    case catch list_to_integer(List) of
	{'EXIT', _Reason} -> list_to_float(List);
	Res -> Res
    end;
type_cast(SqlValue, Desc) when Desc#field.type == float ->
    List = binary_to_list(SqlValue),
    case catch list_to_float(List) of
	{'EXIT', _Reason} ->list_to_integer(List);
	Res -> Res
    end;
type_cast(SqlValue, Desc) when Desc#field.type == bool ->
    if
	SqlValue == <<"f">> -> false;
	true -> true
    end;
type_cast(SqlValue, Desc) when Desc#field.type == date ->
    List = binary_to_list(SqlValue),
    [Yr,Mh,Dy] = string:tokens(List, "-"),
    {list_to_integer(Yr),
     list_to_integer(Mh),
     list_to_integer(Dy)};
type_cast(SqlValue, Desc) when Desc#field.type == time ->
    List = binary_to_list(SqlValue),
    [Hr,Mt,Sd] = string:tokens(List, "-: "),
    {list_to_integer(Hr),
     list_to_integer(Mt),
     case lists:member($., Sd) of
	 false -> list_to_integer(Sd);
	 true -> list_to_integer(hd(string:tokens(Sd, ".")))
     end};
type_cast(SqlValue, Desc) when Desc#field.type == datetime ->
    List = binary_to_list(SqlValue),
    [Yr,Mh,Dy,Hr,Mt,Sd] = string:tokens(List, "-: "),
    {{list_to_integer(Yr),
      list_to_integer(Mh),
      list_to_integer(Dy)}, 
     {list_to_integer(Hr),
      list_to_integer(Mt),
      case lists:member($., Sd) of
	  false -> list_to_integer(Sd);
	  true -> list_to_integer(hd(string:tokens(Sd, ".")))
      end}};
type_cast(SqlValue, #field{type = T}) when T == char_array; T == varchar_array ->
    List = binary_to_list(SqlValue),
    Values = string:substr(List, 2, length(List) - 2),
    Tokens = string:tokens(Values, ","),
    F = fun([$"|Rest] = String) ->
		case lists:reverse(Rest) of
		    [$"|RRest] ->
			lists:reverse(RRest);
		    _Otherwise ->
			String
		end;
	   (String) ->
		String
	end,
    list_to_tuple(lists:map(F, Tokens));
type_cast(SqlValue, Desc) when Desc#field.type == integer_array ->
    List = binary_to_list(SqlValue),
    {_, Tokens, _} = erl_scan:string(List ++ "."),
    {ok, Res} = erl_parse:parse_term(Tokens),
    Res;
type_cast(SqlValue, Desc) ->
    Res = binary_to_list(SqlValue),
    io:format("SQL: Unknown code: ~p (~p)", [Desc#field.type, Res]),
    Res.

%%
%% Row description
%%
row_description(<<_:1/big-unit:16, Fields/binary>>) ->
    list_to_tuple(field_desc_parser(Fields, [])).

field_desc_parser(<<>>, Acc) ->
	lists:reverse(Acc);
field_desc_parser(Fields, Acc) ->
    {Name, <<Table:1/big-signed-unit:32,
	    Col:1/big-signed-unit:16,
	    Type:1/big-signed-unit:32,
	    Size:1/big-signed-unit:16,
	    _Mod:1/big-signed-unit:32,
	    Format:1/big-signed-unit:16,
	    Tail/binary>>} = field_name_parser(Fields, []),
    Field = #field{name = Name,
		   table_code = Table,
		   field_code = Col,
		   type = psql_protocol:data_type(Type),
		   max_length = Size,
		   format = Format},
    field_desc_parser(Tail, [Field|Acc]).

field_name_parser(<<0, Tail/binary>>, Acc) ->
    {lists:reverse(Acc), Tail};
field_name_parser(<<C:8, Tail/binary>>, Acc) ->
    field_name_parser(Tail, [C|Acc]).


%%
%% Row Parser
%%
row(<<_:1/big-signed-unit:16, Cols/binary>>, Desc) ->
    col_parser(Cols, Desc, 1, []);
row(_, no_result) ->
    no_result.

col_parser(<<>>, _, _, Acc) ->
    list_to_tuple(lists:reverse(Acc));
col_parser(<<Length:1/big-signed-unit:32, Rest/binary>>, Desc, N, Acc) ->
    if
	Length == -1 ->
	    Value = [],
	    Tail = Rest;
	Length > size(Rest) ->
	    Value = [],
	    Tail = Rest,
	    erlang:error({badarg, Length, N, Rest});
	true ->
	    <<SQLValue:Length/binary, Tail/binary>> = Rest,
	    Value = psql_lib:type_cast(SQLValue, element(N, Desc))
    end,
    col_parser(Tail, Desc, N + 1, [Value|Acc]).

%%
%% Error parsing (This should be moved here)
%% 
error(Error) ->
    psql_protocol:error(Error).

%%
%% Parse command
%%
command(Command) ->
    Command.

%%====================================================================
%% Internal functions
%%====================================================================
