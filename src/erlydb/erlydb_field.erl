%% @title erlydb_field
%% @author Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com)
%% @doc This module has useful functions for getting the attributes
%%   of database fields.
%%
%% @license For license information see license.txt
-module(erlydb_field).
-author("Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com)").

-export(
   [
    new/0,
    new/6,
    name/1,
    name_str/1,
    name_bin/1,
    type/1,
    maxlength/1,
    options/1,
    modifier/1,
    erl_type/1,
    html_input_type/1,
    null/1,
    key/1,
    default/1,
    extra/1
   ]).

-record(erlydb_field,
	{name, name_str, name_bin, type, modifier, erl_type,
	 html_input_type,
	 null, key,
	 default, extra}).

new() ->
    #erlydb_field{}.

new(Name, {Type, Modifier}, Null, Key, Default, Extra) ->
    NameStr = atom_to_list(Name),
    #erlydb_field{name = Name,
		  name_str = NameStr,
		  name_bin = list_to_binary(NameStr),
		  type = Type,
		  erl_type = get_erl_type(Type),
		  html_input_type = get_html_input_type(Type),
		  modifier = Modifier,
		  null = Null,
		  key = Key,
		  default = Default,
		  extra = Extra};
new(Name, Type, Null, Key, Default, Extra) ->
    new(Name, {Type, undefined}, Null, Key, Default, Extra).

name(Field) ->
    Field#erlydb_field.name.

name_str(Field) ->
    Field#erlydb_field.name_str.

name_bin(Field) ->
    Field#erlydb_field.name_bin.

type(Field) ->
    Field#erlydb_field.type.

modifier(Field) ->
    Field#erlydb_field.modifier.

maxlength(Field) ->
    Field#erlydb_field.modifier.

options(Field) ->
    Field#erlydb_field.modifier.

erl_type(Field) ->
    Field#erlydb_field.erl_type.

html_input_type(Field) ->
    Field#erlydb_field.html_input_type.

null(Field) ->
    Field#erlydb_field.null.

key(Field) ->
    Field#erlydb_field.key.

default(Field) ->
    Field#erlydb_field.default.

extra(Field) ->
    Field#erlydb_field.extra.

get_erl_type({Type, _Len}) -> get_erl_type(Type);
get_erl_type(Type) ->
    case Type of
	varchar -> binary;
	char -> binary;
	binary -> binary;
	varbinary -> binary;
	blob -> binary;
	text -> binary;
	enum -> binary;
	set -> binary;
	tinyint -> integer;
	smallint -> integer;
	mediumint -> integer;
	int -> integer;
	bigint -> integer;
	bit -> integer;
	float -> float;
	double -> float;
	numeric -> float;
	datetime -> datetime;
	date -> date;
	timestamp -> datetime;
	time -> time;
	year -> integer;
	Other -> Other
    end.

get_html_input_type(Type) ->
  case get_erl_type(Type) of
      integer -> text_field;
      float -> text_field;
      date -> text_field;
      time -> text_field;
      datetime -> text_field;
      binary -> get_html_binary_input_type(Type)
  end.

get_html_binary_input_type({Type, _Len}) ->
    get_html_binary_input_type(Type);

get_html_binary_input_type(Type) ->
    case Type of
	varchar -> text_field;
	char -> text_field;
	binary -> text_field;
	varbinary -> text_field;
	blob -> text_area;
	text -> text_area;
	enum -> select;
	set -> select
  end.
	      
