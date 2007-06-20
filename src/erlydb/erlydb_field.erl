%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com]
%% @copyright Yariv Sadan 2006-2007
%%
%% @doc This module contains data structures and functions for
%% exposing database fields' metadata.
%%
%% After calling {@link erlydb:codegen/3}, generated modules contain a few
%% functions for getting database field metadata as well as transforming
%% records to iolists and setting their field values from strings. Those
%% functions use opaque erlydb_field structures, whose values can be
%% retrieved using the functions in this module. For more information,
%% refer to {@link erlydb_base}.

%% @type erlydb_field(). An opaque structure holding database field
%% metadata.

%% For license information see license.txt

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
    extra/1,
    attributes/1,
    attributes/2,
    is_transient/1
   ]).

-record(erlydb_field,
	{name, name_str, name_bin, type, modifier, erl_type,
	 html_input_type,
	 null, key,
	 default, extra, attributes}).

%% @doc Create a new erlydb_field record.
%%
%% @spec new() -> erlydb_field()
new() ->
    #erlydb_field{}.

%% @doc Create a new erlydb_field record initialized with the given values.
%%
%% 'Type' is the DBMS datatype, ('integer', 'varchar', etc.), represented
%% as an atom.
%%
%% 'Modifier' is used to define the maximum length of the field, or the list
%% of options for an enum field. This value is set to 'undefined' if it's not
%% provided.
%% 
%% 'Null' a boolean value indicating if the field is allowed to have a null
%%  value.
%%
%% 'Key' indicates if the field is used as a primary or a unique key.
%%  The possible values are 'primary', 'unique', 'multiple' and 'undefined'.
%%
%% 'Default' is the field's default value.
%%
%% 'Extra' is any additional information used to describe the field.
%% Currently, the possible values are 'identity' and 'undefined'
%%
%% @spec new(Name::atom(), {Type::atom(), Modifier::term()},
%%  Null::boolean(), Key::term(), Default::term(), Extra::term()) ->
%%  erlydb_field()
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

%% @doc Get the field's name.
%%
%% @spec name(Field::erlydb_field()) -> atom()
name(Field) ->
    Field#erlydb_field.name.

%% @doc Get the field's name as a string.
%%
%% @spec name_str(Field::erlydb_field()) -> string()
name_str(Field) ->
    Field#erlydb_field.name_str.

%% @doc Get the field's name as a binary.
%%
%% @spec name_bin(Field::erlydb_field()) -> binary()
name_bin(Field) ->
    Field#erlydb_field.name_bin.

%% @doc Get the field's type.
%%
%% @spec type(Field::erlydb_field()) -> term()
type(Field) ->
    Field#erlydb_field.type.

%% @doc Get the field's modifier.
%%
%% @spec modifier(Field::erlydb_field()) -> term()
modifier(Field) ->
    Field#erlydb_field.modifier.

%% @doc If this is a text field, get its max length. This is identical to
%% modifier/1.
%%
%% @spec maxlength(Field::erlydb_field()) -> term()
maxlength(Field) ->
    Field#erlydb_field.modifier.

%% @doc If this is an enum field, get its list of options. This is identical to
%% modifier/1.
%%
%% @spec options(Field::erlydb_field()) -> term()
options(Field) ->
    Field#erlydb_field.modifier.

%% @doc Get the field's corresponding Erlang type. Possible values are
%% 'binary', 'integer', 'float', 'date', 'time', and 'datetime'.
%%
%% Date, time and datetime fields have the following forms:
%% 
%% ```
%% {date, {Year, Month, Day}}
%% {time, {Hour, Minute, Second}}
%% {datetime, {{Year, Month, Day}, {Hour, Minute, Second}}}
%% '''
%%
%% @spec erl_type(Field::erlydb_field()) -> binary | integer | float | date |
%%  time | datetime
erl_type(Field) ->
    Field#erlydb_field.erl_type.

%% @doc Get the field's default HTML input field type.
%% Possible values are 'text_field', 'text_area' and 'select'.
%%
%% @spec html_input_type(Field::erlydb_field()) -> text_field |
%%  text_area | select
html_input_type(Field) ->
    Field#erlydb_field.html_input_type.

%% @doc Get the field's 'null' status.
%%
%% @spec null(Field::erlydb_field()) -> boolean()
null(Field) ->
    Field#erlydb_field.null.

%% @doc Get the field's key definition.
%%
%% @spec key(Field::erlydb_field()) -> undefined | primary | unique | multiple
key(Field) ->
    Field#erlydb_field.key.

%% @doc Get the field's default value.
%%
%% @spec default(Field::erlydb_field()) -> undefined | term()
default(Field) ->
    Field#erlydb_field.default.

%% @doc Get the field's extra metadata.
%%
%% @spec extra(Field::erlydb_field()) -> undefined | identity
extra(Field) ->
    Field#erlydb_field.extra.

%% @doc Get the field's user-defined attributes.
%%
%% @spec attributes(Field::erlydb_field()) -> undefined | [term()]
attributes(Field) ->
    Field#erlydb_field.attributes.

%% @doc Set the field's user-defined attributes.
%%
%% @spec attributes(Field::erlydb_field(), Attributes::[term()]) ->
%%   erlydb_field()
attributes(Field, Attributes) ->
    Field#erlydb_field{attributes = Attributes}.

%% @doc Transient flag of field's user-defined attributes.
%%
%% @spec is_transient(Field::erlydb_field()) -> true | false
is_transient(Field) ->
  lists:member(transient, Field#erlydb_field.attributes).

get_erl_type({Type, _Len}) -> get_erl_type(Type);
get_erl_type(Type) ->
    case Type of
	varchar -> binary;
	char -> binary;
  'character varying' -> binary;
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
  'timestamp without time zone' -> datetime;
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
      decimal -> text_field;
      binary -> get_html_binary_input_type(Type);
      _ -> text_field
  end.

get_html_binary_input_type({Type, _Len}) ->
    get_html_binary_input_type(Type);

get_html_binary_input_type(Type) ->
    case Type of
	varchar -> text_field;
  'character varying' -> text_field;
	char -> text_field;
	binary -> text_field;
	varbinary -> text_field;
	blob -> text_area;
	text -> text_area;
	enum -> select;
	set -> select
  end.
	      
