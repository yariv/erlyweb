%% @author Yariv Sadan <yarivvv@gmail.com> [http://yarivsblog.com]
%% @copyright Yariv Sadan 2006-2007
%% @doc erlydb_base is the base module that all modules that ErlyDB generates
%% extend.
%%
%% Generated modules inherit many of erlydb_base's exported functions
%% directly, but some of the functions in erlydb_base undergo
%% changes before they attain their final forms the generated modules.
%% For an exact description of how each function in
%% erlydb_base is used in generated modules, refer to the function's
%% documentation.
%%
%% You can override some of the default code generation behavior by
%% providing your own implementations for some of erlydb_base's functions in
%% generated modules.
%% This is useful for telling ErlyDB about relations (one-to-many
%% and many-to-many) and mappings between Erlang modules and database tables
%% and fields.
%%
%% Starting from ErlyWeb 0.6, you can add arbitrary metadata to database
%% fields as well as define specific fields to be read-only.
%% For more information, see {@link fields/0}.

%% @type record(). An Erlang tuple containing the values for (some of)
%% the fields of a database row, as well as additional data used by
%% ErlyDB. To ensure future compatibility, it is recommended to use the
%% getters and setters ErlyDB adds to generated modules in order to
%% access the record's fields instead of accessing them directly.
%%
%% @type where_expr(). An ErlSQL (see {@link erlsql}) statement fragment
%% that defines the conditions in a {where, Conditions} clause.
%%
%% Examples:
%% ```
%% {age, '=', 34}
%% {{name, 'like', "Bob%"}, 'or', {not, {age, '>', 26}}}
%% '''
%%
%% If you pass the option {allow_unsafe_statements, true} to
%% {@link erlydb:code_gen/3}, you can use string and/or binary Where
%% expressions, but this isn't recommended because it exposes to you
%% SQL injection attacks if you forget to quote your strings.

%% @type extras_expr(). ErlSQL (see {@link erlsql}) statement fragments
%% that appear at the end of the statement, following the 'where' clause
%% (if it exists). Currently, this includes 'order_by' and 'limit' clauses.
%%
%% Examples:
%% ```
%% {order_by, age}
%% {limit, 6, 8}
%% [{order_by, [{age, {height, asc}, {gpa, desc}}]}, {limit, 5}]
%% '''
%%
%% If you pass the option {allow_unsafe_statements, true} to
%% {@link erlydb:code_gen/3}, you can use string and/or binary Extras
%% expressions, but this isn't recommended because it exposes to you
%% SQL injection attacks if you forget to quote your strings.


-module(erlydb_base).
-author("Yariv Sadan (yarivvv@gmail.com, http://yarivsblog.com)").

%% debugging helpers

-define(L(Msg), io:format("~p:~b ~p ~n", [?MODULE, ?LINE, Msg])).
%-define(L(Rec), io:format("LOG ~w ~p\n", [?LINE, Rec])).
-define(S(Rec), io:format("LOG ~w ~s\n", [?LINE, Rec])).

-export(
   [
    %% functions that the user can override give ErlyDB
    %% additional information about model relations 
    %% and how to map modules to database tables
    relations/0,
    fields/0,
    table/0,
    type_field/0,

    %% functions for getting information about the module's table and database
    %% fields
    db_table/1,
    db_fields/1,
    db_field_names/1,
    db_field_names_str/1,
    db_field_names_bin/1,
    db_num_fields/1,
    db_field/2,
    db_pk_fields/1,

    %% functions for getting information about a record
    is_new/1,
    is_new/2,
    get_module/1,

    %% functions for converting a record to an iolist
    to_iolist/2,
    to_iolist/3,
    field_to_iolist/1,
    field_to_iolist/2,

    %% functions for creating a record and setting its fields
    new/1,
    new_with/2,
    new_with/3,
    new_from_strings/2,
    set_fields/3,
    set_fields/4,
    set_fields_from_strs/3,
    field_from_string/2,

    %% CRUD functions
    save/1,
    insert/1,
    update/2,
    update/3,
    increment/2,
    increment/3,
    decrement/2,
    decrement/3,
    delete/1,
    delete_where/2,
    delete_id/2,
    delete_all/1,
    transaction/2,

    %% hooks
    before_save/1,
    after_save/1,
    before_delete/1,
    after_delete/1,
    after_fetch/1,

    %% SELECT functions
    find/3,
    find_id/2,
    find_first/3,
    find_max/4,
    find_range/5,
    aggregate/5,
    count/1,
    
    %% miscellaneous functions
    driver/1,
    get/2,
    set/3,

    %% private exports

    %% one-to-many functions
    find_related_one_to_many/3,
    set_related_one_to_many/3,

    %% many-to-one functions
    find_related_many_to_one/5,
    aggregate_related_many_to_one/7,

    %% many-to-many functions
    is_related/3,
    add_related_many_to_many/3,
    remove_related_many_to_many/3,
    remove_related_many_to_many_all/5,
    find_related_many_to_many/5,
    aggregate_related_many_to_many/7,
    
    %% variations on one-to-many and many-to-many find functions
    find_related_many_first/4,
    find_related_many_max/5,
    find_related_many_range/6,
    aggregate_related_many/6,

    %% internal functions
    field_names_for_query/1,
    field_names_for_query/2,
    do_save/1,
    do_delete/1,
    get_pk_fk_fields/1,
    get_pk_fk_fields2/1
   ]).

%% @doc Return the list of relations of the module. By overriding the function,
%%   you can tell ErlyDB what relations the module has. Possible relations
%%   are {one_to_many, [atom()]} and {many_to_many, [atom()]}. This function
%%   returns a list of such relations.
%%
%% @spec relations() -> [{one_to_many, [atom()]} | {many_to_many, [atom()]}]
relations() ->
    [].

%% @doc Return the list of fields that ErlyDB should use for the module.
%% You can override this function to specify which
%% database fields from the table besides the id field should be exposed
%% to records from the module.
%% The '*' atom indicates all fields, which is the default setting.
%%
%% Starting from ErlyWeb 0.6, you can add arbitrary metadata to each field
%% by describing it as a tuple of the form
%% `{FieldName::atom(), Attributes::[term()]}'. To retrieve the list of
%% attributes for a field in a given model, you can call
%% `erlydb_field:attributes(Model:db_field(FieldName))'.
%%
%% If the list of attributes contains the atom `read_only', ErlyDB
%% excludes the field from INSERT and UPDATE statements.
%%
%% It is also possible to add transient fields which do not exist in the 
%% database schema. To add transient fields it is currently necessary to 
%% overwrite the fields/0 function with the complete list of persistent 
%% fields plus the transient fields in any desired order (which also will be
%% effective in the generated model functions), e.g.:
%%
%% ``fields() -> [mydbfield, {mytransientfield, [transient]}, mydbfield2]''
%%
%% Note: You are free to call the fields() function from other modules
%% to create arbitrary field set relations.
%% For example, in a module called 'artist', you could have the function
%%
%% ``fields() -> person:fields() ++ [genre, studio]''
%%
%% @spec fields() -> '*' | [atom()]
fields() ->
    '*'.

%% @doc Return the name of the table that holds the records for this module.
%%   By default, the table name is identical to the Module's name, but you
%%   can override this to use a different table name.
%%
%% @spec table() -> atom()
table() ->
    default.

%% @doc Return the column that identifies the types of the records in a table.
%%   This is useful when storing records from multiple modules in a single
%%   table, where each module uses a different subset of fields.
%%   If you override this function, in most cases you should also override
%%   fields/0.
%%
%% @spec type_field() -> atom()
type_field() ->
    undefined.

%% functions for getting information about the database table and fields
%% for a module

%% @doc Get the table name for the module.
%%
%% @spec db_table(Module::atom()) -> atom()
db_table(Module) ->
    case Module:table() of
	default ->
	    Module;
	Other ->
	    Other
    end.

%% @doc Get the number of fields for the module.
%%
%% In generated modules, this function takes 0 parameters.
%%
%% @spec db_num_fields(NumFields::integer()) -> integer()
db_num_fields(NumFields) ->
    NumFields.

%% @doc Get a list of {@link erlydb_field} records representing the database
%%  fields for the module.
%%
%% In generated modules, this function takes 0 parameters.
%%
%% @spec db_fields(Fields::[erlydb_field()]) -> [erlydb_field()]
db_fields(Fields) ->
    Fields.

%% @doc Get the module's database fields' names as atoms.
%%
%% In generated modules, this function takes 0 parameters.
%%
%% @spec db_field_names(FileNames::[atom()]) -> [atom()]
db_field_names(FieldNames) ->
    FieldNames.

%% @doc Get the module's database fields' names as strings.
%%
%% In generated modules, this function takes 0 parameters.
%%
%% @spec db_field_names_str(FieldNameStrs::[string()]) -> [string()]
db_field_names_str(FieldNameStrs) ->
    FieldNameStrs.

%% @doc Get the module's database fields' names as binaries.
%%
%% In generated modules, this function takes 0 parameters.
%%
%% @spec db_field_names_bin(FieldNamesBin::[binary()]) -> [binary()]
db_field_names_bin(FieldNamesBin) ->
    FieldNamesBin.

%% @doc Get the {@link erlydb_field} record matching the given field name.
%%   If the field isn't found, this function exits.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @spec db_field(Module::atom(), FieldName::string() | atom()) ->
%%   erlydb_field() | exit(Err)
db_field(Module, FieldName) ->
    Pred = if is_list(FieldName) ->
		    fun(Field) ->
			    erlydb_field:name_str(Field) == FieldName
		    end;
	     true ->
		    fun(Field) ->
			    erlydb_field:name(Field) == FieldName
		    end
	  end,

    case find_val(Pred, Module:db_fields()) of
	none -> exit({no_such_field, {Module, FieldName}});
	{value, Field} -> Field
    end.

%% @doc Return the list of fields (see @link erlydb_field)
%% for which `erlydb_field:key(Field) == primary' is true.
%%
%% In generated modules, the 'Fields' parameter is omitted.
%%
%% @spec db_pk_fields(Fields::[erlydb_field()]) -> [erlydb_field()]
db_pk_fields(Fields) ->
    Fields.

%% @doc Check if the record has been saved in the database.
%%
%% @spec is_new(Rec::record()) -> boolean()
is_new(Rec) ->
    element(2, Rec).

%% @doc Set the record's 'is_new' field to the given value.
%%
%% @spec is_new(Rec::record(), Val::boolean()) -> NewRec::record()
is_new(Rec, Val) ->
    setelement(2, Rec, Val).

%% @doc Get the name of the module to which the record belongs.
%%
%% @spec get_module(Rec::record()) -> atom()
get_module(Rec) ->
    element(1, Rec).

%% @equiv to_iolist(Module, Recs, fun field_to_iolist/2)
%% @spec to_iolist(Module::atom(), Recs::record() | [record()]) -> [iolist()] |
%%   [[iolist()]]
to_iolist(Module, Recs) ->
    to_iolist(Module, Recs, fun field_to_iolist/2).

%% @doc If Recs is a single record, convert each of a record's fields into
%% an iolist
%% and return the list of the converted records. If Recs is a list of records,
%% to_iolist is recursively called on each record, and the list of results is
%% returned.
%%
%% ToIoListFun is a function that accepts an {@link erlydb_field} structure
%% and a field value and returns an iolist (see {@link field_to_iolist/2}
%% for an example).
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @spec to_iolist(Module::atom(), Rec::record() | [Rec::record()],
%%   ToIolistFun::to_iolist_function()) -> [iolist()] | [[iolist()]]
to_iolist(Module, Recs, ToIolistFun) when is_list(Recs) ->
    [to_iolist1(Module, Rec, ToIolistFun) || Rec <- Recs];
to_iolist(Module, Recs, ToIolistFun) ->
    to_iolist1(Module, Recs, ToIolistFun).

to_iolist1(Module, Rec, ToIolistFun) ->
    Fields = Module:db_fields(),
    IsDefaultFun = ToIolistFun == fun field_to_iolist/2,
    WrapperFun = 
	if IsDefaultFun -> ToIolistFun;
	   true ->
		fun(Val, Field) ->
			case ToIolistFun(Val, Field) of
			    default ->
				field_to_iolist(Val, Field);
			    Other ->
				Other
			end
		end
	end,
    lists:map(
      fun(Field) ->
	      FieldName = erlydb_field:name(Field),
	      Val = Module:FieldName(Rec),
	      WrapperFun(Val, Field)
      end, Fields).

%% @doc A helper function used for converting field values to iolists.
%%
%% @equiv field_to_iolist(Val, undefined)
%% @spec field_to_iolist(Val::term()) -> iolist()
field_to_iolist(Val) ->
    field_to_iolist(Val, undefined).

%% @doc This function converts standard ErlyDB field values to iolists.
%% This is its source code:
%% ```
%%
%%	Type = 	if 
%%        		Field =:= undefined -> undefined;
%%     			true -> erlydb_field:erl_type(Field)
%%    		end,
%%
%%  case Val of
%% 	Bin when is_binary(Bin) -> Val;
%% 	List when is_list(List) -> Val;
%% 	Int when is_integer(Int) -> integer_to_list(Val);
%% 	Float when is_float(Float) -> float_to_list(Val);
%%
%%  {datetime, {{_Year,_Month,_Day},{_Hour,_Minute,_Second}} = DateTime} -> 
%%        format_datetime(DateTime);
%%  {{_Year,_Month,_Day},{_Hour,_Minute,_Second}} = DateTime when Type =:= datetime -> 
%%        format_datetime(DateTime);
%%	{date, {_Year,_Month,_Day} = Date} -> format_date(Date);
%%  {_Year,_Month,_Day} = Date when Type =:= date -> format_date(Date);
%%	{time, {_Hour,_Minute,_Second} = Time}  -> format_time(Time);
%%  {_Hour,_Minute,_Second} = Time when Type =:= time -> format_time(Time);
%%    
%% 	undefined -> [];
%% 	_Other ->
%% 	    io_lib:format("~p", [Val])
%%    end.
%% '''
%%
%% @spec field_to_iolist(Val::term, Field::erlydb_field()) -> iolist()
field_to_iolist(Val, Field) ->
    Type = 	if 
           		Field =:= undefined -> undefined;
       			true -> erlydb_field:erl_type(Field)
    		end,
    
    case Val of
	Bin when is_binary(Bin) -> Val;
	List when is_list(List) -> Val;
	Int when is_integer(Int) -> integer_to_list(Val);
	Float when is_float(Float) -> float_to_list(Val);
	{datetime, {{_Year,_Month,_Day},{_Hour,_Minute,_Second}}
	 = DateTime} -> 
	    format_datetime(DateTime);
	{{_Year,_Month,_Day},{_Hour,_Minute,_Second}}
	= DateTime when Type =:= datetime -> 
        format_datetime(DateTime);
	{date, {_Year,_Month,_Day} = Date} -> format_date(Date);
	{_Year,_Month,_Day} = Date when Type =:= date -> format_date(Date);
	{time, {_Hour,_Minute,_Second} = Time}  -> format_time(Time);
	{_Hour,_Minute,_Second} = Time when Type =:= time -> format_time(Time);
	undefined -> [];
	_Other ->
	    io_lib:format("~p", [Val])
    end.

format_datetime({{Year,Month,Day},{Hour,Minute,Second}}) ->
    io_lib:format("~b/~b/~b ~2.10.0b:~2.10.0b:~2.10.0b",
		  [Year, Month, Day, Hour, Minute, Second]).
format_date({Year, Month, Day}) ->
    io_lib:format("~b/~b/~b", [Year, Month, Day]).
format_time({Hour, Minute, Second}) ->
    io_lib:format("~2.10.0b:~2.10.0b:~2.10.0b",[Hour, Minute, Second]).


%% @doc Create a new record with all fields set to 'undefined'.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% Generated modules also have the function new/N, where N is the number of
%% fields the module uses (as returned from db_num_fields/0), minus
%% 1 if the module has an 'identity' primary key field, which is initialized
%% by the DBMS. This function lets you create a new record and initialize
%% its fields with a single call. Note that fields that end with '_id' have
%% a special property: they accept either a literal id value, or a record
%% from a related table that has an 'id' primary key. For example, if the
%% 'project' module had the fields 'name' and 'language_id',
%% `project:new("ErlyWeb", Erlang)' would be equivalent to
%% `project:new("ErlyWeb", language:id(Erlang))'.
%%
%% @spec new(Module::atom()) -> record()
new(Module) ->
    Rec = erlang:make_tuple(Module:db_num_fields() + 2, undefined),
    Rec1 = set_is_new(Rec, true),
    set_module(Rec1, Module).

%% @doc Create a new record, setting its field values
%% according to the key/value pairs in the Fields property list.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @see set_fields/3
%% @spec new_with(Module::atom(), Fields::proplist()) -> record() | exit(Err)
new_with(Module, Fields) ->
    Module:set_fields(Module:new(), Fields).

%% @doc Similar to {@link new_with/2}, but uses the ToFieldFun to convert
%% property list values to field values before setting them. ToFieldFun
%% accepts an {@link erlydb_field} record and the original value and returns
%% the new value.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @spec new_with(Module::atom(), Fields::proplist(),
%%   ToFieldFun::function()) -> record() | exit(Err)
new_with(Module, Fields, ToFieldFun) ->
    Module:set_fields(Module:new(), Fields, ToFieldFun).

%% @equiv new_with(Module, Fields, fun field_from_string/2)
%% @see field_from_string/2
%% @spec new_from_strings(Module::atom(),
%%  Fields::[{atom() | list(), list()}]) -> record() | exit(Err)
new_from_strings(Module, Fields) ->
    Module:set_fields(Module:new(), Fields, fun field_from_string/2).

%% @doc Set the record's fields according to the name/value pairs in the
%% property list, e.g.
%%
%% ```
%% Language1 = language:set_fields(Language, [{name,"Erlang"},
%%                     {creation_year, 1981}])
%% '''
%%
%% The property list can have keys that are either strings or atoms.
%% If a field name doesn't match an existing field for this record,
%% this function exits.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @spec set_fields(Module::atom(), Record::record(), Fields::proplist()) ->
%%   record() | exit(Err)
set_fields(Module, Record, Fields) ->
    lists:foldl(
      fun({FieldName, Val}, Rec) ->
	      FieldName1 = get_field_name(Module, FieldName),
	      Module:FieldName1(Rec, Val)
      end, Record, Fields).

get_field_name(_Module, FieldName) when is_atom(FieldName) -> FieldName;
get_field_name(Module, FieldName) ->
    ErlyDbField = Module:db_field(FieldName),
    erlydb_field:name(ErlyDbField).

%% @doc Set the record's fields using according to the property list
%% name/value pairs,
%% after first converting the values using the ToFieldFun. ToFieldFun accepts
%% an {@link erlydb_field} record and the original value and returns the new
%% value.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @spec set_fields(Module::atom(), Record::record(), Fields::proplist(),
%%   ToFieldFun::function()) -> record() | exit(Err)
set_fields(Module, Record, Fields, ToFieldFun) ->
    lists:foldl(
      fun({FieldName, Val}, Rec) ->
	      ErlyDbField = Module:db_field(FieldName),
	      FieldName1 = erlydb_field:name(ErlyDbField),
	      Module:FieldName1(Rec, ToFieldFun(ErlyDbField, Val))
      end, Record, Fields).


%% @equiv set_fields(Module, Record, Fields, fun field_from_string/2)
%% @see field_from_string/2
%% @spec set_fields_from_strs(Module::atom(), Record::record(),
%%   Fields::proplist()) -> record() | exit(Err)
set_fields_from_strs(Module, Record, Fields) ->
    set_fields(Module, Record, Fields, fun field_from_string/2).

%% @doc A helper function for converting values encoded as strings to their
%% corresponding Erlang types.
%%
%% This function assumes field values are formatted according to the logic in
%% {@link field_to_iolist/2}. In addition, it checks the following ranges:
%%
%% second: 0-59<br/>
%% minute: 0-59<br/>
%% hour: 0-23<br/>
%% day: 1-31<br/>
%% month: 1-12<br/>
%% year: 1-9999<br/>
%%
%% @spec field_from_string(ErlyDbField::erlydb_field(), Str::list()) -> term()
%%  | exit(Err)
field_from_string(_ErlyDbField, undefined) -> undefined;
field_from_string(ErlyDbField, Str) ->
    case erlydb_field:erl_type(ErlyDbField) of
	%% If the value started as a string, we keep it
	%% as a string and let the database driver convert it
	%% to binary before sending it to the socket.
	%% Note: this may change in a future version.
	binary -> Str; 
	integer -> fread_val("~d", Str);
	float ->
	    case catch fread_val("~f", Str) of
		{'EXIT', _} -> fread_val("~d", Str);
		Val -> Val
	    end;
	datetime ->
	    [Year, Month, Day, Hour, Minute, Second] =
		fread_vals("~d/~d/~d ~d:~d:~d", Str),
	    {datetime, {make_date(Year, Month, Day),
			make_time(Hour, Minute, Second)}};
	date -> 
	    [Year, Month, Day] = fread_vals("~d/~d/~d", Str),
	    {date, make_date(Year, Month, Day)};
	time ->
	    [Hour, Minute, Second] = fread_vals("~d:~d:~d", Str),
	    {time, make_time(Hour, Minute, Second)}
    end.


%% @doc Save an object by executing a INSERT or UPDATE query.
%% This function returns a modified tuple representing
%% the saved record or throws an exception if an error occurs.
%%
%% With MySQL, for INSERT statements for records with identity primary keys,
%% this function sets the primary key field to the value returned from 
%% calling "SELECT last_insert_id()".
%%
%% You can override the return value by implementing the after_save
%% hook.
%%
%% @spec save(Rec::record()) -> record() | exit(Err)    
save(Rec) ->
    hook(Rec, do_save, before_save, after_save).

%% @doc Insert one or more records into the database.
%%
%% If you don't need to get the saved records' auto-generated ids, this
%% function is much more
%% efficient than calling save/1 on each record as this function saves
%% the entire list of records in one INSERT statement.
%%
%% @spec insert(Rec::record() | [record()]) -> NumInserts::integer() | exit(Err)
insert(Recs) ->
    case Recs of 
	[] -> 0;
	Rec when is_tuple(Rec) ->
	    insert1([Rec]);
	Recs ->
	    insert1(Recs)
    end.

insert1(Recs) ->
    Mod = get_module(hd(Recs)),
    Fields = Mod:db_fields(),
    Fields1 = [erlydb_field:name(Field) ||
		  Field <- Fields,
		  not is_read_only(Field),
                  not is_transient(Field)],
    Rows1 = lists:map(
	      fun(Rec) ->
		      Rec1 = Mod:before_save(Rec),
		      [Mod:Field(Rec1) || Field <- Fields1]
	      end, Recs),
    {Driver, Options} = Mod:driver(),
    case Driver:transaction(
	   fun() ->
		   Driver:update({esql, {insert, db_table(Mod), Fields1,
					 Rows1}}, Options)
	   end, Options) of
	{atomic, {ok, Num}} -> Num;
	{aborted, Err} -> exit(Err)
    end.

%% @equiv update(Module, Props, undefined)
update(Module, Props) ->
    update(Module, Props, undefined).

%% @doc Execute an UPDATE statement against the module's database table
%% and return the number of rows updated.
%%
%% 'Props' is a list of 2 element tuples, where the first element is an
%% atom representing the field's name, and the second value is an ErlSQL
%% expression representing its value.
%%
%% 'Where' is an ErlSQL 'where' expression.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% Example:
%% Calling ``person:update([{name,<<"Jane">>}, {age, {age, '+', 1}}],
%% {id,'=',7})''
%% would yield the statement ``UPDATE person SET name='Jane', age=age+1
%% WHERE id=7''.
%%
%% The UPDATE statement is executed in a transactional context.
%%
%% @spec update(Module::atom(), Props::proplist(), Where::where_expr()) ->
%%  NumUpdated::integer() | exit(Err)
update(Module, Props, Where) ->
    {Driver, Options} = Module:driver(),
    case Driver:transaction(
	   fun() ->
		   Driver:update(
		     {esql,
		      {update, db_table(Module), Props, Where}},
		     Options)
	   end, Options) of 
	{atomic, {ok, Num}} ->
	    Num;
	{aborted, Err} ->
	    exit(Err)
    end.

%% @equiv increment(Module, Fields, undefined)
increment(Module, Fields) ->
    increment(Module, Fields, undefined).

%% @doc Increment the values for the listed fields in the module's table.
%% This executes the query
%% ```
%% UPDATE [table] SET [field1] = [field1] + 1, [field2] = [field2] + 1... 
%% WHERE [where_expr]
%% '''
%%
%% @spec increment(Module::atom(), Fields::[atom()], Where::where_expr()) ->
%%   NumRowsUpdated::integer()
increment(Module, Fields, Where) ->
    inc_dec(Module, Fields, Where, '+').

%% @equiv decrement(Module, Fields, undefined)
decrement(Module, Fields) ->
    decrement(Module, Fields, undefined).

%% @doc Similar to {@link increment/3}, but decrements the fields' values.
decrement(Module, Fields, Where) ->
    inc_dec(Module, Fields, Where, '-').


inc_dec(Module, Fields, Where, Op) ->
    Module:update([{Field, {Field, Op, 1}} || Field <- Fields], Where).

%% @doc Delete the record from the database. To facilitate the after_delete
%% hook, this function expects a single record to be deleted. 
%%
%% You can override the return value by implementing the after_delete
%% hook.
%%
%% @spec delete(Rec::record()) -> ok | exit(Err)
delete(Rec) ->
    hook(Rec, do_delete, before_delete, after_delete).

%% @equiv delete_where(Module, {id,'=',Id})
%% @spec delete_id(Module::atom(), Id::integer()) -> NumDeleted::integer()
delete_id(Module, Id) ->
    delete_where(Module, {id,'=',Id}).

%% @doc Delete all records matching the Where expressions,
%% and return the number of deleted records.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @spec delete_where(Module::atom(), Where::where_expr()) ->
%%    NumDeleted::integer() | exit(Err)
delete_where(Module, Where) ->
    {DriverMod, Options} = Module:driver(),
    case DriverMod:transaction(
	   fun() ->
		   DriverMod:update(
		     {esql, {delete, {from, db_table(Module)},
			     make_where_expr(Module, Where)}},
		     Options)
	   end, Options)
	of
	{atomic, {ok, NumDeleted}} ->
	    NumDeleted;
	{aborted, Err} ->
	    exit(Err)
    end.

%% @doc Delete all records from the module and return the number of records
%% actually deleted.
%%
%% Needless to say, use this function with extreme care.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @spec delete_all(Module::atom()) -> NumDeleted::integer() | exit(Err)
delete_all(Module) ->
    delete_where(Module, undefined).

%% @doc Execute a transaction using the module's driver settings, as defined
%% by the parameters passed to {@link erlydb:code_gen/3}.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @spec transaction(Module::atom(), Fun::function()) ->
%%   {atomic, Result::term()} | {aborted, Details}
transaction(Module, Fun) ->
    {DriverMod, Options} = Module:driver(),
    DriverMod:transaction(Fun, Options).


%% hooks

%% @doc A hook that gets called before a record is saved.
%%
%% By default, this function returns the original record. You can implement
%% this function in the target module to override the default behavior.
%%
%% @spec before_save(Rec::record()) -> record() 
before_save(Rec) ->
    Rec.

%% @doc A hook that gets called after a record is saved.
%%
%% By default, this function returns the original record. You can implement
%% this function in the target module to override the default behavior.
%%
%% @spec after_save(Rec::record()) -> record()
after_save(Rec) ->
    Rec.

%% @doc A hook that gets called before a record is deleted.
%%
%% By default, this function returns the original record. You can implement
%% this function in the target module to override the default behavior.
%%
%% @spec before_delete(Rec::record()) -> record()
before_delete(Rec) ->
    Rec.

%% @doc A hook that gets called after a record is deleted. 
%%
%% By default, this function returns an integer indicating the number of rows
%% deleted.
%%
%% You can implement
%% this function in the target module to override the default behavior.
%%
%% @spec after_delete({Rec::record(), NumDeleted::integer()}) -> integer()
after_delete({_Rec, Num}) ->
    Num.


%% @doc A hook that gets called after a record is fetched from the database.
%%
%% By default, this function returns the original record. You can implement
%% this function in the target module to override the default behavior.
%%
%% @spec after_fetch(Rec::record()) -> record()
after_fetch(Rec) ->
    Rec.


%% find functions

%% @doc Find records for the module. The Where and Extras clauses are,
%%  by default, ErlSQL expressions (see {@link erlsql}).
%%  Example Where expressions are<br/>
%%  `` {name,'=',"Joe"}''<br/>
%%  and<br/>
%%  `` {{age,'>',26},'and',{country,like,"Australia"}}''
%%  
%%  Example Extras expressions are<br/>
%%  ``  {limit, 7}''<br/>
%%  and<br/>
%%  ``  [{limit, 4,5}, {order_by, [name, {age, desc}, {height, asc}]}]''
%%
%%  The main benefits of using ErlSQL are<br/>
%%  - It protects against SQL injection attacks by quoting all string
%%    values.<br/>
%%  - It simplifies embedding runtime variables in SQL expressions
%%    by automatically stringifying
%%    values such as numbers, atoms, dates and times.<br/>
%%  - It's more efficient than string concatenation because it generates
%%    iolists of binaries, which generally consume less memory than
%%    strings.
%%
%% Some drivers (e.g. the MySQL driver), let you use string and binary
%% expressions directly when you pass the {allow_unsafe_statements, true}
%% option to
%% {@link erlydb:code_gen/3}. This usage is discouraged, however, because it
%% makes you vulnerable to SQL injection attacks if you don't properly
%% encode all your strings.
%%
%% During code generation, ErlyDB creates a few derivatives from this function
%% in target modules:
%%
%% ```
%%   find()  %% returns all records
%%   find(Where)
%%   find_with(Extras)
%%   find(Where, Extras)
%% '''
%%
%% (Note that in generated modules, the 'Module' parameter is omitted.)
%%
%% ErlyDB creates similar derivatives for all find_x and aggregate functions
%% in erlydb_base (e.g. find_first(), find_first(Where),
%% find_first_with(Extras), find_first(Where, Extras)...).
%%
%% @spec find(Module::atom(), Where::where_expr(), Extras::extras_expr()) ->
%%   [record()] | exit(Err)
find(Module, Where, Extras) ->
    do_find(Module, field_names_for_query(Module, true), Where, Extras).

%% @doc Find the first record for the module according to the Where and
%% Extras expressions. If no records match the conditions, the function
%% returns 'undefined'.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @see find/3
%% @spec find_first(Modue::atom(), Where::where_expr(),
%%  Extras::extras_expr()) -> record() | undefined | exit(Err)
find_first(Module, Where, Extras) ->
    as_single_val(find_max(Module, 1, Where, Extras)).

%% @doc Find up to Max records from the module according
%%   to the Where and Extras expressions.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @see find/3
%% @spec find_max(Module::atom(), Max::integer(), Where::where_expr(),
%%   Extras::extras_expr()) -> [record()] | exit(Err)
find_max(Module, Max, Where, Extras) ->
    find(Module, Where, append_extras({limit, Max}, Extras)).

%% @doc Find up to Max records, starting from offset First,
%%   according to the Where and Extras expressions.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @see find/3
%% @spec find_range(Module::atom(), First::integer(), Max::integer(),
%%   Where::where_expr(), Extras::extras_expr()) -> [record()] | exit(Err)
find_range(Module, First, Max, Where, Extras) ->
    find(Module, Where, append_extras({limit, First, Max}, Extras)).

%% @doc Find the record with the given id value.
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @spec find_id(Module::atom(), Id::term()) -> Rec | exit(Err)
find_id(Module, Id) ->
    as_single_val(find(Module, {id,'=',Id}, undefined)).


%% @doc ErlyDB uses this function to generate derivative functions in
%% target modules for calculating aggregate values for database
%% fields. Drivative functions have the form `Module:FuncName(Field)',
%% where 'Module' is the module name, 'FuncName' is 'count', 'avg',
%% 'sum', 'min', 'max' or 'stddev', and Field is the name of the field.
%% Derivative functions also have variations as described in {@link find/3}.
%%
%% For example, in a module called 'person', ErlyDB
%% would generate the following functions:
%%
%% ```
%%   person:count(Field)
%%   person:count(Field, Where)
%%   person:count_with(Field, Extras)
%%   person:count(Field, Where, Extras)
%%   person:avg(Field)
%%   ...
%% '''
%% where Field can be any field in the person module (such as 'age', 'height',
%% etc.).
%% 
%% @see find/3
%% @spec aggregate(Module::atom(), AggFunc::atom(), Field::atom(),
%%  Where::where_expr(), Extras::extras_expr()) -> integer() | float() |
%%  exit(Err)
aggregate(Module, AggFunc, Field, Where, Extras) ->
    do_find(Module, {call, AggFunc, Field}, Where, Extras,
	    false).

%% @doc A shortcut for counting all the records for a module. In generated
%% modules, this function lets you can call Module:count() instead of
%% Module:count('*').
%%
%% In generated modules, the 'Module' parameter is omitted.
%%
%% @spec count(Module::atom()) -> integer() | exit(Err)
count(Module) ->
    aggregate(Module, 'count', '*', undefined, undefined).


%% miscellaneous functions

%% @doc A generic getter function ErlyDB uses to generate getters, e.g.
%% person:name(Person), for all of a module's database fields.
%%
%% @spec get(Idx::integer(), Rec::record()) -> term()
get(Idx, Rec) ->
    element(Idx, Rec).

%% @doc A generic setter function ErlyDB uses to generate setters, e.g.
%% person:name(Person, NewName), for all of a module's database fields.
%%
%% @spec set(Idx::integer(), Rec::record(), NewVal::term()) -> record()
set(Idx, Rec, Val) ->
    setelement(Idx, Rec, Val).

%% @doc Get the driver settings, defined in the call to
%% {@link erlydb:code_gen/3}, ErlyDB uses for the module.
%%
%% In generated modules, the 'Driver' parameter is omitted.
%%
%% @spec driver(Driver::term()) -> term()
driver(Driver) ->
    Driver.



%% many-to-one functions

%% @doc Set the foreign key fields of a record from a module having a
%% many-to-one relation to the primary key values of the Other record.
%%
%% This function isn't meant to be used directly; ErlyDB uses it to generate
%% special setters for related records in modules that define many-to-one
%% relations.
%%
%% For example, if you had a module 'bone' that defined the relation
%% `{many_to_one, [dog]}', and the 'dog' module had a single primary key
%% field called 'id', ErlyDB would add the
%% function `bone:dog(Bone, Dog)'
%% to the 'bone' module. This function would be equivalent to
%% ``bone:dog_id(Bone, dog:id(Dog))'', with an extra check to verify
%% that Dog is saved in the database.
%%
%% If 'dog' had more than one primary key field, this function would
%% set the values for all foreign key fields in the 'bone' record
%% to the values of the 'dog' record's corresponding primary key
%% values.
%%
%% @spec set_related_one_to_many(
%%      Rec::record(), PkFkFields::proplist(), Other::record()) -> record()
%%   | exit(Err)
set_related_one_to_many(PkFkFields, Rec, Other) ->
    OtherModule = get_module(Other),
    Module = get_module(Rec),
    lists:foldl(
      fun({PkField, FkField}, Rec1) ->
	      Module:FkField(Rec1, OtherModule:PkField(Other))
      end, Rec, PkFkFields).

%% @doc Find the related record for a record from a module having a
%% many-to-one relation.
%%
%% This function isn't meant to be used directly; ErlyDB uses it to generate
%% special 'find' functions for related records in modules
%% defining many-to-one relations.
%%
%% For example, if you had a module 'bone' that defined the relation
%% `{many_to_one, [dog]}', and 'dog' had a single primary key field called
%% 'id', ErlyDB would add the function `bone:dog(Bone)'
%% to the 'bone' module. This function would be equivalent to
%% `dog:find({id,'=',bone:dog_id(Bone)}).'.
%%
%% This function works as expected when the related module has multiple
%% primary key fields.
%%
%% @spec find_related_one_to_many(OtherModule::atom(),
%%  PkFkfields::proplist(), Rec::record()) ->
%%  record() | exit(Err)
find_related_one_to_many(OtherModule, PkFkFields, Rec) ->
    Module = get_module(Rec),
    WhereClause =
	{'and', [{PkField, '=', Module:FkField(Rec)} ||
		    {PkField, FkField} <- PkFkFields]},
    as_single_val(OtherModule:find(WhereClause)).

%% one-to-many functions

%% @doc Find the set of related records in a one-to-many relation.
%%
%% This function isn't meant to be used directly; ErlyDB uses this function
%% to generate special 'find' functions in modules that define
%% one-to-many relations.
%%
%% For example, if you had a module 'dog' that defined the relation
%% `{one_to_many, [bone]}', ErlyDB would add the following
%% functions to the 'dog' module:
%%
%% ```
%% dog:bones(Dog)
%% dog:bones(Dog, Where)
%% dog:bones_with(Dog, Extras)
%% dog:bones(Dog, Where, Extras)
%%
%% dog:bones_first(Dog)
%% dog:bones_first(Dog, Where)
%% dog:bones_first_with(Dog, Extras)
%% dog:bones_first(Dog, Where, Extras)
%%
%% dog:bones_max(Dog, Max)
%% dog:bones_max(Dog, Max, Where)
%% dog:bones_max_with(Dog, Max, Extras)
%% dog:bones_max(Dog, Max, Where, Extras)
%%
%% dog:bones_range(Dog, First, Max)
%% dog:bones_range(Dog, First, Max, Where)
%% dog:bones_range_with(Dog, First, Max, Extras)
%% dog:bones_range(Dog, First, Max, Where, Extras)
%% '''
%%
%% @see find/3
%% @see find_first/3
%% @see find_max/4
%% @see find_range/5
%% @spec find_related_many_to_one(OtherModule::atom(),
%%    PkFks::term(),
%%    Rec::record(),
%%    Where::where_expr(), Extras::extras_expr()) -> [record()] | exit(Err)
find_related_many_to_one(OtherModule, PkFks, Rec, Where,
			Extras) ->
    OtherModule:find(
      make_fk_expr(PkFks, Rec, Where), Extras).

%% @doc Get aggregate statistics about fields from related records in
%% one-to-many relations.
%%
%% This function isn't meant to be used directly; ErlyDB uses this function
%% to generate special aggregate functions in modules that define
%% one-to-many relations.
%%
%% For example, if you had a module 'dog' that defined the relation
%% `{one_to_many, [bone]}', ErlyDB would add the following
%% functions to the 'dog' module:
%%
%% ```
%% dog:avg_of_bones(Dog, Field)
%% dog:avg_of_bones(Dog, Field, Where)
%% dog:avg_of_bones_with(Dog, Field, Extras)
%% dog:avg_of_bones(Dog, Field, Where, Extras)
%% '''
%%
%% where 'Field' is the name of the field in the 'bone' module (e.g. 'size').
%%
%% ErlyDB generates similar derivatives for all aggregate functions listed in
%% {@link aggregate/5}.
%%
%% @see aggregate/5
%% @spec aggregate_related_many_to_one(OtherModule::atom(),
%% PkFks::term(),
%% AggFunc::atom(),
%% Rec::record(), Field::atom(),
%% Where::where_expr(), Extras::extras_expr()) -> float() | integer() |
%%   exit(Err)
aggregate_related_many_to_one(OtherModule, PkFks, AggFunc, Rec, Field, Where,
			      Extras) ->
    OtherModule:AggFunc(Field,
			make_fk_expr(PkFks, Rec, Where), Extras).

%% many-to-many functions

%% @doc Add a related record in a many-to-many relation.
%%
%% This function isn't meant to be used directly; ErlyDB uses this function
%% to generate special add_[RelatedModule] functions in modules that define
%% many-to-many relations.
%%
%% For instance, if you had a module 'student' that defined the relation
%% `{many_to_many, [class]}', and both 'student' and 'class' had a single
%% primary key field called 'id', ErlyDB would add the function
%% `student:add_class(Student, Class)' to the 'student' module. This
%% function would insert the row [class:id(Class), student:id(Student)] to
%% the class_student table, where the first column is 'class_id'
%% and the second column is 'student_id'.
%%
%% If either module has multiple primary key fields, all those fields are
%% mapped to foreign keys in the many-to-many relation table.
%%
%% @spec add_related_many_to_many(JoinTable::atom(), Rec::record(),
%%   OtherRec::record() | [record()]) -> {ok, NumAdded} | exit(Err)
add_related_many_to_many(JoinTable, Rec, OtherRec) ->
    case OtherRec of
	[] ->
	    {ok, 0};
	_ ->
	    {DriverMod, Options} = get_driver(Rec),
	    Query = {esql, make_add_related_many_to_many_query(
			     JoinTable, Rec, OtherRec)},
	    Res =
		DriverMod:transaction(
		  fun() ->
			  DriverMod:update(Query, Options)
		  end, Options),
	    case Res of
		{atomic, {ok, 1}} when is_tuple(OtherRec) ->
		    ok;
		{atomic, Other} -> Other;
		{aborted, Err} -> exit(Err)
	    end
    end.

make_add_related_many_to_many_query(JoinTable, Rec, [OtherRec]) ->
    make_add_related_many_to_many_query(JoinTable, Rec, OtherRec);
make_add_related_many_to_many_query(JoinTable, Rec, OtherRec)
  when not is_list(OtherRec) ->
    Fields = get_join_table_fields(Rec, OtherRec),
    {insert, JoinTable, Fields};
make_add_related_many_to_many_query(JoinTable, Rec, OtherRecs) ->
    Mod = get_module(Rec),
    OtherMod = get_module(hd(OtherRecs)),
    Table = db_table(Mod),
    OtherTable = db_table(OtherMod),
    if OtherTable == Table ->
	    Fields = Mod:get_pk_fk_fields2(),
	    Rows1 = 
		lists:map(
		  fun(OtherRec) ->
			  [R1, R2] = 
			      sort_records(Rec, OtherRec,
					   Fields),
			  Mod1 = element(1, R1),
			  Mod2 = element(1, R2),
			  lists:foldl(
			    fun({PkField, _FkField1, _FkField2}, Acc) ->
				    [Mod1:PkField(R1), Mod2:PkField(R2) | Acc]
			    end, [], Fields)
		  end, OtherRecs),
	    FkFields =
		lists:foldl(
		  fun({_PkField, FkField1, FkField2}, Acc) ->
			  [FkField1, FkField2 | Acc]
		  end, [], Fields),
	    {insert, JoinTable, FkFields, Rows1};
       true ->
	    Fields = Mod:get_pk_fk_fields(),
	    OtherFields = OtherMod:get_pk_fk_fields(),
	    FkFields = [Fk || {_Pk, Fk} <-
				  Fields ++ OtherFields],
	    Rows = lists:map(
		     fun(OtherRec) ->
			     OtherVals =
				 [OtherMod:PkField(OtherRec) ||
				     {PkField, _} <- OtherFields],
			     lists:foldr(
			       fun({PkField, _}, Acc2) ->
				       [Mod:PkField(Rec) | Acc2]
			       end, OtherVals, Fields)
		     end, OtherRecs),
	    {insert, JoinTable, FkFields, Rows}
    end.

%% @doc Remove a related record in a many-to-many relation.
%%
%% This function isn't meant to be used directly; ErlyDB uses this function
%% to generate special remove_[RelatedModule] functions in modules that define
%% many-to-many relations.
%%
%% For instance, if you had a module 'student' that defined the relation
%% `{many_to_many, [class]}', and module 'class' and 'student' had a single
%% primary key field called 'id', ErlyDB would add the function
%% `student:remove_class(Student, Class)' to the 'student' module. This
%% function would remove the row [class:id(Class), student:id(Student)]
%% from the class_student table, where the first column is 'class_id'
%% the second column is 'student_id'.
%%
%% This function expects a single record to be removed. 
%%
%% @spec remove_related_many_to_many(JoinTable::atom(), Rec::record(),
%%   OtherRec::record()) -> NumRemoved::interger() |
%%   exit(Err)
remove_related_many_to_many(JoinTable, Rec, OtherRec) ->
    do_remove(Rec, make_remove_related_many_to_many_query(
		     JoinTable, Rec, OtherRec)).

do_remove(Rec, Query) ->
    {DriverMod, Options} = get_driver(Rec),
    Res =
	DriverMod:transaction(
	  fun() ->
		  DriverMod:update({esql, Query}, Options)
	  end, Options),
    case Res of
	{atomic, {ok, Num}} -> Num;
	{aborted, Err} -> exit(Err)
    end.

%% @doc Remove all related recorded according to a Where and Extras clause
%% in a many-to-many relation.
%%
%% This function isn't meant to be used directly; ErlyDB uses this function
%% to generate special remove_all_[RelatedModuleAsPlural] functions in
%% modules that define many-to-many relations.
%%
%% For instance, if you had a module 'student' that defined the relation
%% `{many_to_many, [class]}', and module 'class' and 'student' had a single
%% primary key field called 'id', ErlyDB would add the function
%% `student:remove_all_classes(Student, Where, Extras)' to the 'student'
%% module. This function would remove [class:id(Class), student:id(Student)]
%% rows from the class_student table according to the Where and Extras clauses.
%%
%% In addition to student:remove_all_classes/3, ErlyDB would generate
%% additional variations. This would be the full list:
%%
%% ```
%% student:remove_all_classes(Student)
%% student:remove_all_classes(Student, Where)
%% student:remove_all_classes_with(Student, Extras)
%% student:remove_all_classes(Student, Where, Extras)
%% '''
%%
%% Limitation: for self-referencing many-to-many relations, all variations
%% accepting a Where clause are currently not generated.
%% 
%% @spec remove_related_many_to_many_all(JoinTable::atom(), OtherTable::atom(),
%%   Rec::record(), Where::where_clause(), Extras::extras_clause()) ->
%%     {ok,NumDeleted}  | exit(Err)
remove_related_many_to_many_all(JoinTable, OtherTable,
				Rec, Where, Extras) ->
    Query = make_remove_related_many_to_many_all_query(
	      JoinTable, OtherTable, Rec, Where, Extras),
    do_remove(Rec, Query).

make_remove_related_many_to_many_all_query(JoinTable, OtherMod, Rec,
					   Where, Extras) ->
    OtherTable = db_table(OtherMod),
    Mod = get_module(Rec),
    Table = db_table(Mod),
    Expr =
	if Table == OtherTable ->
		Fields = Mod:get_pk_fk_fields2(),
		Conds1 =
		    [{{JoinTable, FkField1}, '=', Mod:PkField(Rec)} ||
			{PkField, FkField1, _FkField2} <- Fields],
		Conds2 =
		    [{{JoinTable, FkField2}, '=', Mod:PkField(Rec)} ||
			{PkField, _FkField1, FkField2} <- Fields],
		{'or', [{'and', Conds1}, {'and', Conds2}]};
	   true ->
		Conds =
		    [{{JoinTable, FkField}, '=', Mod:PkField(Rec)} ||
			{PkField, FkField} <-
			     Mod:get_pk_fk_fields()],
		{'and', Conds}
	end,
    if Where == undefined ->
	    {delete, JoinTable, undefined, Expr, Extras};
       true ->
	    {Using, Where1} = 
		if Table == OtherTable ->
			{undefined, Expr};
		   true ->
			Fields1 = OtherMod:get_pk_fk_fields(),
			Where2 =
			     lists:foldl(
			       fun({PkField, FkField}, Acc) ->
				       [{{OtherTable, PkField}, '=',
					 {JoinTable, FkField}} | Acc]
			       end, [Where, Expr], Fields1),
			{[JoinTable, OtherTable], {'and', Where2}}
		end,
	    {delete, JoinTable, Using, Where1, Extras}
    end.
	    

make_remove_related_many_to_many_query(JoinTable, Rec, OtherRec) ->
    {delete, JoinTable, make_join_table_expr(Rec, OtherRec)}.

make_join_table_expr(Rec, OtherRec) ->
    {'and', [{Field,'=',Val} ||
		{Field, Val} <- get_join_table_fields(Rec, OtherRec)]}.

get_join_table_fields(Rec, OtherRec) -> 
    Mod = get_module(Rec),
    OtherMod = get_module(OtherRec),
    case db_table(Mod) == db_table(OtherMod) of
	true ->
	    Fields = Mod:get_pk_fk_fields2(),
	    [Rec1, Rec2] = sort_records(Rec, OtherRec, Fields),
	    Mod1 = element(1, Rec1),
	    Mod2 = element(1, Rec2),
	    lists:foldl(
	      fun({PkField, FkField1, FkField2}, Acc) ->
		      [{FkField1, Mod1:PkField(Rec1)},
		       {FkField2, Mod2:PkField(Rec2)} | Acc]
	      end, [], Fields);
       _ ->
	    Fields1 = [{FkField, Mod:PkField(Rec)} ||
			  {PkField, FkField} <-
			      Mod:get_pk_fk_fields()],
	    lists:foldl(
	      fun({PkField, FkField}, Acc) ->
		      [{FkField, OtherMod:PkField(OtherRec)} | Acc]
	      end, Fields1, OtherMod:get_pk_fk_fields())
    end.

sort_records(R1, R2, []) -> [R1, R2];
sort_records(R1, R2, [{PkField, _, _} | Rest]) ->
    case getf(R1, PkField) < getf(R2, PkField) of
	true ->
	    [R1, R2];
	_ ->
	    case getf(R1, PkField) == getf(R2, PkField) of
		true ->
		    sort_records(R1, R2, Rest);
		false ->
		    [R2, R1]
	    end
    end.

getf(Rec, Field) ->
    Mod_F = element(1, Rec),
    Mod_F:Field(Rec).

is_related(JoinTable, Rec, OtherRec) ->
    Mod = get_module(Rec),
    {Driver, Options} = Mod:driver(),
    Expr = make_join_table_expr(Rec, OtherRec),
    Q = {select, {call, count, '*'}, {from, [JoinTable]},
	 {where, Expr}},
    Res = Driver:select({esql, Q}, Options),
    case Res of
	{ok, [{0}]} ->
	    false;
	{ok, [{_Num}]} ->
	    true;
	Err ->
	    exit(Err)
    end.


%% @doc This function works as {@link find_related_many_to_one/4}, but
%% for modules defining many-to-many relations.
%%
%% @see find_related_many_to_one/4
%% @spec find_related_many_to_many(OtherModule::atom(), JoinTable::atom(),
%% Rec::record(), Where::where_clause(), Extras::extras_clause()) ->
%%   [record()] | exit(Err)   
find_related_many_to_many(OtherModule, JoinTable, Rec, Where, Extras) ->
    Fields = [{db_table(OtherModule), Field} ||
		 Field <- OtherModule:field_names_for_query()],
    find_related_many_to_many2(OtherModule, JoinTable, Rec, Fields,
			       Where, Extras, true).

find_related_many_to_many2(OtherModule, JoinTable, Rec, Fields,
			   Where, Extras, AsModule) ->
    Query = {esql, make_find_related_many_to_many_query(
		     OtherModule, JoinTable, Rec, Fields,
		     Where, Extras)},
    select(OtherModule, Query, AsModule).

make_find_related_many_to_many_query(OtherModule, JoinTable, Rec, Fields,
				     Where, Extras) ->
    OtherTable = db_table(OtherModule),
    Module = get_module(Rec),
    Cond =
	case OtherTable == db_table(Module) of
	    true ->
		PkFks = Module:get_pk_fk_fields2(),
		{'or', 
		 [{'and',
		   [{{JoinTable, FkField2},'=',
		     Module:PkField(Rec)} ||
		       {PkField, _FkField1, FkField2} <- PkFks] ++
		   [{{OtherTable, PkField}, '=',
		     {JoinTable, FkField1}} ||
		       {PkField, FkField1, _FkField2} <- PkFks]},
		  {'and',
		   [{{JoinTable, FkField1},'=',
		     Module:PkField(Rec)} ||
		       {PkField, FkField1, _FkField2} <- PkFks] ++
		   [{{OtherTable, PkField}, '=',
		     {JoinTable, FkField2}} ||
		       {PkField, _FkField1, FkField2} <- PkFks]}]};
	    _ ->
		{'and',
		 [{{JoinTable, FkField},'=',Module:PkField(Rec)} ||
		     {PkField, FkField} <- Module:get_pk_fk_fields()] ++
		 [{{OtherTable, PkField},'=',{JoinTable, FkField}} ||
		     {PkField, FkField} <- OtherModule:get_pk_fk_fields()]}
	end,
    {select, Fields,
     {from, [JoinTable, db_table(OtherModule)]},
     make_where_expr(OtherModule, Cond, Where),
     Extras}.

%% @doc This function works as {@link aggregate_related_many_to_one/5}, but
%% for modules defining many-to-many relations.
%%
%% @see aggregate_related_many_to_one/5
%% @spec aggregate_related_many_to_many(OtherModule::atom(), JoinTable::atom(),
%% AggFunc::atom(), Rec::record(), Field::atom(), Where::where_clause(),
%% Extras::extras_clause()) -> [term()] | exit(Err)   
aggregate_related_many_to_many(OtherModule, JoinTable, AggFunc, Rec, Field,
			       Where, Extras) ->
    find_related_many_to_many2(
      OtherModule, JoinTable, Rec,
      {call, AggFunc, Field}, Where, Extras, false).



%% generic find functions for both many-to-one and many-to-many relations

%% @hidden
find_related_many(Func, Rec, Where, Extras) ->
    Mod = get_module(Rec),
    Mod:Func(Rec, Where, Extras).

%% @hidden
find_related_many_first(Func, Rec, Where, Extras) ->
    as_single_val(find_related_many_max(Func, Rec, 1, Where, Extras)).

%% @hidden
find_related_many_max(Func, Rec, Num, Where, Extras) ->
    find_related_many(Func, Rec, Where, append_extras({limit, Num},Extras)).

%% @hidden
find_related_many_range(Func, Rec, First, Last, Where, Extras) ->
    find_related_many(Func, Rec, Where,
		      append_extras({limit, First, Last}, Extras)).

%% @hidden
aggregate_related_many(Func, AggFunc, Rec, Field, Where, Extras) ->
    Module = get_module(Rec),
    Module:Func(AggFunc, Rec, Field, Where, Extras).




%% internal functions

%% @hidden
do_save(Rec) ->
    {DriverMod, Options} = get_driver(Rec),
    Res =
	case make_save_statement(Rec) of
	    {insert, Stmt} ->
		DriverMod:transaction(
		  fun() ->
			  case DriverMod:update({esql, Stmt}, Options) of
			      {ok, 1} ->
				  Module = get_module(Rec),
				  Rec1 = set_is_new(Rec, false),
				  PkField = hd(Module:db_pk_fields()),

				  HasIdentity = erlydb_field:extra(PkField)
				      == identity,
				  if HasIdentity ->
					  case DriverMod:get_last_insert_id(db_table(Module),
						 Options) of
					      {ok, Val} ->
						  FName = erlydb_field:name(
							    PkField),
						  Module:FName(Rec1, Val);
					      Err ->
						  Err
					  end;
				     true ->
					  Rec1
				  end;
			      Err ->
				  Err
			  end
		  end, Options);
	    {update, Stmt} ->
		DriverMod:transaction(
		  fun() ->
			  case DriverMod:update({esql, Stmt}, Options) of
			      {ok, Num} when Num == 0; Num == 1 ->
				  Rec;
			      Other ->
				  Other
			  end
		  end,  Options)
	end,
    case Res of
	{atomic, NewRec} -> NewRec;
	{aborted, Err} -> exit(Err)
    end.

make_save_statement(Rec) ->
    Module = get_module(Rec),
    Fields =
	[erlydb_field:name(Field) ||
	    Field <- Module:db_fields(),
	    not is_read_only(Field),
	    not is_transient(Field)],
    case is_new(Rec) of
	false ->
	    Vals = [{Field, Module:Field(Rec)} || Field <- Fields],
	    {update, {update, get_table(Rec), Vals,
		      {where, make_pk_expr(Rec)}}};
	true ->
	    %% filter out the fields whose values are null
	    {Fields1, Vals1} =
		lists:foldl(
		  fun(Field, {FieldsAcc, ValsAcc} = Acc) ->
			  case Module:Field(Rec) of
			      undefined -> Acc;
			      Val ->
				  {[Field | FieldsAcc],
				   [Val | ValsAcc]}
			  end
		  end, {[], []}, Fields),
	    {Fields2, Vals2} = 
		case Module:type_field() of
		    undefined -> {Fields1, Vals1};
		    TypeField -> {[TypeField | Fields1],
				  [atom_to_list(Module) | Vals1]}
		end,
	    {insert, {insert, get_table(Rec), Fields2, [Vals2]}}
    end.

%% @hidden
do_delete(Rec) ->
    {DriverMod, Options} = get_driver(Rec),
    Stmt = make_delete_stmt(Rec),
    Res = DriverMod:transaction(
	    fun() ->
		    case DriverMod:update(Stmt, Options) of
			{ok, Num} ->
			    {Rec, Num};
			Err ->
			    Err
		    end
	    end, Options),
    case Res of
	{atomic, Result} -> Result;
	{aborted, Err} -> exit(Err)
    end.

make_delete_stmt(Rec) ->
    {esql, {delete, get_table(Rec),
	    {where, make_pk_expr(Rec)}}}.

%% @hidden
get_pk_fk_fields(Fields) ->
    Fields.

%% @hidden
get_pk_fk_fields2(Fields) ->
    Fields.

do_find(Module, Fields, Where, Extras) ->
    do_find(Module, Fields, Where, Extras, true).

do_find(Module, Fields, Where, Extras, AsModule) ->
    select(Module, make_find_query(Module, Fields, Where, Extras), AsModule).
	
make_find_query(Module, Fields, Where, Extras) ->
    {esql,
     {select, Fields, {from, db_table(Module)},
      make_where_expr(Module, Where),
      Extras}}.


make_pk_expr(Rec) ->
    make_pk_expr(Rec, undefined).

make_pk_expr(Rec, Where) ->
    Mod = get_module(Rec),
    FieldsExpr = [{PkField, '=', Mod:PkField(Rec)} ||
		     {PkField, _FkField} <- Mod:get_pk_fk_fields()],
    make_pk_expr2(Where, FieldsExpr).

make_fk_expr(PkFks, Rec, Where) ->
    Mod = get_module(Rec),
    PkFks1 = if PkFks == undefined ->
		     Mod:get_pk_fk_fields();
		true ->
		     PkFks
	     end,
    FieldsExpr = [{FkField, '=', Mod:PkField(Rec)} ||
		 {PkField, FkField} <- PkFks1],
    make_pk_expr2(Where, FieldsExpr).

make_pk_expr2(WhereExpr, FieldsExpr) ->
    WhereList =
	case WhereExpr of
	    undefined -> [];
	    _ -> [WhereExpr]
	end,
    {'and', WhereList ++ FieldsExpr}.

append_extras(Clause, Extras) ->
    case Extras of
	undefined ->
	    Clause;
	L when is_list(L) ->
	    Extras ++ Clause;
	OtherClause ->
	    [OtherClause, Clause]
    end.

select(Module, Query, true) ->
    {DriverMod, Options} = Module:driver(),
    Res = DriverMod:select_as(Module, Query, Options),
    case Res of
	{ok, Rows} ->
	    [Module:after_fetch(Rec) || Rec <- Rows];
	Err ->
	    exit(Err)
    end;
select(Module, Query, false) ->
    {DriverMod, Options} = Module:driver(),
    as_single_val(DriverMod:select(Query, Options), true).
    

%% Enables the before_X and after_X hooks.
hook(Rec, Func, BeforeFunc, AfterFunc) ->
    Module = get_module(Rec),
    Rec1 = Module:BeforeFunc(Rec),
    Rec2 = Module:Func(Rec1),
    Module:AfterFunc(Rec2).

find_val(_Pred, []) -> none;
find_val(Pred, [First | Rest]) ->
    case Pred(First) of
	true -> {value, First};
	false -> find_val(Pred, Rest)
    end.
    

%% internal functions

get_driver(Rec) ->
    Module = get_module(Rec),
    Module:driver().


%% @hidden
field_names_for_query(Module) ->
    field_names_for_query(Module, false).

%% @hidden
field_names_for_query(Module, UseStar) ->
    case Module:fields() of
	'*' ->
	    case Module:type_field() of
		undefined ->
		    if UseStar -> '*';
		       true -> Module:db_field_names()
		    end;
		_TypeCol ->
		    Module:db_field_names()
	    end;
	_Fields ->
	    Module:db_field_names()
    end.


%% if_saved(Rec, Fun) when not is_list(Rec) ->
%%     if_saved([Rec], Fun);
%% if_saved(Recs, Fun) ->
%%     case catch lists:foreach(
%% 	    fun(Rec) ->
%% 		    case is_new(Rec) of
%% 			true ->
%% 			    exit({no_such_record, Rec});
%% 			false ->
%% 			    ok
%% 		    end
%% 	    end, Recs)
%% 	of
%% 	ok ->
%% 	    Fun();
%% 	{'EXIT', Err} ->
%% 	    exit(Err)
%%     end.


set_module(Rec, Module) ->
    setelement(1, Rec, Module).

get_table(Rec) ->
    Module = get_module(Rec),
    db_table(Module).

set_is_new(Rec, Val) ->
    setelement(2, Rec, Val).


make_where_expr(Module, Expr) ->
    make_where_expr(Module, Expr, undefined).
make_where_expr(Module, Expr1, Expr2) ->
    case Module:type_field() of
	undefined ->
	    {where, and_expr(Expr1, Expr2)};
	FieldName ->
	    Expr3 = and_expr(Expr1, Expr2),
	    {where, and_expr({{db_table(Module),FieldName}, '=',
			      atom_to_list(Module)},
				 Expr3)}
    end.

and_expr(undefined, Expr2) -> Expr2;
and_expr(Expr1, undefined) -> Expr1;
and_expr(Expr1, Expr2) -> {Expr1, 'and', Expr2}.

as_single_val(Res) -> as_single_val(Res, false).

as_single_val({ok, [{Val}]}, true) -> Val;
as_single_val([], _) -> undefined;
as_single_val([Val], false) -> Val;
as_single_val({error, _} = Err, _) -> exit(Err);
as_single_val(Res, _) -> exit({too_many_results, Res}).

fread_val(Format, Str) ->
    [Val] = fread_vals(Format, Str),
    Val.

fread_vals(Format, Str) ->
    case io_lib:fread(Format, Str) of
	{ok, Vals, []} -> Vals;
	{ok, _, Rest} -> exit({invalid_input, Rest});
	{more, _, _, _} -> exit({missing_values, Str});
	_Err -> exit({parse_error, Format, Str})
    end.

make_date(Year, Month, Day) ->
    check_limits(Month, month, 1, 12),
    check_limits(Day, day, 1, 31),
    check_limits(Year, year, 1, 9999),
    {Year, Month, Day}.

make_time(Hour, Minute, Second) ->
    check_limits(Hour, hour, 0, 23),
    check_limits(Minute, minute, 0, 59),
    check_limits(Second, second, 0, 59),
    {Hour, Minute, Second}.

check_limits(Val, Name, Min, Max) ->
    if Val > Max orelse Val < Min ->
	    exit({invalid_value, Name, Val});
       true -> Val
    end.

is_read_only(Field) ->
    lists:member(read_only, erlydb_field:attributes(Field)).

is_transient(Field) ->
    lists:member(transient, erlydb_field:attributes(Field)).
