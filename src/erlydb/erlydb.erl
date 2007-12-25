%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com]
%% @copyright Yariv Sadan 2006-2007
%% @doc ErlyDB: The Erlang Twist on Database Abstraction.
%%
%% == Contents ==
%%
%% {@section Introduction}<br/>
%% {@section Primary and Foreign Key Conventions}
%%
%% == Introduction ==
%% ErlyDB is a database abstraction layer generator for Erlang. ErlyDB
%% combines database metadata and user-provided metadata to generate
%% functions that let you perform common data access operations in
%% an intuitive manner. It also provides a single API for working with
%% different database engines (although currently, only MySQL is supported),
%% letting you write portable data access code.
%%
%% ErlyDB is designed to work with relational schemas, supporting both
%% one-to-many and many-to-many relations. For more details on how to
%% define relations between modules, see {@link erlydb_base:relations/0}.
%%
%% By using {@link erlsql} under the hood for SQL statement generation, ErlyDB
%% provides a simple and effective mechanism for protection against
%% SQL injection attacks. (It's possible to use ErlyDB in 'unsafe' mode,
%% which lets you write SQL statement snippets as strings, but this isn't
%% recommended.) Many of the functions that ErlyDB generates let you extend
%% the automatically generated queries by passing WHERE
%% conditions and/or extras (e.g. LIMIT, ORDER BY) clauses, expressed as
%% ErlSQL snippets, as parameters.
%%
%% ErlyDB uses the module erlydb_base as a generic template for database
%% access modules. During code generation, ErlyDB calls
%% smerl:extend(erlydb_base, Module), and then performs different
%% manipulations on the functions in the resulting module in order to
%% specialize them for the specific model.
%% 
%% To learn about the functions that ErlyDB generates and how to implement
%% functions that provide ErlyDB extra database metadata prior to code
%% generation, refer to the documentation for erlydb_base.
%%
%% You can find sample code illustrating how to use many of ErlyDB's features
%% in the test/erlydb directory.
%%
%% == Primary and Foreign Key Conventions ==
%%
%% Prior to ErlyWeb 0.4, ErlyDB assumed that all tables have an identity
%% primary key field named 'id'. From ErlyWeb 0.4, ErlyDB lets users define
%% arbitrary primary key fields for their tables. ErlyDB
%% figures out which fields are the primary key fields automatically by
%% querying the database' metadata.
%%
%% ErlyDB currently relies on a naming convention to map primary key field
%% names to foreign key field names in related tables. Foreign key field
%% names are constructed as follows: [TableName]_[FieldName]. For example,
%% if the 'person' table had
%% primary key fields named 'name' and 'age', then related tables would have
%% the foreign key fields 'person_name' and 'person_age', referencing the
%% 'name' and 'age' fields of the 'person' table.
%%
%% Important: Starting from ErlyWeb 0.4, when a module defines a different
%% table name (by overriding the {@link erlydb_base:table/0} function),
%% the table name is used in foreign key field names, not the module name.
%%
%% In one-to-many/many-to-one relations, the foreign key fields for the 'one'
%% table exist in the 'many' table. In many_to_many relations, all
%% foreign key fields for both modules exist in a separate table named
%% [Table1]_[Table2], where Table1 &lt; Table2 by alphabetical ordering.
%%
%% Starting from v0.4, ErlyDB has special logic to handle the case where a
%% module has a
%% many-to-many relation to itself. In such a case, the relation table
%% would be called [TableName]_[TableName], and its fields would be the
%% table's primary key corresponding foreign key fields,
%% first with the postfix "1", and
%% then with the postfix "2". For example, if the 'person' module defined
%% the relation `{many_to_many, [person]}' (and the table name were 'person',
%% i.e., the default), then there should exist a
%% 'person_person' relation table with the following fields: person_name1,
%% person_name2, person_age1, and person_age2.
%%
%% (In addition to using a different foreign key naming convention, ErlyDB uses
%% different query construction rules when working with self-referencing
%% many-to-many relations.)
%%
%% In a future version, ErlyDB may allow users to customize the foreign
%% key field names as well as many_to_many relation table names.

%% For license information see LICENSE.txt

-module(erlydb).
-author("Yariv Sadan (yarivsblog@gmail.com) (http://yarivsblog.com)").

-export(
   [start/1,
    start/2,
    code_gen/2,
    code_gen/3,
    code_gen/4]).

-import(erlyweb_util, [log/5]).
-define(Debug(Msg, Params), log(?MODULE, ?LINE, debug, Msg, Params)).
-define(Info(Msg, Params), log(?MODULE, ?LINE, info, Msg, Params)).
-define(Error(Msg, Params), log(?MODULE, ?LINE, error, Msg, Params)).

%% useful for debugging
-define(L(Obj), io:format("LOG ~w ~p\n", [?LINE, Obj])).
-define(S(Obj), io:format("LOG ~w ~s\n", [?LINE, Obj])).

%% You can add more aggregate function names here and they will be generated
%% automatically for your modules.
aggregate_functions() ->
    [count, avg, min, max, sum, stddev].

%% @doc Start an ErlyDB session for the driver using the driver's default
%% options.
%% This only works for some drivers. For more details, refer to the driver's
%% documentation.
%%
%% @spec start(Driver::atom()) -> ok | {error, Err}
start(Driver) ->
    start(Driver, []).

%% @doc Start an ErlyDB sessions for the driver using the list of 
%% user-defined options. For information on which options are available for
%% a driver, refer to the driver's documentation.
%%
%% @spec start(Driver::atom(), Options::proplist()) -> ok | {error, Err}
start(mysql, Options) ->
    erlydb_mysql:start(Options);

start(mnesia, Options) ->
    erlydb_mnesia:start(Options);
                         
start(_Driver, _Options) ->
    {error, driver_not_supported}.
    

driver_mod(mysql) -> erlydb_mysql;
driver_mod(psql) -> erlydb_psql;
driver_mod(mnesia) -> erlydb_mnesia;
driver_mod(odbc) -> erlydb_odbc.


%% @equiv code_gen(Modules, Drivers, [])
code_gen(Modules, Drivers) ->
    code_gen(Modules, Drivers, []).

%% @equiv code_gen(Modules, Drivers, Options, [])
code_gen(Modules, Drivers, Options) ->
    code_gen(Modules, Drivers, Options, []).

%% @doc Generate code for the list of modules using the provided drivers.
%%
%% If you're using ErlyWeb, you shouldn't need to call this function directly.
%% Instead, refer to {@link erlyweb:compile/2}.
%%
%% === Usage ===
%%
%% In ErlyWeb 0.7, the signature for this function has changed.
%% ErlyDB used to support only a single driver with a single connection
%% pool in a session. As of ErlyWeb 0.7, ErlyDB supports multiple
%% drivers in a session, and multiple connection pools for each
%% driver.
%%
%% ==== Modules ====
%% The 'Modules' parameter is a list of files or modules for which to
%% generate ErlyDB code. If a list item is an atom, ErlyDB assumes it's
%% a module that has been loaded into the VM or that resides in the VM's
%% code path. In either case, the module's source code should be discoverable
%% either through Erlang's path conventions or because the module
%% was compiled with debug_info.
%%
%% If a list item is a string, ErlyDB treats it as a file name (relative
%% or absolute) and attempts to read it from disk.
%%
%% ==== Drivers ====
%% The 'Drivers' parameter is either a single element or a list of
%% elements of the form
%% `Driver::atom()',
%% `{Driver::atom(), DriverOptions::proplist()}', or
%% `{Driver::atom(), DriverOptions::proplist(), Pools::pool()}'.
%%
%% The first element in the Drivers list is
%% the default driver that ErlyDB will use for all modules that don't
%% override the driver option.
%%
%% 'Driver' can be `mysql', `psql' or `mnesia'. 'Options' is a list of
%% driver-specific options. For a list of available options, refer to
%% the driver's documentation.
%%
%% 'DriverOptions' is a property list that contains driver-specific options
%% (e.g. '{allow_unsafe_statements, Bool}').
%% For more information refer to the driver's documentation.
%%
%% 'Pools' is a list of available connection pools for the driver.
%% Note that the driver must be started and the pools must be connected
%% before code_gen/2 is called. Each item in 'Pools' is an atom indicating
%% the pool id, or a tuple of the form `{PoolId, default}', which indicates
%% that this pool will be used as the default pool for the driver.
%% If you don't provide a `{PoolId, default}' pool option, ErlyDB will use
%% the driver-defined default pool id if it exists (you can obtain it by
%% calling Mod:get_default_pool_id(), where 'Mod' is the driver's
%% module, e.g. 'erlydb_mysql').
%%
%% ==== Options ====
%%
%% 'Options' is a list of options that are used for all modules. This may
%% include global driver options as well as options that are passed to
%% compile:file/2. For more information, refer to this function's documentation
%% in the OTP documentation.
%%
%% ==== IncludePaths ====
%% 
%% Additional include paths that will be used to search for header files
%% when compiling the modules.
%%
%% === Examples ===
%%
%% Generate code for "musician.erl" using the MySQL driver. Only the default
%% pool is enabled.
%%
%% ```
%% code_gen(["musician.erl"], mysql).
%% '''
%%
%% Use the previous settings but allow unsafe SQL statements, and compile
%% with debug_info:
%%
%% ```
%% code_gen(["musician.erl"],
%%   {mysql, [{allow_unsafe_statements, true}]},
%%   [debug_info]).
%% '''
%%
%% Generate code for the modules using the MySQL driver with two additional
%% pools, 'pool1' and 'pool2'. The default pool is remains `erlydb_mysql':
%%
%% ```
%% code_gen(["musician.erl", "instrument.erl"],
%%   {mysql, [], [pool1, pool2]}).
%% '''
%%
%% Similar to the previous setting, but allow unsafe statement and use
%% `pool2' as the default pool name:
%%
%% ```
%% code_gen(["src/musician.erl", "src/instrument.erl"],
%%   {mysql, [{allow_unsafe_statements, true}],
%%     [{pool1, {pool2, default}}]})
%% '''
%%
%% Generate code for the modules using both the MySQL and Postgres driver.
%% The MySQL driver has 2 pools enabled: mysql_pool1 and mysql_pool2, which is
%% the default. The Postgres driver has a single default pool, pg_pool1.
%% The MySQL driver allows unsafe statements:
%%
%% ```
%% code_gen(["src/musician.erl", "src/instrument.erl", "src/song.erl"],
%%   [{mysql, [{allow_unsafe_statements, true}],
%%     [{mysql_pool1, {mysql_pool2, default}}]},
%%    {psql, [], [{pg_pool1, default}]}])
%% '''
%% 
%% === Module-Specific Settings ===
%%
%% To specify which connection pool ErlyDB should for a specific module, add
%% the following line to the module's source code:
%%
%% ```
%% -erlydb_options([{driver, Driver}, {pool_id, PoolId}]).
%% '''
%% 
%% The 'driver' option tells ErlyDB to use a non-default driver for the
%% module. The 'pool_id' option tells ErlyDB to use a non-default pool id
%% for the module. Neither option is required -- you can specify only
%% a 'driver' option or only a 'pool_id' option.
%%
%% @spec code_gen([Module::atom() | string()], [driver()] | driver(),
%%   Options::[term()], [IncludePath::string()]) -> 
%%    ok | {error, Err}
%% @type driver() = Driver::atom() |
%%    {Driver::atom(),  DriverOptions::proplist()} |
%%   {Driver::atom(), DriverOptions::proplist(), [pool()]}
%% @type pool() = PoolId::atom() | {default, PoolId::atom()}
code_gen(_Modules, [], _Options, _IncludePaths) ->
    exit(no_drivers_specified);
code_gen(Modules, Driver, Options, IncludePaths) when not is_list(Driver) ->
    code_gen(Modules, [Driver], Options, IncludePaths);
code_gen(Modules, Drivers, Options, IncludePaths) ->

    %% Normalize the driver tuples.
    DriverTuples =
	lists:map(
	  fun(Driver) when is_atom(Driver) ->
		  {Driver, [], []};
	     ({Driver, DriverOptions}) ->
		  {Driver, DriverOptions, []};
	     ({_Driver, _DriverOptions, _Pools} = Tpl) ->
		  Tpl;
	     (Other) ->
		  exit({invalid_driver_option, Other})
	  end, Drivers),

    DriversData =
	lists:foldl(
	  fun({Driver, DriverOptions, Pools}, Acc) ->
		  DriverMod = driver_mod(Driver),

		  %% Add the driver's default pool id to the list
		  %% if the default wasn't overriden.
		  Pools1 = case lists:any(
				  fun({_Id, default}) -> true;
				     (_) -> false
				  end, Pools) of
			       true ->
				   Pools;
			       _ ->
				   [{case catch DriverMod:get_default_pool_id() of
					 {'EXIT', _} ->
					     undefined;
					 DefaultPoolId ->
					     DefaultPoolId
				     end,
				     default} |
				    Pools]
			   end,

		  %% Get the metadata for all pools in a given driver,
		  %% and figure out which is the default pool.
		  {PoolsData, DefaultPool} =
		      lists:foldl(
			fun(PoolData, {Acc1, DefaultPool1}) ->
				{PoolId, NewDefaultPool} =
				    case PoolData of
					Id when is_atom(Id) ->
					    {Id, DefaultPool1};
					{Id, default} ->
					    {Id, Id}
				    end,
				Metadata = DriverMod:get_metadata(
					     [{pool_id, PoolId} | Options]),
				{gb_trees:enter(
				   PoolId, Metadata, Acc1), NewDefaultPool}
			end, {gb_trees:empty(), undefined}, Pools1),
		  gb_trees:enter(
		    Driver,
		    {PoolsData, DefaultPool, DriverOptions}, Acc)
	  end, gb_trees:empty(), DriverTuples),


    case proplists:get_value(skip_fk_checks, Options) of
	true ->
	    ok;
	_ ->
	    ?Debug("~n~n--- To skip foreign key checks, compile with the {skip_fk_checks, true} option~n~n", [])
    end,

    %% create the modules
    lists:foreach(
      fun(Module) ->
	      DefaultDriverMod = element(1, hd(DriverTuples)),
	      gen_module_code(Module, DefaultDriverMod, DriversData,
				  Options, IncludePaths)
      end, Modules).

gen_module_code(ModulePath, DefaultDriverMod,
	       DriversData, Options, IncludePaths) ->   
    case smerl:for_module(ModulePath, IncludePaths) of
	{ok, C1} ->
	    C2 = preprocess_and_compile(C1),
	    Module = smerl:get_module(C2),

	    %% get the ErlyDB settings for the driver, taking the defaults
	    %% into account
	    {Driver, PoolsData, PoolId, DriverOptions} =
		get_driver_settings(Module, DriversData, DefaultDriverMod),
	    DriverMod = driver_mod(Driver),

	    TablesData =
		case gb_trees:lookup(PoolId, PoolsData) of
		    {value, Val} ->
			Val;
		    _ ->
			exit({invalid_erlydb_pool_option,
			      {{module, Module},
			       {pool_id, PoolId}}})
		end,

    	    case gb_trees:lookup(get_table(Module), TablesData) of
		{value, Fields} ->
		    ?Debug("Generating code for ~w", [Module]),
		    Options2 = DriverOptions ++ Options,
		    MetaMod =
			make_module(DriverMod, C2, Fields,
				    [{pool_id, PoolId} | Options2],
				    TablesData),
		    smerl:compile(MetaMod, Options);
		none ->
		    exit(
		      {no_such_table, {{module, Module},
				       {table, get_table(Module)}}})
	    end;
	Err ->
	    Err
    end.

preprocess_and_compile(MetaMod) ->
    %% extend the base module, erlydb_base
    M10 = smerl:extend(erlydb_base, MetaMod),

    %% This is an optimization to avoid the remote function call
    %% to erlydb_base:set/3 in order to allow the compiler to decide to
    %% update the record destructively.
    M20 = smerl:remove_func(M10, set, 3),
    {ok, M30} = smerl:add_func(M20,
			 "set(Idx, Rec, Val) -> "
			 "setelement(Idx, Rec, Val)."),
    case smerl:compile(M30) of
	ok ->
	    M30;
	Err ->
	    exit(Err)
    end.


get_driver_settings(Module, DriversData, DefaultDriverMod) ->
    {DefaultPoolsData, DefaultDriverPoolId, DefaultOptions} =
	gb_trees:get(DefaultDriverMod, DriversData),
    case proplists:get_value(
	   erlydb_options,
	   Module:module_info(attributes)) of
	undefined ->
	    {DefaultDriverMod,
	     DefaultPoolsData,
	     DefaultDriverPoolId,
	     DefaultOptions};
	DriverOpts ->
	    {DriverMod, PoolsData, DefaultPoolId, Options} =
		case proplists:get_value(driver, DriverOpts) of
		    undefined ->
			{DefaultDriverMod,
			 DefaultPoolsData,
			 DefaultDriverPoolId,
			 DefaultOptions};
		    OtherDriver ->
			case gb_trees:lookup(OtherDriver, DriversData) of
			    {value, {PoolsData2, DefaultPoolId2, Options2}} ->
				{OtherDriver,
				 PoolsData2,
				 DefaultPoolId2,
				 Options2};
			    none ->
				exit({invalid_erlydb_driver_option,
				      {{module, Module},
				       {driver, OtherDriver}}})
			end
		end,
	    PoolId =
		case proplists:get_value(pool_id, DriverOpts) of
		    undefined ->
			DefaultPoolId;
		    OtherPoolId ->
			OtherPoolId
		end,
	    {DriverMod, PoolsData, PoolId, Options}
    end.

get_table(Module) ->
    case catch Module:table() of
	{'EXIT', _} -> Module;
	default -> Module;
	Res -> Res
    end.

%% Make the abstract forms for the module.
make_module(DriverMod, MetaMod, DbFields, Options, TablesData) ->
    Module = smerl:get_module(MetaMod),

    {Fields, FieldNames} = get_db_fields(Module, DbFields), 
    
    PkFields = filter_pk_fields(Fields),
    PkFieldNames = 
	[erlydb_field:name(Field) || Field <- PkFields],
    
    {ok, M24} = smerl:curry_replace(MetaMod, db_pk_fields, 1, [PkFields]),

    M26 = add_pk_fk_field_names(M24, PkFieldNames),
    
    %% inject the fields list into the db_fields/1 function
    {ok, M30} = smerl:curry_replace(M26, db_fields, 1, [Fields]),

    {ok, M32} = smerl:curry_replace(
		  M30, db_field_names, 1,
		  [FieldNames]),

    {ok, M34} = smerl:curry_replace(
		  M32, db_field_names_str, 1,
		  [[erlydb_field:name_str(Field) || Field <- Fields]]),

    {ok, M36} = smerl:curry_replace(
		  M34, db_field_names_bin, 1,
		  [[erlydb_field:name_bin(Field) || Field <- Fields]]),

    {ok, M42} = smerl:curry_replace(
		  M36, db_num_fields, 1, [length(Fields)]),

    {M60, _Count} = lists:foldl(
		     fun(Field, {M50, Count}) ->
			     Idx = Count,
			     {make_field_forms(M50, Field, Idx),
			      Count+1}
		     end, {M42, 3}, FieldNames),

    %% create the constructor
    M70 = case make_new_func(Module, Fields) of
	      undefined ->
		  M60;
	      NewFunc ->
		  {ok, Temp} = smerl:add_func(M60, NewFunc),
		  Temp
	  end,

    %% inject the driver configuration into the driver/1 function
    {ok, M80} = smerl:curry_replace(
		  M70, driver, 1,
		  [{DriverMod, Options}]),
    
    %% make the relations function forms
    M90 = make_rel_funcs(M80, TablesData, Options),
    
    %% make the aggregate function forms
    M100 = make_aggregate_forms(M90, aggregate, 5, [Module],
			     undefined),

    %% add extra configurations to the different find functions
    M120 = lists:foldl(
	   fun({FindFunc, Arity}, M110) ->
		   add_find_configs(M110, FindFunc, Arity)
	   end, M100, [{find, 3}, {find_first, 3}, {find_max, 4},
		     {find_range,5}]),

    %% embed the generated module's name in
    %% place of all corresponding parameters in the base forms
    M130 = smerl:embed_all(M120, [{'Module', smerl:get_module(MetaMod)}]),

    M130.

%% Return a list of database fields that belong to the module based on the
%% fields/0 and type_field/0 functions as (potentially)
%% implemented by the user as well as the database metadata for the table.
%%
%% Throw an error if any user-defined non-transient fields aren't in the 
%% database.
get_db_fields(Module, DbFields) ->
    DbFieldNames = [erlydb_field:name(Field) || Field <- DbFields],
    DbFields1 =
	case Module:fields() of
	    '*' -> [set_attributes(Field, []) || Field <- DbFields];
	    DefinedFields ->
		DefinedFields1 =
		    lists:map(fun({_Name, _Atts} = F) -> F;
				 (Name) -> {Name, []}
			      end, lists:usort(DefinedFields)),
		
		PkFields = [{erlydb_field:name(Field), []} ||
			       Field <- DbFields,
			       erlydb_field:key(Field) == primary,
			       not lists:keymember(
				     erlydb_field:name(Field),
				     1, DefinedFields1)],

		DefinedFields2 = PkFields ++ DefinedFields1,

		InvalidFieldNames =
		    [Name || {Name, Atts} <- DefinedFields2,
				  not lists:member(Name, DbFieldNames) 
                                 and not lists:member(transient, Atts)],

		case InvalidFieldNames of
		    [] ->
                        DbFields2 = [add_transient_field(Field, DbFields) || 
                                        Field <- DefinedFields2],
			lists:foldr(
			  fun(Field, Acc) ->
				  FieldName = erlydb_field:name(Field),
				  case lists:keysearch(
					 FieldName, 1, DefinedFields2) of
				      {value, {_Name, Atts}} ->
					  Field1 = 
					      set_attributes(Field, Atts),
					  [Field1 | Acc];
				      false ->
					  Acc
				  end
			  end, [], DbFields2);
		    _ -> exit({no_such_fields, {Module, InvalidFieldNames}})
		end
	end,

    DbFieldNames1 = [erlydb_field:name(Field) || Field <- DbFields1],

    Res =
	case Module:type_field() of
	    undefined -> {DbFields1, DbFieldNames1};
	    Name ->
		case lists:member(Name, DbFieldNames) of
		    true ->
			{[Field || Field <- DbFields1,
				   erlydb_field:name(Field) =/= Name],
			 DbFieldNames1 -- [Name]};
		    false -> exit({no_such_type_field, {Module, Name}})
		end
	end,
    Res.

add_transient_field({Name, Atts}, DbFields) ->
    case lists:member(transient, Atts) of
        true ->
            erlydb_field:new(Name, {varchar, undefined}, true, 
                             undefined, undefined, undefined);
        _ ->
            {value, Val} = lists:keysearch(Name, 2, DbFields),
            Val
    end.     
                     
set_attributes(Field, Atts) ->
    Atts1 = case erlydb_field:extra(Field) == identity orelse 
		erlydb_field:type(Field) == timestamp of
		true ->
		    [read_only |
		     Atts --
		     [read_only]];
		_ ->
		    Atts
	    end,
    erlydb_field:attributes(Field, Atts1).

add_pk_fk_field_names(MetaMod, PkFieldNames) ->
    Module = smerl:get_module(MetaMod),
    PkFkFieldNames = pk_fk_fields(get_table(Module), PkFieldNames),
    {ok, M2} = smerl:curry_replace(
		  MetaMod, get_pk_fk_fields, 1, [PkFkFieldNames]),

    PkFkFieldNames2 =
	[{PkField, append([FkField, '1']), append([FkField, '2'])} ||
	    {PkField, FkField} <- PkFkFieldNames],

    {ok, M5} = smerl:curry_replace(
		  M2, get_pk_fk_fields2, 1, [PkFkFieldNames2]),
    M5.

%% Create the abstract form for the given Module's 'new' function,
%% accepting all field values as parameters, except for fields that
%% are designated as 'identity' primary key fields.
%%
%% For related records with an 'id' primary key, the 'new'
%% function accepts as a parameter
%% either a tuple representing the related record, or the record's
%% id directly.
%%
%% Example:
%% {ok, Erlang} = language:find_id(1),
%% project:new("Yaws", Lang) == project:new("Yaws", language:id(Lang))
make_new_func(Module, Fields) ->
    L = 1,
    {Params2, Vals2} =
	lists:foldl(
	  fun(Field, {Params, Vals}) ->
		  case erlydb_field:extra(Field) == identity of
		      true -> {Params, [{atom, L, undefined} | Vals]};
		      false ->
			  Name = erlydb_field:name(Field),
			  {Stripped, Name1} = strip_id_chars(Name),
			  Params1 = [{var,L,Name1} | Params],
			  Vals1 =
			      case Stripped of
				  true ->
				      [make_new_func_if_expr(Name1) | Vals];
				  false ->
				      [{var,L,Name1} | Vals]
			      end,
			  {Params1, Vals1}
		  end
	  end, {[], []}, Fields),
    NumParams = length(Params2),
    if NumParams > 0 ->
	    {function,L,new,length(Params2),
	     [{clause,L,lists:reverse(Params2),[],
	       [{tuple,L,
		 [{atom,L,Module},{atom,L,true} | lists:reverse(Vals2)]}
	       ]}
	     ]};
       true ->
	    undefined
    end.

%% Return the following expression:
%%
%% if is_tuple(Param) -> 'Param':id(Param); true -> Param end
%%
%% This allows you to pass into the constructor either a related record
%% or the related record's id directly. If you pass in a related,
%% record, its id is automatically substituted as the parameter's
%% value.
make_new_func_if_expr(Param) ->
    L = 1,
    {'if',L,
     [{clause,L,
       [],
       [[{call,L,{atom,L,is_tuple},[{var,L,Param}]}]],
       [{call,L,
	 {remote,L,{atom,L,Param},{atom,L,id}},
	 [{var,L,Param}]}]},
      {clause,L,[],[[{atom,L,true}]],[{var,L,Param}]}]}.

%% If Field is an atom such as 'person_id', return the atom 'person'.
%% Otherwise, return the original atom.
strip_id_chars(Field) ->
    FieldName = atom_to_list(Field),
    FieldLen = length(FieldName),
    if
	FieldLen < 4 ->
	    {false, Field};
	true ->
	    case string:substr(FieldName,
			       FieldLen - 2, 3) of
		"_id" ->
		    {true,
		     list_to_atom(string:substr(FieldName, 1, FieldLen - 3))};
		_ ->
		    {false, Field}
	    end
    end.
	    
%% Add getters and setters
make_field_forms(MetaMod, Field, Idx) ->
    {ok, C1} = smerl:curry_add(MetaMod, get, 2, Idx, Field),
    {ok, C2} = smerl:curry_add(C1, set, 3, Idx, Field),
    C2.

make_aggregate_forms(MetaMod, BaseFuncName, Arity, CurryParams, PostFix) ->
    lists:foldl(
      fun(Func, M1) ->
	      NewName = append([Func, PostFix]),
	      NewCurryParams = CurryParams ++ [Func],
	      {ok, M2} = smerl:curry_add(M1, BaseFuncName, Arity,
					 NewCurryParams,
					 NewName),
	      add_find_configs(M2, NewName, Arity - length(NewCurryParams))
      end, MetaMod, aggregate_functions()).

%% Generate the forms for functions that enable working with related
%% records.
make_rel_funcs(MetaMod, TablesData, Opts) ->
    Module = smerl:get_module(MetaMod),
    lists:foldl(
      fun({RelType, Modules}, MetaMod1) ->
	      make_rel_forms(RelType,
			     Modules, MetaMod1, TablesData, Opts)
      end, MetaMod, Module:relations()).

make_rel_forms(RelType, Relations, MetaMod, TablesData, Opts) ->
    Fun =
	case RelType of
	    many_to_one ->
		fun make_many_to_one_forms/4;
	    one_to_many ->
		fun make_one_to_many_forms/4;
	    many_to_many ->
		fun make_many_to_many_forms/4
	end,

    %% currying would be nice :)
    Fun1 = fun(Relation, MetaMod1) ->
		   Fun(Relation, MetaMod1, TablesData, Opts)
	   end,

    lists:foldl(Fun1, MetaMod, Relations).

make_many_to_one_forms(Relation, MetaMod, TablesData, Opts) ->
    {OtherModule, Alias, PkFks} =
	get_rel_options(smerl:get_module(MetaMod),
			Relation, TablesData, true, Opts),

    {ok, M1} = smerl:curry_add(
		 MetaMod, find_related_one_to_many, 3,
		 [OtherModule, PkFks], Alias),

    {ok, M2} = smerl:curry_add(M1, set_related_one_to_many, 3,
		   [PkFks], Alias),
    M2.



make_one_to_many_forms(Relation, MetaMod, TablesData, Opts) ->
    {OtherModule, Alias, PkFks} =
	get_rel_options(smerl:get_module(MetaMod),
			Relation, TablesData, false, Opts),
    make_some_to_many_forms(
      MetaMod, OtherModule, Alias, [PkFks],
      find_related_many_to_one, 5,
      aggregate_related_many_to_one, 7).

make_many_to_many_forms(Relation, MetaMod, TablesData, _Opts) ->
    Module = smerl:get_module(MetaMod),
    {OtherModule, RelationTable, Alias} =
	case Relation of
	    Mod when is_atom(Mod) ->
		{Mod, undefined, Mod};
	    {Mod, Opts} ->
		RelationTable1 = proplists:get_value(relation_table, Opts),
		if RelationTable1 =/= undefined ->
			case gb_trees:lookup(RelationTable1, TablesData) of
			    none ->
				exit({relation_table_not_found,
				      {{module, Module},
				       {relatedModule, Mod},
				       {relation_table, RelationTable1}}});
			    _ ->
				ok
			end;
		   true ->
			ok
		end,
		Alias1 = case proplists:get_value(alias, Opts) of
			    undefined ->
				Mod;
			    Alias2 ->
				Alias2
			end,
		{Mod, RelationTable1, Alias1}
	end,
    
    %% The name of the join table is by default assumed
    %% to be the alphabetical ordering of the two
    %% tables,
    %% separated by an underscore.
    %% Good example: person_project
    %% Bad example: project_person
    RelationTable2 = if RelationTable == undefined ->
			    [Module1, Module2] = 
				lists:sort(
				  fun(Mod1, Mod2) ->
					  get_table(Mod1) < get_table(Mod2)
				  end,
				  [Module, OtherModule]),
			    append([get_table(Module1), "_",
				    get_table(Module2)]);
		       true ->
			    RelationTable
		    end,
    RemoveAllFuncName = append(["remove_all_", pluralize(Alias)]),
    IsRelatedFuncName = append(["is_", Alias, "_related"]),

    CurryFuncs =
	[{add_related_many_to_many, 3, [],
	  append(["add_", Alias])},
	 {remove_related_many_to_many, 3, [],
	  append(["remove_", Alias])},
	 {remove_related_many_to_many_all, 5, [get_table(OtherModule)],
	  RemoveAllFuncName},
	 {is_related, 3, [], IsRelatedFuncName}],
    
    M3 = lists:foldl(
	   fun({FuncName, Arity, ExtraParams, NewName}, M1) ->
		   {ok, M2} = smerl:curry_add(
				M1, FuncName, Arity,
				[RelationTable2 | ExtraParams],
				NewName),
		   M2
	   end, MetaMod, CurryFuncs),

    M4 = add_find_configs(M3, RemoveAllFuncName, 3),

    M6 = case get_table(Module) == get_table(OtherModule) of
	     true ->
		 M5 = smerl:remove_func(M4, RemoveAllFuncName, 2),
		 smerl:remove_func(M5, RemoveAllFuncName, 3);
	     _ ->
		 M4
	 end,

    M7 = make_some_to_many_forms(
	   M6, OtherModule, Alias, [RelationTable2],
	   find_related_many_to_many, 5,
	   aggregate_related_many_to_many, 7),
				 
    M7.
    
make_some_to_many_forms(MetaMod, OtherModule, Alias, ExtraCurryParams,
			BaseFindFuncName, BaseFindFuncArity,
			AggregateFuncName, AggregateFuncArity) ->

    FindFuncName = pluralize(Alias),
    {ok, M1} = smerl:curry_add(MetaMod, BaseFindFuncName, BaseFindFuncArity,
		     [OtherModule | ExtraCurryParams], FindFuncName),

    M2 = add_find_configs(M1, FindFuncName, BaseFindFuncArity -
			  (1 + length(ExtraCurryParams))),

    AggPostFix = "_of_" ++ atom_to_list(pluralize(Alias)),
    M3 = make_aggregate_forms(M2, AggregateFuncName, AggregateFuncArity,
			      [OtherModule | ExtraCurryParams], AggPostFix),
    
    FindFuncs = [
 		 {find_related_many_first,4},
 		 {find_related_many_max,5},
		 {find_related_many_range,6}
		],

    M6 = lists:foldl(
	   fun({FuncName, Arity}, M4) ->
		   PostFix = lists:nthtail(length("find_related_many"),
					   atom_to_list(FuncName)),
		   NewName = append([FindFuncName, PostFix]),
		   {ok, M5} = smerl:curry_add(M4, FuncName, Arity,
					      [FindFuncName], NewName),
		   add_find_configs(M5, NewName, Arity-1)
	   end, M3, FindFuncs),

    CountFuncName = append(["count", AggPostFix]),
    {ok, M7} =
	smerl:embed_params(M6, CountFuncName, 2, [{'Field', '*'}]),
    M7.


%% Get the relation name and primary/foreign key field mappings for
%% a give one-to-many or many-to-one relation.
get_rel_options(Module, OtherModule, TablesData, ReverseFieldOrder, Opts) ->
    Res = {OtherMod, _Alias, PkFks} =
	case OtherModule of
	    OtherMod1 when is_atom(OtherMod1) ->
		{OtherMod1, OtherMod1,
		 if ReverseFieldOrder ->
			 pk_fk_fields2(OtherModule,
				       get_table(OtherModule), TablesData);
		    true ->
			 pk_fk_fields2(Module,
				       get_table(Module), TablesData)
		 end};
	    {OtherMod2, Opts2} ->
		Alias1 =
		    case proplists:get_value(alias, Opts2) of
			undefined ->
			    OtherMod2;
			Other ->
			    Other
		    end,
		FkBase =
		    case proplists:get_value(fk_base, Opts2) of
			undefined ->
			    OtherMod2;
			RevAlias ->
			    RevAlias
		    end,
		PkFks1 = case proplists:get_value(foreign_keys, Opts2) of
			    undefined ->
				pk_fk_fields2(OtherMod2,
					      FkBase, TablesData);
			    Other1 ->
				Other1
			end,
		{OtherMod2, Alias1, PkFks1}
	end,
    case proplists:get_value(skip_fk_checks, Opts) of
	true ->
	    Res;
	_ ->
	    ?Debug("Checking foreign keys for ~w\t->\t~w", [Module, OtherMod]),
	    verify_field_mappings(Module, OtherMod,
				  TablesData, PkFks, ReverseFieldOrder)
    end,
    Res.

%% Verify all mapped fields are present.
%%
%% TODO We can add additional validations, e.g. test data types compatibility
%% and ensure no entries have duplicates.
verify_field_mappings(Module, OtherModule, TablesData, PkFks,
		      ReverseFieldOrder) ->
    Fields1 = get_fields(Module, TablesData),
    FieldNames1 = [erlydb_field:name(F) || F <- Fields1],
    Fields2 = get_fields(OtherModule, TablesData),
    FieldNames2 = [erlydb_field:name(F) || F <- Fields2],
    Errs = lists:foldr(
	     fun(Pair = {F1, F2}, Acc) ->
		     {Field1, Field2} =
			 if ReverseFieldOrder ->
				 {F2, F1};
			    true ->
				 Pair
			 end,
		     lists:foldr(
		       fun({Field, FieldNames, Module1}, Acc1) ->
			       case lists:member(Field, FieldNames) of
				   true ->
				       Acc1;
				   false ->
				       [{missing_field,
					 {{module, Module1},
					  {table, get_table(Module1)},
					  {field, Field}}} | Acc1]
			       end
		       end, Acc, [{Field1, FieldNames1, Module},
				  {Field2, FieldNames2, OtherModule}])
	     end, [], PkFks),
    if Errs == [] ->
	    ok;
       true ->
	    exit({bad_relation_definition,
		  {{module, Module},
		   {table, get_table(Module)},
		   {related_module, OtherModule},
		   {related_table, get_table(OtherModule)},
		   {errors, Errs}}})
    end.


get_fields(Module, TablesData) ->
    case gb_trees:lookup(get_table(Module), TablesData) of
	none ->
	    exit({missing_table_data,
		  {{module, Module},
		   {table, get_table(Module)}}});
	{value, Fields} ->
	    Fields
    end.

filter_pk_fields(Fields) ->
    [Field || Field <- Fields, erlydb_field:key(Field) == primary].

pk_fk_fields(Module, PkFieldNames) ->
    [{FieldName, append([Module, '_', FieldName])} ||
	FieldName <- PkFieldNames].

pk_fk_fields2(Module, Alias, TablesData) ->
    pk_fk_fields(
      Alias,
      [erlydb_field:name(F) ||
	  F <- filter_pk_fields(get_fields(get_table(Module), TablesData))]).

add_find_configs(MetaMod, BaseFuncName, Arity) ->
    NoWhere = {'Where', undefined},
    NoExtras = {'Extras', undefined},

    Configs = 
	[{BaseFuncName, [NoWhere, NoExtras]},
	 {BaseFuncName, [NoExtras]},
	 {append([BaseFuncName, "_with"]), [NoWhere]}],

    M4 = lists:foldl(
	   fun({NewName, Replacements}, M2) ->
		   {ok, M3} =
		       smerl:embed_params(M2, BaseFuncName, Arity,
					  Replacements, NewName),
		   M3
	   end, MetaMod, Configs),
    M4.


%% TODO There are probably a bunch of additional cases, but this is good
%% enough for now :)
pluralize(Module) ->
    pluralize(Module, undefined).

pluralize(Module, Postfix) ->
    Str = atom_to_list(Module),
    [LastChar, CharBeforeLast|Rest] = Rev = lists:reverse(Str),
    Suffix = [CharBeforeLast,LastChar],
    Irregulars =
	[{man, men},{foot,feet},{child,children},{person,people},
	 {tooth,teeth},{mouse,mice},{sheep,sheep},{deer,deer},{fish,fish}],
    PluralForm =
	case lists:keysearch(Module,1,Irregulars) of
	    {value, {_, Plural}} ->
		atom_to_list(Plural);
	    _ ->
		if Suffix == "fe" ->
			lists:reverse([$s,$e,$v | Rest]);
		   LastChar == $f ->
			lists:reverse([$s,$e,$v,CharBeforeLast | Rest]);
		   
		   %% These rules only work in some special cases, so we'll
		   %% comment them out for now and possibly deal with them
		   %% later
		   %% Suffix == "is" ->
		   %%	lists:reverse([$s,$e|Rest]);
		   %% Suffix == "on" ->
		   %%   lists:reverse([$a | Rest]);
		   %% Suffix == "us" ->
		   %%   lists:reverse([$i | Rest]);

		   true ->
			Cond1 = LastChar == $y andalso
			    lists:member(CharBeforeLast,
					 "bcdfghjklmnpqrtvwxyz"),
			if
			    Cond1 ->
				lists:reverse([$s,$e,$i,CharBeforeLast|Rest]);
			    true ->
				Cond2 = case Rev of
					    [$s|_] -> true;
					    [$h,$c|_] -> true;
					    [$h,$s|_] -> true;
					    [$x|_] -> true;
					    [$o|_] -> true;
					    _ -> false
					end,
				if Cond2 ->
					Str ++ "es";
				   true ->
					Str ++ "s"
				end
			end
		end
	end,
    Result =
	case Postfix of
	    undefined ->
		PluralForm;
	    _ ->
		PluralForm ++ Postfix
	end,
    list_to_atom(Result).

%% append a list of strings and atoms and return the result as an atom
append(Terms) ->
    list_to_atom(
      lists:flatten(
	lists:map(
	  fun(undefined) -> [];
	     (Atom) when is_atom(Atom) ->
		  atom_to_list(Atom);
	     (List) -> List
	  end, Terms))).
