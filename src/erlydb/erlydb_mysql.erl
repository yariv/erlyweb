%% @title The ErlyDB MySQL driver.
%%
%% @doc This module implements the MySQL driver for ErlyDB.
%%
%% Some of the functions accept a SQL statement as a parameter.
%% This statement can be expressed as a string, an iolist, a binary or
%% a ErlSQL expression. The preferred type is ErlSQL because it can
%% be easily converted to a binary representation and because it protects
%% against SQL injection attacks.
%% For more information on ErlSQL, visit http://code.google.com/p/erlsql.
%%
%% Many of the functions in the module accept an Options parameter.
%% The value for this parameter can be defined by the user when calling
%% erlydb:code_gen/3. The available options are:
%%
%% pool_id::atom() - the connection pool id used for the module
%% allow_unsafe_sql::bool() - whether the driver should accept string
%%   and/or binary SQL queries.
%% 
%%
%% @author Yariv Sadan (yarivvv@gmail.com) (http://yarivsblog.com)

-module(erlydb_mysql).

-author("Yariv Sadan (yarivvv@gmail.com) (http://yarivsblog.com)").

-export([start/1,
	 add_connections/7,
	 get_metadata/1,
	 q/1,
	 q/2,
	 q2/2,
	 transaction/2,
	 select/2,
	 select_as/3,
	 update/2,
	 get_last_insert_id/1,
	 prepare/2,
	 execute/2,
	 execute/3,
	 execute_select/2,
	 execute_select/3,
	 execute_update/2,
	 execute_update/3]).


%% Useful for debugging
-define(L(Obj), io:format("LOG ~w ~p\n", [?LINE, Obj])).
-define(S(Obj), io:format("LOG ~w ~s\n", [?LINE, Obj])).

-define(Epid, erlydb_mysql).

%% @type esql() = {esql, term()}. An ErlSQL expression.
%% @type statement() = esql() | binary() | string()

%% @doc Start the MySQL driver using the options property list.
%%  The available options are
%%  - pool_id (optional): an atom identifying the connection pool id.
%%        An 'undefined' value indicates that the default connection
%%        pool should be used. 
%%  - hostname: a string indicating the database host name.
%%  - port (optional): an integer indicating the database port
%%      ('undefined' indicates the default MySQL port, 3306).
%%  - username: a string indicating the username.
%%  - password: a string indicating the password.
%%  - database: a string indicating the database name.
%%  - num_conns (optional): an integer indicating the number of connections
%%      to start in the connection pool.
%%
%% @spec start(StartOptions::proplist()) -> ok | {error, Error}
start(Options) ->
    [PoolId, Hostname, Port, Username, Password, Database, NumConns] =
	lists:foldl(
	  fun(Key, Acc) ->
		  [proplists:get_value(Key, Options) | Acc]
	  end, [],
	  lists:reverse([pool_id, hostname, port, username,
			 password, database, num_conns])),

    PoolId1 = if PoolId == undefined -> ?Epid; true -> PoolId end,
    NumConns1 = if NumConns == undefined -> 1; true -> NumConns end,
    mysql:start_link(PoolId1, Hostname, Port, Username, Password, Database),
    add_connections(PoolId1, Hostname, Port, Username, Password,
		    Database, NumConns1-1).
    
%% @doc Add more connections to the given connection pool. If PoolId is
%%   'undefined', the default pool is used.
%%
%% @spec add_connections(PoolId::atom(), Hostname::string, Port::integer(),
%%    Username::string(), Password::string(), Database::string(),
%%    NumConns::integer())
add_connections(_PoolId, _Hostname, _Port, _Username, _Password, _Database,
		0) ->
    ok;

add_connections(PoolId, Hostname, Port, Username, Password, Database,
		NumConns) ->
    mysql:connect(PoolId, Hostname, Port, Username, Password, Database, true),
    add_connections(PoolId, Hostname, Port, Username, Password, Database,
		    NumConns-1).

%% @doc Get the table names and fields for the database.
%%
%% @spec get_metadata(PoolId::atom()) -> gb_trees()
get_metadata(Options) ->
    {data, Res} = q2(<<"show tables">>, Options),
    Tables = mysql:get_result_rows(Res),
    case catch lists:foldl(
		 fun([Table | _], TablesTree) ->
			 case q2(<<"describe ", Table/binary>>, Options) of
			     {data, FieldRes} ->
				 Rows = mysql:get_result_rows(FieldRes),
				 Fields =
				     [new_field(FieldData) ||
					 FieldData <- Rows],
				 gb_trees:enter(binary_to_atom(Table), Fields,
						TablesTree);
			     {error, _Err} = Res ->
				 throw(Res)
			 end
		 end, gb_trees:empty(), Tables) of
	{error, _} = Err ->
	    Err;
	Tree ->
	    {ok, Tree}
    end.

new_field([Name, Type, Null, Key, Default, Extra]) ->
    Type1 = parse_type(binary_to_list(Type)),
    Null1 = case Null of
		<<"YES">> -> true;
		_ -> false
	    end,
    Key1 = case Key of
	       <<"PRI">> -> primary;
	       <<"UNI">> -> unique;
	       <<"MUL">> -> multiple;
	       _Other -> none
	   end,
    Extra1 = case Extra of
		 <<"auto_increment">> -> identity;
		 _ -> none
	     end,

    erlydb_field:new(
      binary_to_atom(Name), Type1, Null1, Key1, Default, Extra1).

parse_type(TypeStr) ->
    case string:chr(TypeStr, 40) of  %% 40 == '('
	0 ->
	    {list_to_atom(TypeStr), undefined};
	Idx ->
	    {TypeStr1, [_| Vals]} = lists:split(Idx - 1, TypeStr),
	    Extras = 
		case TypeStr1 of
		    "set" -> parse_list(Vals);
		    "enum" -> parse_list(Vals);
		    _Other -> 
			{ok, [Len], _} = io_lib:fread("~d", Vals),
			Len
		end,
	    {list_to_atom(TypeStr1), Extras}
    end.

parse_list(Str) ->
    [_Last | Body] = lists:reverse(Str),
    Body1 = lists:reverse(Body),
    Toks = string:tokens(Body1, ","),
    [list_to_binary(string:strip(Tok, both, 39)) || Tok <- Toks].
    

%% @doc Execute a statement directly against the MySQL driver. If 
%%   Options contains the value {allow_unsafe_sql, true}, binary
%%   and string queries as well as ErlSQL queries with binary and/or
%%   string expressions are accepted. Otherwise, this function crashes.
%%
%% @spec q(Statement::statement(), Options::options()) ->
%%   mysql_result() | exit({unsafe_statement, Statement})
q(Statement) ->
    q(Statement, undefined).

q({esql, Statement}, Options) ->
    case allow_unsafe_statements(Options) of
	true -> q2(erlsql:unsafe_sql(Statement), Options);
	_ ->
	    case catch erlsql:sql(Statement) of
		{error, _} = Err -> exit(Err);
		Res -> q2(Res, Options)
	    end
    end;
q(Statement, Options) when is_binary(Statement); is_list(Statement) ->
    case allow_unsafe_statements(Options) of
	true -> q2(Statement, Options);
	_ -> exit({unsafe_statement, Statement})
    end.

q2(Statement, Options) ->
    mysql:fetch(get_pool_id(Options), Statement).

%% @doc Execute a group of statements in a transaction.
%%   Fun is the function that implements the transaction.
%%   It can contain an arbitrary sequence of calls to
%%   the erlydb_mysql's query functions. If Fun crashes or returns
%%   or throws 'error' or {error, Err}, the transaction is automatically
%%   rolled back. 
%%
%% @spec transaction(Fun::function(), Options::options()) ->
%%   {atomic, Result} | {aborted, Reason}
transaction(Fun, Options) ->
    mysql:transaction(get_pool_id(Options), Fun).

    
%% @doc Execute a SELECT statment.
%%
%% @spec select(PoolId::atom(), Statement::statement()) ->
%%   {ok, Rows::list()} | {error, Error}
select(Statement, Options) ->
    select2(Statement, Options, []).

%% @doc Perform a SELECT query for records belonging to the given module,
%%   returning all rows with additional data to support
%%   higher-level ErlyDB features.
%%
%% @spec select_as(Statement::statement(), FixedCols::tuple()) ->
%%   {ok, Rows} | {error, Error}
select_as(Module, Statement, Options) ->
    select2(Statement, Options, [Module, false]).

select2(Statement, Options, FixedVals) ->
    get_select_result(q(Statement, Options), FixedVals).

get_select_result(MySQLRes) ->
    get_select_result(MySQLRes, undefined).

get_select_result({data, Data}, undefined) ->
    {ok, mysql:get_result_rows(Data)};
get_select_result({data, Data}, FixedVals)->
    Rows = mysql:get_result_rows(Data),
    Result =
	lists:foldl(
	  fun(Fields, Acc) ->
		  Row = FixedVals ++ Fields,
		  [list_to_tuple(Row) | Acc]
	  end, [], Rows),
    {ok, lists:reverse(Result)};

get_select_result(Other, _) -> Other.

%% @doc Execute a DELETE or UPDATE statement.
%%
%% @spec update(Statement::statement()) -> {ok, NumAffected} | {error, Err}
update(Statement, Options) ->
    get_update_result(q(Statement, Options)).

get_update_result({updated, MySQLRes}) ->
    {ok, mysql:get_result_affected_rows(MySQLRes)};
get_update_result(Other) -> Other.


%% @doc Get the id of the last inserted record.
%%
%% @spec get_last_insert_id(PoolId::atom()) -> term()
get_last_insert_id(Options) ->
    case q2(<<"SELECT last_insert_id()">>, Options) of
	{data, Result} ->
	    [[Val]] = mysql:get_result_rows(Result),
	    {ok, Val};
	Err ->
	    Err
    end.
	    

%% @doc Register a prepared statement with the MySQL dispatcher.
%%   If the dispatcher has a prepared statement with the same name,
%%   the old statement is overwritten and the statement's version
%%   is incremented.
%%
%% @spec prepare(Name::atom(), Stmt::iolist()) -> ok | {error, Err}
prepare(Name, Stmt) ->
    mysql:prepare(Name, Stmt).

%% @doc Execute a statement that was previously prepared with
%%  prepare/2.
%%
%% @spec execute(Name::atom(), Options::proplist()) -> mysql_result()
execute(Name, Options) ->
    mysql:execute(get_pool_id(Options), Name).

%% @doc Execute a prepared statement with the list of parameters.
%%
%% @spec execute(Name::atom(), Params::[term()], Options::options()) ->
%%   mysql_result().
execute(Name, Params, Options) ->
    mysql:execute(get_pool_id(Options), Name, Params).

%% @doc Execute a prepared statement and return the result as the select()
%%   function.
%%
%% @spec execute_select(Name::atom(), Options::options()) ->
%%   {ok, [row]} | {error, Err}
execute_select(Name, Options) ->
    get_select_result(execute(Name, Options)).

%% @doc Execute a prepared statement with the list of parameters
%%   and return the result as the select() function.
%% 
%% @spec execute_select(Name::atom(), Params::[term()], Options::options()) -> 
%%   {ok, [Row::tuple()]} | {error, Err}
execute_select(Name, Params, Options) ->
    get_select_result(execute(Name, Params, Options)).

%% @doc Execute a prepared statement and return the result as the the
%%   update() function.
%%
%% @spec execute_upate(Name::atom(), Options::options) ->
%%   {ok, NumUpdated::integer()} | {error, Err}
execute_update(Name, Options) ->
    get_update_result(execute(Name, Options)).

%% @doc Execute a prepared statement with the list of parameters and
%%   and return the result as the the update() function.
%%
%% @spec execute_update(Name::atom(), Params::[term()], Options::options) ->
%%   {ok, NumUpdated::integer()} | {error, Err}
execute_update(Name, Params, Options) ->
    get_update_result(execute(Name, Params, Options)).



binary_to_atom(Bin) ->
    list_to_atom(binary_to_list(Bin)).


allow_unsafe_statements(undefined) -> false;
allow_unsafe_statements(Options) -> 
    proplists:get_value(allow_unsafe_statements, Options).

get_pool_id(undefined) -> erlydb_mysql;
get_pool_id(Options) ->
    case proplists:get_value(pool_id, Options) of
	undefined ->
	    erlydb_mysql;
	Other ->
	    Other
    end.
