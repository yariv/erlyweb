%% @author Yariv Sadan (yarivvv@gmail.com) (http://yarivsblog.com)
%% @copyright Yariv Sadan 2006-2007
%% 
%% @doc This module implements the MySQL driver for ErlyDB.
%%
%% This is an internal ErlyDB module that you normally shouldn't have to
%% use directly. For most situations, all you have to know
%% about this module is the options you can pass to {@link start/1}, which
%% is called by {@link erlydb:start/2}.
%%

%% For license information see LICENSE.txt

-module(erlydb_mysql).

-author("Yariv Sadan (yarivsblog@gmail.com) (http://yarivsblog.com)").

-export([start/1,
	 start_link/1,
	 connect/5,
	 connect/7,
	 connect/8,
	 connect/9,
	 get_metadata/1,
	 get_default_pool_id/0,
	 q/1,
	 q/2,
	 q2/1,
	 q2/2,
	 transaction/2,
	 select/2,
	 select_as/3,
	 update/2,
	 get_last_insert_id/2,
	 prepare/2,
	 execute/2,
	 execute/3,
	 execute_select/2,
	 execute_select/3,
	 execute_update/2,
	 execute_update/3]).


%% Useful for debugging

-define(L(Msg), io:format("~p:~b ~p ~n", [?MODULE, ?LINE, Msg])).
-define(S(Obj), io:format("LOG ~w ~s\n", [?LINE, Obj])).

-define(Epid, erlydb_mysql).

%% @type esql() = {esql, term()}. An ErlSQL expression.
%% @type statement() = esql() | binary() | string()
%% @type options() = [option()]
%% @type option() = {pool_id, atom()} | {allow_unsafe_statements, boolean()}

%% @doc Start the MySQL dispatcher using the options property list.
%%  The available options are:
%%
%%  `pool_id' (optional): an atom identifying the connection pool id.
%%     An 'undefined' value indicates that the default connection
%%     pool, i.e. 'erlydb_mysql', should be used.
%%
%%  `hostname': a string indicating the database host name.
%%
%%  `port' (optional): an integer indicating the database port
%%      ('undefined' indicates the default MySQL port, 3306).
%%
%%  `username': a string indicating the username.
%%
%%  `password': a string indicating the password.
%%
%%  `database': a string indicating the database name.
%%
%%  `allow_unsafe_statements': a boolean value indicating whether the driver
%%  should
%%  accept string and/or binary SQL queries and query fragments. If you
%%  set this value to
%%  'true', ErlyDB lets you use string or binary Where and Extras expressions
%%  in generated functions. For more information, see {@link erlydb}.
%%
%% `encoding': the character encoding MySQL will use.
%%
%% `poolsize': the number of connections to start.
%%
%% This function calls mysql:start(), not mysql:start_link(). To
%% link the MySQL dispatcher to the calling process, use {@link start_link/1}.
%%
%% @spec start(StartOptions::proplist()) -> ok | {error, Error}
start(Options) ->
    start(Options, fun mysql:start/8, false).

%% @doc This function is similar to {@link start/1}, only it calls
%% mysql:start_link() instead of mysql:start().
%%
%% @spec start_link(StartOptions::proplist()) -> ok | {error, Error}
start_link(Options) ->
    start(Options, fun mysql:start_link/8, true).

start(Options, Fun, LinkConnections) ->
    [PoolId, Hostname, Port, Username, Password, Database, LogFun,
     Encoding, PoolSize] =
	lists:foldl(
	  fun(Key, Acc) ->
		  [proplists:get_value(Key, Options) | Acc]
	  end, [],
	  lists:reverse([pool_id, hostname, port, username,
			 password, database, logfun, encoding, poolsize])),

    PoolId1 = if PoolId == undefined -> ?Epid; true -> PoolId end,
    PoolSize1 = if PoolSize == undefined ->
			1;
		   true ->
			PoolSize
		end,
    Fun(PoolId1, Hostname, Port, Username, Password, Database, LogFun,
	Encoding),
    make_connection(PoolSize1-1, PoolId, Database, Hostname, Port,
		    Username, Password, Encoding, LinkConnections).

%% @doc Create a a number of database connections in the pool.
make_connection(PoolSize, PoolId, Database, Hostname, Port,
		  Username, Password, Encoding, LinkConnections) ->
    if PoolSize > 0 ->
	    connect(PoolId, Hostname, Port, Username, Password, Database,
		    Encoding, LinkConnections),
	    make_connection(PoolSize-1, PoolId, Database, Hostname, Port,
			    Username, Password, Encoding,
			    LinkConnections);
       true ->
	    ok
    end.

%% @doc Call connect/7 with Port set to 3306 and Reconnect set to 'true'.
%% If the connection is lost, reconnection is attempted.
%% The connection process is linked to the calling process.
%%
%% @spec connect(PoolId::atom(), Hostname::string(),
%%    Username::string(), Password::string(), Database::string()) -> ok
connect(PoolId, Hostname, Username, Password, Database) ->
    mysql:connect(PoolId, Hostname, 3306, Username, Password, Database,
		  undefined, true).

%% @doc Add a connection to the connection pool. If PoolId is
%%   'undefined', the default pool, 'erlydb_mysql', is used. The connection
%%   process is linked to the calling process.
%%
%% @spec connect(PoolId::atom(), Hostname::string, Port::integer(),
%%    Username::string(), Password::string(), Database::string(),
%%    Reconnect::boolean()) -> ok
connect(PoolId, Hostname, Port, Username, Password, Database,
	 Reconnect) ->
    mysql:connect(PoolId, Hostname, Port, Username, Password, Database,
		  Reconnect).

%% @doc Add a connection to the connection pool, with encoding specified.
%% The connection process is linked to the calling process.
%%
%% @spec connect(PoolId::atom(), Hostname::string, Port::integer(),
%%    Username::string(), Password::string(), Database::string(),
%%    Encoding,
%%    Reconnect::boolean()) -> ok
connect(PoolId, Hostname, Port, Username, Password, Database,
	 Encoding, Reconnect) ->
    mysql:connect(PoolId, Hostname, Port, Username, Password, Database,
		  Encoding, Reconnect).

%% @doc Add a connection to the connection pool, with encoding specified.
%% If LinkConnection == false, the connection will not be linked to the
%% current process.
%%
%% @spec connect(PoolId::atom(), Hostname::string, Port::integer(),
%%    Username::string(), Password::string(), Database::string(),
%%    Encoding::string(),
%%    Reconnect::boolean(), LinkConnection::bool()) -> ok
connect(PoolId, Hostname, Port, Username, Password, Database,
	 Encoding, Reconnect, LinkConnection) ->
    mysql:connect(PoolId, Hostname, Port, Username, Password, Database,
		  Encoding, Reconnect, LinkConnection).

%% @doc Get the table names and fields for the database.
%%
%% @spec get_metadata(Options::options()) -> gb_trees()
get_metadata(Options) ->
    case q2(<<"show tables">>, Options) of
	{data, Res} ->
	    Tables = mysql:get_result_rows(Res),
	    lists:foldl(
	      fun([Table | _], TableTree) ->
		      case q2(<<"describe ", Table/binary>>, Options) of
			  {data, FieldRes} ->
			      Rows = mysql:get_result_rows(FieldRes),
			      Fields =
				  [new_field(FieldData) ||
				      FieldData <- Rows],
			      gb_trees:enter(binary_to_atom(Table), Fields,
					     TableTree);
			  {error, Err} ->
			      exit(Err)
		      end
	      end, gb_trees:empty(), Tables);
	{error, Err} ->
	    exit(Err)
    end.

%% @doc Get the default connection pool name for the driver.
%%
%% @spec get_default_pool_id() -> atom()
get_default_pool_id() ->
    ?Epid.

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
	       _Other -> undefined
	   end,
    Extra1 = case Extra of
		 <<"auto_increment">> -> identity;
		 _ -> undefined
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
    

%% @doc Execute a statement against the MySQL driver with the default options.
%% The connection default pool name ('erlydb_mysql') is used.
%%
%% @spec q(Statement::statement()) -> mysql_result()
q(Statement) ->
    q(Statement, undefined).

%% @doc Execute a statement directly against the MySQL driver. If 
%%   Options contains the value {allow_unsafe_statements, true}, binary
%%   and string queries as well as ErlSQL queries with binary and/or
%%   string expressions are accepted. Otherwise, this function exits.
%%
%% @spec q(Statement::statement(), Options::options()) ->
%%   mysql_result() | exit({unsafe_statement, Statement})
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

%% @doc Execute a (binary or string) statement against the MySQL driver
%% using the default options.
%% ErlyDB doesn't use this function, but it's sometimes convenient for
%% testing.
%%
%% @spec q2(Statement::string() | binary()) ->
%%   mysql_result()
q2(Statement) ->
    q2(Statement, undefined).

%% @doc Execute a (binary or string) statement against the MySQL driver.
%% ErlyDB doesn't use this function, but it's sometimes convenient for testing.
%%
%% @spec q2(Statement::string() | binary(), Options::options()) ->
%%   mysql_result()
q2(Statement, Options) ->
    mysql:fetch(get_pool_id(Options), Statement, get_timeout(Options)).

%% @doc Execute a group of statements in a transaction.
%%   Fun is the function that implements the transaction.
%%   Fun can contain an arbitrary sequence of calls to
%%   the erlydb_mysql's query functions. If Fun crashes or returns
%%   or throws 'error' or {error, Err}, the transaction is automatically
%%   rolled back. 
%%
%% @spec transaction(Fun::function(), Options::options()) ->
%%   {atomic, Result} | {aborted, Reason}
transaction(Fun, Options) ->
    mysql:transaction(get_pool_id(Options), Fun, get_timeout(Options)).

    
%% @doc Execute a raw SELECT statement.
%%
%% @spec select(Statement::statement(), Options::options()) ->
%%   {ok, Rows::list()} | {error, Error}
select(Statement, Options) ->
    select2(Statement, Options, []).

%% @doc Execute a SELECT statements for records belonging to the given module,
%%   returning all rows with additional data to support
%%   higher-level ErlyDB features.
%%
%% @spec select_as(Module::atom(), Statement::statement(),
%%   Options::options()) -> {ok, Rows} | {error, Error}
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
%% @spec update(Statement::statement(), Options::options()) ->
%%  {ok, NumAffected} | {error, Err}
update(Statement, Options) ->
    R = q(Statement, Options),
    get_update_result(R).


get_update_result({updated, MySQLRes}) ->
    {ok, mysql:get_result_affected_rows(MySQLRes)};
get_update_result(Other) -> Other.


%% @doc Get the id of the last inserted record.
%%
%% @spec get_last_insert_id(undefined, Options::options()) -> term()
get_last_insert_id(_Table, Options) ->
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
%% @spec execute(Name::atom(), Options::options()) -> mysql_result()
execute(Name, Options) ->
    mysql:execute(get_pool_id(Options), Name).

%% @doc Execute a prepared statement with the list of parameters.
%%
%% @spec execute(Name::atom(), Params::[term()], Options::options()) ->
%%   mysql_result()
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
%% @spec execute_update(Name::atom(), Options::options()) ->
%%   {ok, NumUpdated::integer()} | {error, Err}
execute_update(Name, Options) ->
    get_update_result(execute(Name, Options)).

%% @doc Execute a prepared statement with the list of parameters and
%%   and return the result as the the update() function.
%%
%% @spec execute_update(Name::atom(), Params::[term()], Options::options()) ->
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
	    get_default_pool_id();
	Other ->
	    Other
    end.

get_timeout(undefined) -> 5000;
get_timeout(Options) ->
    case proplists:get_value(erlydb_timeout, Options) of
	undefined ->
	    5000;
	Timeout ->
	    Timeout
    end.
