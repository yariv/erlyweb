%% @author Roberto Saccon (rsaccon@gmail.com)
%% @copyright Roberto Saccon 2007
%% 
%% @doc This module implements the Postgresql driver for ErlyDB.
%%
%% Based on code initially developed by Brian Olson, see 
%% (http://groups.google.com/group/erlyweb/browse_frm/thread/e1585240f790c87c)
%%
%% This is an internal ErlyDB module that you normally shouldn't have to
%% use directly. 
%% 
%% Postgresql driver is an OTP application.
%% Define database details and authetication credentials for the driver pool 
%% in psql.app.src and run 'make app' to crate an OTP-ish ebin/psql.app
%%
%% For license information see LICENSE.txt

-module(erlydb_psql).

-author("Roberto Saccon (rsaccon@gmail.com)").

-export([start/0,
         stop/0, 
         get_metadata/1,
         q/1, 
         q/2, 
         q2/1,
         q2/2, 
         transaction/2,
         select/2,
         select_as/3,
         update/2,
         get_last_insert_id/2]).


%% Useful for debugging
-define(L(Msg), io:format("~p:~b ~p ~n", [?MODULE, ?LINE, Msg])).
-define(S(Obj), io:format("LOG ~w ~s\n", [?LINE, Obj])).


%% @doc Starts the psql and sql applications up if they are not
%% already started. Any errors that are returned are ignored.
%%
%% @todo catch when a connection is not possible:
%% for example, it returns {connection_failed, econnrefused}
start() ->
    application:load(psql),
    application:start(psql).
   

%% @doc Stops the psql and sql applications.
stop() ->
    application:stop(psql).


table_names_sql(SchemaName) ->
    "select tablename from pg_tables where schemaname = '" ++
	SchemaName ++ "'".
        
column_attributes_sql(TableName) ->
    "SELECT a.attname, format_type(a.atttypid, a.atttypmod), d.adsrc, "
	"a.attnotnull"
	" FROM pg_attribute a LEFT JOIN pg_attrdef d"
	" ON a.attrelid = d.adrelid AND a.attnum = d.adnum"
	" WHERE a.attrelid = '" ++ TableName ++ "'::regclass"
	" AND a.attnum > 0 AND NOT a.attisdropped"
	" ORDER BY a.attnum".

constraints_sql(TableName, SchemaName) ->
    "SELECT column_name, constraint_name  FROM"
	" information_schema.constraint_column_usage where"
	" table_name = '" ++ TableName ++ "' AND table_schema = '" ++
	SchemaName ++ "'".

%% @doc Get the table names and fields for the database.
get_metadata(_Options) ->
    Pid = psql:allocate(),
    Schema = "public",
    TableNames = q3(Pid, table_names_sql(Schema)),
    ConstraintInfo =
	lists:flatten(
	  [q3(Pid, constraints_sql(element(1, Name), Schema)) ||
	      Name <- TableNames]),
    Result = case catch lists:foldl(
			  fun(Table, TablesTree) ->
				  get_metadata(Pid, Table, ConstraintInfo,
					       TablesTree)
			  end, 
			  gb_trees:empty(), TableNames) of
                 {error, Err} -> exit(Err);
                 Tree -> Tree
             end,
    psql:free(),
    Result.

get_metadata(Pid, Table, ConstraintInfo, TablesTree) ->
    Columns = q3(Pid, column_attributes_sql(element(1, Table))),
    Fields = [new_field(Column, element(1, Table), ConstraintInfo) ||
		 Column <- Columns],
    TableName = list_to_atom(element(1, Table)),
    gb_trees:enter(TableName, lists:reverse(Fields), TablesTree).
                                                          
new_field(FieldInfo, TableName, ConstraintInfo) ->
    Name = element(1, FieldInfo),
    Type = parse_type(element(2, FieldInfo)),
    {Default, Extra} = parse_default(element(3, FieldInfo)),
    Null = case element(4, FieldInfo) of
               true -> false;
               false -> true
           end,
    Keys = lists:map(fun(Elem) ->
                             CName = TableName ++ "_pkey",
                             case element(2, Elem) of
                                 CName -> element(1, Elem);
                                 _ -> none
                             end  
                     end,
                     ConstraintInfo),    
    Key = case lists:member(Name, Keys) of
              true -> primary;
              _ -> undefined
          end,
    erlydb_field:new(list_to_atom(Name), Type, Null, Key, Default, Extra).


parse_type(TypeStr) ->
    case string:chr(TypeStr, 40) of  %% 40 == '('
        0 ->
            {list_to_atom(TypeStr), undefined};
       Idx ->
            {TypeStr1, [_| Vals]} = lists:split(Idx - 1, TypeStr),
            {ok, [Len], _} = io_lib:fread("~d", Vals),
            {list_to_atom(TypeStr1), Len}
    end.


parse_default([]) ->
    {undefined, undefined};
 
parse_default(DefaultStr) ->
    case string:str(DefaultStr, "nextval") of
        0 -> 
            Default = hd(string:tokens(DefaultStr, "::")),  
            {Default, undefined};    
        _ ->
            {undefined, identity}
    end.

%% @doc Execute a statement directly against the PostgreSQL driver. If
%% Options contains the value {allow_unsafe_sql, true}, binary and string
%% queries as well as ErlSQL queries with binary and/or string expressions are
%% accepted. Otherwise the function crashes.
q(Statement) ->
    q(Statement, undefined).           


q({esql, Statement}, Options) ->
    case allow_unsafe_statements(Options) of
        true -> 
            {ok, q2(erlsql:unsafe_sql(Statement), Options)};
        _ ->
            case catch erlsql:sql(Statement) of
                {error, _} = Err ->
                    exit(Err);
                Sql ->
                    {ok, q2(Sql, Options)}
                    %% TODO: catch errors
            end
    end.


%% @doc Execute a (binary or string) statement against the Postgresql driver
%% using the default options.
%%
%% @spec q2(Statement::string() | binary()) -> 
%%   psql_result()
q2(Statement) ->
    q2(Statement, undefined).
  
            
%% @doc Execute a (binary or string) statement against the MySQL driver.
%%
%% @spec q2(Statement::string() | binary(), Options::proplist()) ->
%%   psql_result()                      
q2(Statement, _Options) ->
    Pid = psql:allocate(),
    Result = q3(Pid, Statement),
    psql:free(),
    Result.


q3(Pid, Sql) ->    
    case psql:sql_query(Pid, Sql) of     
        {_FieldInfo,[{_Status, Rows}]} ->      
            Rows;
        [{Status, _Rows}] ->
            Status;
        Other ->
            Other
    end.


allow_unsafe_statements(undefined) ->
    false;
allow_unsafe_statements(Options) ->
    proplists:get_value(allow_unsafe_statements, Options).


%% get_pool_id(undefined) ->
%%   erlydb_psql;
%% get_pool_id(Options) ->
%%       case proplists:get_value(pool_name, Options) of
%% undefined -> erlydb_psql;
%% Other -> Other
%%   end.


%% @doc Models a transaction. If an error occurs in the function provided, then
%% the transaction will rollback. Otherwise it will commit.
transaction(Fun, _Options) ->
    Pid = psql:allocate(),
    psql:transaction(Pid),
    case catch Fun() of
        {'EXIT', Reason} ->
            psql:rollback(Pid),
            psql:free(),
            {aborted, Reason};
        Val ->
            psql:commit(Pid),
            psql:free(),
            {atomic, Val}
    end.


%% @doc Execute a raw SELECT statement.
%%
%% @spec select(PoolId::atom(), Statement::statement()) ->
%%   {ok, Rows::list()} | {error, Error}
select(Statement, Options) ->
    select2(Statement, Options, []).


%% @doc Execute a SELECT statements for records belonging to the given module,
%%   returning all rows with additional data to support
%%   higher-level ErlyDB features.
%%
%% @spec select_as(Module::atom(), Statement::statement(),
%%   FixedCols::tuple()) -> {ok, Rows} | {error, Error}
select_as(Module, Statement, Options) ->
    select2(Statement, Options, [Module, false]).

select2(Statement, Options, FixedVals) ->
    get_select_result(q(Statement, Options), FixedVals).

get_select_result({ok, _Rows}=Result, undefined) ->
    Result;
get_select_result({ok, Rows}, FixedVals)->
    Result = lists:foldl(
	       fun(Fields, Acc) ->
		       Row = FixedVals ++
			   lists_to_binaries(tuple_to_list(Fields)),
		       [list_to_tuple(Row) | Acc]
	       end, [], Rows),
    {ok, Result};
get_select_result(Other, _) -> 
    Other.

lists_to_binaries(Row) ->
    [to_binary(Field) || Field <- Row].
    
to_binary(Field) when is_list(Field) -> 
    list_to_binary(Field);
to_binary(Row) -> Row.    


%% @doc Execute a INSERT, DELETE or UPDATE statement.
%%
%% @spec update(Statement::statement(), Options::options()) ->
%%  {ok, NumAffected} | {error, Err}
%%
update(Statement, Options) ->
    R = q(Statement, Options),
    get_update_result(R).
                         
get_update_result({ok, <<"INSERT", Rest/binary>>}) -> 
    {ok, get_update_element(2, Rest)};  
get_update_result({ok, <<"DELETE", Rest/binary>>}) -> 
    {ok, get_update_element(1, Rest)};  
get_update_result({ok, <<"UPDATE", Rest/binary>>}) -> 
    {ok, get_update_element(1, Rest)};
            
get_update_result(Other) -> 
    Other.

get_update_element(N, Bin) -> 
    {S, <<0>>} = split_binary(Bin, size(Bin)-1),
    R = lists:nth(N, string:tokens(binary_to_list(S), " ")),
    {AffectedRows, _} = string:to_integer(R), 
    AffectedRows.


%% @doc Get the id of the last inserted record.
%%
%% @spec get_last_insert_id(TableName::atom(), Options::proplist()) -> term()
get_last_insert_id(Table, Options) -> 
    TableName = atom_to_list(Table),   
    Sql = "SELECT currval('" ++ TableName ++ "_id_seq');", 
    case q2(Sql, Options) of
        [{N}] ->  {ok, N};
        Err -> exit(Err)   
    end.

