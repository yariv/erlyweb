%%%-------------------------------------------------------------------
%%%    BASIC INFORMATION
%%%-------------------------------------------------------------------
%%% @copyright 2006 Erlang Training & Consulting Ltd
%%% @author  Martin Carlson <martin@erlang-consulting.com>
%%% @version 0.2.0
%%% @doc Interface module for the PostgreSQL driver
%%% @end
%%%-------------------------------------------------------------------
-module(psql).
-author("support@erlang-consulting.com").
-copyright("Erlang Training & Consulting Ltd").
-vsn("$Rev").

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% API
-export([connect/4,
	 connect/6,
	 allocate/0,
	 free/0,
	 sql_query/2,
	 parse/4,
	 bind/4,
	 describe/3,
	 execute/3,
	 execute/4,
	 close/3,
	 transaction/1, 
	 commit/1, 
	 rollback/1]).

-define(REREQUEST_FREQ, 1000).
-define(DEFAULT_PORT, 5432).

%% Global types
%% @type query() = string(). 
%% SQL conformant query with omitted semicolon
%%
%% @type result() = [] | [{Command::binary(), Data::rows()}] | 
%%                        {sql_error, term()} | term(). 
%% Query results, rows are represented as tuples and 
%% values are converted to the closest erlang type 
%%
%% @type rows() = [row()] | []. 
%%
%% @type row() = tuple() | binary(). 
%% A tuple with one element per column converted to erlang terms
%% if the sql_query is used or if a description is passed to execute
%% else a binary representation for each column 
%%
%% @type portal() = string() | []. 
%% A string representation of a postgre portal, see postgres documentation
%%
%% @type statement() = string() | []. 
%% A string representation of a postgre statement, see postgres documentation
%%
%% @type sql_type() = integer(). 
%% A integer representing the OID of the type, see postgres documentation


%%====================================================================
%% Application callbacks
%%====================================================================
start(normal, []) ->
    case psql_sup:start_link() of
	{ok, Pid} ->
	    {ok, Pools} = application:get_env(psql, pools),
	    F = fun({Pool, _}) ->
			{ok, PS} = application:get_env(psql, Pool),
			psql_con_sup:start_connection({Pool, PS})
		end,
	    lists:foreach(F, Pools),
	    {ok, Pid};
	Error ->
	    Error
    end.

stop(_State) ->
    ok.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec connect(Pid::pid(), Host::string(), 
%%               Usr::string(), Pwd::string()) -> ok
%% @doc Connect to database
%% @end
%%--------------------------------------------------------------------
connect(Pid, Host, Usr, Pwd) ->
    connect(Pid, Host, ?DEFAULT_PORT, Usr, Pwd, Usr).

%%--------------------------------------------------------------------
%% @spec connect(Pid::pid(), Host::string(), Port::integer(), 
%%               Usr::string(), Pwd::string(), DB::string) -> ok
%% @doc Connect to database
%% @end
%%--------------------------------------------------------------------
connect(Pid, Host, Port, Usr, Pwd, DB) ->
    psql_logic:command(Pid, {connect, self(), Host, Port, Usr, Pwd, DB}),
    receive
	ready_for_query -> 
	    ok;
	Error -> 
	    Error
    end.		    

%%--------------------------------------------------------------------
%% @spec allocate() -> pid()
%% @doc Allocate a connection from the pool
%% @end
%%--------------------------------------------------------------------
allocate() ->
    psql_pool:alloc(self()).

%%--------------------------------------------------------------------
%% @spec free() -> ok
%% @doc Free a connection back to the pool
%% @end
%%--------------------------------------------------------------------
free() ->
    psql_pool:free(self()).

%%--------------------------------------------------------------------
%% @spec sql_query(Pid::pid(), Query::query()) -> result()
%% @doc Simple sql query
%% @end
%%--------------------------------------------------------------------
sql_query(Pid, Query) ->
    psql_logic:command(Pid, {simple_query, self(), Query}),
    result([], []).

%%--------------------------------------------------------------------
%% @spec parse(Pid::pid(), Name::statement(), 
%%             Query::query(), Args::[sql_type()]) -> {[],[]}
%% @doc Parse a sql query, can contain placeholders, i.e $N
%% @end
%%--------------------------------------------------------------------
parse(Pid, Name, Query, Args) ->
    psql_logic:command(Pid, {parse, self(), Name, Query, Args}),
    result([], []).

%%--------------------------------------------------------------------
%% @spec bind(Pid::pid(), Portal::portal(), 
%%            Statement::statement(), Args::[string()]) -> {[], []}
%% @doc Bind variables to placeholders. Variables must be strings
%% @end
%%--------------------------------------------------------------------
bind(Pid, Portal, Statement, Args) ->
    psql_logic:command(Pid, {bind, self(), Portal, Statement, Args}),
    result([], []).

%%--------------------------------------------------------------------
%% @spec describe(Pid, Type::statement|portal, 
%%                Name::statement()|portal()) -> result()
%% @doc Describes a resultset from a statement or a portal.
%% @end
%%--------------------------------------------------------------------
describe(Pid, Type, Name) ->
    psql_logic:command(Pid, {describe, self(), Type, Name}),
    result([], []).

%%--------------------------------------------------------------------
%% @spec execute(Pid::pid(), Portal::portal(), Size::integer()) -> result()
%% @doc Execute a portal returns at most Size rows
%% @end
%%--------------------------------------------------------------------
execute(Pid, Portal, Size) ->
    psql_logic:command(Pid, {execute, self(), Portal, Size}),
    result([], []).

%%--------------------------------------------------------------------
%% @spec execute(Pid::pid(), Portal::portal(), 
%%               Size::integer(), Description::term()) ->
%%       result()
%% @doc Execute a portal returns at most Size rows converted to erlang terms.
%% @end
%%--------------------------------------------------------------------
execute(Pid, Portal, Size, Desc) ->
    psql_logic:command(Pid, {execute, self(), Portal, Size}),
    result(Desc, []).

%%--------------------------------------------------------------------
%% @spec close(Pid, Type::statement|portal, Name::portal()|statement()) -> 
%%       result()
%% @doc Closes a statement or portal
%% @end
%%--------------------------------------------------------------------
close(Pid, Type, Name) ->
    psql_logic:command(Pid, {close, self(), Type, Name}),
    result([], []).    

%%--------------------------------------------------------------------
%% @spec transaction(Pid::pid()) -> result() 
%% @doc Starts a transaction
%% @end
%%--------------------------------------------------------------------
transaction(Pid) ->
    sql_query(Pid, "BEGIN").

%%--------------------------------------------------------------------
%% @spec commit(Pid::pid()) -> result()
%% @doc Commits a transaction
%% @end
%%--------------------------------------------------------------------
commit(Pid) ->
    sql_query(Pid, "COMMIT").

%%--------------------------------------------------------------------
%% @spec rollback(Pid::pid()) -> result() 
%% @doc Roll back a transaction
%% @end
%%--------------------------------------------------------------------
rollback(Pid) ->
    sql_query(Pid, "ROLLBACK").

%%====================================================================
%% Internal functions
%%====================================================================
collect_result(Desc, Acc) ->
    handle_result(receive_loop(infinite), Desc, Acc).


handle_result({row_description, Data}, _Desc, Acc) ->
    collect_result(psql_lib:row_description(Data), Acc);
handle_result({command_complete, Command, []}, Desc, Acc) ->
    collect_result(Desc, [{Command, []}|Acc]);
handle_result({command_complete, Command, Rows}, Desc, Acc) when Desc /= [] ->
    collect_result(Desc, [{Command, [psql_lib:row(Row, Desc) || Row <- Rows]}|Acc]);
handle_result({command_complete, Command, Rows}, Desc, Acc) ->
    collect_result(Desc, [{Command, Rows}|Acc]);
handle_result({sql_error, Error}, _Desc, _Acc) ->
    psql_lib:error(Error);
handle_result(parse_complete, Desc, Acc) ->
    collect_result(Desc, Acc);
handle_result(bind_complete, Desc, Acc) ->
    collect_result(Desc, Acc);
handle_result(fetch_more, Desc, Acc) ->    
    {Desc, Acc}; %% TODO: Handle this differently
handle_result(ready_for_query, Desc, Acc) ->
    case receive_loop(5) of %% TODO: Clean this up
	timeout ->
	    {Desc, Acc};
	Msg ->
	    handle_result(Msg, Desc, Acc)
    end.


result(Desc, Acc) ->
    case collect_result(Desc, Acc) of	
	{[], Result} ->
	    Result;
	{Result, []} ->
	    Result;
	Result ->
	    Result
    end.

receive_loop(infinite) ->
    receive
	{psql_server,Msg} -> Msg
    end;
receive_loop(Timeout) ->
    receive
	{psql_server,Msg} -> Msg
    after
	Timeout -> timeout
    end.
    
