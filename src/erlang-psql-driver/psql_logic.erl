%%--------------------------------------------------------------------
%%% @copyright (C) 2006, Erlang Traingin & Consulting, Inc.
%%% @version 0.2.0
%%% @author  <martin@martins-desktop.office.erlangsystems.com>
%%% @doc 
%%% @end
%%--------------------------------------------------------------------
-module(psql_logic).
-author("support@erlang-consulting.com").
-copyright("2006 (C) Erlang Trining & Consulting Ltd.").
-vsn("$Rev$").

%% API
-export([start_link/0, start_link/1, command/2]).
-export([connection_event/2, connection_closed/1]).
-export([init/1, system_continue/3, system_terminate/4, system_code_change/4]).

-record(state, {connection,
		host,
		port,
		user,
		password,
		database,
		connected = false,
    tx_level = 0}).

-define(PROTOCOL_VERSION, 196608).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok, pid()} | {error, term()}
%% @doc Start the special process
%% @end
%%--------------------------------------------------------------------
start_link() ->
    proc_lib:start_link(?MODULE, init, [self()]).

%%--------------------------------------------------------------------
%% @spec start_link(Pool::atom()) -> {ok, pid()} | {error, term()}
%% @doc Start the special process and connect to `Pool'
%% @end
%%--------------------------------------------------------------------
start_link({_PoolName, {Host, Port, Usr, Pwd, Db}}) ->
    {ok, Pid} = proc_lib:start_link(?MODULE, init, [self()]),
    command(Pid, {connect, self(), Host, Port, Usr, Pwd, Db}),
    receive
	{psql_server,ready_for_query} ->
	    {ok, Pid}	    
    end.


%%--------------------------------------------------------------------
%% @spec command(Logic::pid(), Command::term()) -> ok
%% @doc send a command to the logic process
%% @end
%%--------------------------------------------------------------------
command(Logic, Command) ->
    Logic ! Command,
    ok.

%%--------------------------------------------------------------------
%% @spec init(Parent) -> exit()
%% @doc Start the special process
%% @end
%%--------------------------------------------------------------------
init(Parent) ->
    Debug = sys:debug_options([]),
    {ok, Connection} = psql_connection:start_link(self()),
    psql_pool:register(self()),
    proc_lib:init_ack(Parent, {ok, self()}),
    loop(Parent, Debug, #state{connection = Connection}).

%%--------------------------------------------------------------------
%% @spec loop(Parent::term(), Debug::term(), State::#state{}) -> exit()
%% @doc Server Loop
%% @end
%%--------------------------------------------------------------------
loop(Parent, Debug, State) ->
    receive
	{system, From, Req} ->
	    sys:handle_system_msg(Req, From, Parent, ?MODULE, Debug, State);
	{get_modules, From} = Msg ->
	    Debug2 = sys:handle_debug(Debug, fun write_debug/3, State, Msg),
	    reply(From, {modules, [?MODULE]}),
	    loop(Parent, Debug2, State);
	{'EXIT', Parent, Reason} ->
	    exit(Reason);
	{psql, connection_closed, _} = Msg -> %% Connection drops
	    Debug2 = sys:handle_debug(Debug, fun write_debug/3, State, Msg),
	    loop(Parent, Debug2, State#state{connected = false});
	{psql, connection_established, _} = Msg -> %% Reconnections
	    Debug2 = sys:handle_debug(Debug, fun write_debug/3, State, Msg),
	    State0 = authenticate(self(), State),
	    loop(Parent, Debug2, State0);
	{connect, From, Host, Port, Usr, Pwd, DB} = Msg 
	when State#state.connected == false ->
	    Debug2 = sys:handle_debug(Debug, fun write_debug/3, State, Msg),
	    State0 = connect(From, State#state{host = Host,
					       port = Port,
					       user = Usr,
					       password = Pwd,
					       database = DB}),
	    loop(Parent, Debug2, State0);
	{parse, From, Statement, Query, Types} = Msg
	when State#state.connected == true ->
	    Debug2 = sys:handle_debug(Debug, fun write_debug/3, State, Msg),
	    State0 = parse(From, Statement, Query, Types, State),
	    loop(Parent, Debug2, State0);
	{bind, From, Portal, Statement, Parameters} = Msg
	when State#state.connected == true ->
	    Debug2 = sys:handle_debug(Debug, fun write_debug/3, State, Msg),
	    State0 = bind(From, Portal, Statement, Parameters, State),
	    loop(Parent, Debug2, State0);	    
	{describe, From, Type, Name} = Msg
	when State#state.connected == true ->
	    Debug2 = sys:handle_debug(Debug, fun write_debug/3, State, Msg),
	    State0 = describe(From, Type, Name, State),
	    loop(Parent, Debug2, State0);	    
	{execute, From, Name, ResultSet} = Msg
	when State#state.connected == true ->
	    Debug2 = sys:handle_debug(Debug, fun write_debug/3, State, Msg),
	    State0 = execute(From, Name, ResultSet, State),
	    loop(Parent, Debug2, State0);	    	    
	{close, From, Type, Name} = Msg
	when State#state.connected == true ->
	    Debug2 = sys:handle_debug(Debug, fun write_debug/3, State, Msg),
	    State0 = close(From, Name, Type, State),
	    loop(Parent, Debug2, State0);	    	    
	{simple_query, From, Query} = Msg
	when State#state.connected == true ->
    	    Debug2 = sys:handle_debug(Debug, fun write_debug/3, State, Msg),
	    State0 = simple_query(From, Query, State),
	    loop(Parent, Debug2, State0);
	Msg ->
    	    Debug2 = sys:handle_debug(Debug, fun write_debug/3, State, Msg),
	    loop(Parent, Debug2, State)
    end.

%% ===================================================================
%% States
%% ===================================================================
%% -------------------------------------------------------------------
%% Connect to the database server
%% -------------------------------------------------------------------
connect(From, #state{host = Host, port = Port} = State) ->
    psql_connection:command(State#state.connection, {connect, Host, Port}),
    receive
	{psql, connection_established, _} ->
	    authenticate(From, State);
	dissconnect ->
	    psql_connection:command(State#state.connection, disconnect),
	    State
    end.

%% -------------------------------------------------------------------
%% Authenticate with the database server
%% -------------------------------------------------------------------
authenticate(From, State) ->
    {Auth, Digest} = psql_protocol:authenticate(?PROTOCOL_VERSION, 
						State#state.user,
						State#state.password,
						State#state.database),
    psql_connection:command(State#state.connection, {send, Auth}),
    receive
	{psql, authentication, <<0,0,0,5, Salt/binary>>} ->
	    AuthDigest = psql_protocol:md5digest(Digest, Salt),
	    psql_connection:command(State#state.connection, {send, AuthDigest}),
	    receive
		{psql, authentication, <<0,0,0,0>>} ->
		    setup(From, State);
		{psql, error, Error} ->
		    reply(From, {sql_error, Error}),
		    State
	    end;
	{psql, connection_closed, _} ->
	    reply(From, disconnected),
	    State;
	{psql, error, Error} ->
	    reply(From, {sql_error, Error}),
	    State
    end.

%% -------------------------------------------------------------------
%% Set parameters to match the servers
%% -------------------------------------------------------------------
setup(From, State) ->
    receive
	{psql, ready_for_query, _} ->
	    reply(From, ready_for_query),
	    State#state{connected = true};
	{psql, error, Error} ->
	    reply(From, {sql_error, Error}),
	    State;
	_Parameter ->						
	    setup(From, State)	% TODO: Parse and set the parameter
    end.

parse(From, Statement, Query, Types, State) ->
    QueryMsg = psql_protocol:parse(Statement, Query, Types),
    psql_connection:command(State#state.connection, {send, QueryMsg}),
    psql_connection:sync(State#state.connection),
    receive
	{psql, parse_complete, _} ->
	    reply(From, parse_complete),
	    receive
		{psql, ready_for_query, _} ->
		    reply(From, ready_for_query),
		    State
	    end;
	{psql, error, Error} ->
	    reply(From, {sql_error, Error}),
	    State
    end.

bind(From, Portal, Statement, Parameters, State) ->
    QueryMsg = psql_protocol:bind(Portal, Statement, Parameters),
    psql_connection:command(State#state.connection, {send, QueryMsg}),
    psql_connection:sync(State#state.connection),
    receive
	{psql, bind_complete, _} ->
	    reply(From, bind_complete),
	    receive
		{psql, ready_for_query, _} ->
		    reply(From, ready_for_query),
		    State
	    end;
	{psql, error, Error} ->
	    reply(From, {sql_error, Error}),
	    State
    end.

%% There be dragons here.... 
describe(From, Type, Name, State) ->
    QueryMsg = psql_protocol:describe(Name, Type),
    psql_connection:command(State#state.connection, {send, QueryMsg}),
    psql_connection:sync(State#state.connection),
    receive
	{psql, row_description, Data} ->
	    reply(From, {row_description, Data}),
	    reply(From, ready_for_query), %% Should really be fetch_result 
	    State;
	{psql, error, Error} ->
	    reply(From, {sql_error, Error}),
	    State
    after
	1000 ->
	    reply(From, disconnected),
	    State
    end.

execute(From, Name, ResultSet, State) ->
    QueryMsg = psql_protocol:execute(Name, ResultSet),
    psql_connection:command(State#state.connection, {send, QueryMsg}),
    psql_connection:sync(State#state.connection),
    fetch_result(From, [], State).

close(From, Name, Type, State) ->
    QueryMsg = psql_protocol:close(Name, Type),
    psql_connection:command(State#state.connection, {send, QueryMsg}),
    psql_connection:sync(State#state.connection),
    fetch_result(From, [], State).    

%% modified by rsaccon to handle nested transactions
%%
simple_query(From, "BEGIN"=Query, State) -> 
  case State#state.tx_level of
0 ->
    %% start parent transaction
    State2 = do_simple_query(From, Query, State),
    State2#state{tx_level = 1};
_ ->      
    %% transform nested BEGIN into SAVEPONIT
    TxLevel = State#state.tx_level + 1,
    Query2 = "SAVEPOINT sp-" ++ integer_to_list(TxLevel),
    State2 = do_simple_query(From, Query2, State),   
    State2#state{tx_level = TxLevel}
  end;

%% modified by rsaccon to handle nested transactions
%%
simple_query(From, "COMMIT"=Query, State) ->     
  case State#state.tx_level of
0 -> 
    %% invalid query
    %% TODO: thorw error
    reply(From, ready_for_query),    
    State;   
1 ->
    %% COMMIT parent transaction
    State2 = do_simple_query(From, Query, State),
    State2#state{tx_level = 0};
_ ->    
    %% transform nested COMMIT into RELEASE SAVEPOINT
    TxLevel = State#state.tx_level,
    Sql = "RELEASE SAVEPOINT sp-" ++ integer_to_list(TxLevel),
    State2 = do_simple_query(From, Sql, State),       
    State2#state{tx_level = TxLevel-1}
  end;      

%% modified by rsaccon to handle nested transactions
%%
simple_query(From, "ROLLBACK"=Query, State) -> 
  case State#state.tx_level of
0 -> 
    %% invalid query 
    %% TODO: throw error
    reply(From, ready_for_query),    
    State;
1 ->
    %% ROLLBACK parent transaction
    State2 = do_simple_query(From, Query, State),
    State2#state{tx_level = 0};
_ ->   
    %% transform nested ROLLBACK into ROLLBACK TO SAVEPOINT
    TxLevel = State#state.tx_level,
    Sql = "ROLLBACK TO SAVEPOINT sp-" ++ integer_to_list(TxLevel),
    State2 = do_simple_query(From, Sql, State),       
    State2#state{tx_level = TxLevel-1}
  end;

%% modified by rsaccon to handle nested transactions
%%
simple_query(From, Query, State) ->
    do_simple_query(From, Query, State).

do_simple_query(From, Query, State) ->
    QueryMsg = psql_protocol:q(Query),
    psql_connection:command(State#state.connection, {send, QueryMsg}),
    psql_connection:sync(State#state.connection),
    fetch_result(From, [], State).
                                  
fetch_result(From, Result, State) ->
    receive
	{psql, row_description, Data} ->
	    reply(From, {row_description, Data}),
	    fetch_result(From, [], State);
	{psql, data_row, Data} ->	    
	    fetch_result(From, [Data|Result], State);
	{psql, no_data, _} ->
	    fetch_result(From, no_data, State);
	{psql, command_complete, Command} ->
	    reply(From, {command_complete, Command, Result}),
	    fetch_result(From, [], State);
	{psql, error, Error} ->
	    reply(From, {sql_error, Error}),
	    fetch_result(From, Result, State);
	{psql, portal_suspended, _} ->
	    reply(From, fetch_more),
	    State;
	{psql, ready_for_query, _} ->
	    reply(From, ready_for_query),
	    State
    end.
    
    
%% ===================================================================
%% Connection Callbacks
%% ===================================================================
connection_event(Logic, Message) ->
    Logic ! Message,
    ok.

connection_closed(Logic) ->
    Logic ! {psql, connection_closed, self()}.

%% ===================================================================
%% System Callbacks
%% ===================================================================
%%--------------------------------------------------------------------
%% @spec system_continue(Parent::pid(), 
%%                       Debug::list(), 
%%                       State::term()) -> exit()
%% @doc Called by sys
%% @end
%% @see //sys. sys
%%--------------------------------------------------------------------
system_continue(Parent, Debug, State) ->
    loop(Parent, Debug, State).

%%--------------------------------------------------------------------
%% @spec system_terminate(Reason::atom(), Parent::pid(), 
%%                        Debug::list(), State::term()) -> exit()
%% @doc Terminate the process called by sys
%% @end
%% @see //sys. sys
%%--------------------------------------------------------------------
system_terminate(Reason, _Parent, _Debug, _State) ->
    psql_pool:unregister(self()),
    if
	Reason == normal; Reason == shutdown ->
	    exit(Reason);
	true ->
	    error_logger:error_report({unknown_reason, self(), Reason}),
	    exit(Reason)
    end.

%%--------------------------------------------------------------------
%% @spec (State::term(), Module::atom(), Vsn::term(), Extra::term()) -> 
%% {ok, term()}
%% @doc Updates the process state called by sys
%% @end
%% @see //sys. sys
%%--------------------------------------------------------------------
system_code_change(State, _Module, _OldVsn, _Extra) ->
    io:format("Updating code...~n"),
    {ok, State}.

%%--------------------------------------------------------------------
%% @spec write_debug(Device::ref(), Event::term(), State::term()) -> ok
%% @doc Write debug messages called by sys
%% @end
%% @see //sys. sys
%%--------------------------------------------------------------------
write_debug(Device, Event, State) ->
    io:format(Device, "*DBG* ~p, ~w", [Event, State]).

%%====================================================================
%% Internal functions
%%====================================================================
reply(From, Message) ->
    From ! {psql_server,Message},
    ok.
