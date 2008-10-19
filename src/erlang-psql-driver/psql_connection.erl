%%%-------------------------------------------------------------------
%%%    BASIC INFORMATION
%%%-------------------------------------------------------------------
%%% @copyright 2006 Erlang Training & Consulting Ltd
%%% @author  Martin Carlson <martin@erlang-consulting.com>
%%% @version 0.0.1
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(psql_connection).

%% API
-export([start_link/1, command/2, sync/1]).

%% Internal Import
-export([init/2]).

%% System exports
-export([system_continue/3,
	 system_terminate/4,
	 system_code_change/4,
	 print_event/3]).

-record(state, {parent,
		debug,
		logic,
		host = undefined,
		port,
		socket = undefined,
		buffer = []}).

-define(TCP_OPTIONS, [binary, {active, true}, {packet, 0}]).
-define(RECONNECT_TIMEOUT, 1000).

%%====================================================================
%% API
%%====================================================================
start_link(Logic) ->
    proc_lib:start_link(?MODULE, init, [self(), Logic]).

command(Pid, Data) ->
    Pid ! {logic, self(), Data},
    ok.

sync(Pid) ->
    Pid ! {logic, self(), {send, psql_protocol:sync()}},
    ok.

%%====================================================================
%% Server
%%====================================================================
init(Parent, Logic) ->
    Debug = sys:debug_options([]),
    proc_lib:init_ack(Parent, {ok, self()}),
    loop(#state{parent = Parent, debug = Debug, logic = Logic}).

%%
%% This make sure we get into a reconnect loop if the connection goes down
%% by setting the socket to 'undefined'
%%
loop(State) when State#state.socket == undefined, State#state.host /= undefined ->
    case gen_tcp:connect(State#state.host, State#state.port, ?TCP_OPTIONS) of
	{ok, Socket} ->
	    psql_logic:connection_event(State#state.logic, 
					{psql, connection_established, []}),
	    loop(State#state{socket = Socket});
	{error, Reason} ->
	    error_logger:error_report({connection_failed, Reason}),
	    receive
		Message ->
		    handle_receive(Message, State)
	    after
		?RECONNECT_TIMEOUT ->
		    loop(State)
	    end
    end;
loop(State) ->
    receive
	Message ->
	    handle_receive(Message, State)	    
    end.

%%====================================================================
%% Receive Handling
%%====================================================================
handle_receive({system, From, Req}, State) ->
    sys:handle_system_msg(Req, From, State#state.parent, 
			  ?MODULE, State#state.debug, State);
handle_receive({logic, From, Data}, State) when From == State#state.logic ->
    DState = handle_debug({logic, Data, State}, State),
    case handle_message(Data, DState) of
	{ok, NewState} ->
	    loop(NewState);
	{stop, _State} ->
	    ok
    end;
handle_receive({tcp, Socket, Data}, State) when Socket == State#state.socket ->
    DState = handle_debug({socket, Data, State}, State),
    {ok, NewState} = handle_message(Data, DState),
    loop(NewState);
handle_receive({tcp_closed, Socket}, State) when Socket == State#state.socket ->
    DState = handle_debug({tcp_closed, Socket, State}, State),
    psql_logic:connection_closed(State#state.logic),
    loop(DState#state{socket = undefined});
handle_receive(Message, State) ->
    loop(handle_debug({unknown_message, Message, State}, State)).

%%====================================================================
%% Message Handling
%%====================================================================
handle_message({connect, Host, Port}, State) ->
    {ok, State#state{host = Host, port = Port}};
handle_message(disconnect, State) ->
    gen_tcp:close(State#state.socket),
    {ok, State#state{socket = undefined, host = undefined, port = undefined}};
handle_message(shutdown, State) ->
    gen_tcp:close(State#state.socket),
    {stop, State};
handle_message({send, Message}, State) ->
    Data = psql_protocol:encode(Message),
    case gen_tcp:send(State#state.socket, Data) of
	ok ->
	    {ok, State};
	{error, Reason} ->
	    psql_logic:connection_event(State#state.logic, {error, Reason}),
	    gen_tcp:close(State#state.socket),
	    {ok, State#state{socket = undefined}}
    end;
handle_message({send, Type, Message}, State) ->
    Data = psql_protocol:encode(Type, Message),
    case gen_tcp:send(State#state.socket, Data) of
	ok ->
	    {ok, State};
	{error, Reason} ->
	    psql_logic:connection_event(State#state.logic, {error, Reason}),
	    gen_tcp:close(State#state.socket),
	    {ok, State#state{socket = undefined}}
    end;
handle_message(Fragment, State) ->
    if
	State#state.buffer == []; State#state.buffer == <<>> ->
	    Msg = Fragment;
	true ->
	    Msg = <<(State#state.buffer)/binary, Fragment/binary>>
    end,
    case dispatch_messages(State#state.logic, psql_protocol:decode(Msg)) of
	{fragment, Tail} ->
	    {ok, State#state{buffer = Tail}};
	done ->
	    {ok, State#state{buffer = <<>>}}
    end.

%%====================================================================
%% System Callbacks
%%====================================================================
system_continue(Parent, Debug, State) ->
    loop(State#state{parent = Parent, debug = Debug}).

system_terminate(Reason, _Parent, _Debug, _State) ->
    exit(Reason).

system_code_change(State, _Module, _OldVsn, _Extra) ->
    {ok, State}.

%%====================================================================
%% Debug Callbacks
%%====================================================================
print_event(Dev, Event, []) ->
    io:format(Dev, "*DBG* ~p dbg  ~p~n", [self(), Event]);
print_event(Dev, Event, Name) ->
    io:format(Dev, "*DBG* ~p dbg  ~p~n", [Name, Event]).



%%====================================================================
%% Internal functions
%%====================================================================
handle_debug(Data, State) ->
    Debug = sys:handle_debug(State#state.debug, 
			     {?MODULE, print_event}, [],
			     Data),
    State#state{debug = Debug}.

dispatch_messages(_Logic, {next, Fragment}) ->
    {fragment, Fragment};
dispatch_messages(Logic, {{Type, Data}, <<>>}) ->
    psql_logic:connection_event(Logic, {psql, Type, Data}),
    done;
dispatch_messages(Logic, {{Type, Data}, Tail}) ->
    psql_logic:connection_event(Logic, {psql, Type, Data}),
    dispatch_messages(Logic, psql_protocol:decode(Tail)).
			 
