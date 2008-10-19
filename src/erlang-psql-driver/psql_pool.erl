%%%-------------------------------------------------------------------
%%% @author Martin Carlson <martin@erlang-consulting.com>
%%% @doc 
%%% @end
%%% Revisions:
%%%-------------------------------------------------------------------
-module(psql_pool).
-author('code@erlang-consulting.com').
-copyright('Erlang Training & Consulting Ltd.').
-vsn("$Rev$").

-behaviour(gen_server).

%% API
-export([start_link/0,
	 register/1,
	 unregister/1,
	 alloc/1,
	 free/1,
	 ref/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include_lib("stdlib/include/qlc.hrl").

-define(SERVER, ?MODULE).

-record(state, {pool}).
-record(pool, {pid, type, ref}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?SERVER, [], []).

%%--------------------------------------------------------------------
%% @spec register(Resource::pid()) -> ok
%% @doc Register a resource
%% @end
%%--------------------------------------------------------------------
register(Resource) ->
    gen_server:call(?SERVER, {register, Resource}).

%%--------------------------------------------------------------------
%% @spec unregister(Resource::pid()) -> ok
%% @doc Unregisters a resource
%% @end
%%--------------------------------------------------------------------
unregister(Resource) ->
    gen_server:call(?SERVER, {unregister, Resource}).

%%--------------------------------------------------------------------
%% @spec alloc(Pid::pid()) -> pid()
%% @doc Allocates a resource
%% @end
%%--------------------------------------------------------------------
alloc(Pid) ->
    case ref(Pid) of
	nomatch ->
	    gen_server:call(?SERVER, {alloc, Pid});
	Res ->
	    Res
    end.

%%--------------------------------------------------------------------
%% @spec free(Pid::pid()) -> pid()
%% @doc Frees a pid
%% @end
%%--------------------------------------------------------------------
free(Pid) ->
    case ref(Pid) of
	nomatch ->
	    ok;
	_ ->	    
	    gen_server:call(?SERVER, {free, Pid})
    end.

%%--------------------------------------------------------------------
%% @spec ref(Pid) -> pid() | nomatch
%% @doc Get the resource bound to pid
%% @end
%%--------------------------------------------------------------------
ref(Pid) ->
    gen_server:call(?SERVER, {ref, Pid}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{pool = ets:new(pool, [{keypos, 2}])}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({register, Res}, _From, #state{pool = P} = State) ->
    link(Res),
    ets:insert(P, #pool{type = free, pid = Res}),
    {reply, ok, State};
handle_call({unregister, Res}, _From, #state{pool = P} = State) ->
    unlink(Res),
    case find_pid(Res, P) of
	#pool{type = free} ->
	    ets:delete(P, Res);
	#pool{type = alloc, pid = Client} ->
	    exit(Client, {error, psql_resource_deregistered}),
	    ets:delete(P, Client)
    end,
    {reply, ok, State};
handle_call({alloc, Pid}, From, #state{pool = P} = State) ->
    link(Pid),
    case first(free, ets:first(P), P) of
	nomatch ->	  
	    ets:insert(P, #pool{type = queue, pid = Pid, ref = From}),
	    {noreply, State};
	#pool{pid = Res} ->
	    ets:delete(P, Res),
	    ets:insert(P, #pool{type = alloc, pid = Pid, ref = Res}),
	    {reply, Res, State}
    end;
handle_call({free, Pid}, _From, #state{pool = P} = State) ->
    unlink(Pid),
    [#pool{ref = Res}] = ets:lookup(P, Pid),
    ets:delete(P, Pid),
    case first(queue, ets:first(P), P) of
	nomatch ->
	    ets:insert(P, #pool{type = free, pid = Res});
	#pool{pid = QPid, ref = QFrom} ->
	    ets:insert(P, #pool{type = alloc, pid = QPid, ref = Res}),
	    gen_server:reply(QFrom, Res)
    end,
    {reply, ok, State};
handle_call({ref, Pid}, _From, #state{pool = P} = State) ->
    case find_pid(Pid, P) of
	#pool{type = alloc, ref = Res} ->
	    {reply, Res, State};
	_ ->
	    {reply, nomatch, State}
    end.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({'EXIT', Pid, Reason}, #state{pool = P} = State) ->
    case find_pid(Pid, P) of
	#pool{type = free} ->
	    ets:delete(P, Pid);
	#pool{type = queue} ->
	    ets:delete(P, Pid);
	#pool{type = alloc, pid = Pid, ref = Res} ->
	    ets:delete(P, Pid),
	    unlink(Res),	
	    exit(Res, Reason);	    
	#pool{type = alloc, pid = Key} ->
	    ets:delete(P, Key),
	    unlink(Key),	
	    exit(Key, Reason)
    end,	    
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
first(_, '$end_of_table', _) ->
    nomatch;
first(Type, Key, TID) ->
    case ets:lookup(TID, Key) of
	[#pool{type = Type} = P] ->
	    P;	
	_ ->
	    first(Type, ets:next(TID, Key), TID)
    end.

find_pid(Pid, TID) ->
    QH = qlc:q([P || P <- ets:table(TID), 
		     (P#pool.pid == Pid) or (P#pool.ref == Pid)]),
    case qlc:e(QH) of
	[] ->
	    nomatch;
	[Pool] ->
	    Pool
    end.
