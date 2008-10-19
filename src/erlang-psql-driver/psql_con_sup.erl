%%%-------------------------------------------------------------------
%%%    BASIC INFORMATION
%%%-------------------------------------------------------------------
%%% @copyright 2006 Erlang Training & Consulting Ltd
%%% @author  Martin Carlson <martin@erlang-consulting.com>
%%% @version 0.0.1
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(psql_con_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_connection/0, start_connection/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the supervisor
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).    
			  
start_connection() ->
    supervisor:start_child(?SERVER, []).

start_connection(PoolSpec) ->
    supervisor:start_child(?SERVER, [PoolSpec]).


%%====================================================================
%% Supervisor callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Func: init(Args) -> {ok,  {SupFlags,  [ChildSpec]}} |
%%                     ignore                          |
%%                     {error, Reason}
%% Description: Whenever a supervisor is started using 
%% supervisor:start_link/[2,3], this function is called by the new process 
%% to find out about restart strategy, maximum restart frequency and child 
%% specifications.
%%--------------------------------------------------------------------
init([]) ->
    Logic = {psql_logic, {psql_logic, start_link, []},
	      permanent, 2000, worker, [psql_logic]},
    {ok, {{simple_one_for_one, 10, 60}, [Logic]}}.

%%====================================================================
%% Internal functions
%%====================================================================
    
