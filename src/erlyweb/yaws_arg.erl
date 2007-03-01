%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com]
%% @copyright Yariv Sadan 2006-2007
%%
%% @doc
%% This module provides functions for getting and setting
%% values of a Yaws 'arg' record. You can use these functions
%% instead of using the record access syntax, and without
%% having to include yaws_api.hrl.
%%
%% Most functions have two forms: one for getting the value of a field and
%% one for setting it. Getters accept the record as a parameter and return
%% the value of its field. Setters take
%% two parameters -- the record and the new value -- and return a new record
%% with the modified value.
%%
%% @end

%% For license information see LICENSE.txt

-module(yaws_arg).
-author("Yariv Sadan (yarivsblog@gmail.com)").

-export([new/0, clisock/1, clisock/2, client_ip_port/1, client_ip_port/2,
	 headers/1, headers/2, req/1, req/2,
	 method/1, clidata/1, clidata/2, server_path/1, server_path/2,
	 querydata/1, querydata/2, appmoddata/1, appmoddata/2, docroot/1,
	 docroot/2, fullpath/1, fullpath/2, cont/1, cont/2, state/1,
	 state/2, pid/1, pid/2, opaque/1, opaque/2, appmod_prepath/1,
	 appmod_prepath/2, pathinfo/1, pathinfo/2]).
-include("yaws_api.hrl").

%% @doc Create a new 'arg' record.
new() ->
    #arg{}.

clisock(Arg) ->
    Arg#arg.clisock.

clisock(Arg, Val) ->
    Arg#arg{clisock = Val}.

client_ip_port(Arg) ->
    Arg#arg.client_ip_port.

client_ip_port(Arg, Val) ->
    Arg#arg{client_ip_port = Val}.

headers(Arg) ->
    Arg#arg.headers.

headers(Arg, Val) ->
    Arg#arg{headers = Val}.

req(Arg) ->
    Arg#arg.req.

req(Arg, Val) ->
    Arg#arg{req = Val}.

method(Arg) ->
    (Arg#arg.req)#http_request.method.

clidata(Arg) ->
    Arg#arg.clidata.

clidata(Arg, Val) ->
    Arg#arg{clidata = Val}.

server_path(Arg) ->
    Arg#arg.server_path.

server_path(Arg, Val) ->
    Arg#arg{server_path = Val}.		  
    
querydata(Arg) ->
    Arg#arg.querydata.

querydata(Arg, Val) ->
    Arg#arg{querydata = Val}.

appmoddata(Arg) ->
    Arg#arg.appmoddata.

appmoddata(Arg, Val) ->
    Arg#arg{appmoddata = Val}.

docroot(Arg) ->
    Arg#arg.docroot.

docroot(Arg, Val) ->
    Arg#arg{docroot = Val}.

fullpath(Arg) ->
    Arg#arg.fullpath.

fullpath(Arg, Val) ->
    Arg#arg{fullpath = Val}.

cont(Arg) ->
    Arg#arg.cont.

cont(Arg, Val) ->
    Arg#arg{cont = Val}.

state(Arg) ->
    Arg#arg.state.

state(Arg, Val) ->
    Arg#arg{state = Val}.

pid(Arg) ->
    Arg#arg.pid.

pid(Arg, Val) ->
    Arg#arg{pid = Val}.

opaque(Arg) ->
    Arg#arg.opaque.

opaque(Arg, Val) ->
    Arg#arg{opaque = Val}.

appmod_prepath(Arg) ->
    Arg#arg.appmod_prepath.

appmod_prepath(Arg, Val) ->
    Arg#arg{appmod_prepath = Val}.

pathinfo(Arg) ->
    Arg#arg.pathinfo.

pathinfo(Arg, Val) ->
    Arg#arg{pathinfo = Val}.
