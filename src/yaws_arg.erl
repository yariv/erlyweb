%% @title yaws_arg
%% @author Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com)
%%
%% @doc This module provides functions for getting and setting
%%   values of the Yaws 'arg' record. You can use these functions
%%   instead of using the record access syntax, and without
%%   having to include yaws_api.hrl.
%%
%% @license for license information see LICENSE.txt

-module(yaws_arg).
-author("Yariv Sadan (yarivsblog@gmail.com)").

-compile(export_all).

-include("yaws/include/yaws_api.hrl").

new() ->
    #arg{}.

clisock(Arg) ->
    Arg#arg.clisock.

clisock(Arg, Val) ->
    Arg#arg{clisock = Val}.

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
