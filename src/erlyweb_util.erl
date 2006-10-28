%% @title erlyweb_util
%% @author Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com)
%%
%% @doc This module contains utility functions for ErlyWeb.
%%
%% @license For license information see LICENSE.txt

-module(erlyweb_util).
-author("Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com").
-compile(export_all).

-define(Debug(Msg, Params), log(?MODULE, ?LINE, debug, Msg, Params)).
-define(Info(Msg, Params), log(?MODULE, ?LINE, info, Msg, Params)).
-define(Error(Msg, Params), log(?MODULE, ?LINE, error, Msg, Params)).

log(Module, Line, Level, Msg, Params) ->
    io:format("~p:~p:~p: " ++ Msg, [Level, Module, Line] ++ Params),
    io:format("~n").


%% Create a new ErlyWeb application in the given directory.
%%
%% @spec create_app(AppName::string(), Dir::string()) -> ok | exit(Err)
create_app(AppName, Dir) ->
    case filelib:is_dir(Dir) of
	true ->
	    lists:foreach(
	      fun(SubDir) ->
		      NewDir = Dir ++ "/" ++ SubDir,
		      ?Info("creating ~p", [NewDir]),
		      case file:make_dir(NewDir) of
			  ok ->
			      ok;
			  Err ->
			      exit({Err, NewDir})
		      end
	      end, [AppName,
		    AppName ++ "/src",
		    AppName ++ "/src/components",
		    AppName ++ "/ebin",
		    AppName ++ "/www"]),

	    SrcDir = Dir ++ "/" ++ AppName ++ "/src",
	    WebDir = Dir ++ "/" ++ AppName ++ "/www",
	    BaseName = SrcDir ++ "/" ++ AppName,
	    Files =
		[{BaseName ++ "_app_view.et",
		  view(AppName)},
		 {BaseName ++ "_app_controller.erl",
		  controller(AppName)},
		 {WebDir ++ "/index.html",
		  index(AppName)},
		 {WebDir ++ "/style.css",
		  css()}],
	    lists:foreach(
	      fun({FileName, Bin}) ->
		      create_file(FileName, Bin)
	      end, Files),
	    ok;
	false ->
	    ?Error("%p isn't a directory", [Dir])
    end.

create_file(FileName, Bin) ->
    ?Info("creating ~p", [FileName]),
    case file:write_file(FileName, Bin) of
	ok ->
	    ok;
	Err ->
	    exit({Err, FileName})
    end.
	
controller(AppName) ->
    Text =
	["-module(", AppName, "_app_controller).\n"
	 "-export([hook/1]).\n\n"
	 "hook(A) ->\n"
	 "\t{ewc, A}."],
    iolist_to_binary(Text).

view(AppName) ->
    Text =
	["<html>\n"
	 "<head>\n"
	 "<title>", AppName, "</title>\n"
	 "<link rel=\"stylesheet\" href=\"/", AppName, "/style.css\""
	 " type=\"text/css\">\n"
	 "</style>\n"
	 "</head>\n"
	 "<body>\n"
	 "<div id=\"content\">\n"
	 "<h1>", AppName, " app</h1>\n"
	 "<% Data %>\n"
	 "<br>\n"
	 "<div width=\"80%\" align=\"right\">\n"
	 "powered by <a href=\"http://erlyweb.org\">ErlyWeb</a>"
	 " / <a href=\"http://yaws.hyber.org\">Yaws</a>\n"
	 "</div>\n"
	 "</div>\n"
	 "</body>\n"
	 "</html>\n"],
    iolist_to_binary(Text).


index(AppName) ->	
    Text =
	["<html>\n"
	 "<head>\n"
	 "<link rel=\"stylesheet\" href=\"/" ++ AppName ++ "/style.css\">\n"
	 "<title>", AppName, "</title>\n</head>\n",
	 "<body>\n"
	 "<div id=\"content\">\n"
	 "Welcome to '", AppName, "', your new "
	 "<a href=\"http://erlyweb.org\">ErlyWeb</a> "
	 "app.<br><br>\n"
	 "Let the <a href=\"http://erlang.org\">Erlang</a> "
	 "hacking begin!\n"
	 "</div>\n",
	 "</body>\n</html>"],
    iolist_to_binary(Text).

css() ->
    Text =
	"body {\n"
	"  font-family: arial, verdana,  helvetica, sans-serif;\n"
	"  color: #353535;\n"
	"  margin:10px 0px; padding:0px;\n"
	"  text-align:center;\n"
	"}\n\n"
	"#Content {\n"
	"  width:80%;\n"
	"  margin:0px auto;\n"
	"  text-align:left;\n"
	"  padding:15px;\n"
	"} \n"
	"H1 {font-size: 1.5em;}",
    iolist_to_binary(Text).

create_component(ComponentName, AppDir) ->
    Files =
	[{ComponentName ++ ".erl",
	  "-module(" ++ ComponentName ++ ")."},
	 {ComponentName ++ "_controller.erl",
	  "-module(" ++ ComponentName ++ "_controller).\n"
	  "-erlyweb_magic(on)."},
	 {ComponentName ++ "_view.erl",
	  "-module(" ++ ComponentName ++ "_view).\n"
	  "-erlyweb_magic(on)."}],
    lists:foreach(
      fun({FileName, Text}) ->
	      create_file(AppDir ++ "/src/components/" ++
			  FileName, iolist_to_binary(Text))
      end, Files).
			 

get_appname(A) ->
    case lists:keysearch("appname", 1, yaws_arg:opaque(A)) of
	{value, {_, AppName}} ->
	    AppName;
	false ->
	    exit({missing_appname,
		  "Did you forget to add the 'appname = [name]' "
		  "to the <opaque> directive in yaws.conf?"})
    end.


get_app_root(A) ->
    ServerPath = yaws_arg:server_path(A),
    {First, _Rest} =
	lists:split(
	  length(ServerPath) -
	  length(yaws_arg:appmoddata(A)),
	  ServerPath),
    First.
