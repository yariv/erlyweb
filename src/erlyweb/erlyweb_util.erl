%%
%% @doc This module contains a few utility functions useful
%% for ErlyWeb apps.
%%
%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com)]
%% @copyright Yariv Sadan 2006-2007


%% For license information see LICENSE.txt

-module(erlyweb_util).
-author("Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com").
-export([log/5, create_app/2, create_component/3,
	 get_url_prefix/1,
	 get_cookie/2, indexify/2]).

-define(Debug(Msg, Params), log(?MODULE, ?LINE, debug, Msg, Params)).
-define(Info(Msg, Params), log(?MODULE, ?LINE, info, Msg, Params)).
-define(Error(Msg, Params), log(?MODULE, ?LINE, error, Msg, Params)).

%% @hidden
log(Module, Line, Level, Msg, Params) ->
    io:format("~p:~p:~p: " ++ Msg, [Level, Module, Line] ++ Params),
    io:format("~n").

%% @hidden
create_app(AppName, Dir) ->
    case filelib:is_dir(Dir) of
	true ->
	    AppDir = Dir ++ "/" ++ AppName,
	    Dirs =
		[SrcDir, ComponentsDir, WebDir, _EbinDir]
		= [AppDir ++ "/src",
		   AppDir ++ "/src/components",
		   AppDir ++ "/www",
		   AppDir ++ "/ebin"],
	    lists:foreach(
	      fun(SubDir) ->
		      ?Info("creating ~p", [SubDir]),
		      case file:make_dir(SubDir) of
			  ok ->
			      ok;
			  Err ->
			      exit(Err)
		      end
	      end, [AppDir | Dirs]),

	    Files =
		[{ComponentsDir ++ "/html_container_view.et",
		  html_container_view(AppName)},
		 {ComponentsDir ++ "/html_container_controller.erl",
		  html_container_controller()},
		 {SrcDir ++ "/" ++ AppName ++ "_app_controller.erl",
		  app_controller(AppName)},
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
	    ?Error("~p isn't a directory", [Dir]),
	    exit({invalid_directory, Dir})
    end.

create_file(FileName, Bin) ->
    ?Info("creating ~p", [FileName]),
    case file:open(FileName, [read]) of
	{ok, File} ->
	    file:close(File),
	    exit({already_exists, FileName});
	_ ->
	    case file:write_file(FileName, Bin) of
		ok ->
		    ok;
		Err ->
		    exit({Err, FileName})
	    end
    end.
	
app_controller(AppName) ->
    Text =
	["-module(", AppName, "_app_controller).\n"
	 "-export([hook/1]).\n\n"
	 "hook(A) ->\n"
	 "\t{phased, {ewc, A},\n"
	 "\t\tfun(_Ewc, Data) ->\n"
	 "\t\t\t{ewc, html_container, index, [A, {data, Data}]}\n"
	 "\t\tend}."],
    iolist_to_binary(Text).

html_container_controller() ->
    Text =
	["-module(html_container_controller).\n"
	 "-export([private/0, index/2]).\n\n"
	 "private() ->\n"
	 "\ttrue.\n\n"
	 "index(_A, Ewc) ->\n"
	 "\tEwc."],
    iolist_to_binary(Text).

html_container_view(AppName) ->
    Text =
	["<%@ index(Data) %>\n"
	 "<html>\n"
	 "<head>\n"
	 "<title>", AppName, "</title>\n"
	 "<link rel=\"stylesheet\" href=\"/style.css\""
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
	 "<link rel=\"stylesheet\" href=\"/style.css\">\n"
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

%% @hidden
create_component(ComponentName, AppDir, Magic) ->
    MagicStr = if Magic == on ->
		       "erlyweb";
		  true ->
		       if is_atom(Magic) ->
			       atom_to_list(Magic);
			  true ->
			       Magic
		       end
	       end,
    Files = 
	[{ComponentName ++ ".erl",
	  "-module(" ++ ComponentName ++ ")."},
	 {ComponentName ++ "_controller.erl",
	  "-module(" ++ ComponentName ++ "_controller).\n"
	  "-erlyweb_magic(" ++ MagicStr ++"_controller)."},
	 {ComponentName ++ "_view.erl",
	  "-module(" ++ ComponentName ++ "_view).\n"
	  "-erlyweb_magic(" ++ MagicStr ++ "_view)."}],
    lists:foreach(
      fun({FileName, Text}) ->
	      create_file(AppDir ++ "/src/components/" ++ FileName, Text)
      end, Files).

%% @doc Get the  of the arg's appmoddata value up to the
%% first '?' symbol.
%%
%% @spec get_url_prefix(A::arg()) -> string()
get_url_prefix(A) ->
    lists:dropwhile(
      fun($?) -> true;
	 (_) -> false
      end, yaws_arg:appmoddata(A)).


%% @doc Get the cookie's value from the arg.
%% @equiv yaws_api:find_cookie_val(Name, yaws_headers:cookie(A))
%%
%% @spec get_cookie(Name::string(), A::arg()) -> string()
get_cookie(Name, A) ->
    yaws_api:find_cookie_val(
      Name, yaws_headers:cookie(A)).

%% @doc Translate requests such as '/foo/bar' to '/foo/index/bar' for the given
%% list of components. This function is useful for rewriting the Arg in the
%% app controller prior to handling incoming requests.
%%
%% @deprecated This function is deprecated. Implement catch_all/3 in your
%% controllers instead.
%%
%% @spec indexify(A::arg(), ComponentNames::[string()]) -> arg()
indexify(A, ComponentNames) ->
    Appmod = yaws_arg:appmoddata(A),
    Sp = yaws_arg:server_path(A),

    Appmod1 = indexify1(Appmod, ComponentNames),
    A1 = yaws_arg:appmoddata(A, Appmod1),

    {SpRoot, _} = lists:split(length(Sp) - length(Appmod), Sp),
    yaws_arg:server_path(A1, SpRoot ++ Appmod1).
     

indexify1(S1, []) -> S1;
indexify1(S1, [Prefix | Others]) ->
    case indexify2(S1, [$/ | Prefix]) of
	stop -> S1;
	{stop, Postfix} ->
	    [$/ | Prefix] ++ "/index" ++ Postfix;
	next ->
	    indexify1(S1, Others)
    end.

indexify2([], []) -> stop;
indexify2([$/ | _] = Postfix, []) -> {stop, Postfix};
indexify2([C1 | Rest1], [C1 | Rest2]) ->
    indexify2(Rest1, Rest2);
indexify2(_, _) -> next.
