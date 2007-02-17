%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com)]
%%
%% @doc This module contains utility functions for ErlyWeb.

%% For license information see LICENSE.txt

-module(erlyweb_util).
-author("Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com").
-export([log/5, create_app/2, create_component/2, get_appname/1,
	 get_app_root/1, validate/3, get_cookie/2, indexify/2]).

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
	    lists:foreach(
	      fun(SubDir) ->
		      NewDir = Dir ++ "/" ++ SubDir,
		      ?Info("creating ~p", [NewDir]),
		      case file:make_dir(NewDir) of
			  ok ->
			      ok;
			  Err ->
			      exit(Err)
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
	    ?Error("%p isn't a directory", [Dir]),
	    exit({invalid_directory, Dir})
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

%% @hidden
get_appname(A) ->
    erlyweb:get_app_name(A).

%% @hidden
get_app_root(A)->
    erlyweb:get_app_root(A).


%% @doc This function helps with form validation. It takes a Yaws arg
%% (or the arg's POST data in the form of a name-value property list), a
%% list of parameter names to validate, and a validation function, and returns
%% a tuple of the form {Values, Errors}.
%% 'Values' contains the list of values for the checked parameters
%% and Erros is a list of errors returned from the validation function.
%% If no validation errors occured, this list is empty.
%%
%% If the name of a field is missing from the arg's POST data, this function
%% calls exit({missing_param, Name}).
%%
%% The validation function takes two parameters: the parameter name and
%% its value, and it may return one of the following values:
%%
%% - `ok' means the parameter's value is valid
%%
%% - `{ok, Val}' means the parameter's value is valid, and it also lets you
%%   set the value inserted into 'Values' for this parameter.
%%
%% - `{error, Err}' indicates the parameter didn't validate. Err is inserted
%%   into 'Errors'.
%%
%% - `{error, Err, Val}' indicates the parameter didn't validate. Err is
%%   inserted into 'Errors' and Val is inserted into 'Values' instead of
%%   the parameter's original value.
%%
%% @spec validate(A::arg() | Params::proplist(), Fields::[string()],
%%   Fun::function()) -> {Values::[term()], Errors::[term()]}.
validate(A, Fields, Fun) when is_tuple(A), element(1, A) == arg ->
    validate(yaws_api:parse_post(A), Fields, Fun);
validate(Params, Fields, Fun) ->
    lists:foldl(
      fun(Field, {Vals, Errs}) ->
	      case proplists:lookup(Field, Params) of
		  none -> exit({missing_param, Field});
		  {_, Val} ->
		      Val1 = case Val of undefined -> ""; _ -> Val end,
		      case Fun(Field, Val1) of
			  ok ->
			      {[Val1 | Vals], Errs};
			  {ok, Val2} ->
			      {[Val2 | Vals], Errs};
			  {error, Err, Val2} ->
			      {[Val2 | Vals], [Err | Errs]};
			  {error, Err} ->
			      {[Val1 | Vals], [Err | Errs]}
		      end
	      end
      end, {[], []}, lists:reverse(Fields)).


%% @doc Get the cookie's value from the arg.
%% @equiv yaws_api:find_cookie_val(Name, yaws_headers:cookie(A)).
%%
%% @spec get_cookie(Name::string(), A::arg()) -> string()
get_cookie(Name, A) ->
    yaws_api:find_cookie_val(
      Name, yaws_headers:cookie(A)).

%% @doc Translate requests such as '/foo/bar' to '/foo/index/bar' for the given
%% list of components. This function is useful for rewriting the Arg in the
%% app controller prior to handling incoming requests.
%%
%% @spec indexify(A::arg(), ComponentNames::[string()]) -> arg()
indexify(A, ComponentNames) ->
    Str = indexify1(yaws_arg:appmoddata(A), ComponentNames),
    yaws_arg:appmoddata(A, Str).

indexify1(Str, []) -> Str;
indexify1(Str, [Prefix | Others]) ->
    case indexify2(Str, [$/ | Prefix]) of
	stop ->
	    Str;
	{stop, Postfix} ->
	    [$/ | Prefix] ++ "/index" ++ Postfix;
	next ->
	    indexify1(Str, Others)
    end.

indexify2([], []) -> stop;
indexify2([$/ | _] = Postfix, []) -> {stop, Postfix};
indexify2([C1 | Rest1], [C1 | Rest2]) ->
    indexify2(Rest1, Rest2);
indexify2(_, _) -> next.
