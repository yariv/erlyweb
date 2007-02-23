%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com)]
%%
%% @doc This module contains utility functions for ErlyWeb.

%% For license information see LICENSE.txt

-module(erlyweb_util).
-author("Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com").
-export([log/5, create_app/2, create_component/2, get_appname/1,
	 get_app_root/1, validate/3, validate1/3, get_cookie/2, indexify/2,
	 to_recs/2]).

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
    lists:foldr(
      fun(Field, Acc) ->
	      case proplists:lookup(Field, Params) of
		  none -> exit({missing_param, Field});
		  {_, Val} ->
		      check_val(Field, Val, Fun,Acc)
	      end
      end, {[], []}, Fields).

%% @doc validate1/3 is similar to validate/3, but it expects the parameter
%% list to match the field list both in the number of elements and in their
%% order. validate1/3 is more efficient and is also stricter than validate/3.
%% @see validate/3
%%
%% @spec validate1(Params::proplist() | arg(), Fields::[string()],
%% Fun::function() -> {Vals, Errs} | exit({missing_params, [string()]}) |
%% exit({unexpected_params, proplist()}) | exit({unexpected_param, string()})
validate1(A, Fields, Fun) when is_tuple(A), element(1, A) == arg ->
    validate1(yaws_api:parse_post(A), Fields, Fun);
validate1(Params, Fields, Fun) ->
    validate1_1(Params, Fields, Fun, {[], []}).

validate1_1([], [], _Fun, {Vals, Errs}) ->
    {lists:reverse(Vals), lists:reverse(Errs)};
validate1_1([], Fields, _Fun, _Acc) -> exit({missing_params, Fields});
validate1_1(Params, [], _Fun, _Acc) -> exit({unexpected_params, Params});
validate1_1([{Field, Val} | Params], [Field | Fields], Fun, Acc) ->
    Acc1 = check_val(Field, Val, Fun, Acc),
    validate1_1(Params, Fields, Fun, Acc1);
validate1_1([{Param, _} | _Params], [Field | _], _Fun, _Acc) ->
    exit({unexpected_param, Field, Param}).

check_val(Field, Val, Fun, {Vals, Errs}) ->
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
    end.
    
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


%% @doc This function helps process POST requests containing fields that
%% represent multiple models. This function is similar to but more powerful than
%% {@link erlydb_base:set_fields_from_strs/3} because it works with forms
%% containing fields from multiple models. This function expects each field
%% to be mapped to its record by being named with a unique prefix identifying
%% the record.
%%
%% For example, suppose you have to process an HTML form whose fields
%% represent a house and 2 cars. The house's fields have the
%% prefix "house_" and the cars' fields have the prefixes "car1_" and
%% "car2_". The arg's POST parameters are
%% `[{"house_rooms", "3"}, {"car1_year", "2007"}, "car2_year", "2006"]'.
%% With such a setup, calling `to_recs(A, [{house, "house_"}, {car, "car1_"},
%% {car, "car2_"}])'
%% returns the list `[House, Car1, Car2]', where `house:rooms(House) == "3"',
%% `car:year(Car1) == "2007"' and `car:year(Car2) == "2006"', and all other
%% fields are `undefined'.
%%
%% @spec to_recs(A::arg() | [{ParamName::string(), ParamVal::term()}],
%% [{Prefix::string(), Model::atom()}]) -> [record::tuple()]
to_recs(A, ModelDescs) when is_tuple(A), element(1, A) == arg ->
    to_recs(yaws_arg:parse_post(A), ModelDescs);
to_recs(Params, ModelDescs) ->
    Models = 
	[{Prefix, Model, Model:new()} || {Prefix, Model} <- ModelDescs],
    Models1 =
	lists:foldl(
	  fun({Name, Val}, Acc) ->
		  {First, [{Prefix1, Model1, Rec} | Rest]} =
		      lists:splitwith(
			fun({Prefix2, _Module2, _Rec2}) ->
				not lists:prefix(Prefix2, Name)
			end, Acc),
		  {_, FieldName} = lists:split(length(Prefix1), Name),
		  Field = erlydb_field:name(Model1:db_field(FieldName)),
		  First ++ [{Prefix1, Model1, Model1:Field(Rec, Val)} | Rest]
	  end, Models, Params),
    [element(3, Model3) || Model3 <- Models1].
    
	      
