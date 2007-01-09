%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com]
%% @copyright Yariv Sadan 2006
%%
%% @doc ErlyWeb: The Erlang Twist on Web Framworks.
%%
%% This module contains a few functions for creating and using ErlyWeb
%% applications and components. It is also the module set as the YAWS
%% appmod for ErlyWeb applications.

%% For license information see LICENSE.txt

-module(erlyweb).
-author("Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com)").

-export([
	 create_app/2,
	 create_component/2,
	 compile/1,
	 compile/2,
	 out/1,
	 get_app_name/1,
	 get_app_root/1
	]).

-import(erlyweb_util, [log/5]).

-include_lib("kernel/include/file.hrl").

-define(DEFAULT_RECOMPILE_INTERVAL, 30).
-define(SHUTDOWN_WAIT_PERIOD, 5000).

-define(Debug(Msg, Params), log(?MODULE, ?LINE, debug, Msg, Params)).
-define(Info(Msg, Params), log(?MODULE, ?LINE, info, Msg, Params)).
-define(Error(Msg, Params), log(?MODULE, ?LINE, error, Msg, Params)).

-define(L(Msg), io:format("~b ~p~n", [?LINE, Msg])).

%% @doc Create a new ErlyWeb application in the directory AppDir.
%% This function creates the standard ErlyWeb directory structure as well as
%% a few basic files for a rudimantary application.
%%
%% @spec create_app(AppName::string(), AppDir::string()) -> ok | {error, Err}
create_app(AppName, AppDir) ->
    case catch erlyweb_util:create_app(AppName, AppDir) of
	{'EXIT', Err} ->
	    {error, Err};
	Other -> Other
    end.

%% @doc Create all the files (model, view and controller) for a component
%%  that implements basic CRUD features for a database table.
%%  'Component' is the name of the component and 'AppDir' is the application's
%%  root directory.
%%
%% To disable the build-in CRUD features, remove the '-erlyweb_magic(on).'
%% lines in the view and the model.
%%
%% @spec create_component(Component::atom(), AppDir::string()) ->
%%   ok | {error, Err}
create_component(Component, AppDir) ->    
    case catch erlyweb_util:create_component(Component, AppDir) of
	{'EXIT', Err} ->
	    {error, Err};
	Other -> Other
    end.

%% @doc Compile all the files for an application. Files with the '.et'
%%   extension are compiled with ErlTL.
%%
%%   This function returns {ok, Now}, where Now is
%%   the result of calendar:local_time().
%%   You can pass the second value in the options for the next call to
%%   compile/2 to telling ErlyWeb to avoid recompiling files that haven't
%%   changed (ErlyWeb does this automatically when the auto-compilation
%%   is turned on).
%%
%% @spec compile(DocRoot::string()) -> ok | {error, Err}
compile(DocRoot) ->
    compile(DocRoot, []).

%% @doc Compile all the files for an application using the compilation
%%  options as described in the 'compile' module in the Erlang
%%  documentation ([http://erlang.org]).
%%  ErlyWeb also lets you define the following options:
%%
%%  - {last_compile_time, LocalTime}: Tells ErlyWeb to not compile files
%%    that haven't changed since LocalTime.
%%
%%  - {erlydb_driver, Name}: Tells ErlyWeb which ErlyDB driver to use
%%    when calling erlydb:code_gen on models that are placed in src/components.
%%    If you aren't using ErlyDB, i.e., you don't have any model files in
%%    src/components, you can omit this option.
%%
%%  - {auto_compile, Val}, where Val is 'true', or 'false'.
%%    This option tells ErlyWeb whether it should turn on auto-compilation.
%%    Auto-compilation is helpful during development because it spares you
%%    from having to call erlyweb:compile every time you make a code change
%%    to your app. Just remember to turn this option off when you are in
%%    production mode because it will slow your app down (to turn auto_compile
%%    off, just call erlyweb:compile without the auto_compile option).
%%
%% - 'suppress_warnings' and 'suppress_errors' tell ErlyWeb to not pass the
%%   'report_warnings' and 'report_errors' to compile:file/2.
%% 
%% @spec compile(AppDir::string(), Options::[option()]) ->
%%  {ok, Now::datetime()} | {error, Err}
compile(AppDir, Options) ->
    AppDir1 = case lists:reverse(AppDir) of
		   [$/ | _] -> AppDir;
		   Other -> lists:reverse([$/ | Other])
	       end,
    
    Options1 = case lists:member(suppress_warnings, Options) of
		   true -> Options;
		   false -> [report_warnings | Options]
	       end,
    Options2 = case lists:member(suppress_errors, Options1) of
		   true -> Options1;
		   false -> [report_errors | Options1]
	       end,
    
    {Options3, OutDir} =
	get_option(outdir, AppDir1 ++ "ebin", Options2),

    file:make_dir(OutDir),

    AppName = filename:basename(AppDir),
    AppData = get_app_data_module(AppName),
    InitialAcc =
	case catch AppData:components() of
	    {'EXIT', {undef, _}} -> {gb_trees:empty(), []};
	    LastControllers -> {LastControllers, []}
	end,

    {Options4, LastCompileTime} =
	get_option(last_compile_time, {{1980,1,1},{0,0,0}}, Options3),
    LastCompileTime1 = case LastCompileTime of
			   {{1980,1,1},{0,0,0}} -> undefined;
			   OtherTime -> OtherTime
		       end,
    
    LastCompileTimeInSeconds =
	calendar:datetime_to_gregorian_seconds(LastCompileTime),

    AppControllerStr = AppName ++ "_app_controller",
    AppControllerFile = AppControllerStr ++ ".erl",
    case compile_file(AppDir ++ "/src/" ++ AppControllerFile,
		      AppControllerStr, ".erl", undefined,
		      LastCompileTimeInSeconds, Options4) of
	{ok, _} -> ok;
	ok -> ok;
	Err -> ?Error("Error compiling app controller", []),
	       exit(Err)
    end,

    AppController = list_to_atom(AppControllerStr),
    ?Debug("Trying to invoke ~s:before_compile/1", [AppControllerStr]),
    try_func(AppController, before_compile, [LastCompileTime1], ok),
    
    {ComponentTree1, Models} =
	filelib:fold_files(
	  AppDir1 ++ "src", "\.(erl|et)$", true,
	  fun(FileName, Acc) ->
		  compile_component_file(
		    AppDir1 ++ "src/components", FileName,
		    LastCompileTimeInSeconds, Options4, Acc)
	  end, InitialAcc),

    ErlyDBResult =
	case Models of
	    [] -> ok;
	    _ -> ?Debug("Generating ErlyDB code for models: ~p",
			[lists:flatten(
			  [[filename:basename(Model), " "] ||
			      Model <- Models])]),
		 case lists:keysearch(erlydb_driver, 1, Options) of
		     {value, {erlydb_driver, Driver}} ->
			 erlydb:code_gen(Driver, lists:reverse(Models),
					 Options4);
		     false -> {error, missing_erlydb_driver_option}
		 end
	end,
    
    Result = 
	if ErlyDBResult == ok ->
		AppDataModule = make_app_data_module(
				     AppData, AppName,
				     ComponentTree1,
				     Options4),
		smerl:compile(AppDataModule, Options4);
	   true -> ErlyDBResult
	end,

    if Result == ok ->
	    ?Debug("Trying to invoke ~s:after_compile/1",
		   [AppControllerStr]),
	    try_func(AppController, after_compile, [LastCompileTime1],
		     ok),
	    {ok, calendar:local_time()};
       true -> Result
    end.

get_option(Name, Default, Options) ->
    case lists:keysearch(Name, 1, Options) of
	{value, {_Name, Val}} -> {Options, Val};
	false -> {[{Name, Default} | Options], Default}
    end.

make_app_data_module(AppData, AppName, ComponentTree, Options) ->
    M1 = smerl:new(AppData),
    {ok, M2} =
	smerl:add_func(
	  M1,
	  {function,1,components,0,
	   [{clause,1,[],[],
	     [erl_parse:abstract(ComponentTree)]}]}),
    M2,
    {ok, M4} = smerl:add_func(
		 M2, "get_view() -> " ++ AppName ++ "_app_view."),
    {ok, M5} = smerl:add_func(
		 M4, "get_controller() -> " ++
		 AppName ++ "_app_controller."),
    
    ViewsTree = lists:foldl(
		  fun(Component, Acc) ->
			  gb_trees:enter(
			    list_to_atom(Component ++ "_controller"),
			    list_to_atom(
			      Component ++ "_view"),
			    Acc)
		  end, gb_trees:empty(),
		  gb_trees:keys(ComponentTree)),

    {ok, M6} = smerl:add_func(
		 M5, {function,1,get_views,0,
		      [{clause,1,[],[],
			[erl_parse:abstract(ViewsTree)]}]}),

    {_Options1, AutoCompile} =
	get_option(auto_compile, false, Options),

    LastCompileTimeOpt = {last_compile_time, calendar:local_time()},

    AutoCompileVal =
	case AutoCompile of
	    false -> false;
	    true -> {true, [LastCompileTimeOpt | Options]};
	    {true, Options} when is_list(Options) ->
		Options1 = lists:keydelete(last_compile_time, 1, Options),
		Options2 = [LastCompileTimeOpt | Options1],
		{true, Options2}
	end,

    {ok, M7} =
	smerl:add_func(
	  M6, {function,1,auto_compile,0,
	       [{clause,1,[],[],
		 [erl_parse:abstract(AutoCompileVal)]}]}),
    M7.


compile_component_file(ComponentsDir, FileName, LastCompileTimeInSeconds,
		       Options, {ComponentTree, Models} = Acc) ->
    BaseName = filename:rootname(filename:basename(FileName)),
    Extension = filename:extension(FileName),
    BaseNameTokens = string:tokens(BaseName, "_"),
       
    Type = case lists:prefix(ComponentsDir, FileName) of
	       true ->
		   case lists:last(BaseNameTokens) of
		       "controller" -> controller;
		       "view" -> view;
		       _ -> model
		   end;
	       false ->
		   other
	   end,
    case {compile_file(FileName, BaseName, Extension, Type,
		       LastCompileTimeInSeconds, Options),
	  Type} of
	{{ok, Module}, controller} ->
	    %% Convert atom to strings so they can be compared
	    %% against values from incoming requests
	    %% (see out/1 for more details).
	    [{exports, Exports} | _] =
		Module:module_info(),
	    Exports1 =
		lists:map(
		  fun({Name, Arity}) ->
			  {atom_to_list(Name), Arity, Name}
		  end, Exports),
	    {ActionName, _} = lists:split(length(BaseName) - 11, BaseName),
	    {gb_trees:enter(
	       ActionName, {list_to_atom(BaseName), Exports1}, ComponentTree),
	     Models};
	{{ok, _Module}, model} ->
	    {ComponentTree, [FileName | Models]};
	{{ok, _Module}, _} -> Acc;
	{ok, _} -> Acc;
	{Err, _} -> exit(Err)
    end.


compile_file(FileName, BaseName, Extension, Type,
	     LastCompileTimeInSeconds, Options) ->
    case file:read_file_info(FileName) of
	{ok, FileInfo} ->
	    ModifyTime =
		calendar:datetime_to_gregorian_seconds(
		  FileInfo#file_info.mtime),
	    if ModifyTime > LastCompileTimeInSeconds ->
		    case Extension of
			".et" ->
			    ?Debug("Compiling ErlTL file ~p", [BaseName]),
			    erltl:compile(FileName, Options);
			".erl" ->
			    ?Debug("Compiling Erlang file ~p", [BaseName]),
			    compile_file(FileName, BaseName, Type, Options)
		    end;
	       true -> 
%		    ?Debug("Skipping compilation of ~p", [BaseName]),
		    ok
	    end;
	{error, _} = Err1 ->
	    Err1
    end.

compile_file(FileName, BaseName, Type, Options) ->
    case smerl:for_file(FileName) of
	{ok, M1} ->
	    M2 = add_forms(Type, BaseName, M1),
	    case smerl:compile(M2, Options) of
		ok ->
		    {ok, smerl:get_module(M2)};
		Err ->
		    Err
	    end;
	Err ->
	    Err
    end.

add_forms(controller, BaseName, MetaMod) ->
    M1 = add_func(MetaMod, private, 0, "private() -> false."),
    M2 = add_func(M1, before_return, 3, "before_return(_FuncName, _Params, Response) -> Response."),
    case smerl:get_attribute(M2, erlyweb_magic) of
	{ok, on} ->
	    {ModelNameStr, _} = lists:split(length(BaseName) - 11, BaseName),
	    ModelName = list_to_atom(ModelNameStr),
	    M3 = smerl:extend(erlyweb_controller, M2, 1),
	    smerl:embed_all(M3, [{'Model', ModelName}]);
	_ -> M2
    end;
add_forms(view, _BaseName, MetaMod) ->
    case smerl:get_attribute(MetaMod, erlyweb_magic) of
	{ok, on} ->
	    smerl:extend(erlyweb_view, MetaMod);
	_ -> MetaMod
    end;
add_forms(_, _BaseName, MetaMod) ->
     MetaMod.

add_func(MetaMod, Name, Arity, Str) ->
    case smerl:get_func(MetaMod, Name, Arity) of
	{ok, _} ->
	    MetaMod;
	{error, _} ->
	    {ok, M1} = smerl:add_func(
			 MetaMod, Str),
	    M1
    end.

%% @doc This is the 'out' function that Yaws calls when passing
%%  HTTP requests to the ErlyWeb appmod.
%%
%% @spec out(A::yaws_arg()) -> ret_val()
out(A) ->
    AppName = get_app_name(A),
    AppData = get_app_data_module(AppName),
    case catch AppData:get_controller() of
	{'EXIT', {undef, _}} ->
	    exit({no_application_data,
		  "Did you forget to call erlyweb:compile(AppDir) or "
		  "add the app's previously compiled .beam files to the "
		  "Erlang code path?"});
	AppController ->
	    case AppData:auto_compile() of
		false -> ok;
		{true, Options} ->
		    AppDir = yaws_arg:docroot(A),
		    AppDir1 = case lists:last(AppDir) of
				  '/' ->
				      filename:dirname(
					filename:dirname(AppDir));
				  _ -> filename:dirname(AppDir)
			      end,
		    case compile(AppDir1, Options) of
			{ok, _} -> ok;
			Err -> exit(Err)
		    end
	    end,
	    app_controller_hook(AppController, A, AppData)
     end.

app_controller_hook(AppController, A, AppData) ->
    HookRes = try_func(AppController, hook, [A], {ewc, A}),
    Ewc = get_initial_ewc(HookRes, AppData),
    Response = ewc(Ewc, AppData),
    process_response(Response, AppData).

process_response({response, Elems}, AppData) ->
    {_Config1, Output1} =
	lists:foldl(
	  fun({app_view, _Val} = Elem, {Config, Output}) ->
		  {[Elem | Config], Output};
	     ({app_view_param, _Val} = Elem, {Config, Output}) ->
		  {[Elem | Config], Output};
	     ({rendered, Rendered}, {Config, Output}) ->
		  AppView =
		      case proplists:lookup(app_view, Config) of
			  none -> AppData:get_view();
			  {app_view, undefined} -> undefined;
			  {app_view, default} -> AppData:get_view();
			  {app_view, Other} -> Other
		      end,
		  Rendered1 =
		      case AppView of
			  undefined -> Rendered;
			  _ ->
			      case proplists:get_value(app_view_param,
						       Config) of
				  undefined ->
				      render(Rendered, AppView);
				  AppViewParam ->
				      render([ewc(AppViewParam,
						  AppView),
					      Rendered], AppView)
			      end
		      end,
		  {Config, [tag_output(Rendered1) | Output]};
	     (Val, {Config, Output}) when is_list(Val) ->
		  {Config, lists:reverse(Val) ++ Output};
	     (Val, {Config, Output}) ->
		  {Config, [Val | Output]}
	  end, {[], []}, Elems),
    lists:reverse(Output1);
process_response(Other, _AppData) ->
    Other.

get_initial_ewc({ewc, _A} = Ewc, AppData) ->
    case get_ewc(Ewc, AppData) of
	{controller, Controller, _FuncName, _Params} = Ewc1 ->
	    case Controller:private() of
		true -> exit({illegal_request, Controller});
		false -> Ewc1
	    end;
	Other -> Other
    end;
get_initial_ewc(Ewc, _AppData) -> Ewc.
	    
ewc(Ewc, AppData) when is_list(Ewc) ->
    [ewc(Child, AppData) || Child <- Ewc];


ewc({data, Data}, _AppData) -> Data;

ewc({ewc, A}, AppData) ->
    Ewc = get_ewc({ewc, A}, AppData),
    ewc(Ewc, AppData);

ewc({ewc, Component, Params}, AppData) ->
    ewc({ewc, Component, index, Params}, AppData);

ewc({ewc, Component, FuncName, Params}, AppData) ->
    Result =
	case gb_trees:lookup(atom_to_list(Component),
			     AppData:components()) of
	    {value, {Controller, _Exports}} ->
		{controller, Controller, FuncName, Params};
	    none ->
		exit({no_such_component, Component})
	end,
    ewc(Result, AppData);

ewc({controller, Controller, FuncName, [A | _] = Params}, AppData) ->
    Response = apply(Controller, FuncName, Params),
    case ewr(A, Response) of
	{redirect_local, _} = Res -> Res;
	_ ->
	    Response1 = Controller:before_return(FuncName, Params, Response),
	    Views = AppData:get_views(),
	    {value, View} = gb_trees:lookup(Controller, Views),
	    render_response(A, Response1, View, FuncName, AppData)
    end;

ewc(Other, _AppData) -> Other.

ewr(A, {ewr, Component}) -> ewr2(A, [Component]);
ewr(A, {ewr, Component, FuncName}) -> ewr2(A, [Component, FuncName]);
ewr(A, {ewr, Component, FuncName, Params}) ->
	    Params1 = [erlydb_base:field_to_iolist(Param) ||
			  Param <- Params],
	    ewr2(A, [Component, FuncName | Params1]);
ewr(_A, Other) -> Other.

ewr2(A, PathElems) ->
    Strs = [if is_atom(Elem) -> atom_to_list(Elem);
	       true -> Elem
	    end || Elem <- PathElems],
    AppDir = get_app_root(A),
    {redirect_local,
     {any_path,
      filename:join([AppDir | Strs])}}.

render_response(A, {response, Elems}, View, FuncName, AppData) ->
    {_Config2, Elems2} =
	lists:foldl(
	  fun({view_param, _Val} = Elem, {Config1, Elems1}) ->
		  {[Elem | Config1], Elems1};
	     ({body, BodyEwc}, {Config1, Elems1}) ->
		  RenderFun =
		      fun(Output) ->
			      case proplists:get_value(view_param, Config1) of
				  undefined ->
				      render(Output, View);
				  ViewParam ->
				      render([ewc(ViewParam, AppData),
					      Output], View)
			      end
		      end,
		  Rendered = render_ewc(BodyEwc, View, FuncName, AppData,
				       RenderFun),
		  {Config1, [{rendered, Rendered} | Elems1]};
	     (Elem, {Config1, Elems1}) ->
		  {Config1, [ewr(A, Elem) | Elems1]}
	  end, {[], []}, Elems),
    {response, lists:reverse(Elems2)};
render_response(_A, Ewc, View, FuncName, AppData) ->
    Output1 = render_ewc(Ewc, View, FuncName, AppData),
    {response, [{rendered, Output1}]}.

render_ewc(Ewc, View, FuncName, AppData) ->
    render_ewc(Ewc, View, FuncName, AppData, undefined).

render_ewc(Ewc, View, FuncName, AppData, RenderFun) ->
    %% in nested components, we ignore all response elements except for
    %% 'body' and 'view_param'
    Output = case ewc(Ewc, AppData) of
		 {response, Elems} ->
		     proplists:get_value(rendered, Elems);
		 Other -> Other
	     end,
    case catch View:FuncName(Output) of
	{'EXIT', {undef, [{View, FuncName, _} | _]}} -> Output;
	{'EXIT', Err} -> exit(Err);
	Output1 -> case RenderFun of
		       undefined-> render(Output1, View);
		       _ -> RenderFun(Output1)
		   end
    end.


render(Output, _View) when is_tuple(Output) -> Output;
render(Output, View) -> try_func(View, render, [Output], Output).


try_func(Module, FuncName, Params, Default) ->
    case catch apply(Module, FuncName, Params) of
	{'EXIT', {undef, [{Module, FuncName, _} | _]}} -> Default;
	{'EXIT', Err} -> exit(Err);
	Val -> Val
    end.

get_ewc({ewc, A}, AppData) ->
    Tokens = string:tokens(yaws_arg:appmoddata(A), "/"),
    case Tokens of
	[] -> {page, "/"};
	[ComponentStr]->
	    get_ewc(ComponentStr, "index", [A],
		    AppData);
	[ComponentStr, FuncStr | Params] ->
	    get_ewc(ComponentStr, FuncStr, [A | Params],
		    AppData)
    end.

get_ewc(ComponentStr, FuncStr, [A | _] = Params,
		      AppData) ->
  Controllers = AppData:components(),
  case gb_trees:lookup(ComponentStr, Controllers) of
      none ->
	  %% if the request doesn't match a controller's name,
	  %% redirect it to /path
	  Path = case yaws_arg:appmoddata(A) of
		     [$/ | _ ] = P -> P;
		     Other -> [$/ | Other]
		 end,
	  {page, Path};
      {value, {Controller, Exports}} ->
	  Arity = length(Params),
	  get_ewc1(Controller, FuncStr, Arity,
			  Params, Exports)
  end.


get_ewc1(Controller, FuncStr, Arity, _Params, []) ->
    exit({no_such_function,
	  {Controller, FuncStr, Arity,
	   "You tried to invoke a controller function that doesn't exist or "
	   "that isn't exported"}});
get_ewc1(Controller, FuncStr, Arity, Params,
	  [{FuncStr1, Arity1, FuncName} | _Rest]) when FuncStr1 == FuncStr,
						       Arity1 == Arity->
    {controller, Controller, FuncName, Params};
get_ewc1(Controller, FuncStr, Arity, Params, [_First | Rest]) ->
    get_ewc1(Controller, FuncStr, Arity, Params, Rest).

%% Helps sending Yaws a properly tagged output when the output comes from
%% an ErlTL template. ErlyWeb currently only supports ehtml in up to one
%% level of nesting, i.e. the following work: {ehtml, Output} and
%% [{ehtml, Output1}, ...], but not this: [[{ehtml, Output}, ...], ...]
tag_output(Output) when is_tuple(Output) -> Output;
tag_output(Output) when is_binary(Output) -> {html, Output};
tag_output(Output) when is_list(Output) ->
    [tag_output1(Elem) || Elem <- Output];
tag_output(Output) -> Output.

tag_output1(Output) when is_tuple(Output) -> Output;
tag_output1(Output) -> {html, Output}.

get_app_data_module(AppName) when is_atom(AppName) ->
    get_app_data_module(atom_to_list(AppName));
get_app_data_module(AppName) ->
    list_to_atom(AppName ++ "_erlyweb_data").


%% @doc Get the name for the application as specified in the opaque 
%% 'appname' field in the YAWS configuration.
%%
%% @spec get_app_name(A::arg()) -> AppName::string() | exit(Err)
get_app_name(A) ->
    case lists:keysearch("appname", 1, yaws_arg:opaque(A)) of
	{value, {_, AppName}} ->
	    AppName;
	false ->
	    exit({missing_appname,
		  "Did you forget to add the 'appname = [name]' "
		  "to the <opaque> directive in yaws.conf?"})
    end.

%% @doc Get the relative URL for the application's root path.
%%
%% @spec get_app_root(A::arg()) -> string()
get_app_root(A) ->
    ServerPath = yaws_arg:server_path(A),
    {First, _Rest} =
	lists:split(
	  length(ServerPath) -
	  length(yaws_arg:appmoddata(A)),
	  ServerPath),
    First.
