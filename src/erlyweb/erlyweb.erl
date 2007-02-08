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
	 get_initial_ewc/2,
	 get_ewc/2,
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

    IncludePaths =
	lists:foldl(
	  fun({i, [$/ | _] = Path}, Acc) ->
		  [Path | Acc];
	     ({i, Path}, Acc) ->    
		  [AppDir1 ++ "src/" ++ Path | Acc];
	     (_Opt, Acc) ->
		  Acc
	  end, [], Options),
    
    Options1 = set_default_option(report_warnings, suppress_warnings, Options),
    Options2 = set_default_option(report_errors, suppress_errors, Options1),
    Options3 = set_default_option(debug_info, no_debug_info, Options2),

    {Options4, OutDir} =
	get_option(outdir, AppDir1 ++ "ebin", Options3),

    file:make_dir(OutDir),

    AppName = filename:basename(AppDir),
    AppData = get_app_data_module(AppName),
    InitialAcc =
	case catch AppData:components() of
	    {'EXIT', {undef, _}} -> {gb_trees:empty(), []};
	    LastControllers -> {LastControllers, []}
	end,

    {Options5, LastCompileTime} =
	get_option(last_compile_time, {{1980,1,1},{0,0,0}}, Options4),
    LastCompileTime1 = case LastCompileTime of
			   {{1980,1,1},{0,0,0}} -> undefined;
			   OtherTime -> OtherTime
		       end,
    
    LastCompileTimeInSeconds =
	calendar:datetime_to_gregorian_seconds(LastCompileTime),

    AppControllerStr = AppName ++ "_app_controller",
    AppControllerFile = AppControllerStr ++ ".erl",
    AppControllerFilePath = AppDir1 ++ "src/" ++ AppControllerFile,
    case compile_file(AppControllerFilePath,
		      AppControllerStr, ".erl", undefined,
		      LastCompileTimeInSeconds, Options5, IncludePaths) of
	{ok, _} -> ok;
	ok -> ok;
	Err -> ?Error("Error compiling app controller", []),
	       exit(Err)
    end,

    AppController = list_to_atom(AppControllerStr),
    try_func(AppController, before_compile, [LastCompileTime1], ok),
    
    {ComponentTree1, Models} =
	filelib:fold_files(
	  AppDir1 ++ "src", "\.(erl|et)$", true,
	  fun(FileName, Acc) ->
		  if FileName =/= AppControllerFilePath ->
			  compile_component_file(
			    AppDir1 ++
			    "src/components", FileName,
			    LastCompileTimeInSeconds, Options5, IncludePaths,
			    Acc);
		     true ->
			  Acc
		  end
	  end, InitialAcc),

    ErlyDBResult =
	case Models of
	    [] -> ok;
	    _ -> ?Debug("Generating ErlyDB code for models: ~p",
			[lists:flatten(
			  [[filename:basename(Model), " "] ||
			      Model <- Models])]),
		 case lists:keysearch(erlydb_driver, 1, Options5) of
		     {value, {erlydb_driver, Driver}} ->
			 erlydb:code_gen(Driver, lists:reverse(Models),
					 Options5);
		     false -> {error, missing_erlydb_driver_option}
		 end
	end,
    
    Result = 
	if ErlyDBResult == ok ->
		AppDataModule = make_app_data_module(
				     AppData, AppName,
				     ComponentTree1,
				     Options5),
		smerl:compile(AppDataModule, Options5);
	   true -> ErlyDBResult
	end,

    if Result == ok ->
	    try_func(AppController, after_compile, [LastCompileTime1],
		     ok),
	    {ok, calendar:local_time()};
       true -> Result
    end.

set_default_option(Option, Override, Options) ->
    case lists:member(Override, Options) of
	true -> Options;
	false -> [Option | lists:delete(Option,
					Options)]
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
    
    {ok, M4} = smerl:add_func(
		 M2, "get_view() -> " ++ AppName ++ "_app_view."),
    {ok, M5} = smerl:add_func(
		 M4, "get_controller() -> " ++
		 AppName ++ "_app_controller."),

    AbsFunc =
	make_get_component_function(ComponentTree),

    {ok, M6} = smerl:add_func(
		 M5, AbsFunc),
    
    {_Options1, AutoCompile} =
	get_option(auto_compile, false, Options),

    LastCompileTimeOpt = {last_compile_time, calendar:local_time()},

    AutoCompileVal =
	case AutoCompile of
	    false -> false;
	    true -> 
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

%% This function generates the abstract form for the
%% AppData:get_component/3 function.
%%
%% This function's signature is:
%% get_component(ComponentName::string() | atom(), FuncName::string() | atom(),
%%  Params::list()) ->
%%    {ok, {component, Controller::atom(), View::atom(), Params::list()}} |
%%    {error, no_such_component} |
%%    {error, no_such_function}
make_get_component_function(ComponentTree) ->
    Clauses1 =
	lists:foldl(
	  fun(ComponentStr, Acc) ->
		  Exports = gb_trees:get(ComponentStr, ComponentTree),
		  Clauses = make_clauses_for_component(ComponentStr, Exports),
		  Clauses ++ Acc
	  end, [], gb_trees:keys(ComponentTree)),
    Clauses2 = [{clause,1,
		 [{var,1,'_'},
		  {var,1,'_'},
		  {var,1,'_'}],
		 [],
		 [{tuple,1,
		   [{atom,1,error},
		    {atom,1,no_such_component}]}]} | Clauses1],
    %exit(lists:reverse(Clauses2)),
    {function,1,get_component,3,lists:reverse(Clauses2)}.


%% This function generates the abstract form for the AppData:get_component/3
%% function clauses that apply to a the given component.
make_clauses_for_component(ComponentStr, Exports) ->
    Clauses =
	lists:foldl(
	  fun({Func, Arity}, Acc) ->
		  Guards =
		      [[{op,1,'==',
			 {call,1,{atom,1,length},
			  [{var,1,'Params'}]},
			 {integer,1,Arity}}]],
		  Body = 
		      [{tuple,1,
			[{atom,1,ok},
			 {tuple,1,
			  [{atom,1,ewc},
			   {atom,1,
			    list_to_atom(ComponentStr ++
					 "_controller")},
			   {atom,1,
			    list_to_atom(ComponentStr ++ "_view")},
			   {atom,1, Func},
			   {var,1,'Params'}]}]}],

		  Clause1 = 
		      {clause,1,
		       [{string,1,ComponentStr},
			{string,1,atom_to_list(Func)},
			{var,1,'Params'}],
		       Guards, Body},
		  Clause2 = 
		      {clause,1,
		       [{atom,1,list_to_atom(ComponentStr)},
			{atom,1,Func},
			{var,1,'Params'}],
		       Guards, Body},
		  [Clause1, Clause2 | Acc]
	  end, [], Exports),
    LastClause = 
	{clause,1,
	 [{string,1,ComponentStr},
	  {var,1,'_'},
	  {var,1,'_'}],
	 [],
	 [{tuple,1,
	   [{atom,1,error},
	    {atom,1,no_such_function}]}]},
    [LastClause | Clauses].



compile_component_file(ComponentsDir, FileName, LastCompileTimeInSeconds,
		       Options, IncludePaths, {ComponentTree, Models} = Acc) ->
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
		       LastCompileTimeInSeconds, Options, IncludePaths),
	  Type} of
	{{ok, Module}, controller} ->
	    [{exports, Exports} | _] =
		Module:module_info(),
	    Exports1 =
		lists:foldl(
		  fun({before_return, _}, Acc1) ->
			  Acc1;
		     ({module_info, _}, Acc1) ->
			  Acc1;
		     ({_, 0}, Acc1) ->
			  Acc1;
		     ({Name, Arity}, Acc1) ->
			  [{Name, Arity} | Acc1]
		  end, [], Exports),
	    {ActionName, _} = lists:split(length(BaseName) - 11, BaseName),
	    {gb_trees:enter(
	       ActionName, Exports1, ComponentTree),
	     Models};
	{{ok, _Module}, model} ->
	    {ComponentTree, [FileName | Models]};
	{{ok, _Module}, _} -> Acc;
	{ok, _} -> Acc;
	{Err, _} -> exit(Err)
    end.


compile_file(FileName, BaseName, Extension, Type,
	     LastCompileTimeInSeconds, Options, IncludePaths) ->
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
			    compile_file(FileName, BaseName, Type, Options,
					 IncludePaths)
		    end;
	       true -> 
		    ok
	    end;
	{error, _} = Err1 ->
	    Err1
    end.

compile_file(FileName, BaseName, Type, Options, IncludePaths) ->
    case smerl:for_file(FileName, IncludePaths) of
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
    M2 = add_func(M1, before_return, 3,
		  "before_return(_FuncName, _Params, Response) -> Response."),
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
    HookRes = AppController:hook(A),
    Ewc = get_initial_ewc(HookRes, AppData),
    Response = ewc(Ewc, AppData),
    process_response(Response, AppData).


%% @doc Get the expanded ewc tuple for the request.
%%
%% This function is similar to {@link get_ewc/2} but it crashes
%% if the Arg represents a request for a private component
%% (i.e. if its controller has the function 'private() -> true.').
%% @see get_ewc/2.
get_initial_ewc({ewc, _A} = Ewc, AppData) ->
    case get_ewc(Ewc, AppData) of
	{ewc, Controller, _View, _FuncName, _Params} = Ewc1 ->
	    case Controller:private() of
		true -> exit({illegal_request, Controller});
		false -> Ewc1
	    end;
	Other -> Other
    end;
get_initial_ewc(List, AppData) when is_list(List) ->
    [get_initial_ewc(Ewc, AppData) || Ewc <- List];
get_initial_ewc(Ewc, _AppData) -> Ewc.
	    
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
				      Param = get_rendered(ewc(AppViewParam,
							       AppData)),
				      render({Param,
					      Rendered}, AppView)
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

ewc(List, AppData) when is_list(List) ->
    {AllRendered, AllOthers} =
	lists:foldl(
	  fun(Ewc, {RenderedAcc, ElemAcc}) ->
		  case ewc(Ewc, AppData) of
		      {response, Elems} ->
			  lists:foldl(
			    fun({rendered, Rendered},
				{RenderedAcc1, ElemAcc1}) ->
				    {[Rendered | RenderedAcc1], ElemAcc1};
			       (Elem, {RenderedAcc1, ElemAcc1}) ->
				    {RenderedAcc1, [Elem | ElemAcc1]}
			    end, {RenderedAcc, ElemAcc}, Elems);
		      Other ->
			  {RenderedAcc, [Other | ElemAcc]}
		  end
	  end, {[], []}, List),
    case AllRendered of
	[] ->
	    {response, AllOthers};
	_ ->
	    {response, AllOthers ++ [{rendered, lists:reverse(AllRendered)}]}
    end;

ewc({data, Data}, _AppData) -> {response, [{rendered, Data}]};

ewc({ewc, A}, AppData) ->
    Ewc = get_ewc({ewc, A}, AppData),
    ewc(Ewc, AppData);

ewc({ewc, Component, Params}, AppData) ->
    ewc({ewc, Component, index, Params}, AppData);

ewc({ewc, Component, FuncName, Params}, AppData) ->
    case AppData:get_component(Component, FuncName, Params) of
	{error, no_such_component} ->
	    exit({no_such_component, Component});
	{error, no_such_function} ->
	    exit({no_such_function, Component, FuncName, length(Params)});
	{ok, Ewc} ->
	    ewc(Ewc, AppData)
    end;

ewc({ewc, Controller, View, FuncName, [A | _] = Params}, AppData) ->
    Response = apply(Controller, FuncName, Params),
    case ewr(A, Response) of
	{redirect_local, _} = Res -> Res;
	_ ->
	    Response1 = Controller:before_return(FuncName, Params, Response),
	    render_response(A, Response1, View, FuncName, AppData)
    end;

ewc(Other, _AppData) -> Other.

ewr(A, ewr) -> ewr2(A, []);
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
				      render({get_rendered(
						ewc(ViewParam, AppData)),
					      Output}, View)
			      end
		      end,
		  Elems2 = render_ewc(BodyEwc, View, FuncName, AppData,
					RenderFun, Elems1),
		  {Config1, Elems2};
	     (Elem, {Config1, Elems1}) ->
		  {Config1, [ewr(A, Elem) | Elems1]}
	  end, {[], []}, Elems),
    {response, lists:reverse(Elems2)};
render_response(_A, Ewc, View, FuncName, AppData) ->
    Elems = render_ewc(Ewc, View, FuncName, AppData,
			 fun(Output) ->
				 render(Output, View)
			 end, []),
    {response, lists:reverse(Elems)}.

render_ewc(Ewc, View, FuncName, AppData, RenderFun, Acc) ->
    case ewc(Ewc, AppData) of
	{response, Elems1} ->
	    lists:foldl(
	      fun({rendered, Rendered}, Elems2) ->
		      Output = RenderFun(View:FuncName(Rendered)),
		      [{rendered, Output} | Elems2];
		 (Elem, Elems2) ->
		      [Elem | Elems2]
	      end, Acc, Elems1);
	Other ->
	    [Other | Acc]
    end.
    
render(Output, View) -> View:render(Output).

get_rendered({response, Elems}) ->
    proplists:get_value(rendered, Elems);
get_rendered(Resp) ->
    Resp.

try_func(Module, FuncName, Params, Default) ->
    case catch apply(Module, FuncName, Params) of
	{'EXIT', {undef, [{Module, FuncName, _} | _]}} -> Default;
	{'EXIT', Err} -> exit(Err);
	Val -> Val
    end.

%% @doc Get the expanded ewc tuple for the request.
%%
%% The second parameter is the name of the application's erlyweb data
%% module, i.e. [AppName]_erlyweb_data.
%%
%% @spec get_ewc({ewc, A::arg()}, AppDataModule::atom()) ->
%%   {page, Path::string()} |
%%   {ewc, Controller::atom(), View::atom(), Function::atom(),
%%     Params::[string()]} |
%%   exit({no_such_function, Err})
get_ewc({ewc, A}, AppData) ->
    case string:tokens(yaws_arg:appmoddata(A), "/") of
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
    case AppData:get_component(ComponentStr, FuncStr, Params) of
	{error, no_such_component} ->
	    %% if the request doesn't match a controller's name,
	    %% redirect it to /path
	    Path = case yaws_arg:appmoddata(A) of
		       [$/ | _ ] = P -> P;
		       Other -> [$/ | Other]
		   end,
	    {page, Path};
	{error, no_such_function} ->
	    exit({no_such_function,
		  {ComponentStr, FuncStr, length(Params),
		   "You tried to invoke a controller function that doesn't "
		   "exist or that isn't exported"}});
	{ok, Component} ->
	    Component
    end.

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
