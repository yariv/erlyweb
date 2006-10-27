%% @title erlyweb
%% @author Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com)
%%
%% @doc This is the main file for ErlyWeb: The Erlang Twist on Web
%%   Frameworks. For more information, visit http://erlyweb.com.
%%
%% @license For license information see LICENSE.txt

-module(erlyweb).
-author("Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com)").

-export([
	 create_app/2,
	 create_component/2,
	 compile/1,
	 compile/2,
	 out/1
	]).

-import(erlyweb_util, [log/5]).

-include_lib("kernel/include/file.hrl").

-define(DEFAULT_RECOMPILE_INTERVAL, 30).
-define(SHUTDOWN_WAIT_PERIOD, 5000).

-define(Debug(Msg, Params), log(?MODULE, ?LINE, debug, Msg, Params)).
-define(Info(Msg, Params), log(?MODULE, ?LINE, info, Msg, Params)).
-define(Error(Msg, Params), log(?MODULE, ?LINE, error, Msg, Params)).

-define(L(Msg), io:format("~b ~p~n", [?LINE, Msg])).

%% @doc Create a new ErlyWeb application in Dir.
create_app(AppName, Dir) ->
    erlyweb_util:create_app(AppName, Dir).

%% @doc Create all the files for a new component -- 
%%  controller, view and model (although the model and the component
%%  don't necessarily have to be tied to each other) -- in the
%%  application's directory.
create_component(Component, Dir) ->    
    erlyweb_util:create_component(Component, Dir).

%% @doc Compile all the files for an application. Files with the '.et'
%%   extension are compiled with ErlTL.
%%
%%   This function returns {ok, Now}, where Now is the result of calling
%%   calendar:local_time(). You can pass this value as a parameter to
%%   your next call of compile/3, telling ErlyWeb to avoid recompiling
%%   files that haven't changed.
compile(DocRoot) ->
    compile(DocRoot, []).
    
%% @doc Compile all the files for an application using the compilation
%%  options as described in the 'compile' module in the Erlang
%%  documentation. 
compile(DocRoot, Options) ->
    compile(DocRoot, Options, {{1980,1,1},{0,0,0}}).

%% @doc Compile an application using the compilation options while avoiding
%%   the recompilation of files that haven't changed since LastCompileTime.
compile(DocRoot, Options, LastCompileTime) ->
    DocRoot1 = case lists:reverse(DocRoot) of
		   [$/ | _] -> DocRoot;
		   Other -> lists:reverse([$/ | Other])
	       end,

    {Options1, OutDir1} =
	case lists:keysearch(outdir, 1, Options) of
	    {value, {outdir, OutDir}} -> {Options, OutDir};
	    false -> OutDir = DocRoot1 ++ "ebin",
		     {[{outdir, OutDir}], OutDir}
	end,
    file:make_dir(OutDir1),

    AppName = filename:basename(DocRoot),
    AppData = get_app_data_module(AppName),
    InitialAcc =
	case catch AppData:components() of
	    {'EXIT', {undef, _}} -> {gb_trees:empty(), 1, []};
	    LastControllers -> {LastControllers, 0, []}
	end,

    LastCompileTimeInSeconds =
	calendar:datetime_to_gregorian_seconds(LastCompileTime),

    
    {ComponentTree1, TotalControllerCompiles, Models} =
	filelib:fold_files(
	  DocRoot1 ++ "src", "\.(erl|et)$", true,
	  fun(FileName, Acc) ->
		  compile_component_file(
		    DocRoot1 ++ "src/components", FileName,
		    LastCompileTimeInSeconds, Options1, Acc)
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
					 Options);
		     false -> {error, missing_erlydb_driver_option}
		 end
	end,
    
    Result = 
	if ErlyDBResult == ok ->
		AppDataAbsModule = make_app_data_module(
				     AppData, AppName,
				     ComponentTree1,
				     TotalControllerCompiles > 0),
		smerl:compile(AppDataAbsModule);
	   true -> ErlyDBResult
	end,

    if Result == ok ->
	    {ok, calendar:local_time()};
       true -> Result
    end.

make_app_data_module(AppData, AppName,
		     ComponentTree, ControllersDidChange) ->
    M1 = smerl:new(AppData),
    M3 = 
	if ControllersDidChange ->
		{ok, M2} =
		    smerl:add_func(
		      M1,
		      {function,1,components,0,
		       [{clause,1,[],[],
			 [erl_parse:abstract(ComponentTree)]}]}),
		M2;
	   true -> M1
	end,
    {ok, M4} = smerl:add_func(
		 M3, "get_view() -> " ++ AppName ++ "_app_view."),
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
    M6.


compile_component_file(ComponentsDir, FileName, LastCompileTimeInSeconds,
		       Options, {ComponentTree, NumControllerCompiles,
				 Models} = Acc) ->
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
	     NumControllerCompiles + 1, Models};
	{{ok, _Module}, model} ->
	    {ComponentTree, NumControllerCompiles, [FileName | Models]};
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
			    compile_file(FileName, BaseName, Type)
		    end;
	       true -> 
		    ?Debug("Skipping compilation of ~p", [BaseName]),
		    ok
	    end;
	{error, _} = Err1 ->
	    Err1
    end.

compile_file(FileName, BaseName, Type) ->
    case smerl:for_file(FileName) of
	{ok, M1} ->
	    M2 = add_forms(Type, BaseName, M1),
	    case smerl:compile(M2) of
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
    case smerl:get_attribute(M1, erlyweb_magic) of
	{ok, on} ->
	    {ModelNameStr, _} = lists:split(length(BaseName) - 11, BaseName),
	    ModelName = list_to_atom(ModelNameStr),
	    M2 = smerl:extend(erlyweb_controller, M1, 1),
	    smerl:embed_all(M2, [{'Model', ModelName}]);
	_ -> M1
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
out(A) ->
    AppName = erlyweb_util:get_appname(A),
    AppData = get_app_data_module(AppName),

    case catch AppData:get_controller() of
	{'EXIT', {undef, _}} ->
	    exit({no_application_data,
		  "Did you forget to call erlyweb:compile(DocRoot)?"});
	 AppController ->
	     app_controller_hook(AppController, A, AppData)
     end.

app_controller_hook(AppController, A, AppData) ->
    Res = case catch AppController:hook(A) of
	      {'EXIT', {undef, [{AppController, hook, _} | _]}} ->
		  {ewc, A};
	      {'EXIT', Err} -> exit(Err);
	      Other -> Other
	end,

    Ewc = get_initial_ewc(Res, AppData),
    Output = ewc(Ewc, AppData),
    Output1 = tag_output(render(Output, AppData:get_view())),
    Output1.


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
    Views = AppData:get_views(),
    {value, View} = gb_trees:lookup(Controller, Views),

    case apply(Controller, FuncName, Params) of
	{data, Data} -> render(View:FuncName(Data), View);
	{ewr, FuncName1} ->
	    {redirect_local,
	     {any_path,
	      atom_to_list(FuncName1)}};
	{ewr, Component1, FuncName1} ->
	    AppRoot = erlyweb_util:get_app_root(A),
	    {redirect_local,
	     {any_path,
	      filename:join([AppRoot, atom_to_list(Component1),
			     atom_to_list(FuncName1)])}};
	{ewr, Component1, FuncName1, Params1} ->
	    Params2 = [erlydb_base:field_to_iolist(Param) ||
			  Param <- Params1],
	    AppRoot = erlyweb_util:get_app_root(A),
	    {redirect_local,
	     {any_path,
	      filename:join(
		filename:join([AppRoot, atom_to_list(Component1),
			       atom_to_list(FuncName1)]),
	       Params2)}}; 
	Ewc ->
	    Output = ewc(Ewc, AppData),
	    render(Output, View)
    end;

ewc(Other, _AppData) -> Other.

render(Output, _View) when is_tuple(Output) -> Output;
render(Output, View) -> try_func(View, render, Output).


try_func(Module, FuncName, Param) ->
    case catch Module:FuncName(Param) of
	{'EXIT', {undef, [{Module, FuncName, _} | _]}} -> Param;	    
	{'EXIT', Err} -> exit(Err);
	Val -> Val
    end.

get_ewc({ewc, A}, AppData) ->
    Tokens = string:tokens(yaws_arg:appmoddata(A), "/"),
    case Tokens of
	[] -> {page, "/www/index.html"};
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
	  %% redirect it to www/[request]
	  [_ | Url] = yaws_arg:appmoddata(A),
	  {page, "/www/" ++ Url};
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

