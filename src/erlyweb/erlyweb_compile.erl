%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com]
%% @copyright Yariv Sadan 2006-2007
%% @hidden
%% @doc This file containes the compilation logic for ErlyWeb.

%% For license information see LICENSE.txt
-module(erlyweb_compile).
-export([compile/2, get_app_data_module/1, compile_file/5]).

-include_lib("kernel/include/file.hrl").

-import(erlyweb_util, [log/5]).

-define(Debug(Msg, Params), log(?MODULE, ?LINE, debug, Msg, Params)).
-define(Info(Msg, Params), log(?MODULE, ?LINE, info, Msg, Params)).
-define(Error(Msg, Params), log(?MODULE, ?LINE, error, Msg, Params)).

-define(L(Msg), io:format("~b ~p~n", [?LINE, Msg])).


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
	  end, [AppDir1 ++ "src"], Options),

    Options1 =
	lists:foldl(
	  fun({Opt, NoOpt}, Acc) ->
		  set_default_option(Opt, NoOpt, Acc)
	  end, Options,
	  [{return_errors, no_return_errors},
	   {report_warnings, suppress_warnings},
	   {report_errors, suppress_errors},
	   {debug_info, no_debug_info}]),

    {Options2, OutDir} =
	get_option(outdir, AppDir1 ++ "ebin", Options1),

    file:make_dir(OutDir),

    AppName = filename:basename(AppDir),
    AppData = get_app_data_module(AppName),
    InitialAcc =
	case catch AppData:components() of
	    {'EXIT', {undef, _}} -> {gb_trees:empty(), []};
	    LastControllers -> {LastControllers, []}
	end,

    {Options3, LastCompileTime} =
	get_option(last_compile_time, {{1980,1,1},{0,0,0}}, Options2),
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
		      LastCompileTimeInSeconds, Options3, IncludePaths) of
	{ok, _} -> ok;
	{ok, _, _, _} -> ok;
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
			    LastCompileTimeInSeconds, Options3, IncludePaths,
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
		 case lists:keysearch(erlydb_driver, 1, Options3) of
		     {value, {erlydb_driver, Driver}} ->
			 erlydb:code_gen(Driver, lists:reverse(Models),
					 Options3, IncludePaths);
		     false -> {error, missing_erlydb_driver_option}
		 end
	end,

    Result = 
	if ErlyDBResult == ok ->
		AppDataModule = make_app_data_module(
				  AppDir1,
				  AppData, AppName,
				  ComponentTree1,
				  Options3),
		smerl:compile(AppDataModule, Options3);
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

make_app_data_module(AppDir, AppData, AppName,
		     ComponentTree, Options) ->
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

    {ok, M8} =
	smerl:add_func(
	  M7, "get_app_dir() -> \"" ++ AppDir ++ "\"."),

    M8.

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
		  Body = get_body_for_func(ComponentStr, Func,
					  {var,1,'Params'}),
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
	  end, [], lists:keydelete(catch_all, 1, Exports)),
    addFinalClauses(Clauses, ComponentStr, Exports).

get_body_for_func(ComponentStr, Func, Params) ->
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
	 Params]}]}].


addFinalClauses(Clauses, ComponentStr, Exports) ->
    {LastBody, FuncParam, ParamsParam} =
	case lists:member({catch_all,2}, Exports) of
	    false ->
		{[{tuple,1,
		  [{atom,1,error},
		   {atom,1,no_such_function}]}],
		 '_Func',
		 '_Params'};
	    true ->
		{get_body_for_func(ComponentStr, catch_all,
				   {cons,1,
				     {call,1,{atom,1,hd},[{var,1,'Params'}]},
				     {cons,1,
				      {cons,1,
				       {var,1,'Func'},
				       {call,1,{atom,1,tl},[{var,1,'Params'}]}},
				      {nil,1}}}),
		 'Func',
		 'Params'}
				   
	end,
    LastClauses = 
	[{clause,1,
	  [{string,1,ComponentStr},
	   {var,1,FuncParam},
	   {var,1,ParamsParam}],
	  [],
	  LastBody},
	 {clause,1,
	  [{atom,1,list_to_atom(ComponentStr)},
	   {var,1,FuncParam},
	   {var,1,ParamsParam}],
	  [],
	  LastBody}],
    LastClauses ++ Clauses.


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
		  fun({Name, _}, Acc1)
		     when Name == before_return;
			  Name == before_call;
			  Name == module_info ->
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

compile_file(_FileName, [$. | _] = BaseName, _Extension, _Type,
_LastCompileTimeInSeconds, _Options, _IncludePaths) ->
    ?Debug("Ignoring file ~p", [BaseName]),
    {ok, ignore};
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
			    erltl:compile(FileName,
					  Options ++ [nowarn_unused_vars] ++
					  [{i, P} || P <- IncludePaths]);
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
    M2 = case smerl:get_attribute(MetaMod, erlyweb_magic) of
	     {ok, Val} ->
		 Base = case Val of
			    on -> erlyweb_controller;
			    Other -> Other
			end,
		 {ModelNameStr, _} = lists:split(length(BaseName) - 11,
						 BaseName),
		 ModelName = list_to_atom(ModelNameStr),
		 M1 = smerl:extend(Base, MetaMod, 1),
		 smerl:embed_all(M1, [{'Model', ModelName}]);
	     _ -> MetaMod
	 end,
    M3 = add_func(M2, private, 0, "private() -> false."),
    M4 = add_func(M3, before_call, 2,
		  "before_call(FuncName, Params) -> "
		  "{FuncName, Params}."),
    add_func(M4, before_return, 3,
	     "before_return(_FuncName, _Params, Response) -> "
	     "Response.");
add_forms(view, BaseName, MetaMod) ->
    add_forms1(erlyweb_view, BaseName, MetaMod);
add_forms(_, BaseName, MetaMod) ->
    add_forms1(undefined, BaseName, MetaMod).

add_forms1(SubstitutionMod, BaseName, MetaMod) ->
    case smerl:get_attribute(MetaMod, erlyweb_magic) of
	{ok, Val} ->
	    Base = case Val of
		       on -> if SubstitutionMod == undefined ->
				     exit({invalid_erlyweb_magic_declaration,
					   {in_module, BaseName},
					   "only controllers and views may "
					   "declare '-erlyweb_magic(on).'"});
				true ->
				     SubstitutionMod
			     end;
		       Other -> Other
		   end,
	    smerl:extend(Base, MetaMod);
	_ -> MetaMod
    end.


add_func(MetaMod, Name, Arity, Str) ->
    case smerl:get_func(MetaMod, Name, Arity) of
	{ok, _} ->
	    MetaMod;
	{error, _} ->
	    {ok, M1} = smerl:add_func(
			 MetaMod, Str),
	    M1
    end.

get_app_data_module(AppName) when is_atom(AppName) ->
    get_app_data_module(atom_to_list(AppName));
get_app_data_module(AppName) when is_list(AppName) ->
    list_to_atom(AppName ++ "_erlyweb_data").

try_func(Module, FuncName, Params, Default) ->
    case catch apply(Module, FuncName, Params) of
	{'EXIT', {undef, [{Module, FuncName, _} | _]}} -> Default;
	{'EXIT', Err} -> exit(Err);
	Val -> Val
    end.

