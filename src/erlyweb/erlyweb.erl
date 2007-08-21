%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com]
%% @copyright Yariv Sadan 2006-2007
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
	 create_component/3,
	 compile/1,
	 compile/2,
	 out/1,
	 get_initial_ewc/1,
	 get_ewc/1,
	 get_app_name/1,
	 get_app_root/1
	]).

-import(erlyweb_util, [log/5]).

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

%% @equiv create_component(Component, AppDir, on)
create_component(Component, AppDir) ->
    create_component(Component, AppDir, on).

%% @doc Create all the files (model, view and controller) for a component
%%  that implements basic CRUD features for a database table.
%%  'Component' is the name of the component and 'AppDir' is the application's
%%  root directory.
%%
%% The view and controller source files this function creates have the line
%% "-erlyweb_magic(on)."
%% This tells ErlyWeb that the module in which the line appears should extend
%% the erlyweb_view or erlyweb_controller modules, which provide basic CRUD
%% capabilities.
%%
%% You can override the default base module setting by replace 'on' with
%% the name of the module you want to extend. You can also disable the
%% build-in CRUD features by removing the '-erlyweb_magic(on).' lines from
%% the view and/or controller source files.
%%
%% @spec create_component(Component::atom(), AppDir::string(),
%%    Magic::on | atom() | string()) ->
%%   ok | {error, Err}
create_component(Component, AppDir, Magic) ->
    case catch erlyweb_util:create_component(Component, AppDir, Magic) of
	{'EXIT', Err} ->
	    {error, Err};
	Other -> Other
    end.

%% @doc Compile all the files for an application. Files with the '.et'
%%   extension are compiled with ErlTL.
%%
%%   This function returns `{ok, Now}', where Now is
%%   the result of `calendar:local_time()'.
%%   You can pass the second value in the options for the next call to
%%   compile/2 to telling ErlyWeb to avoid recompiling files that haven't
%%   changed (ErlyWeb does this automatically when the auto-compilation
%%   is turned on).
%%
%% @spec compile(AooDir::string()) -> ok | {error, Err}
compile(AppDir) ->
    compile(AppDir, []).

%% @doc Compile all the files for an application using the compilation
%%  options as described in the 'compile' module in the Erlang
%%  documentation ([http://erlang.org]).
%%  ErlyWeb also lets you define the following options:
%%
%%  - `{last_compile_time, LocalTime}': Tells ErlyWeb to not compile files
%%    that haven't changed since LocalTime.
%%
%%  - `{erlydb_driver, Name}': Tells ErlyWeb which ErlyDB driver to use
%%    when calling erlydb:code_gen on models that are placed in src/components.
%%    If you aren't using ErlyDB, i.e., you don't have any model files in
%%    src/components, you can omit this option.
%%
%%  - `{auto_compile, Val}', where Val is 'true', or 'false'.
%%    This option tells ErlyWeb whether it should turn on auto-compilation.
%%    Auto-compilation is helpful during development because it spares you
%%    from having to call erlyweb:compile every time you make a code change
%%    to your app. Just remember to turn this option off when you are in
%%    production mode because it will slow your app down (to turn auto_compile
%%    off, just call erlyweb:compile without the auto_compile option).
%%
%% -  `{auto_compile_exclude, Val}', where Val is a folder in docroot (or a
%%    URL path) which gets excluded from auto-compiling. This is useful when
%%    erlyweb is mapped to '/' and there are a lots of static files in that 
%%    excluded folder. Example: {auto_compile_exclude, "/static"} 
%%
%% - `suppress_warnings' and `suppress_errors' tell ErlyWeb to not pass the
%%   `report_warnings' and `report_errors' to compile:file/2.
%%
%% In addition, Options may include the option values available for
%% for the ErlyDB driver you're using. For example, the MySQL driver
%% accepts the option {pool_id, PoolId}, which indicates which connection
%% pool the MySQL dispatcher should use for querying database metadata.
%% For more information, refer to your ErlyDB driver's documentation.
%% 
%% @spec compile(AppDir::string(), Options::[option()]) ->
%%  {ok, Now::datetime()} | {error, Err}
compile(AppDir, Options) ->
    erlyweb_compile:compile(AppDir, Options).


%% @doc This is the out/1 function that Yaws calls when passing
%%  HTTP requests to the ErlyWeb appmod.
%%
%% @spec out(A::yaws_arg()) -> ret_val()
out(A) ->
    AppName = get_app_name(A),
    AppData = erlyweb_compile:get_app_data_module(AppName),
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
		    case lists:keysearch(auto_compile_exclude, 1, Options) of
			{value, {_, Val}} -> 
			    case string:str(yaws_arg:appmoddata(A), Val) of
				1 -> ok;
				_ -> auto_compile(AppData, Options)
			    end;
			false -> auto_compile(AppData, Options)
		    end
 	    end,
	    A1 = yaws_arg:opaque(A,
  				 [{app_data_module, AppData} |
				  yaws_arg:opaque(A)]),
 	    handle_request(A1,
			   AppController, AppController:hook(A1),
			   AppData)
    end.

auto_compile(AppData, Options) ->
    AppDir = AppData:get_app_dir(),
    case compile(AppDir, Options) of
        {ok, _} -> ok;
        Err -> exit(Err)
    end.

handle_request(A,
	       AppController,
	       {phased, Ewc, Func}, AppData) ->
    {Ewc1, Rest} = get_initial_ewc1(Ewc, AppData),
    handle_request(
      A,
      AppController,
      Ewc1, Rest, AppData,
      fun(Data) ->
	      DataEwc = Func(Ewc1, Data),
	      render_subcomponent(DataEwc, AppData)
      end);
handle_request(A,
	       AppController,
	       Ewc, AppData) ->
    {Ewc1, Rest} = get_initial_ewc1(Ewc, AppData),
    handle_request(
      A,
      AppController,
      Ewc1, Rest, AppData,
      fun(Data) ->
	      Data
      end).

handle_request(A, AppController, Ewc, Rest, AppData, DataFun) -> 
    case catch ewc(Ewc, AppData) of
	{response, Elems} -> 
 	    Rest ++ lists:map(
		      fun({rendered, Data}) -> 
			      {html, DataFun(Data)};
			 ({rendered, MimeType, Data}) -> 
			      {content, MimeType, DataFun(Data)};
			 (Header) ->
			      Header
		      end, Elems);
	{'EXIT', _} = Err ->
	    case catch AppController:error(A, Ewc, Err) of
		{'EXIT', _} ->
		    Err;
		Other ->
		    handle_request(A,
				   AppController,
				   Other,
				   AppData)
	    end
    end.

%% @doc Get the expanded 'ewc' tuple for the request.
%%
%% This function can be useful in the app controller in case the
%% application requires special
%% logic for handling client requests for different components.
%%
%% If the request is for a component whose controller implements the function
%% `private() -> true.', this function calls
%%  `exit({illegal_request, Controller})'.
%%
%% If the request matches an existing component but no function in the
%% component's controller, and the controller exports `catch_all/3',
%% this function returns
%% `{ewc, Controller, View, catch_all, [A, Function, Params]}'.
%%
%% Otherwise, if the request matches a component but no function in the
%% component's controller, this function calls
%% `exit({no_such_function, Err})'.
%%
%% If the request doesn't match any components, this function returns
%% `{page, Path}', where Path is the arg's appmoddata field.
%%
%% If the parameter isn't in the form `{ewc, A}', this function returns
%% the parameter unchanged without any extra processing.
%%
%% @spec get_initial_ewc({ewc, A::arg()}) ->
%%   {page, Path::string()} |
%%   {ewc, Controller::atom(), View::atom(), Function::atom(),
%%     Params::[string()]} |
%%   exit({no_such_function, Err}) |
%%   exit({illegal_request, Controller})
%% @see handle_request/1
get_initial_ewc(Ewc) ->
    element(1, get_initial_ewc1(Ewc)).
get_initial_ewc(Ewc, AppData) ->
    element(1, get_initial_ewc1(Ewc, AppData)).

get_initial_ewc1({ewc, A} = Ewc) ->
    AppData = lookup_app_data_module(A),
    get_initial_ewc1(Ewc, AppData).
get_initial_ewc1({ewc, A}, AppData) ->
    case get_ewc(A, AppData) of
	{ewc, Controller, _View, _FuncName, _Params} = Ewc ->
	    case Controller:private() of
		true -> exit({illegal_request, Controller});
		false -> {Ewc, []}
	    end;
	Ewc -> {Ewc, []}
    end;
get_initial_ewc1({response, Elems} = Resp, AppData) ->
    case lists:partition(
	   fun({body, _}) -> true;
	      (_) -> false
	   end, Elems) of
	{[], Rest} ->
	    {undefined, Rest};
	{[{body, Body}], Rest} ->
	    {element(1, get_initial_ewc(Body, AppData)), Rest};
	{_Bodies, _Rest} ->
	    exit({multiple_response_bodies, Resp})
    end;
get_initial_ewc1(Ewc, _AppData) -> {Ewc, []}.

    

ewc(Ewcs, AppData) when is_list(Ewcs) ->
    Rendered = lists:map(
		 fun(Ewc) ->
			 render_subcomponent(Ewc, AppData)
		 end, Ewcs),
    {response, [{rendered, Rendered}]};

ewc({data, Data}, _AppData) -> {response, [{rendered, Data}]};

ewc({ewc, A}, AppData) ->
    Ewc = get_ewc(A, AppData),
    ewc(Ewc, AppData);

ewc({ewc, Component, Params}, AppData) ->
    ewc({ewc, Component, index, Params}, AppData);

ewc({ewc, Component, FuncName, Params}, AppData) ->
    case AppData:get_component(Component, FuncName, Params) of
	{error, no_such_component} ->
	    exit({no_such_component, Component, FuncName, length(Params)});
	{error, no_such_function} ->
	    exit({no_such_function, {Component, FuncName, length(Params)}});
	{ok, Ewc} ->
	    ewc(Ewc, AppData)
    end;

ewc({ewc, Controller, View, FuncName, [A | _] = Params}, AppData) ->
    {FuncName1, Params1} = Controller:before_call(FuncName, Params),
    Response = apply(Controller, FuncName1, Params1),    
    Response1 = Controller:before_return(FuncName1, Params1, Response),
    Response2 = case Response1 of
		    {response, _} ->
			Response1;
		    {replace, Ewc} ->
			{response, [{replace, Ewc}]};
		    Body ->
			case is_redirect(A, Body) of
			    {true, Val} ->
				{response, [Val]};
			    _ ->
				{response, [{body, Body}]}
			end
		end,
    Response3 = handle_response(A, Response2, View, FuncName1, AppData),
    [_A | ParamsRest] = Params1, 
    case Response3 of
	{response, [{rendered, Val2}]} -> 
	    catch Controller:after_render(FuncName1, ParamsRest, Val2);
	{response, [{rendered, _MimeType, Val3}]} ->
	    catch Controller:after_render(FuncName1, ParamsRest, Val3);
	_ -> done
    end,
    Response3;
ewc(Other, _AppData) ->  {response, [Other]}.


is_redirect(A, Elem) ->
    case ewr(A, Elem) of
	{Redirect, _} = Val when Redirect == redirect;
				 Redirect == redirect_local ->
	    {true, Val};
	_ -> false
    end.

ewr(A, ewr) -> ewr2(A, []);
ewr(A, {ewr, Component}) -> ewr2(A, [Component]);
ewr(A, {ewr, Component, FuncName}) -> ewr2(A, [Component, FuncName]);
ewr(A, {ewr, Component, FuncName, Params}) ->
    Params1 = [erlydb_base:field_to_iolist(Param) ||
		  Param <- Params],
    ewr2(A, [Component, FuncName | Params1]);
ewr(_A, Other) -> Other.

ewr2(A, PathElems) ->
    Elems = [if is_atom(Elem) -> atom_to_list(Elem);
		true -> Elem
	     end || Elem <- PathElems],
    AppDir = get_app_root(A),
    Path = lists:foldr(
	     fun(Elem, []) -> [Elem];
		(Elem, Acc) -> [Elem, $/ | Acc]
	     end, [], [AppDir | Elems]),
    {redirect_local, Path}.

handle_response(A, {response, Elems}, View, FuncName, AppData) ->
    Elems2 = lists:map( 
	       fun({body, Ewc}) ->
		       {rendered, View:FuncName(
				    render_subcomponent(Ewc, AppData))};
		  ({body, MimeType, Ewc}) ->
		       {rendered, MimeType,
			View:FuncName(render_subcomponent(Ewc, AppData))};
		  ({replace, Ewc})  when is_tuple(Ewc), element(1, Ewc) ==
					 'ewc'->
		       {rendered, render_subcomponent(Ewc, AppData)};
		  ({replace, Ewc}) ->
		       exit({expecting_ewc_tuple, Ewc});
		  (Elem) ->
		       ewr(A, Elem)
	       end, Elems),
    {response, Elems2}.

render_subcomponent(Ewc, AppData) ->
    case ewc(Ewc, AppData) of
	{response, [{rendered, Rendered}]} ->
	    Rendered;
	{response, Other} ->
	    exit({invalid_response, Other,
		  "Response values other than 'data' and "
		  "'ewc' tuples must be enclosed a 'response' tuple. "
		  "In addition, subcomponents may only return "
		  "'data' and/or 'ewc' tuples."})
    end.

get_ewc(A) ->
    get_ewc(A, lookup_app_data_module(A)).

get_ewc(A, AppData) ->
    Prefix = erlyweb_util:get_url_prefix(A),
    case string:tokens(Prefix, "/") of
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
	    exit({no_such_function, {ComponentStr, FuncStr, length(Params)}});
	{ok, Component} ->
	    Component
    end.

%% @doc Get the name for the application as specified in the opaque 
%% 'appname' field in the YAWS configuration.
%%
%% @spec get_app_name(A::arg()) -> AppName::string() | exit(Err)
get_app_name(A) ->
    case proplists:get_value("appname", yaws_arg:opaque(A)) of
	undefined ->
	    exit({missing_appname,
		  "Did you forget to add the 'appname = [name]' "
		  "to the <opaque> directive in yaws.conf?"});
	Val ->
	    Val
    end.


%% @doc Get the relative URL for the application's root path.
%% 
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

lookup_app_data_module(A) ->
    proplists:get_value(app_data_module, yaws_arg:opaque(A)).

