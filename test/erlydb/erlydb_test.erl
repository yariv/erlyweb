%% @title Test code for ErlyDB
%%
%% @author Yariv Sadan (yarivvv@gmail.com, http://yarivsblog.com)

-module(erlydb_test).
-author("Yariv Sadan").
-export(
   [erlydb_mysql_init/0,
    erlydb_mnesia_init/0,
    erlydb_psql_init/0,
    test/0,
    test/1]).

-define(L(Obj), io:format("LOG ~w ~p\n", [?LINE, Obj])).
-define(S(Obj), io:format("LOG ~w ~s\n", [?LINE, Obj])).


erlydb_mysql_init() ->
    %% connect to the database
    erlydb:start(mysql,
		 [{hostname, "localhost"},
		  {username, "root"},
		  {password, "password"},
		  {database, "test"}]).

erlydb_mnesia_init() ->
    erlydb:start(mnesia, []).

erlydb_psql_init() ->
    erlydb_psql:start().


code_gen(Database) ->
    erlydb:code_gen(Database, [language, project, developer, musician, employee, 
                               person, customer, store, item]).

test() ->
    test(mysql).

test(Database) ->
    Driver = list_to_atom("erlydb_" ++ atom_to_list(Database)),
    Init = list_to_atom(atom_to_list(Driver) ++ "_init"),
    erlydb_test:Init(),
    % generate the abstraction layer modules
    code_gen(Database),


    %% clean up old records
    Driver:q({esql, {delete, language}}),
    Driver:q({esql, {delete, project}}),
    Driver:q({esql, {delete, person}}),
    Driver:q({esql, {delete, person_project}}),
    
    %% Create some new records
    Languages = 
	[language:new(<<"Erlang">>,
		      <<"A functional language designed for building "
		      "scalable, fault tolerant systems">>,
		      <<"functional/dynamic/concurrent">>, 1981),
	 language:new_with([{name, <<"Java">>},
		       {description, <<"An OO language from Sun">>},
		       {paradigm, <<"OO/static">>},
		       {creation_year, 1992}]),
	 language:new(<<"Ruby">>,
		      <<"An OO/functional language from Matz">>,
		      <<"OO/script/dynamic">>, 1995)],

    %% Save the records in the database and collect the updated
    %% tuples.
    [Erlang, Java, Ruby] =
	lists:map(
	  fun(Language) ->
		  %% executes an INSERT statement
		  language:save(Language)
	  end, Languages),

    %% demonstrate getters
    <<"Erlang">> = language:name(Erlang),
    <<"functional/dynamic/concurrent">> = language:paradigm(Erlang),
    1981 = language:creation_year(Erlang),

    %% demonstrate setter
    J1 = language:creation_year(Java, 1993),

    %% executes an UPDATE statement
    J2 = language:save(J1),

    1993 = language:creation_year(J2),

    %% Let's run some queries
    E1 = language:find_id(language:id(Erlang)),
    true = E1 == Erlang,


    [E2] = language:find({name, '=', "Erlang"}),
    true = E2 == Erlang,

    E3 = language:find_first({paradigm, like, "functional%"}),
    true = E3 == Erlang,

    [E4, J4, R4] = language:find(undefined, {order_by, id}),
    true =
	E4 == Erlang andalso 
	J4 == J1 andalso 
	R4 == Ruby,

    %% Let's make some projects
    Yaws = project:new(
	     <<"Yaws">>, <<"A web server written in Erlang">>, Erlang),
    Ejabberd = project:new(
		 <<"ejabberd">>, <<"The best Jabber server">>, Ruby),
    OpenPoker = 
	project:new(<<"OpenPoker">>, <<"A scalable poker server">>,
		  Erlang),


    %% We call language:id just to demonstrate that constructors accept
    %% both related tuples or, alternatively, their id's. This example
    %% would behave identically if we used the Java variable directly.
    Tomact = 
	project:new(<<"Tomcat">>, <<"A Java Server">>,
		    language:id(Java)),

    JBoss =
	project:new(<<"JBoss">>, <<"A Java Application Server">>,
			Java),
    Spring =
	project:new(<<"Spring Framework">>,
		    <<"A Java IoC Framework">>, Java),
    

    
    Mongrel =
	project:new(<<"Mongerl">>, <<"A web server">>, Ruby),
    Rails =
	project:new(<<"Ruby on Rails">>,
		    <<"A integrated web development framework.">>, Ruby),
    Ferret = project:new(<<"Ferret">>,
			 <<"A Ruby port of Apache Lucene.">>, Ruby),
    Gruff = project:new(<<"Gruff">>,
			<<"A Ruby library for easy graph generation. ">>,
			Ruby),

    Projects =  [Yaws, Ejabberd, OpenPoker, Tomact, JBoss, Spring,
		 Mongrel, Rails, Ferret, Gruff],

    %% Insert our projects into the database
    [Yaws1, Ejabberd1, OpenPoker1 | _Rest] =
	lists:map(
	  fun(Project) ->
		  project:save(Project)
	  end, Projects),

    %% lets get the language associated with Yaws
    Erlang2 = project:language(Yaws1),

    Erlang2 = Erlang,

    %% now let's correct a grave error
    Ejabberd2 = project:save(project:language(Ejabberd1, Erlang)),
    true = language:id(Erlang) == project:language_id(Ejabberd2),

    %% let's get all the projects for a language
    [Yaws3, Ejabberd3, OpenPoker3] = language:projects_with(Erlang, {order_by, id}),

    true =
	Yaws3 == Yaws1
	andalso Ejabberd3 == Ejabberd2
	andalso OpenPoker3 == OpenPoker1,

    %% fancier project queries
    [Yaws4] = language:projects(
		      Erlang, {name,'=',"Yaws"}),
    Yaws4 = Yaws1,

    Yaws5 = language:projects_first_with(Erlang, {order_by, id}),
    Yaws4 = Yaws5,

    Ejabberd4 = language:projects_first(
			  Erlang, {name, like, "%e%"}, {order_by, id}),
    Ejabberd4 = Ejabberd3,


    %% Let's show some many-to-many features

    %% First, add some more projects
    [OTP, Mnesia] =
	lists:map(
	  fun(Proj) ->
		  project:save(Proj)
	  end,
	  [project:new(<<"OTP">>, <<"The Open Telephony Platform">>, Erlang),
	   project:new(<<"Mnesia">>, <<"A distributed database "
		       "engine written in Erlang">>, Erlang)]),

    %% Next, add some developers
    [Joe, Ulf, Klacke] =
	lists:map(
	  fun(Developer) ->
		  developer:save(Developer)
	  end,
	  [developer:new(<<"Joe Armstrong">>, <<"Sweden">>),
	   developer:new(<<"Ulf Wiger">>, <<"Sweden">>),
	   developer:new(<<"Claes (Klacke) Wikstrom">>, <<"Sweden">>)]),

    %% Add some developers to our projects
    ok = project:add_developer(OTP, Joe),
    ok = project:add_developer(OTP, Klacke),
    ok = project:add_developer(OTP, Ulf),
    
    %% Add some projects to our developers
    ok = developer:add_project(Klacke, Yaws1),
    ok = developer:add_project(Klacke, Mnesia),
    ok = developer:add_project(Ulf, Mnesia),

    %% basic SELECT query
    [Joe, Ulf, Klacke] =
	project:developers(OTP),

    %% fancier SELECT queries
    [Ulf, Klacke] =
	project:developers(Mnesia),
    Ulf = project:developers_first(Mnesia),
    [Klacke] =
	project:developers(Mnesia, {name, like, "Claes%"}),
    
    %% SELECT query, from the other direction
    [Yaws1, OTP, Mnesia] =
	developer:projects(Klacke),

    %% Klacke, nothing personal here :)
    1 = developer:remove_project(Klacke, Yaws1),
    [OTP, Mnesia] = developer:projects(Klacke),
    1 = developer:remove_project(Klacke, OTP),
    [Mnesia] = developer:projects(Klacke),
    1 = developer:remove_project(Klacke, Mnesia),
    [] = developer:projects(Klacke),
    0 = developer:remove_project(Klacke, Mnesia),

    1 = language:delete(Java),

    [] = language:find({name, '=', "Java"}),
	

    test2(Driver),
    test3(Driver),
    ok.

%% test some 0.7 features
test2(_Driver) ->
    3 = developer:count(),
    developer:transaction(
      fun() ->
	      developer:delete_where({id, '>', 0}),
	      exit(just_kidding)
      end),
    3 = developer:count(),

    Musicians =
	[
	 musician:new(<<"Jimmy">>, 26, <<"USA">>, <<"Rock">>, <<"Guitar">>),
	 musician:new(<<"Janis">>, 27, <<"USA">>, <<"Blues">>, <<"Vocals">>),
	 musician:new(<<"Jim">>, 28, <<"Australia">>, <<"Rock">>,
		      <<"Vocals">>)],
    
    lists:map(fun(M) ->
		      musician:save(M)
	      end, Musicians),

    2 = musician:count('distinct country'),
    27.0 = musician:avg(age),
    265.0 = musician:avg(age, {country,'=',"USA"}) * 10,

    Otp = project:find_first({name,'=',"OTP"}),

    2 = project:count_of_developers(Otp),
    1 = project:count_of_developers(Otp, name, {name, like, "J%"}),

    1 = project:count_of_developers(Otp, 'distinct country'),

    3 = developer:delete_all(),
    
    3 = musician:count(),

    0 = project:count_of_developers(Otp),
    ok.
    

%% test multi-field custom primary keys
test3(Driver) ->
    Driver:q({esql, {delete, customer_customer}}),
    Driver:q({esql, {delete, customer_store}}),    

    store:delete_all(),
    customer:delete_all(),
    item:delete_all(),

    S1 = store:new_with([{number, 1}, {name, <<"dunkin">>}]),
    S2 = store:save(S1),

    I1 = item:new_with([{size, 3}, {name, <<"coffee">>}, {store, S2}]),
    I2 = item:save(I1),
    I3 = item:new_with([{size, 5}, {name, <<"bagel">>}, {store, S2}]),
    I4 = item:save(I3),
    
    S2 = item:store(I2),
    S2 = item:store(I4),

    [I2, I4] = store:items(S2),
    I2 = store:items_first(S2),
    2 = store:count_of_items(S2),

    C1 = customer:new(<<"bob">>),
    C2 = customer:save(C1),
    C3 = customer:new(<<"jane">>),
    C4 = customer:save(C3),
    
    ok = store:add_customer(S2, C2),
    ok = store:add_customer(S2, C4),

    C2 = store:customers_first(S2),
    [C2, C4] = store:customers(S2),
    2 = store:count_of_customers(S2),
    <<"jane">> = store:max_of_customers(S2, name),
    
    1 = store:remove_customer(S2, C2),
    1 = store:count_of_customers(S2),
    C4 = store:customers_first(S2),
    
    1 = store:remove_customer(S2, C4),
    0 = store:count_of_customers(S2),
    undefined = store:customers_first(S2),

    
    C5 = customer:new(<<"adam">>),
    C6 = customer:save(C5),

    ok = customer:add_customer(C2, C4),
    1 = customer:count_of_customers(C2),
    ok = customer:add_customer(C2, C6),
    2 = customer:count_of_customers(C2),
    C4 = customer:customers_first(C2),
    [C4, C6] = customer:customers(C2),
    
    <<"jane">> = customer:max_of_customers(C2, name),
    <<"adam">> = customer:min_of_customers(C2, name),
    
    1 = customer:remove_customer(C2, C4),
    1 = customer:count_of_customers(C2),
    C6 = customer:customers_first(C2),


    1 = customer:remove_customer(C2, C6),
    0 = customer:count_of_customers(C2),
    undefined = customer:customers_first(C2),

    0 = customer:remove_customer(C2, C6),
    
    ok.
    

