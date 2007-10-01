%% @author Matthew Pflueger (matthew.pflueger@gmail.com)
%% @copyright Matthew Pflueger 2007
%% @doc This module implements the Mnesia driver for ErlyDB.
%%
%% This is an internal ErlyDB module that you normally shouldn't have to
%% use directly. For most situations, all you have to know
%% about this module is the options you can pass to {@link start/1}, which
%% is called by {@link erlydb:start/2}.  Currently (Erlyweb 0.6), no options are 
%% recognized/used.
%%
%%
%% == Contents ==
%%
%% {@section Introduction}<br/>
%% {@section Conventions}<br/>
%% {@section Types}<br/>
%% {@section Example}<br/>
%% {@section What's Not Supported}<br/>
%%
%%
%% == Introduction ==
%%
%% Mnesia is Erlang's distributed DataBase Management System (DBMS).  Please read the
%% Mnesia Reference Manual for more information about Mnesia.
%%
%% This driver executes Erlsql queries against Mnesia.  Most Erlsql queries are 
%% dynamically converted into Query List Comprehension (QLC) expressions before 
%% execution.  Please see the qlc module documentation for more information on QLC.
%% Please read the Erlsql documentation for more information on Erlsql.
%%
%% This driver does not add relational support to Mnesia (constraints, 
%% cascades, etc).  Some relational support for Mnesia has been implemented by 
%% Ulf Wiger in the user contribution rdbms (see http://erlang.org/user.html).  
%% For more information visit http://ulf.wiger.net/rdbms/doc/rdbms.html. You can download a more
%% recent version of rdbms at http://ulf.wiger.net/rdbms/download/.
%%
%%
%% == Conventions ==
%%
%% The driver uses a table named 'counter' for auto-incrementing (identity) primary key columns.
%% (only valid for set or ordered-set tables). The 'counter' table must be created 
%% using the following:
%%
%% mnesia:create_table(counter, [{disc_copies, [node()]}, {attributes, [key, counter]}])
%%
%% The key column will contain table names of the mnesia tables utilizing identity columns.  The
%% counter contains the value of the last used identity (serial integer).  The counter is updated
%% using:
%%
%% mnesia:dirty_update_counter(counter, Table, 1)
%%
%% You can initialize/start the identity of a particular table by executing the above statement
%% with an arbitrary number (greater than 0).  The above operation is atomic (the function name
%% is misleading).  Please read the Mnesia docs for more information.  The use of the 'counter'
%% table is currently not customizable but that will hopefully change soon. 
%% 
%% All columns named 'id' or ending with 'id' are treated as integers.  If the column named 'id'
%% is the first attribute (column) in the mnesia table, then it is also treated as an 
%% auto-incrementing identity column.
%%
%%
%% == Types ==
%%
%% This driver stores all fields as binary unless the field name ends with id and in that 
%% case the field is treated as an integer (as discussed above).  This can be customized 
%% by utilizing the user_properties for a mnesia table. The driver will do a limited 
%% amount of type conversion utilizing these properties. The driver will recognize
%% user_properties for a field if defined in the following format:
%%
%% {Field, {Type, Modifier}, Null, Key, Default, Extra, MnesiaType}
%%
%% where Field is an atom and must be the same as the field (attribute) name,
%% Type through Extra is are as defined in erlydb_field:new/6
%% MnesiaType is the type to store the field as in mnesia.
%%
%% Currently, only the following values for MnesiaType are recognized:
%%
%% atom, list, binary, integer, float, datetime, date, time, undefined
%%
%% The erlydb_mnesia driver will attempt to convert field values into
%% the specified type before insertion/update/query of the record in
%% mnesia...  If the MnesiaType has a value of undefined then no type
%% conversion is attempted for the field.
%%
%%
%% == Example ==
%%
%% Given the following record:
%%
%% -record(person, {myid, type, name, age, country, office, department, genre, instrument, created_on})
%%
%% Create a Mnesia table with types for the driver using:
%% 
%% {atomic, ok} = mnesia:create_table(person, [
%%			{disc_copies, [node()]},
%% 			{attributes, record_info(fields, person)},
%%          {user_properties, [{myid, {integer, undefined}, false, primary, undefined, identity, integer},
%%                             {type, {varchar, undefined}, false, undefined, undefined, undefined, atom},
%%                             {age, {integer, undefined}, true, undefined, undefined, undefined, integer},
%%                             {created_on, {datetime, undefined}, true, undefined, undefined, undefined, undefined}]}])
%% 
%% Note the following:
%% 1) The primary key column is called myid and is an auto-incrementing integer column.  This is the
%%    same as if the column had been named 'id'.
%% 2) The type and age columns have customized types.  The driver will try to convert all values
%%    inserted into the table into the specified types.
%% 3) The created_on column is defined as a datetime for Erlyweb but is of type undefined for the
%%    Mnesia driver.  This means that no type conversion will be attempted for the created_on
%%    column resulting in a Erlang datetime tuple to be stored in the column 
%%    {{Year, Month, Day}, {Hour, Minute, Second}} or {datetime, {{Year, Month, Day},{Hour,Minute,Second}}}
%%    depending on how you create the record (creating a record from strings will result in the
%%    tuple beginning with datetime).
%% 4) Changing the user property for the created_on column to specify a mnesia type of datetime like
%%    {created_on, {datetime, undefined}, true, undefined, undefined, undefined, datetime}
%%    will result in the erlang date time tuple {{Year,Month,Day},{Hour,Minute,Second}}
%%    to be stored regardless of how the record was created (ie. it will strip the redundant
%%    datetime atom from the tuple
%%
%%
%% See test/erlydb/erlydb_mnesia_schema for more examples of how to create mnesia tables
%% with user_properties... 
%%
%%
%% == What's Not Supported ==
%%
%% This driver is very much still alpha quality.  Much is not supported but the most glaring
%% are unions and sub-queries.
%%

%% For license information see LICENSE.txt

-module(erlydb_mnesia).
-author("Matthew Pflueger (matthew.pflueger@gmail.com)").

-export([start/0,
	 start/1,
	 get_metadata/1,
	 q/1,
	 q/2,
	 transaction/2,
	 select/2,
	 select_as/3,
	 update/2,
	 get_last_insert_id/2]).



%% Useful for debugging

-define(L(Msg), io:format("~p:~b ~p ~n", [?MODULE, ?LINE, Msg])).
-define(S(Obj), io:format("LOG ~w ~s\n", [?LINE, Obj])).


-record(qhdesc, {expressions = [],
                 generators = [],
                 filters = [],
                 bindings = erl_eval:new_bindings(),
                 options = [],
                 evalfun = fun qlc:e/2,
                 postqh = fun postqh/2,
                 posteval = fun posteval/1,
                 metadata = dict:new()}).



start() ->
    case mnesia:system_info(is_running) of
        no -> mnesia:start();
        _ -> ok 
        % FIXME this could fail if system_info returns 'stopping'
    end.


%% @doc Start the Mnesia driver using the options property list.  Currently, no options are recognized.
%%
%% @spec start(StartOptions::proplist()) -> ok | {error, Error}
start(_Options) ->
    ok = start().


%% @doc Get the table names and fields for the database.
%%
%% @spec get_metadata(Options::proplist()) -> gb_trees()
get_metadata(_Options) ->
	% NOTE Integration with mnesia_rdbms would be interesting...
    Tables = mnesia:system_info(tables) -- [schema],
    Tree = lists:foldl(
			fun(Table, TablesTree) ->
            	gb_trees:enter(Table, get_metadata(Table, table_fields(Table)), TablesTree)
        	end, gb_trees:empty(), Tables),
	{ok, Tree}.

get_metadata(Table, Fields) when is_list(Fields) ->
    [get_metadata(Table, Field) || Field <- Fields];
get_metadata(Table, Field) ->
    {Field, {Type, Modifier}, Null, Key, Default, Extra, _MnesiaType} = get_user_properties(Table, Field),
    erlydb_field:new(Field, {Type, Modifier}, Null, Key, Default, Extra).



q(Statement) ->
    q(Statement, undefined).

q({esql, Statement}, Options) ->
    ?L(["In q with: ", Statement]),
	q2(Statement, Options);

q(Statement, Options) when is_binary(Statement); is_list(Statement) ->
    ?L(["Unhandled binary or list query", Statement, Options]),
    exit("Unhandled binary or list query").


q2({select, {call, count, _What}, {from, Table}, {where, undefined}, undefined}, _Options) 
		when is_list(Table) == false ->
    {ok, [{table_size(Table)}]};

q2({select, Table}, Options) when is_list(Table) == false ->
    q2({select, [Table]}, Options);
q2({select, Tables}, Options) when is_list(Tables) ->
    {atomic, Results} = transaction(fun() -> lists:foldl(
		fun(Table, Acc) -> Acc ++ mnesia:match_object(mnesia:table_info(Table, wild_pattern)) end, 
        [], Tables) end, Options),
    {data, Results};
q2({select, '*', {from, Tables}}, Options) ->
    q2({select, Tables}, Options);
q2({select, '*', {from, Tables}, {where, undefined}, undefined}, Options) ->
    q2({select, Tables}, Options);

%% QLC queries
q2({select, Fields, {from, Tables}}, Options) ->
    select(undefined, Fields, Tables, undefined, undefined, Options);
q2({select, Fields, {from, Tables}, {where, WhereExpr}}, Options) ->
    select(undefined, Fields, Tables, WhereExpr, undefined, Options);
q2({select, Fields, {from, Tables}, {where, WhereExpr}, Extras}, Options) ->
    select(undefined, Fields, Tables, WhereExpr, Extras, Options);
q2({select, Fields, {from, Tables}, WhereExpr, Extras}, Options) ->
    select(undefined, Fields, Tables, WhereExpr, Extras, Options);
q2({select, Fields, {from, Tables}, Extras}, Options) ->
    select(undefined, Fields, Tables, undefined, Extras, Options);
q2({select, Tables, {where, WhereExpr}}, Options) ->
    select(undefined, undefined, Tables, WhereExpr, undefined, Options);
q2({select, Tables, WhereExpr}, Options) ->
    select(undefined, undefined, Tables, WhereExpr, undefined, Options);
q2({select, Modifier, Fields, {from, Tables}}, Options) ->
    select(Modifier, Fields, Tables, undefined, undefined, Options);
q2({select, Modifier, Fields, {from, Tables}, {where, WhereExpr}}, Options) ->
    select(Modifier, Fields, Tables, WhereExpr, undefined, Options);
q2({select, Modifier, Fields, {from, Tables}, Extras}, Options) ->
    select(Modifier, Fields, Tables, undefined, Extras, Options);
q2({select, Modifier, Fields, {from, Tables}, {where, WhereExpr}, Extras}, Options) ->
    select(Modifier, Fields, Tables, WhereExpr, Extras, Options);
q2({select, Modifier, Fields, {from, Tables}, WhereExpr, Extras}, Options) ->
    select(Modifier, Fields, Tables, WhereExpr, Extras, Options);
%% q2({Select1, union, Select2}, Options) ->
%%     [$(, q2(Select1, Options), <<") UNION (">>, q2(Select2, Options), $)];
%% q2({Select1, union, Select2, {where, WhereExpr}}, Options) ->
%%     [q2({Select1, union, Select2}, Options), where(WhereExpr, Options)];
%% q2({Select1, union, Select2, Extras}, Options) ->
%%     [q2({Select1, union, Select2}, Options), extra_clause(Extras, Options)];
%% q2({Select1, union, Select2, {where, _} = Where, Extras}, Options) ->
%%     [q2({Select1, union, Select2, Where}, Options), extra_clause(Extras, Options)];


q2({insert, Table, Params}, Options) ->
    {Fields, Values} = lists:unzip(Params),
    q2({insert, Table, Fields, [Values]}, Options);
q2({insert, Table, Fields, ValuesList}, _Options) ->
    {Fields1, ValuesList1} = 
    	case get_identity_field(Table) of
        	undefined -> {Fields, ValuesList};
        	Field -> NewFields = [Field | Fields],
					 MaxId = mnesia:dirty_update_counter(counter, Table, length(ValuesList)),
                 	 put(mnesia_last_insert_id, MaxId),
                 	 {NewValuesList, _} = 
                     		lists:mapfoldr(fun(Values, Id) -> {[Id | Values], Id-1} end, MaxId, ValuesList),
                     {NewFields, NewValuesList}
    	end,
    QLCData = get_qlc_metadata(Table),
    lists:foreach(fun(Values) -> 
        			ok = write(dict:fetch({new_record, Table}, QLCData), Fields1, Values, QLCData) 
                  end, ValuesList1),
    {ok, length(ValuesList1)};


q2({update, Table, Params}, _Options) ->
    {Fields, Values} = lists:unzip(Params),
    QLCData = get_qlc_metadata(Table),
    TraverseFun = fun(Record, {Fields1, Values1, QLCData1}) ->
    	write(Record, Fields1, Values1, QLCData1),
        {Fields1, Values1, QLCData1}
    end,
	{atomic, _} = traverse(TraverseFun, {Fields, Values, QLCData}, Table),
    {ok, table_size(Table)};
                            
q2({update, Table, Params, {where, Where}}, Options) ->
    q2({update, Table, Params, Where}, Options);
q2({update, Table, Params, Where}, Options) ->
    QHDesc = #qhdesc{metadata = get_qlc_metadata(Table)},
	{Fields, Values} = lists:unzip(Params),
    QLCData = QHDesc#qhdesc.metadata,
	{atomic, Num} = mnesia:transaction(
		fun() -> 
            {data, Records} = select(undefined, undefined, Table, Where, undefined, Options, QHDesc),
			lists:foreach(fun(Record) -> write(Record, Fields, Values, QLCData) end, Records),
            length(Records)
        end),
	{ok, Num};


q2({delete, {from, Table}, {where, undefined}}, Options) ->
    q2({delete, Table}, Options);
q2({delete, {from, Table}}, Options) ->
    q2({delete, Table}, Options);
q2({delete, Table}, _Options) ->
    % cannot use mnesia:clear_table(Table) here because sometimes this gets called inside a transaction...
    TraverseFun = fun(Record, Num) ->
        mnesia:delete_object(Record),
        Num + 1
    end,
	{atomic, Num} = traverse(TraverseFun, 0, Table),
    {ok, Num};
                                          
q2({delete, {from, Table}, {where, Where}}, Options) ->
	q2({delete, Table, Where}, Options);
q2({delete, Table, {where, Where}}, Options) ->
	q2({delete, Table, Where}, Options);
q2({delete, Table, Where}, Options) ->
    {atomic, Num} = mnesia:transaction(
    	fun() -> 
            {data, Records} = q2({select, Table, Where}, Options),
            lists:foreach(fun(Record) -> mnesia:delete_object(Record) end, Records),
            length(Records)
        end),
    {ok, Num};
    
q2(Statement, Options) ->
    ?L(["Unhandled statement and options: ", Statement, Options]),
    exit("Unhandled statement").



select(Modifier, Fields, Tables, WhereExpr, Extras, Options) ->
    QHDesc = #qhdesc{metadata = get_qlc_metadata(Tables)},
    select(Modifier, Fields, Tables, WhereExpr, Extras, Options, QHDesc).

select(Modifier, Fields, Tables, WhereExpr, Extras, Options, QHDesc) ->
    QHDesc1 = modifier(Modifier, QHDesc),
    QHDesc2 = fields(Fields, QHDesc1),
    QHDesc3 = tables(Tables, QHDesc2),
    QHDesc4 = where(WhereExpr, QHDesc3),
    QHDesc5 = extras(Extras, QHDesc4),
    
    Desc = QHDesc5,
    QLC = if length(Desc#qhdesc.expressions) > 1 -> "[{" ++ comma(Desc#qhdesc.expressions) ++ "}";
             true -> "[" ++ comma(Desc#qhdesc.expressions)
          end,
	QLC1 = QLC ++ " || " ++ comma(Desc#qhdesc.generators ++ lists:reverse(Desc#qhdesc.filters)) ++ "].",
    ?L(["About to execute QLC: ", QLC1]),
    {atomic, Results} = transaction(
    	fun() -> 
            QHOptions = Desc#qhdesc.options,
        	QH = qlc:string_to_handle(QLC1, QHOptions, Desc#qhdesc.bindings),
            PostQH = Desc#qhdesc.postqh,
            QH1 = PostQH(QH, QHOptions),
            EvalFun = Desc#qhdesc.evalfun,
            EvalFun(QH1, QHOptions)
        end, Options),
    ?L(["Found Results: ", Results]),
    PostEval = Desc#qhdesc.posteval,
	PostEval(Results).


modifier(undefined, #qhdesc{} = QHDesc) ->
    QHDesc;
modifier(distinct, #qhdesc{options = Options} = QHDesc) ->
    QHDesc#qhdesc{options = [{unique_all, true} | Options]};
modifier(Other, _QHDesc) ->
    ?L(["Unhandled modifier: ", Other]),
    exit("Unhandled modifier").


fields(undefined, QHDesc) ->
    fields('*', QHDesc);
fields('*', #qhdesc{expressions = Fields, metadata = QLCData} = QHDesc) ->
    QHDesc#qhdesc{expressions = dict:fetch(aliases, QLCData) ++ Fields};

fields({call, avg, Field}, #qhdesc{metadata = QLCData} = QHDesc) when is_atom(Field) ->
	[Table | _Tables] = dict:fetch(tables, QLCData),
	fields({call, avg, {Table, Field}}, QHDesc);
fields({call, avg, {Table, Field}}, #qhdesc{metadata = QLCData} = QHDesc) ->
	Index = dict:fetch({index,Table,Field}, QLCData),
	QHDesc1 = QHDesc#qhdesc{posteval = 
     	fun(Results) ->
            Total = lists:foldl(fun(Record, Sum) -> element(Index, Record) + Sum end, 0, Results),
            {ok, [{Total/length(Results)}]}
        end},
    fields('*', QHDesc1);

%% Count functions
fields({call, count, What}, #qhdesc{metadata = QLCData} = QHDesc) when is_atom(What) ->
    QHDesc1 = case regexp:split(atom_to_list(What), "distinct ") of
        {ok, [[], Field]} -> 
            [Table | _] = resolve_field(Field, QLCData),
            QHDesc#qhdesc{
                expressions = [dict:fetch({alias, Table}, QLCData)],
                postqh = fun(QH, _QHOptions) ->
			       	qlc:keysort(dict:fetch({index,Table,list_to_atom(Field)}, QLCData), QH, [{unique, true}])
        		end};
		_Other -> fields('*', QHDesc)
    end,
	QHDesc1#qhdesc{posteval = fun count/1};
fields({call, count, _What}, QHDesc) ->
    fields('*', QHDesc#qhdesc{posteval = fun count/1});

%% Max/Min functions                 
fields({call, max, Field}, QHDesc) ->
    min_max(Field, QHDesc, [{order, descending}, {unique, true}]);
fields({call, min, Field}, QHDesc) ->
    min_max(Field, QHDesc, [{unique, true}]);

fields([Field | Fields], QHDesc) ->
    fields(Fields, fields(Field, QHDesc));
fields([], QHDesc) ->
    QHDesc#qhdesc{expressions = lists:reverse(QHDesc#qhdesc.expressions)};

fields(Field, #qhdesc{metadata = QLCData} = QHDesc) when is_tuple(Field) == false ->
    [Table | _Tables] = dict:fetch(tables, QLCData),
    fields({Table,Field}, QHDesc);
fields({_,_} = Field, #qhdesc{expressions = Fields, metadata = QLCData} = QHDesc) ->
    QHDesc#qhdesc{expressions = [dict:fetch(Field, QLCData) | Fields]}.


tables(Table, QHDesc) when is_list(Table) == false ->
    tables([Table], QHDesc);
tables([Table | Tables], #qhdesc{generators = Generators, metadata = QLCData} = QHDesc) ->
    tables(Tables, QHDesc#qhdesc{generators = [dict:fetch(Table, QLCData) | Generators]});
tables([], #qhdesc{generators = Generators} = QHDesc) ->
    QHDesc#qhdesc{generators = lists:reverse(Generators)}.


where({Where1, 'and', Where2}, QHDesc) ->
    QHDesc1 = where(Where1, QHDesc),
    where(Where2, QHDesc1);

where({'or', Where}, #qhdesc{filters = Filters} = QHDesc) when is_list(Where) ->
    QHDesc1 = where(Where, QHDesc#qhdesc{filters = []}),
    OrFilter = "(" ++ combinewith(" orelse ", QHDesc1#qhdesc.filters) ++ ")",
    QHDesc1#qhdesc{filters = [OrFilter | Filters]};
where({'not', Where}, #qhdesc{filters = Filters} = QHDesc) ->
    QHDesc1 = where(Where, QHDesc#qhdesc{filters = []}),
    NotFilter = "false == (" ++ combinewith(" andalso ", QHDesc1#qhdesc.filters) ++ ")",
    QHDesc1#qhdesc{filters = [NotFilter | Filters]};
where({'and', Where}, #qhdesc{filters = Filters} = QHDesc) ->
    QHDesc1 = where(Where, QHDesc#qhdesc{filters = []}),
    AndFilter = "(" ++ combinewith(" andalso ", QHDesc1#qhdesc.filters) ++ ")",
    QHDesc1#qhdesc{filters = [AndFilter | Filters]};

where([Where | Rest], QHDesc) ->
    where(Rest, where(Where, QHDesc));
where([], QHDesc) ->
    QHDesc;

where({From, Op, To}, #qhdesc{metadata = QLCData} = QHDesc) when is_tuple(From) == false ->
    [Table | _] = resolve_field(From, QLCData),
    where({{Table,From}, Op, To}, QHDesc);

where({{_,_} = From, 'is', 'null'}, #qhdesc{filters = Filters, metadata = QLCData} = QHDesc) ->
	QHDesc#qhdesc{filters = [dict:fetch(From, QLCData) ++ " == undefined" | Filters]};
where({{_,_} = From, 'like', To}, QHDesc) when is_binary(To) ->
    where({From, 'like', erlang:binary_to_list(To)}, QHDesc);
where({{Table,Field} = From, 'like', To}, #qhdesc{filters = Filters, metadata = QLCData} = QHDesc) ->
    {ok, To1, _RepCount} = regexp:gsub(To, "%", ".*"),
    To2 = "\"^" ++ To1 ++ "$\"",
    Filter = case mnesia_type(Table, Field) of
                 binary -> "erlang:binary_to_list(" ++ dict:fetch(From, QLCData) ++ ")";
                 _Other -> dict:fetch(From, QLCData)
             end,
    QHDesc#qhdesc{filters = ["regexp:first_match(" ++ Filter ++ ", " ++ To2 ++ ") /= nomatch" | Filters]};
                 
where({{_, _} = From, '=', To}, QHDesc) ->
    where({From, "==", To}, QHDesc);
where({{_, _} = From, Op, To}, QHDesc) when is_atom(Op) ->
    where({From, atom_to_list(Op), To}, QHDesc);

where({{_, _} = From, Op, {Table, Field} = To}, #qhdesc{filters = Filters, metadata = QLCData} = QHDesc) 
		when is_atom(Table), is_atom(Field) ->
    QHDesc#qhdesc{filters = [lists:concat([dict:fetch(From, QLCData), " ", Op, " ", 
                                           dict:fetch(To, QLCData)]) | Filters]};
where({{Table, Field} = From, Op, To}, #qhdesc{filters = Filters, bindings = Bindings, metadata = QLCData} = QHDesc) ->
	case resolve_field(To, QLCData) of
        [ToTable | _] -> where({From, Op, {ToTable, To}}, QHDesc);
		[] -> Var = list_to_atom("Var" ++ integer_to_list(random:uniform(100000))),
			  QHDesc#qhdesc{
              		filters = [lists:concat([dict:fetch(From, QLCData), " ", Op, " ", Var]) | Filters],
                  	bindings = erl_eval:add_binding(Var, convert(Table, Field, To), Bindings)}
    end;

where(undefined, QHDesc) ->
    QHDesc;

where(Where, _QHDesc) ->
    ?L(["Unhandled where: ", Where]),
    exit("Unhandled where").    


extras([Extra | Extras], QHDesc) ->
    QHDesc1 = extras(Extra, QHDesc),
    extras(Extras, QHDesc1);
extras([], QHDesc) ->
    QHDesc;

extras({order_by, Field}, #qhdesc{metadata = QLCData} = QHDesc) when is_atom(Field) ->
    QHDesc#qhdesc{postqh =
		fun(QH, QHOptions) ->
            [Table | _Rest] = dict:fetch(tables, QLCData),
            qlc:keysort(dict:fetch({index,Table,Field}, QLCData), QH, QHOptions)
        end};
extras({limit, Limit}, QHDesc) ->
    QHDesc#qhdesc{evalfun = 
        fun(QH, QHOptions) ->
        	QHCursor = qlc:cursor(QH, QHOptions),
        	Results = qlc:next_answers(QHCursor, Limit),
            qlc:delete_cursor(QHCursor),
            Results
		end};
extras({limit, 0, Limit}, QHDesc) ->
    extras({limit, Limit}, QHDesc);
extras({limit, From, Limit}, QHDesc) ->
    QHDesc#qhdesc{evalfun = 
        fun(QH, QHOptions) ->
        	QHCursor = qlc:cursor(QH, QHOptions),
        	qlc:next_answers(QHCursor, From),
            Results = qlc:next_answers(QHCursor, Limit),
            qlc:delete_cursor(QHCursor),
            Results
		end};
extras(undefined, QHDesc) ->
    QHDesc;
extras(Extras, _QHDesc) ->
    ?L(["Unhandled extras: ", Extras]),
    exit("Unhandled extras").


postqh(QueryHandle, _QHOptions) ->
    QueryHandle.
posteval(Results) ->
    {data, Results}.
count(Results) ->
    {ok, [{length(Results)}]}.
min_max(Field, #qhdesc{metadata = QLCData} = QHDesc, Options) ->
    [Table | _] = resolve_field(Field, QLCData),
    QHDesc1 = QHDesc#qhdesc{postqh = 
		fun(QH, _QHOptions) ->
	        qlc:keysort(dict:fetch({index,Table,Field}, QLCData), QH, Options)
        end},
 	QHDesc2 = QHDesc1#qhdesc{posteval = 
     	fun(Results) ->
	        {ok, [{element(dict:fetch({index,Table,Field}, QLCData), hd(Results))}]}
        end},
    QHDesc2#qhdesc{expressions = [dict:fetch({alias, Table}, QLCData)]}.
    


% For each table, add the metadata for the table's fields to the dictionary and then
% add TABLE_ROW_VAR <- mnesia:table(Table) to the dictionary where TABLE_ROW_VAR is the variable
% representing the current row of the table and Table is the table name as an atom (TABLE_ROW_VAR
% defaults to the table name in all caps)
get_qlc_metadata(Table) when is_list(Table) == false ->
    get_qlc_metadata([Table]);
get_qlc_metadata(Tables) when is_list(Tables) ->
    QLCData = dict:store(tables, [], dict:new()),
    QLCData1 = dict:store(aliases, [], QLCData),
    get_qlc_metadata(Tables, QLCData1).

get_qlc_metadata([Table | Tables], QLCData) when is_tuple(Table) == false ->
    % Create an alias for the table (table name in all caps)
    get_qlc_metadata({Table, 'as', string:to_upper(atom_to_list(Table))}, Tables, QLCData);
get_qlc_metadata([{_, 'as', _} = Table | Tables], QLCData) ->
    get_qlc_metadata(Table, Tables, QLCData);
get_qlc_metadata([], QLCData) ->
    QLCData.


% For each table store the following key => value pairs:
% {alias, Table} => Alias where Table is atom and Alias is string
% {table, Alias} => Table where Table is atom and Alias is string
% {new_record, Table} => {Table, undefined, undefined...} where Table is atom and value is a tuple
% Table => MnesiaTable where Table is atom and MnesiaTable is the string "Alias <- mnesia:table(Table)"
%
% Also store:
% tables => [Tables] where Tables is a list of all tables in query
% aliases => [Aliases] where Aliases is a list of all table aliases in query
get_qlc_metadata({Table, 'as', Alias}, Tables, QLCData) ->
    QLCData1 = dict:store({alias, Table}, Alias, QLCData),
    QLCData2 = dict:store({table, Alias}, Table, QLCData1),
    QLCData3 = dict:store({new_record, Table}, {Table}, QLCData2),
    QLCData4 = get_qlc_metadata(table_fields(Table), 2, Table, Alias, QLCData3),
    MnesiaTable = lists:concat([Alias, " <- mnesia:table(", Table, ")"]),
	QLCData5 = dict:store(Table, MnesiaTable, QLCData4),
    QLCData6 = dict:store(tables, dict:fetch(tables, QLCData5) ++ [Table], QLCData5), 
    QLCData7 = dict:store(aliases, dict:fetch(aliases, QLCData6) ++ [Alias], QLCData6), 
	get_qlc_metadata(Tables, QLCData7).

    
% for each table field (column), store the following: 
% {Table, Field} => "element(Alias, FieldIndex)" where Table and Field are atoms
% {Alias, Field} => "element(Alias, FieldIndex)" where Alias is a string and Field is an atom
% {index, Table, Field} => FieldIndex where Table and Field are atoms and FieldIndex is an integer
get_qlc_metadata([Field | Fields], FieldIndex, Table, Alias, QLCData) ->
    Data = "element(" ++ integer_to_list(FieldIndex) ++ ", " ++ Alias ++ ")",
    QLCData1 = dict:store({Table,Field}, Data, QLCData),
    QLCData2 = dict:store({Alias,Field}, Data, QLCData1),
    QLCData3 = dict:store({index,Table,Field}, FieldIndex, QLCData2),
    TableRecord = dict:fetch({new_record, Table}, QLCData3),
    QLCData4 = dict:store({new_record, Table}, erlang:append_element(TableRecord, undefined), QLCData3),
    get_qlc_metadata(Fields, FieldIndex + 1, Table, Alias, QLCData4);
get_qlc_metadata([], _FieldIndex, _Table, _Alias, QLCData) ->
    QLCData.



%% User_properties for field is defined as:
%% {Field, {Type, Modifier}, Null, Key, Default, Extra, MnesiaType}
%% where Field is an atom,
%% Type through Extra is are as defined in erlydb_field:new/6
%% MnesiaType is the type to store the field as in mnesia.
%%
%% Currently the driver tries to do a limited bit of conversion of types.  For example, you may want
%% to store strings as binaries in mnesia.  Erlydb may pass in strings during querying, updates, etc
%% and the string will need to be converted to/from a binary.
get_user_properties(Table, Field) ->
    case lists:keysearch(Field, 1, mnesia:table_info(Table, user_properties)) of
    	{value, {Field, {_Type, _Modifier}, _Null, _Key, _Default, _Extra, _MnesiaType} = UserProperties} -> UserProperties;
        false -> get_default_user_properties(table_type(Table), Field, field_index(Table, Field))
    end.

get_default_user_properties(TableType, Field, 1) ->
    case lists:suffix("id", atom_to_list(Field)) of
        true -> if TableType =:= bag -> {Field, {integer, undefined}, false, primary, undefined, undefined, integer};
                   true -> {Field, {integer, undefined}, false, primary, undefined, identity, integer}
                end;
        _False -> {Field, {varchar, undefined}, false, primary, undefined, undefined, binary}
    end;
get_default_user_properties(_TableType, Field, Index) when Index > 1 ->
    case lists:suffix("id", atom_to_list(Field)) of
        true -> {Field, {integer, undefined}, true, undefined, undefined, undefined, integer};
        _False -> {Field, {varchar, undefined}, true, undefined, undefined, undefined, binary}
	end.    


%% @doc Return the first field of the given table if it is an identity field (auto-incrementing) 
%%		or return undefined
get_identity_field(Table) ->
    [Field | _Rest] = table_fields(Table),
    case get_user_properties(Table, Field) of
        {Field, {_Type, _Modifier}, _Null, _Key, _Default, identity, _MnesiaType} -> Field;
        _Other -> undefined
    end.

                                                                                
%% @doc Find the field's position in the given table
field_index(Table, Field) ->
    field_index(Field, 1, table_fields(Table)).
field_index(Field, Index, [Field | _Rest]) ->
    Index;
field_index(Field, Index, [_Field | Rest]) ->
    field_index(Field, Index + 1, Rest);
field_index(_Field, _Index, []) -> 
    0.


table_type(Table) ->
    mnesia:table_info(Table, type).

table_fields(Table) ->
    mnesia:table_info(Table, attributes).

table_size(Table) ->
    mnesia:table_info(Table, size).

mnesia_type(Table, Field) ->
	{Field, {_Type, _Modifier}, _Null, _Key, _Default, _Extra, MnesiaType} = 
             get_user_properties(Table, Field),
     MnesiaType.


%% Convert Value to the type of the given Table Field.  No conversion takes place if there is
%% no defined type for Field.
convert(Table, Field, Value) ->
    convert(Value, mnesia_type(Table, Field)).


% FIXME there has to be some utility out there to do this conversion stuff...
convert(undefined, _Type) ->
    undefined;
convert(Value, undefined) ->
    Value;

                        
convert(Value, integer) when is_integer(Value) ->
    Value;
convert(Value, float) when is_integer(Value) ->
    Value;
convert(Value, list) when is_integer(Value) ->
    integer_to_list(Value);
convert(Value, atom) when is_integer(Value) ->
    list_to_atom(integer_to_list(Value));
convert(Value, binary) when is_integer(Value) ->
    list_to_binary(integer_to_list(Value));


convert(Value, integer) when is_float(Value) ->
    trunc(Value);
convert(Value, float) when is_float(Value) ->
    Value;
convert(Value, list) when is_float(Value) ->
    float_to_list(Value);
convert(Value, atom) when is_float(Value) ->
    list_to_atom(float_to_list(Value));
convert(Value, binary) when is_float(Value) ->
    list_to_binary(float_to_list(Value));


convert(Value, integer) when is_list(Value) ->
    list_to_integer(Value);
convert(Value, float) when is_list(Value) ->
    list_to_float(Value);
convert(Value, list) when is_list(Value) ->
    Value;
convert(Value, atom) when is_list(Value) ->
    list_to_atom(Value);
convert(Value, binary) when is_list(Value) ->
    list_to_binary(Value);


convert(Value, integer) when is_atom(Value) ->
    list_to_integer(atom_to_list(Value));
convert(Value, float) when is_atom(Value) ->
    list_to_float(atom_to_list(Value));
convert(Value, list) when is_atom(Value) ->
    atom_to_list(Value);
convert(Value, atom) when is_atom(Value) ->
    Value;
convert(Value, binary) when is_atom(Value) ->
    list_to_binary(atom_to_list(Value));


convert(Value, integer) when is_binary(Value) ->
    list_to_integer(binary_to_list(Value));
convert(Value, float) when is_binary(Value) ->
    list_to_float(binary_to_list(Value));
convert(Value, list) when is_binary(Value) ->
    binary_to_list(Value);
convert(Value, atom) when is_binary(Value) ->
    list_to_atom(binary_to_list(Value));
convert(Value, binary) when is_binary(Value) ->
    Value;


convert(Value, binary) ->
    % catch all
    Value;


convert({datetime, Value}, datetime) ->
    Value;
convert(Value, datetime) ->
    Value;
convert({date, Value}, date) ->
    Value;
convert(Value, date) ->
    Value;
convert({time, Value}, time) ->
    Value;
convert(Value, time) ->
    Value.


resolve_field(From, QLCData) ->
    resolve_field(From, dict:fetch(tables, QLCData), QLCData).

resolve_field(From, Tables, QLCData) when is_list(From) ->
    resolve_field(list_to_atom(From), Tables, QLCData);
resolve_field(From, Tables, QLCData) ->
	lists:foldl(
    	fun(Table, Acc) -> case dict:is_key({Table,From}, QLCData) of true -> [Table | Acc]; _ -> Acc end end, 
		[],
		Tables).


write(Record, Field, Value, QLCData) when is_list(Field) == false ->
	write(Record, [Field], [Value], QLCData);
write(Record, [Field | Fields], [Value | Values], QLCData) ->
    Table = element(1, Record),
    FieldIndex = dict:fetch({index, Table, Field}, QLCData),
    Record1 = setelement(FieldIndex, Record, convert(Table, Field, Value)),
    write(Record1, Fields, Values, QLCData);
write(Record, [], [], _QLCData) ->
    ok = mnesia:write(Record).


%% @doc Traverse the table executing the given function with each record.  The function must
%% 		except the record followed by the given arguments and return its arguments which will be
%%		supplied in the next call.  For example: sum(Record, Num) -> Num + 1.
traverse(Fun, Args, Table) ->
    mnesia:transaction(fun() -> traverse(Fun, Args, Table, mnesia:first(Table)) end).
traverse(_Fun, Args, _Table, '$end_of_table') ->
    {ok, Args};
traverse(Fun, Args, Table, Key) ->
    % for set and ordered_set tables this will execute once, for bag tables this could execute many times...
    Args2 = lists:foldl(fun(Record, Args1) -> Fun(Record, Args1) end, Args, mnesia:read(Table, Key, write)),
    traverse(Fun, Args2, Table, mnesia:next(Table, Key)).


comma(List) ->
    combinewith(", ", List).
combinewith(Separator, List) ->
    Length = length(List),
    lists:foldl(
		fun(Elem, {Len, String}) when Len < Length -> {Len + 1, lists:concat([String, Elem, Separator])};
           (Elem, {Len, String}) when Len == Length -> lists:concat([String, Elem]) end, {1, ""}, List).


%% @doc Execute a group of statements in a transaction.
%%   Fun is the function that implements the transaction.
%%   Fun can contain an arbitrary sequence of calls to
%%   the erlydb_mnesia's query functions. If Fun crashes or returns
%%   or throws 'error' or {error, Err}, the transaction is automatically
%%   rolled back. 
%%
%% @spec transaction(Fun::function(), Options::options()) ->
%%   {atomic, Result} | {aborted, Reason}
transaction(Fun, _Options) ->
    mnesia:transaction(Fun).

%% @doc Execute a statement against Mnesia.
%%
%% @spec select(Statement::statement(), Options::options()) ->
%%   {ok, Rows::list()} | {error, Error}
select(Statement, Options) ->
    select2(Statement, Options, []).

%% @doc Execute a statement for records belonging to the given module,
%%   	returning all rows with additional data to support
%%   	higher-level ErlyDB features.
%%
%% @spec select_as(Module::atom(), Statement::statement(), Options::options()) ->
%%	{ok, Rows} | {error, Error}
select_as(Module, Statement, Options) ->
    select2(Statement, Options, [Module, false]).

select2(Statement, Options, FixedVals) ->
    get_select_result(q(Statement, Options), FixedVals).


get_select_result({data, Data}, undefined) ->
	Result = lists:foldl(fun(DataTuple, Acc) -> [tuple_to_list(DataTuple) | Acc] end, [], Data),
    {ok, lists:reverse(Result)};
get_select_result({data, Data}, [Table | _Rest] = FixedVals)->
    Results = lists:foldl(
    	fun(DataTuple, Acc) -> 
			% some data tuples are the records themselves with the table/record name as the first element...
        	[Table2 | Fields] = DataList = tuple_to_list(DataTuple),
            Row = if Table == Table2 -> FixedVals ++ Fields;
                     true -> FixedVals ++ DataList
                  end,
            [list_to_tuple(Row) | Acc]
        end, [], Data),
    {ok, lists:reverse(Results)};
            
get_select_result(Other, _) -> Other.


%% @doc Execute a update to Mnesia.
%%
%% @spec update(Statement::statement(), Options::options()) ->
%%  {ok, NumAffected} | {error, Err}
update(Statement, Options) ->
    q(Statement, Options).


%% @doc Get the id of the last inserted record.
%%
%% @spec get_last_insert_id(Table::atom(), Options::options()) -> term()
get_last_insert_id(_Table, _Options) ->
    Val = get(mnesia_last_insert_id),
    {ok, Val}.
