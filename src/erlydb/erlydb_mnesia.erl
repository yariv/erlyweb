%% @author Matthew Pflueger (matthew.pflueger@gmail.com)
%% @copyright Matthew Pflueger 2007
%% 
%% @doc This module implements the Mnesia driver for ErlyDB.
%%
%% This is an internal ErlyDB module that you normally shouldn't have to
%% use directly. For most situations, all you have to know
%% about this module is the options you can pass to {@link start/1}, which
%% is called by {@link erlydb:start/2}.
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
	 get_last_insert_id/1]).



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
	% NOTE utilizing the metadata capabilities for mnesia tables (see user_properties) would allow
	% us to specify types, limits, etc...  Integration with mnesia_rdbms would also be interesting...
    Tables = mnesia:system_info(tables) -- [schema],
    case catch lists:foldl(
			fun(Table, TablesTree) ->
                	get_metadata(Table, mnesia:table_info(Table, type), TablesTree)
        	end, gb_trees:empty(), Tables) of
	    {error, _} = Err -> Err;
    	Tree -> {ok, Tree}
    end.

get_metadata(Table, Type, TablesTree) when Type =:= set; Type =:= ordered_set ->
    [PrimaryField | Rest] = mnesia:table_info(Table, attributes),
   	Fields = [new_identity_field(PrimaryField) | [new_field(Field) || Field <- Rest]],
	gb_trees:enter(Table, Fields, TablesTree);
get_metadata(Table, bag, TablesTree) ->
    [PrimaryField | Rest] = mnesia:table_info(Table, attributes),
   	Fields = [new_primary_field(PrimaryField) | [new_field(Field) || Field <- Rest]],
	gb_trees:enter(Table, Fields, TablesTree).

new_identity_field(Field) ->
    % FIXME we need to generalize the primary key field instead of assuming an auto-incrementing integer...
	erlydb_field:new(Field, integer, false, primary, undefined, identity).

new_primary_field(Field) ->
	erlydb_field:new(Field, varchar, false, primary, undefined, undefined).

new_field(Field) ->
    % FIXME mnesia doesn't have types but we should do something better than this...
    erlydb_field:new(Field, varchar, true, undefined, undefined, undefined).



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
    {ok, [{mnesia:table_info(Table, size)}]};

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
    {Attributes, Values} = lists:unzip(Params),
    q2({insert, Table, Attributes, [Values]}, Options);
q2({insert, Table, Attributes, [Values]}, _Options) ->
    QLCData = get_qlc_metadata(Table),
    {Attributes1, Values1} = case dict:find({index, Table, id}, QLCData) of
		% FIXME this really needs to be handled better, what if the auto-incrementing primary key is not called id?
        {ok, 2} -> Id = mnesia:dirty_update_counter(counter, Table, 1),
             % FIXME this is probably not the best way to keep track of the last inserted id...
             put(mnesia_last_insert_id, Id),
             {[id | Attributes], [Id | Values]};
        _Other -> {Attributes, Values}
    end,
    ok = write(dict:fetch({new_record, Table}, QLCData), Attributes1, Values1, QLCData),
    {ok, 1};
              

q2({update, Table, Params}, _Options) ->
    {Attributes, Values} = lists:unzip(Params),
    QLCData = get_qlc_metadata(Table),
    TraverseFun = fun(Record, {Attributes1, Values1, QLCData1}) ->
    	write(Record, Attributes1, Values1, QLCData1),
        {Attributes1, Values1, QLCData1}
    end,
	{atomic, _} = traverse(TraverseFun, {Attributes, Values, QLCData}, Table),
    {ok, mnesia:table_info(Table, size)};
                            
q2({update, Table, Params, {where, Where}}, Options) ->
    q2({update, Table, Params, Where}, Options);
q2({update, Table, Params, Where}, Options) ->
    QHDesc = #qhdesc{metadata = get_qlc_metadata(Table)},
	{Attributes, Values} = lists:unzip(Params),
    QLCData = QHDesc#qhdesc.metadata,
	{atomic, Num} = mnesia:transaction(
		fun() -> 
            {data, Records} = select(undefined, undefined, Table, Where, undefined, Options, QHDesc),
			lists:foreach(fun(Record) -> write(Record, Attributes, Values, QLCData) end, Records),
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
    {ok, Num}.
    

 
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

fields({call, avg, Attribute}, #qhdesc{metadata = QLCData} = QHDesc) when is_atom(Attribute) ->
	[Table | _Tables] = dict:fetch(tables, QLCData),
	fields({call, avg, {Table, Attribute}}, QHDesc);
fields({call, avg, {Table, Attribute}}, #qhdesc{metadata = QLCData} = QHDesc) ->
	Index = dict:fetch({index,Table,Attribute}, QLCData),
	QHDesc1 = QHDesc#qhdesc{posteval = 
     	fun(Results) ->
            Total = lists:foldl(fun(Record, Sum) -> element(Index, Record) + Sum end, 0, Results),
            {ok, [{Total/length(Results)}]}
        end},
    fields('*', QHDesc1);

%% Count functions
fields({call, count, What}, #qhdesc{metadata = QLCData} = QHDesc) when is_atom(What) ->
    QHDesc1 = case regexp:split(atom_to_list(What), "distinct ") of
        {ok, [[], Attribute]} -> 
            [Table | _] = resolve_field(Attribute, QLCData),
            QHDesc#qhdesc{
                expressions = [dict:fetch({alias, Table}, QLCData)],
                postqh = fun(QH, _QHOptions) ->
			       	qlc:keysort(dict:fetch({index,Table,list_to_atom(Attribute)}, QLCData), QH, [{unique, true}])
        		end};
		_Other -> fields('*', QHDesc)
    end,
	QHDesc1#qhdesc{posteval = fun count/1};
fields({call, count, _What}, QHDesc) ->
    fields('*', QHDesc#qhdesc{posteval = fun count/1});

%% Max/Min functions                 
fields({call, max, Attribute}, QHDesc) ->
    min_max(Attribute, QHDesc, [{order, descending}, {unique, true}]);
fields({call, min, Attribute}, QHDesc) ->
    min_max(Attribute, QHDesc, [{unique, true}]);

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
where({{_,_} = From, '=', {_,_} = To}, #qhdesc{filters = Filters, metadata = QLCData} = QHDesc) -> 
	QHDesc#qhdesc{filters = [dict:fetch(From, QLCData) ++ " == " ++ dict:fetch(To, QLCData) | Filters]};
where({{_,_} = From, '=', To}, #qhdesc{filters = Filters, bindings = Bindings, metadata = QLCData} = QHDesc) ->
    Var = list_to_atom("Var" ++ integer_to_list(random:uniform(100000))),
	QHDesc#qhdesc{filters = [lists:concat([dict:fetch(From, QLCData), " == ", Var]) | Filters],
                  bindings = erl_eval:add_binding(Var, To, Bindings)};
where({{_,_} = From, 'like', To}, QHDesc) when is_binary(To) ->
    where({From, 'like', erlang:binary_to_list(To)}, QHDesc);
where({{_,_} = From, 'like', To}, #qhdesc{filters = Filters, metadata = QLCData} = QHDesc) ->
    {ok, To1, _RepCount} = regexp:gsub(To, "%", ".*"),
    To2 = "\"^" ++ To1 ++ "$\"",
    QHDesc#qhdesc{filters = ["regexp:first_match(" ++ dict:fetch(From, QLCData) ++ ", " ++ To2 ++ ") /= nomatch" | Filters]};

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

extras({order_by, Attribute}, #qhdesc{metadata = QLCData} = QHDesc) when is_atom(Attribute) ->
    QHDesc#qhdesc{postqh =
		fun(QH, QHOptions) ->
            [Table | _Rest] = dict:fetch(tables, QLCData),
            qlc:keysort(dict:fetch({index,Table,Attribute}, QLCData), QH, QHOptions)
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
min_max(Attribute, #qhdesc{metadata = QLCData} = QHDesc, Options) ->
    [Table | _] = resolve_field(Attribute, QLCData),
    QHDesc1 = QHDesc#qhdesc{postqh = 
		fun(QH, _QHOptions) ->
	        qlc:keysort(dict:fetch({index,Table,Attribute}, QLCData), QH, Options)
        end},
 	QHDesc2 = QHDesc1#qhdesc{posteval = 
     	fun(Results) ->
	        {ok, [{element(dict:fetch({index,Table,Attribute}, QLCData), hd(Results))}]}
        end},
    QHDesc2#qhdesc{expressions = [dict:fetch({alias, Table}, QLCData)]}.
    


% for each table, add the metadata for the table's attributes to the dictionary and then
% add TABLE_ROW_VAR <- mnesia:table(Table) to the dictionary where TABLE_ROW_VAR is the variable
% representing the current row of the table and Table is the table name as an atom
get_qlc_metadata(Table) when is_list(Table) == false ->
    get_qlc_metadata([Table]);
get_qlc_metadata(Tables) when is_list(Tables) ->
    QLCData = dict:store(tables, [], dict:new()),
    QLCData1 = dict:store(aliases, [], QLCData),
    get_qlc_metadata(Tables, QLCData1).

get_qlc_metadata([Table | Tables], QLCData) when is_tuple(Table) == false ->
    get_qlc_metadata({Table, 'as', httpd_util:to_upper(atom_to_list(Table))}, Tables, QLCData);
get_qlc_metadata([{_, 'as', _} = Table | Tables], QLCData) ->
    get_qlc_metadata(Table, Tables, QLCData);
get_qlc_metadata([], QLCData) ->
    QLCData.

get_qlc_metadata({Table, 'as', Alias}, Tables, QLCData) ->
    QLCData1 = dict:store({alias, Table}, Alias, QLCData),
    QLCData2 = dict:store({table, Alias}, Table, QLCData1),
    QLCData3 = dict:store({new_record, Table}, {Table}, QLCData2),
    QLCData4 = get_qlc_metadata(mnesia:table_info(Table, attributes), 2, Table, Alias, QLCData3),
    MnesiaTable = lists:concat([Alias, " <- mnesia:table(", Table, ")"]),
	QLCData5 = dict:store(Table, MnesiaTable, QLCData4),
    QLCData6 = dict:store(tables, dict:fetch(tables, QLCData5) ++ [Table], QLCData5), 
    QLCData7 = dict:store(aliases, dict:fetch(aliases, QLCData6) ++ [Alias], QLCData6), 
	get_qlc_metadata(Tables, QLCData7).

    
% for each table attribute (column), create the following: "element(Alias, AttributeIndex)"
% where Alias is the variable representing the current row of the table
% and AttributeIndex is the attribute's index in the record tuple.
% put the formed string into the dictionary using the key {Table, Attribute}
% where Table is the atom of the table and attribute is the atom of the attribute
get_qlc_metadata([Attribute | Attributes], AttributeIndex, Table, Alias, QLCData) ->
    Data = "element(" ++ integer_to_list(AttributeIndex) ++ ", " ++ Alias ++ ")",
    QLCData1 = dict:store({Table,Attribute}, Data, QLCData),
    QLCData2 = dict:store({Alias,Attribute}, Data, QLCData1),
    QLCData3 = dict:store({index,Table,Attribute}, AttributeIndex, QLCData2),
    TableRecord = dict:fetch({new_record, Table}, QLCData3),
    QLCData4 = dict:store({new_record, Table}, erlang:append_element(TableRecord, undefined), QLCData3),
    get_qlc_metadata(Attributes, AttributeIndex + 1, Table, Alias, QLCData4);
get_qlc_metadata([], _AttributeIndex, _Table, _Alias, QLCData) ->
    QLCData.


resolve_field(From, QLCData) ->
    resolve_field(From, dict:fetch(tables, QLCData), QLCData).

resolve_field(From, Tables, QLCData) when is_list(From) ->
    resolve_field(list_to_atom(From), Tables, QLCData);
resolve_field(From, Tables, QLCData) ->
	lists:foldl(
    	fun(Table, Acc) -> case dict:is_key({Table,From}, QLCData) of true -> [Table | Acc]; _ -> Acc end end, 
		[],
		Tables).


write(Record, Attribute, Value, QLCData) when is_list(Attribute) == false ->
	write(Record, [Attribute], [Value], QLCData);
write(Record, [Attribute | Attributes], [Value | Values], QLCData) ->
    AttributeIndex = dict:fetch({index, element(1, Record), Attribute}, QLCData),
    Record1 = setelement(AttributeIndex, Record, Value),
    write(Record1, Attributes, Values, QLCData);
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
%% @spec select(Statement::statement(), Options:options()) ->
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
%% @spec get_last_insert_id(Options::options()) -> term()
get_last_insert_id(_Options) ->
    Val = get(mnesia_last_insert_id),
    {ok, Val}.
