-module(erlydb_mnesia_schema).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([start/0, up/0, down/0]).



-record(language, {
	id,
	name,
	description,
	paradigm,
	creation_year}).


-record(project, {
	id,
	name,
	description,
	language_id}).


-record(person, {
 	id,
	type,
	name,
	age,
	country,
	office,
	department,
	genre,
	instrument,
	created_on}).


-record(person_project, {
	person_id,
	project_id}).
	
	
	
-record(store, {
	number,
	name}).
	

-record(item, {
	size,
	name,
	store_name,
	store_number}).
	
	
-record(customer, {
	id,
	name}).
	
	
-record(customer_store, {
	store_number,
	store_name,
	customer_id}).
	

-record(customer_customer, {
	customer_id1,
	customer_id2}).

%%
%% API Functions
%%
start() ->
    case mnesia:system_info(is_running) of
        no -> mnesia:start();
        _ -> ok 
        % this could fail if system_info returns the atom stopping
    end.


up() ->
    ok = start(),

    mnesia:create_table(counter, [{disc_copies, [node()]}, {attributes, [key, counter]}]),
    
    {atomic, ok} = mnesia:create_table(language, [
   			{disc_copies, [node()]},
   			{attributes, record_info(fields, language)}]),
    {atomic, ok} = mnesia:create_table(project, [
   			{disc_copies, [node()]},	
   			{attributes, record_info(fields, project)}]),
    {atomic, ok} = mnesia:create_table(person, [
   			{disc_copies, [node()]},
   			{attributes, record_info(fields, person)}]),
    {atomic, ok} = mnesia:create_table(person_project, [
   			{type, bag},
            {disc_copies, [node()]},
   			{attributes, record_info(fields, person_project)}]),
    {atomic, ok} = mnesia:create_table(customer, [
            {disc_copies, [node()]},
   			{attributes, record_info(fields, customer)}]),
    {atomic, ok} = mnesia:create_table(store, [
   			{type, bag},
            {disc_copies, [node()]},
   			{attributes, record_info(fields, store)}]),
    {atomic, ok} = mnesia:create_table(item, [
   			{type, bag},
            {disc_copies, [node()]},
   			{attributes, record_info(fields, item)}]),
    {atomic, ok} = mnesia:create_table(customer_store, [
   			{type, bag},
            {disc_copies, [node()]},
   			{attributes, record_info(fields, customer_store)}]),
    {atomic, ok} = mnesia:create_table(customer_customer, [
   			{type, bag},
            {disc_copies, [node()]},
   			{attributes, record_info(fields, customer_customer)}]),   			
    ok.


down() ->
    ok = start(),
    mnesia:delete_table(counter),
    
    mnesia:delete_table(language),
    mnesia:delete_table(project),
    mnesia:delete_table(person),
    mnesia:delete_table(person_project),
    mnesia:delete_table(customer),
    mnesia:delete_table(store),
    mnesia:delete_table(item),
    mnesia:delete_table(customer_store),
    mnesia:delete_table(customer_customer),
    ok.

	