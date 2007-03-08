%%
%% @doc This module contains a few functions useful when working with
%% HTML forms in ErlyWeb.
%%
%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com)]
%% @copyright Yariv Sadan 2006-2007


%% For license information see LICENSE.txt
-module(erlyweb_forms).
-export([to_recs/2, validate/3, validate1/3, validate_rec/2]).

%% @doc to_recs/2 helps process POST requests containing fields that
%% belong to multiple records from one or more ErlyDB models.
%%
%% This function is useful when {@link erlydb_base:new_fields_from_strs/3}
%% isn't sufficient because the latter is only designed to map POST
%% parameters to the fields of a single record.
%%
%% This function expects each form field to be mapped to its corresponding
%% record by being named with a unique prefix identifying
%% the record to which the form field belongs.
%%
%% For example, suppose you have to process an HTML form whose fields
%% represent a house and 2 cars. The house's fields have the
%% prefix "house_" and the cars' fields have the prefixes "car1_" and
%% "car2_". The arg's POST parameters are
%% `[{"house_rooms", "3"}, {"car1_year", "2007"}, {"car2_year", "2006"}]'.
%% With such a setup, calling `to_recs(A, [{"house_", house}, {"car1_", car},
%% {"car2_", car}])'
%% returns the list `[House, Car1, Car2]', where `house:rooms(House) == "3"',
%% `car:year(Car1) == "2007"' and `car:year(Car2) == "2006"'. All other
%% fields are `undefined'.
%%
%% @spec to_recs(A::arg() | [{ParamName::string(), ParamVal::term()}],
%% [{Prefix::string(), Model::atom()}]) -> [Record::tuple()]
to_recs(A, ModelDescs) when is_tuple(A), element(1, A) == arg ->
    to_recs(yaws_api:parse_post(A), ModelDescs);
to_recs(Params, ModelDescs) ->
    Models = 
	[{Prefix, Model, Model:new()} || {Prefix, Model} <- ModelDescs],
    Models1 =
	lists:foldl(
	  fun({Name, Val}, Acc) ->
		  case lists:splitwith(
			 fun({Prefix2, _Module2, _Rec2}) ->
				 not lists:prefix(Prefix2, Name)
			 end, Acc) of
		      {_, []} ->
			  Acc;
		      {First, [{Prefix1, Model1, Rec} | Rest]} ->
			  {_, FieldName} = lists:split(length(Prefix1), Name),
			  Field = erlydb_field:name(Model1:db_field(FieldName)),
			  Val1 = case Val of
				     undefined -> "";
				     _ -> Val
				 end,
			  First ++ [{Prefix1, Model1,
				     Model1:Field(Rec, Val1)} | Rest]
		  end
	  end, Models, Params),
    [element(3, Model3) || Model3 <- Models1].
    
%% @doc validate/3 helps validate the inputs of arbitary forms.
%% It accepts a Yaws arg
%% (or the arg's POST data in the form of a name-value property list), a
%% list of parameter names to validate, and a validation function, and returns
%% a tuple of the form {Values, Errors}.
%% 'Values' contains the list of values for the checked parameters
%% and 'Errors' is a list of errors returned from the validation function.
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
%% For forms that modify or create ErlyDB records, it's generally more
%% convenient to use {@link to_recs/2}.
%%
%% @spec validate(A::arg() | proplist(), Fields::[string()],
%%   Fun::function()) -> {Values::[term()], Errors::[term()]} |
%%   exit({missing_param, Field})
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
%% Fun::function()) -> {Vals, Errs} | exit({missing_params, [string()]}) |
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


%% @doc When a form has fields that correspond to the fields of an ErlyDB
%% record, validate_rec/2 helps validate the values of the record's fields.
%%
%% validate_rec/2 accepts an ErlyDB record and a validation function.
%% It folds over all the fields of the record (obtained by calling
%% {@link erlydb_base:db_field_names/0}), calling the validation function
%% with each field's existing value. The validation function's
%% return value indicates if the field's value is valid,
%% and it may also define the record field's final value.
%%
%% The result of validate_rec/2 is a tuple of the form `{Rec1, Errs}', where
%% the first element is the modified record and the second element is
%% a list of errors accumulated by the calls to the validation function.
%%
%% The validation function takes 3 parameters: the field name (an atom),
%% the current value (this can be any term, but it's usually a string,
%% especially if the record came from {@link to_recs/2}), and the record
%% after folding over all the previous fields. It returns
%% `ok', `{ok, NewVal}', `{error, Err}', or `{error, Err, NewVal}'.
%%
%% validate_rec/2 is especially useful in conjunction with {@link to_recs/2}.
%% A common pattern is to create the records for the submitted form using
%% to_recs/2 and then validate their fields using validate_rec/2.
%%
%% @spec validate_rec(Rec::erlydb_record(), Fun::function()) ->
%%  {Rec1::erlydb_record(), Errs::[term()]}
validate_rec(Rec, Fun) ->
    Module = element(1, Rec),
    {Rec1, Errs} =
	lists:foldl(
	  fun(Field, {Rec1, Errs1} = Acc) ->
		  case Fun(Field,
			   Module:Field(Rec1), Rec1) of
		      ok ->
			  Acc;
		      {ok, NewVal} ->
			  {Module:Field(Rec1, NewVal),
			   Errs1};
		      {error, Err} ->
			  {Rec1, [Err | Errs1]};
		      {error, Err, NewVal} ->
			  {Module:Field(Rec1, NewVal),
			   [Err | Errs1]}
		  end
	  end, {Rec, []}, Module:db_field_names()),
    {Rec1, lists:reverse(Errs)}.
