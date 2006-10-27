-module(erlyweb_view).
-author("Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com)").
-compile(export_all).

post_render(Module, Rendered) ->
    [{p, [], [Module, <<" controller header">>]},
     Rendered,
     {p, [], [Module, <<" controller footer">>]}].
	     
	     
list(Module,Records) ->
    Fields = Module:used_fields(),
    [{a, [{href, "/foo/project/new"}], "create new"},{br}, {br},
     {table, [],
     [{tr, [],
       [{th, [], atom_to_list(Field)} || Field <- Fields]} |
      [{tr,[],
	[{td, [],
	  format(Module:Field(Record))} || Field <- Fields]}
       || Record <- Records]]}].


%edit(Record) ->
%    Fields = Module:used_fields(),
%    {ehtml, element(1, Record)
       
format(Val) when is_binary(Val) -> Val;
format(Val) -> io_lib:format("~p", [Val]).
    
    
    
    
