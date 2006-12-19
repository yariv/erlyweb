-module(test_erltl).
-export([test/0, test/1]).

test() ->
    test("album.et").

test(AlbumTemplate) ->
    erltl:compile(AlbumTemplate),
    album:render(
      [{<<"Abbey Road">>, <<"The Beatles">>,
	[{1, <<"Come Together">>},
	 {2, <<"Something">>},
	 {3, <<"Maxwell's Silver Hammer">>},
	 {4, <<"Oh! Darling">>},
	 {5, <<"Octopus's Garden">>},
	 {6, <<"I Want You (She's So Heavy)">>}]}]).
