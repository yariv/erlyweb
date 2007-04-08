-module(make_test).
-export([start/0]).

start() ->
    make:all(),
    filelib:fold_files("erltl/",
		       ".+\.et$",
		       true,
		       fun(F, _Acc) ->
			       erltl:compile(F,
					     [{outdir, "../ebin"},
					      debug_info,
					      show_errors,
					      show_warnings])
		       end,
		       []).
