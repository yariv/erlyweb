erlc make_test.erl
mkdir mnesia
erl -run make_test -pa ../ebin -mnesia dir '"./mnesia"'

