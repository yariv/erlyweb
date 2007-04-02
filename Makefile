all:
	sh make.sh
	
app: src/erlang-psql-driver/psql.app.src	
	(cd src/erlang-psql-driver && sed "s|Modules|`ls -x -m *.erl | sed 's|.erl||g' | tr \\\n ' '`|g" `basename $<` > ../../ebin/`basename $< .src`)

clean:
	rm ebin/*.beam
	
cleanapp:
	rm -fv ebin/*.app
