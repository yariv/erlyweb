all:
	sh make.sh
	
app: src/erlang-psql-driver/psql.app.src	
	(cd src/erlang-psql-driver && sed "s|Modules|`ls -x -m *.erl | sed 's|.erl||g' | tr \\\n ' '`|g" `basename $<` > ../../ebin/`basename $< .src`)

docs:
	erl -pa `pwd`/ebin \
	-noshell
	-run edoc_run application "'ErlyWeb'" '"."' '[no_packages]'

clean:
	rm ebin/*.beam
	
cleanapp:
	rm -fv ebin/*.app

cleandocs:
	rm -fv doc/*.html
	rm -fv doc/edoc-info
	rm -fv doc/*.css