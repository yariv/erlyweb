#!/bin/bash

erl -noshell -eval "erlyweb:create_app(\"$1\", \"$2\")" \
            -s erlang halt
