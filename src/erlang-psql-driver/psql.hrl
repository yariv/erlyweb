%%%-------------------------------------------------------------------
%%%    BASIC INFORMATION
%%%-------------------------------------------------------------------
%%% @copyright 2006 Erlang Training & Consulting Ltd
%%% @author  Martin Carlson <martin@erlang-consulting.com>
%%% @version 0.0.1
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-record(field, {name,
		table_code,
		field_code,
		type,
		max_length,
		format}).

-define(SQL_BOOLEAN, 16).
-define(SQL_BINARY, 17).
-define(SQL_CHAR, 18).
-define(SQL_BIGINT, 20).
-define(SQL_SMALLINT, 21).
-define(SQL_INT, 23).
-define(SQL_TEXT, 25).
-define(SQL_OID, 26).
-define(SQL_INET, 896).
-define(SQL_INTARRAY, 1007).
-define(SQL_CHARARRAY, 1014).
-define(SQL_VARCHARARRAY, 1015).
-define(SQL_VARCHAR, 1043).
-define(SQL_DATE, 1082).
-define(SQL_TIME, 1083).
-define(SQL_DATETIME, 1114).
-define(SQL_TIMESTAMP, 1184).
-define(SQL_FLOAT, 1700).

