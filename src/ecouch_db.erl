-module(ecouch_db).

%% Include files

-include("ecouch.hrl").

%% Exported functions

-export([
    restore_by_db/2,
    restore_by_config/2,

    restore/4,
    restore/6,
    restore/3
]).

%% API

-spec restore_by_db(string(), ecouch:db()) -> ok.

restore_by_db(Path, Db) ->
    List = ecouch_cache:load_raw(Path),
    #db{name = DbName, server = Server} = Db,
    [ecouch_cdb_api:save_document(Server, Doc, DbName, maps:get(<<"_id">>, Doc)) || Doc <- List],
    ok.

-spec restore_by_config(string(), [{atom(), term()}]) -> ok.

restore_by_config(Path, Config) ->
    Db = ecouch_utils:db(Config),
    restore_by_db(Path, Db).

-spec restore(string(), string(), string(), integer(), string(), string()) -> ok.

restore(Path, DbName, Host, Port, User, Pwd) ->
    restore_by_config(Path, [
        {db, DbName},
        {host, Host},
        {port, Port},
        {options, [{basic_auth, {User, Pwd}}]}
    ]).

-spec restore(string(), string(), string(), integer()) -> ok.

restore(Path, DbName, Host, Port) ->
    restore_by_config(Path, [
        {db, DbName},
        {host, Host},
        {port, Port}
    ]).

-spec restore(string(), string(), string()) -> ok.

restore(Path, DbName, Host) ->
    restore_by_config(Path, [
        {db, DbName},
        {host, Host}
    ]).

%% Local functions
