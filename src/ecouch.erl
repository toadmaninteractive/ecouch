-module(ecouch).

%% Include files

-include("ecouch.hrl").

%% Exported functions

-export([
    start/0,
    stop/0,

    add_db/2,
    add_all/0,
    servers/0,

    get_doc/2,
    save_doc/2,
    cache/1,
    server/1,
    dbs/0,
    db/1,
    subscribe/0,
    seq_id/1,
    changes/2,

    get/1,
    all_ids/0,
    fold/2
]).

-export_type([
    id/0,
    name/0,
    action/0,
    event/0,
    db/0,
    json/0
]).

%% API

start() ->
    aplib:start_app_recursive(ecouch).

stop() ->
    application:stop(ecouch).

add_db(Name, Config) when is_atom(Name), is_list(Config) ->
    ecouch_server_sup:start_child(Name, Config).

add_all() ->
    [ecouch:add_db(Name, Config) || {Name, Config} <- application:get_env(ecouch, servers, [])].

servers() ->
    ecouch_resolver:servers().

%% Server API

-spec get_doc(name(), id()) -> json().

get_doc(Name, Id) ->
    case ecouch_cache:get(cache(Name), Id) of
        undefined -> erlang:error({ecouch_unknown_doc, Name, Id});
        Obj -> Obj
    end.

-spec save_doc(name(), json()) -> {ok, json()} | {'error', term()}.

save_doc(Name, Doc) ->
    #db{name = Db, server = Server} = db(Name),
    Id = maps:get(<<"_id">>, Doc),
    DocWithRev =
        case ecouch_cache:get(cache(Name), Id) of
            #{<<"_rev">> := Rev} -> maps:put(<<"_rev">>, Rev, Doc);
            _ -> Doc
        end,
    ecouch_cdb_api:save_document(Server, DocWithRev, Db, Id).

-spec cache(name()) -> ets:tab().

cache(Name) ->
    ecouch_resolver:cache(Name).

-spec server(name()) -> pid().

server(Name) ->
    ecouch_resolver:server(Name).

-spec dbs() -> [atom()].

dbs() ->
    [Name || {Name, _Config} <- application:get_env(ecouch, servers, [])].

-spec db(name()) -> #db{}.

db(Name) ->
    ecouch_server:db(Name).

%% events: event()

subscribe() ->
    ecouch_utils:gproc_register().

-spec seq_id(name()) -> non_neg_integer().

seq_id(Name) ->
    ecouch_cache:seq_id(cache(Name)).

-spec changes(name(), Since :: non_neg_integer()) -> json().

changes(Name, Since) ->
    #db{name = DbName, server = Server} = db(Name),
    ecouch_cdb_api:changes(Server, DbName, false, Since).

%% Multi-server API

-spec get(id()) -> [{name(), json()}].

get(Id) ->
    [{Name, ecouch_cache:get(Tab, Id)} || {Name, _, Tab} <- ecouch_resolver:all()].

-spec all_ids() -> [id()].

all_ids() ->
    lists:usort(lists:flatten([ecouch_cache:get_all_ids(Tab) || {_, _, Tab} <- ecouch_resolver:all()])).

-spec fold(Function, T) -> T when
      Function :: fun((name(), id(), json(), T) -> T).

fold(Function, Acc0) ->
    lists:foldl(fun({Name, _, Tab}, Acc) ->
        ecouch_cache:fold(Tab, fun(Id, Json, Acc1) -> Function(Name, Id, Json, Acc1) end, Acc)
    end, Acc0, ecouch_resolver:all()).

%% Local functions
