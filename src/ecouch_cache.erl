-module(ecouch_cache).

%% Include files

-include("ecouch.hrl").

%% Exported functions

-export([
    load_raw/1,

    tab/1,

    open/1,
    load/2,
    save/2,

    seq_id/1,
    set_seq_id/2,

    update/2,
    replace/2,

    get/2,
    get_all_ids/1,
    fold/3,
    get_all_revs/1
]).

-define(seq_id, <<"__seq_id">>).

%% API

-spec load_raw(string()) -> [{binary(), json()}].

load_raw(Path) ->
    {ok, Binary} = file:read_file(Path),
    {ok, Map} = ecouch_utils:try_decode(Binary),
    Map1 = maps:remove(?seq_id, Map),
    maps:values(Map1).

tab(Name) ->
    ecouch_resolver:cache(Name).

open(Name) ->
    ets:new(Name, [protected, set, {keypos, 1}]).

load(Tab, Path) ->
    case file:read_file(Path) of
        {ok, Binary} ->
            case ecouch_utils:try_decode(Binary) of
                {ok, Map} ->
                    ets:insert(Tab, maps:to_list(Map));
                _ ->
                    ignore
            end;
        _ ->
            ignore
    end.

save(Tab, Path) ->
    ok = filelib:ensure_dir(Path),
    List = ets:tab2list(Tab),
    Map = maps:from_list(List),
    file:write_file(Path, ecouch_utils:encode(Map)).

seq_id(Tab) ->
    case ets:lookup(Tab, ?seq_id) of
        [{_, SeqId}] -> SeqId;
        _ -> undefined
    end.

set_seq_id(Tab, SeqId) ->
    ets:insert(Tab, {?seq_id, SeqId}).

update(Tab, Actions) ->
    [case Action of
        {deleted, DocId} ->
            ets:delete(Tab, DocId);
        {changed, DocId, Doc} ->
            ets:insert(Tab, {DocId, Doc})
    end || Action <- Actions],
    ok.

replace(Tab, Actions) ->
    List = [{DocId, Doc} || {changed, DocId, Doc} <- Actions],
    AllIds = get_all_ids(Tab),
    {NewIds, _} = lists:unzip(List),
    ToRemove = AllIds -- NewIds,
    ets:insert(Tab, List),
    [ets:delete(Tab, DocId) || DocId <- ToRemove],
    ok.

-spec get(ets:tab(), binary()) -> json() | 'undefined'.

get(Tab, Id) ->
    case ets:lookup(Tab, Id) of
        [{_, Doc}] -> Doc;
        _ -> undefined
    end.

-spec get_all_ids(ets:tab()) -> [binary()].

get_all_ids(Tab) ->
    lists:flatten(ets:match(Tab, {'$1', '_'})) -- [?seq_id].

get_all_revs(Tab) ->
    ets:foldl(fun
        ({?seq_id, _}, Acc) ->
            Acc;
        ({DocId, Doc}, Acc) ->
            [{DocId, maps:get(<<"_rev">>, Doc)} | Acc]
    end, [], Tab).

-spec fold(ets:tab(), Function, T) -> T when
      Function :: fun((binary(), json(), T) -> T).

fold(Tab, Function, Acc0) ->
    ets:foldl(fun
        ({?seq_id, _}, Acc) -> Acc;
        ({Id, Doc}, Acc) -> Function(Id, Doc, Acc)
    end, Acc0, Tab).

%% Local functions
