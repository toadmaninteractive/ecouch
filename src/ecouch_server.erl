-module(ecouch_server).

-behaviour(gen_server).

%% Include files

-include_lib("kernel/include/logger.hrl").
-include("ecouch.hrl").
-include("ecouch_cdb.hrl").

%% Exported functions

-export([
	start_link/1,
    db/1,
    cache/1,
    save/1
]).

%% gen_server callbacks

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    name        :: ecouch:name(),
    db          :: db(),
    chunks      :: [binary()],
    path        :: string(),
    cache       :: ets:tab(),
    heartbeat   :: time:milliseconds(),
    heartbeat_timeout :: time:milliseconds(),
    heartbeat_timer :: reference() | 'undefined',
    reconnect   :: time:milliseconds(),
    req_id      :: term(),
    stream_pid :: pid()
}).

-define(new_line, 10).

%% API

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

db(Ref) ->
    gen_server:call(pid(Ref), db).

cache(Ref) ->
    gen_server:call(pid(Ref), cache).

save(Ref) ->
    gen_server:call(pid(Ref), save).

pid(Pid) when is_pid(Pid) -> Pid;
pid(Name) -> ecouch_resolver:server(Name).

%% gen_server callbacks

init({Name, Config}) ->
    ecouch_utils:log_register({ecouch, Name}),
    Db = ecouch_utils:db(Config),
    Path = proplists:get_value(cache, Config, default_path(Db)),
    Cache = ecouch_cache:open(Name),
    Heartbeat = proplists:get_value(heartbeat, Config, 30000),
    HeartbeatTimeout = Heartbeat * 3 div 2,
    State = #state{
        name = Name,
        db = Db,
        chunks = [],
        path = Path,
        cache = Cache,
        heartbeat = Heartbeat,
        heartbeat_timeout = HeartbeatTimeout,
        reconnect = proplists:get_value(reconnect, Config, 30000)
    },
    case ecouch_resolver:add(Name, self(), State#state.cache) of
        true ->
            ecouch_cache:load(Cache, Path),
            State1 = request_changes(State),
            {ok, State1};
        false ->
            ?LOG_WARNING(#{what => server_already_exists, name => Name}),
            {stop, normal}
    end.

handle_call(cache, _From, State) ->
    {reply, State#state.cache, State};
handle_call(db, _From, State) ->
    {reply, State#state.db, State};
handle_call(save, _From, State) ->
    do_save(State),
    {reply, ok, State};
handle_call(Request, _From, State) ->
    ?LOG_WARNING(#{what => unknown_call, msg => Request}),
    Reply = ok,
    {reply, Reply, State}.

handle_cast(Msg, State) ->
    ?LOG_WARNING(#{what => unknown_cast, msg => Msg}),
    {noreply, State}.

handle_info({http, {StreamId, stream_start, _Headers, Pid}} = _StreamInfo, State) when State#state.req_id =:= StreamId ->
    % ?LOG_DEBUG(#{what => ecouch_stream, event => stream_start, info => _StreamInfo}),
    httpc:stream_next(Pid),
    State1 = State#state{stream_pid = Pid},
    State2 = start_heartbeat_timer(State1),
    {noreply, State2};
handle_info({http, {StreamId, stream, Body}} = _StreamInfo, State) when State#state.req_id =:= StreamId ->
    % ?LOG_DEBUG(#{what => ecouch_stream, event => stream, info => _StreamInfo}),
    httpc:stream_next(State#state.stream_pid),
    State1 = add_chunk(State, Body),
    State2 = start_heartbeat_timer(State1),
    {noreply, State2};
handle_info({http, {StreamId, stream_end, _Headers}} = StreamInfo, State) when State#state.req_id =:= StreamId ->
    ?LOG_DEBUG(#{what => ecouch_stream, event => stream_end, info => StreamInfo}),
    State1 = reset(State),
    {noreply, State1};
handle_info({http, {StreamId, {error, Error}}} = StreamInfo, State) when State#state.req_id =:= StreamId ->
    ?LOG_ERROR(#{what => ecouch_stream_error, error => error, reason => Error, info => StreamInfo}),
    State1 = reset(State),
    {noreply, State1};
handle_info(reconnect, State) ->
    State1 = request_changes(State),
    {noreply, State1};
handle_info(heartbeat_timeout, State) ->
    ?LOG_ERROR(#{what => ecouch_stream_error, error => error, reason => heartbeat_timeout}),
    State1 = reset(State),
    {noreply, State1};
handle_info(Info, State) ->
    ?LOG_WARNING(#{what => unknown_info, msg => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Local functions

default_path(#db{name = Name}) ->
    "var/cache/" ++ Name ++ "-" ++ integer_to_list(erlang:crc32(atom_to_list(node())));
default_path(undefined) ->
    "var/cache/default".

do_save(State) ->
    #state{cache = Tab, path = Path} = State,
    ecouch_cache:save(Tab, Path).

do_save2(State, SeqId) ->
    #state{cache = Tab, path = Path} = State,
    ecouch_cache:set_seq_id(Tab, SeqId),
    ecouch_cache:save(Tab, Path).

verify_cache(State, LastSeq) ->
    #state{cache = Tab, db = #db{name = Database, server = Server}} = State,
    #cdb_changes{results = Results} = ecouch_cdb_api:changes(Server, Database, false, undefined),
    Revs = parse_revs(Results, LastSeq),
    case lists:sort(ecouch_cache:get_all_revs(Tab)) =:= lists:sort(Revs) of
        true ->
            true;
        false ->
            ?LOG_INFO(#{what => cache_outdated, database => Database}),
            false
    end.

prefetch(State) ->
    #state{cache = Tab, db = #db{name = Database, server = Server}} = State,
    SeqId = ecouch_cache:seq_id(Tab),
    ?LOG_INFO(#{what => prefetch, database => Database, seq_id => SeqId}),
    #cdb_changes{last_seq = LastSeq, results = Results} = ecouch_cdb_api:changes(Server, Database, true, string_seq_id(SeqId)),
    Actions = [Action || Val <- Results, {_, Action} <- [feed_data(Val)]],
    ecouch_cache:update(Tab, Actions),
    case verify_cache(State, LastSeq) of
        true ->
            do_save2(State, LastSeq),
            do_send_actions(State, Actions);
        false ->
            ets:delete_all_objects(Tab),
            if
                % SeqId must be set, otherwise we can go to infinity loop
                SeqId =:= undefined -> erlang:error(prefetch_verify_failed);
                true -> ignore
            end,
            prefetch(State)
    end.

reconnect(#state{reconnect = Reconnect, db = #db{name = Name}}) ->
    ?LOG_WARNING(#{what => schedule_reconnect, database => Name, 'after' => Reconnect}),
    timer:send_after(Reconnect, reconnect).

request_changes(State) when State#state.db =/= undefined  ->
    try
        prefetch(State),
        ReqId = stream_changes(State),
        State1 = State#state{req_id = ReqId},
        start_heartbeat_timer(State1)
    catch
        E:Reason:Stacktrace ->
            ?LOG_ERROR(#{what => prefetch, error => E, reason => Reason}, #{stacktrace => Stacktrace}),
            reconnect(State),
            State
    end;
request_changes(State) ->
    State.

stream_changes(#state{cache = Tab, heartbeat = Heartbeat, db = Db}) ->
    Since = ecouch_cache:seq_id(Tab),
    #db{server = Server, name = Name} = Db,
    ?LOG_DEBUG(#{what => stream_changes, cache => Tab, heartbeat => Heartbeat, db => Db}),
    ecouch_cdb_api:stream_changes(Server, Name, true, string_seq_id(Since), Heartbeat).

add_chunk(State, <<>>) ->
    State;
add_chunk(State, Body) when is_binary(Body) ->
    case binary:split(Body, <<?new_line>>) of
        [Chunk] ->
            State#state{chunks = [Chunk|State#state.chunks]};
        [Chunk, TailChunk] ->
            State1 = State#state{chunks = [Chunk|State#state.chunks]},
            State2 = chunk_completed(State1),
            add_chunk(State2, TailChunk)
    end;
add_chunk(State, Error) ->
    ?LOG_ERROR(#{what => "add_chunk failed", error => error, reason => Error}),
    State.

chunk_completed(State) ->
    #state{chunks = Chunks} = State,
    State1 = State#state{chunks = []},
    case re:replace(iolist_to_binary(lists:reverse(Chunks)), "^\\s+|\\s+$", "", [{return, binary}, global]) of
        <<>> ->
            State1;
        Bin ->
            case ecouch_utils:try_decode(Bin) of
                {ok, Json} ->
                    feed(State, Json),
                    State1;
                {error, Reason} ->
                    ?LOG_WARNING(#{what => "stream wrong json", error => error, reason => Reason, value => Bin}),
                    reset(State1)
            end
    end.

reset(State) ->
    reconnect(State),
    State1 = cancel_request(State),
    stop_heartbeat_timer(State1).

feed(State, Json) ->
    case maps:get(<<"seq">>, Json) =/= undefined of
        true ->
            #state{cache = Tab} = State,
            {NewSeqId, Action} = feed_data(ecouch_cdb:cdb_change_from_json(Json)),
            ecouch_cache:update(Tab, [Action]),
            ecouch_cache:set_seq_id(Tab, NewSeqId),
            do_send_actions(State, [Action]),
            do_save(State);
        false ->
            ?LOG_ERROR(#{what => "invalid_feed", value => Json})
    end.

feed_data(#cdb_change{deleted = true, id = Id, seq = Seq}) ->
    ?LOG_INFO(#{what => document_deleted, id => Id}),
    {Seq, {deleted, Id}};
feed_data(#cdb_change{id = Id, seq = Seq, doc = Doc}) ->
    ?LOG_INFO(#{what => document_changed, id => Id}),
    {Seq, {changed, Id, Doc}}.

parse_revs([#cdb_change{seq = CurSeq} = Change | _], LastSeq) when CurSeq =:= LastSeq ->
    % stop parsing
    parse_revs([Change], undefined);
parse_revs([#cdb_change{deleted = true} | Tail], LastSeq) ->
    % ignore deleted
    parse_revs(Tail, LastSeq);
parse_revs([#cdb_change{id = Id, changes = [#cdb_change_leaf{rev = Rev} | _]} | Tail], LastSeq) ->
    [{Id, Rev} | parse_revs(Tail, LastSeq)];
parse_revs([], _) ->
    [].

do_send_actions(State, Actions) ->
    #state{name = Name} = State,
    [ecouch_utils:gproc_send({ecouch, Name, Action}) || Action <- Actions],
    ok.

string_seq_id(SeqId) when is_integer(SeqId) ->
    integer_to_binary(SeqId);
string_seq_id(SeqId) ->
    SeqId.

stop_heartbeat_timer(#state{heartbeat_timer = undefined} = State) -> State;
stop_heartbeat_timer(#state{heartbeat_timer = Timer} = State) ->
    erlang:cancel_timer(Timer),
    State#state{heartbeat_timer = undefined}.

start_heartbeat_timer(State) ->
    State1 = stop_heartbeat_timer(State),
    Timer = erlang:send_after(State#state.heartbeat_timeout, self(), heartbeat_timeout),
    State1#state{heartbeat_timer = Timer}.

cancel_request(#state{req_id = undefined} = State) -> State;
cancel_request(#state{req_id = RequestId} = State) ->
    httpc:cancel_request(RequestId),
    State#state{req_id = undefined, stream_pid = undefined}.
