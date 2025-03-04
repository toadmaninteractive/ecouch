-module(ecouch_resolver).

-behaviour(gen_server).

%% Include files

-include("ecouch.hrl").

%% Exported functions

-export([
	start_link/0,
    add/3,
    find/1,
    all/0,
    cache/1,
    server/1,
    servers/0
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

-record(state, {}).

%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec add(name(), pid(), ets:tab()) -> boolean().

add(Name, Pid, Tab) ->
    gen_server:call(?MODULE, {add, Name, Pid, Tab}).

-spec find(name()) -> {pid(), ets:tab()}.

find(Name) ->
    case ets:lookup(?MODULE, Name) of
        [{_, Pid, Tab}] -> {Pid, Tab};
        _ -> undefined
    end.

cache(Name) ->
    case ets:lookup(?MODULE, Name) of
        [{_, _Pid, Tab}] -> Tab;
        _ -> erlang:error({ecouch_unknown_cache, Name})
    end.

-spec server(name()) -> pid().

server(Name) ->
    case ets:lookup(?MODULE, Name) of
        [{_, Pid, _Tab}] -> Pid;
        _ -> erlang:error({ecouch_unknown_server, Name})
    end.

-spec servers() -> [name()].

servers() ->
    [Name || {Name, _, _} <- ets:tab2list(?MODULE)].

-spec all() -> [{name(), pid(), ets:tab()}].

all() ->
    ets:tab2list(?MODULE).

%% gen_server callbacks

init([]) ->
    ets:new(?MODULE, [named_table, protected, set, {keypos, 1}]),
    {ok, #state{}}.

handle_call({add, Name, Pid, Tab}, _From, State) ->
    case find(Name) of
        undefined ->
            monitor(process, Pid),
            ets:insert(?MODULE, {Name, Pid, Tab}),
            {reply, true, State};
        _ ->
            {reply, false, State}
    end;
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MonitorRef, _Type, Pid, _Info}, State) ->
    case lists:keyfind(Pid, 2, all()) of
        {Name, _, _} ->
            ets:delete(?MODULE, Name);
        _ ->
            ignore
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Local functions
