-module(ecouch_server_sup).

-behaviour(supervisor).

%% Include files

%% Exported functions

-export([
	start_link/0,
    start_child/2,
    all/0,
    count/0
]).

%% Supervisor callbacks

-export([
    init/1
]).

%% API

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Name, Config) ->
    ChildSpec = #{
        id => Name,
        start => {ecouch_server, start_link, [{Name, Config}]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [ecouch_server]
    },
    supervisor:start_child(?MODULE, ChildSpec).

all() ->
    [element(2, A) || A <- supervisor:which_children(?MODULE)].

count() ->
    proplists:get_value(workers, supervisor:count_children(?MODULE)).

%% Supervisor callbacks

init([]) ->
    Flags = #{
        strategy => one_for_one,
        intensity => 1000,
        period => 3600
    },
    {ok, {Flags, []}}.

%% Local functions
