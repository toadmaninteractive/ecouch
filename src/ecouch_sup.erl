-module(ecouch_sup).

-behaviour(supervisor).

%% Include files

-include("ecouch.hrl").

%% Exported functions

-export([
	start_link/0
]).

%% Supervisor callbacks

-export([
    init/1
]).

%% API

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Supervisor callbacks

init([]) ->
    ServerSup = #{
        id => ecouch_server_sup,
        start => {ecouch_server_sup,start_link,[]},
        restart => permanent,
        shutdown => 2000,
        type => supervisor,
        modules => [ecouch_server_sup]
    },
    Resolver = #{
        id => ecouch_resolver,
        start => {ecouch_resolver,start_link,[]},
        restart => permanent,
        shutdown => 2000,
        type => worker,
        modules => [ecouch_resolver]
    },
    Flags = #{
        strategy => one_for_one,
        intensity => 3,
        period => 10
    },
    {ok, {Flags, [ServerSup, Resolver]}}.

%% Local functions
