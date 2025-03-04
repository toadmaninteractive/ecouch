-module(ecouch_app).

-behaviour(application).

%% Include files

%% Exported functions

-export([
	start/2,
	stop/1,
    prep_stop/1
]).

%% API

start(_StartType, _StartArgs) ->
    case ecouch_sup:start_link() of
        {ok, Pid} ->
            ecouch:add_all(),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    ok.

prep_stop(State) ->
    [ecouch_server:save(Pid) || Pid <- ecouch_server_sup:all()],
    State.

%% Local functions
