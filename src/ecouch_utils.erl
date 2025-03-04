-module(ecouch_utils).

%% Include files

-include_lib("kernel/include/logger.hrl").
-include("ecouch.hrl").

%% Exported functions

-export([
    log_register/1,
    decode/1,
    try_decode/1,
    encode/1,
    gproc_register/0,
    gproc_send/1,
    db/1,
    server/1
]).

%% API

log_register(Name) ->
    logger:update_process_metadata(#{caption => Name}).

encode(Binary) ->
    jsx:encode(Binary).

decode(Json) ->
    jsx:decode(Json, [return_maps]).

try_decode(Binary) ->
    try
        {ok, decode(Binary)}
    catch
        _:_ ->
            {error, no_json}
    end.

gproc_register() ->
    gproc:reg({p, l, ecouch}).

gproc_send(Msg) ->
    gproc:send({p, l, ecouch}, Msg).

db(Config) ->
    case proplists:get_value(db, Config) of
        undefined ->
            undefined;
        Name ->
            #db{
                name = Name,
                server = server(Config)
            }
    end.

server(Config) ->
    Options = proplists:get_value(options, Config, []),
    {value, {username, Username}, Options1} = lists:keytake(username, 1, Options),
    {value, {password, Password}, Options2} = lists:keytake(password, 1, Options1),
    Headers = [{"Authorization", "Basic " ++ base64:encode_to_string(Username ++ ":" ++ Password)}],
    Protocol = proplists:get_value(protocol, Config, http),
    Prefix = proplists:get_value(prefix, Config, ""),
    Host = proplists:get_value(host, Config, "127.0.0.1"),
    Port = proplists:get_value(port, Config, 5984),
    #server{
        protocol = Protocol,
        options = Options2,
        headers = proplists:get_value(headers, Config, []) ++ Headers,
        base_url = lists:flatten([atom_to_list(Protocol), "://", Host, port(Port), Prefix, "/"])
    }.

%% Local functions

port(443) -> "";
port(80) -> "";
port(Port) -> [":", integer_to_list(Port)].
