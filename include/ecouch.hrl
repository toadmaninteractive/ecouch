-record(server, {
    protocol = http     :: atom(),
    base_url            :: string(),
    headers = []        :: [term()],
    options = []        :: [term()]
}).

-type server() :: #server{}.

-record(db, {
    server          :: server(),
    name            :: string()
}).

-type db() :: #db{}.

-record(request, {
    server              :: server(),
    method = get        :: atom(),
    path                :: [term()],
    params = []         :: [{term(), term()}],
    options = []        :: [term()],
    headers = []        :: [{string(), string()}],
    body = []
}).

-type json() :: jsx:json_term().
-type id() :: binary().
-type name() :: term().
-type action() :: {'changed', id(), json()} | {'deleted', id()}.
-type event() :: {'ecouch', name(), action()}.

-define(resolver, ecouch_resolver).
