%%%---------------------------------------------------------------------------
%%% @doc
%%%   Authentication and authorization server.
%%%
%%% @todo Config reload.
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_auth).

-behaviour(gen_server).

%% public interface
-export([authenticate/3]).

%% supervision tree API
-export([start/0, start_link/0]).

%% gen_server callbacks
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([code_change/3]).

-export_type([username/0, password/0, permission/0]).

%%%---------------------------------------------------------------------------
%%% types {{{

-define(RESTART_DELAY,   10000).  % milliseconds
-define(PORT_READ_TIMEOUT, 100).  % milliseconds

-type username() :: string() | binary().

-type password() :: string() | binary().

-type permission() :: create | read | update | delete | tokens.
%% <ul>
%%   <li>`create' -- <i>STORE</i></li>
%%   <li>`read' -- <i>INFO</i>, <i>GET</i>, <i>LIST</i>, <i>WATCH</i></li>
%%   <li>`update' -- <i>UPDATE_TAGS</i> (note that it doesn't cover
%%       tokens)</li>
%%   <li>`delete' -- <i>DELETE</i></li>
%%   <li>`tokens' -- <i>UPDATE_TOKENS</i></li>
%% </ul>

-type request_id() :: non_neg_integer().

-record(state, {
  command :: [string(), ...], % worker's command
  proto :: json | binary,
  workers :: pos_integer(),
  active :: non_neg_integer(),
  ports :: [port()],
  rrports :: [port()],
  request_id :: request_id(),
  requests :: term(),
  restart_timer :: reference() | undefined
}).

%%% }}}
%%%---------------------------------------------------------------------------
%%% public interface
%%%---------------------------------------------------------------------------

%% @doc Authenticate user and, if successful, get user's permissions.
%%
%%   Specific errors are provided only to enable precise logging. The end
%%   users should not be informed of why exactly they were denied the service,
%%   to prevent user enumeration attacks.
%%
%%   `{error,denied}' is a generic authentication error, which includes
%%   authentication worker's crash, protocol error, worker not providing
%%   details on authentication failure, and any other reasons.

-spec authenticate(username(), password(), inet:ip_address() | undefined) ->
  {ok, [Perms]} | {error, Reason}
  when Perms :: {grailbag:artifact_type(), [permission()]},
       Reason :: denied | user_not_found | bad_password.

authenticate(User, Password, IP) ->
  gen_server:call(?MODULE, {authenticate, User, Password, IP}, infinity).

%%%---------------------------------------------------------------------------
%%% supervision tree API
%%%---------------------------------------------------------------------------

%% @private
%% @doc Start auth2 server.

start() ->
  gen_server:start({local, ?MODULE}, ?MODULE, [], []).

%% @private
%% @doc Start auth2 server.

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%---------------------------------------------------------------------------
%%% gen_server callbacks
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% initialization/termination {{{

%% @private
%% @doc Initialize {@link gen_server} state.

init(_Args) ->
  {ok, Command} = application:get_env(auth_script),
  {ok, Proto} = application:get_env(auth_protocol),
  {ok, NWorkers} = application:get_env(auth_workers),
  State = #state{
    command = Command,
    proto = Proto,
    workers = NWorkers,
    active = 0,
    ports = [],
    rrports = [],
    request_id = 0,
    requests = dict:new(),
    restart_timer = undefined
  },
  NewState = schedule_restart(now, State),
  {ok, NewState}.

%% @private
%% @doc Clean up {@link gen_server} state.

terminate(_Arg, _State) ->
  ok.

%% }}}
%%----------------------------------------------------------
%% communication {{{

%% @private
%% @doc Handle {@link gen_server:call/2}.

handle_call({authenticate, _User, _Password, _IP} = _Request, _From,
            State = #state{active = 0}) ->
  % TODO: log the fact that there are no active workers?
  {reply, {error, denied}, State};

handle_call({authenticate, User, Password, IP} = _Request, From,
            State = #state{requests = Reqs, request_id = ReqID, proto = Proto,
                           ports = Ports, rrports = RRPorts}) ->
  try encode_auth_request(ReqID, User, Password, IP, Proto) of
    AuthRequest ->
      case RRPorts of
        [] -> [Port | NewRRPorts] = Ports;
        [Port | NewRRPorts] -> ok
      end,
      try port_command(Port, AuthRequest) of
        _ ->
          NewState = State#state{
            request_id = ReqID + 1,
            rrports = NewRRPorts,
            requests = dict:store({ReqID, Port}, From, Reqs)
          },
          % result will be sent later
          {noreply, NewState}
      catch
        _:_ ->
          % TODO: log this
          % NOTE: ReqID was not really sent, so we can safely reuse it
          NewState = remove_worker(Port, State),
          % TODO: we could send the authentication request to another port
          {reply, {error, denied}, NewState}
      end
  catch
    _:_ ->
      % serialization errors
      {reply, {error, denied}, State}
  end;

%% unknown calls
handle_call(_Request, _From, State) ->
  {reply, {error, unknown_call}, State}.

%% @private
%% @doc Handle {@link gen_server:cast/2}.

%% unknown casts
handle_cast(_Request, State) ->
  {noreply, State}.

%% @private
%% @doc Handle incoming messages.

handle_info(restart_workers = _Message, State) ->
  NewState = start_stop_workers(State),
  {noreply, NewState};

handle_info({Port, {exit_status, _Code}} = _Message, State) ->
  % `Code' > 128 is (most probably) a signal encoded as +128
  % TODO: log this
  NewState = remove_worker(Port, State),
  {noreply, NewState};

handle_info({Port, {data, Data}} = _Message,
            State = #state{requests = Reqs}) ->
  case decode_auth_result(Port, Data) of
    {ReqID, AuthResult} when is_integer(ReqID) ->
      case dict:find({ReqID, Port}, Reqs) of
        {ok, ReplyTo} ->
          case AuthResult of
            {valid, Perms} -> gen_server:reply(ReplyTo, {ok, Perms});
            {invalid, Reason} -> gen_server:reply(ReplyTo, {error, Reason})
          end,
          NewState = State#state{
            requests = dict:erase({ReqID, Port}, Reqs)
          },
          {noreply, NewState};
        error ->
          ignore
      end;
    {error, _Reason} ->
      % protocol error
      % TODO: log this
      NewState = remove_worker(Port, State),
      {noreply, NewState}
  end;

%% unknown messages
handle_info(_Message, State) ->
  {noreply, State}.

%% }}}
%%----------------------------------------------------------
%% code change {{{

%% @private
%% @doc Handle code change.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% }}}
%%----------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% encode/decode authentication delegation protocol

%% @doc Decode authentication result received from port.

-spec decode_auth_result(port(), term()) ->
  {request_id(), AuthResult} | {error, Reason}
  when AuthResult :: {valid, [Perms]}
                   | {invalid, denied | user_not_found | bad_password},
       Perms :: {grailbag:artifact_type(), [permission()]},
       Reason :: protocol | timeout.

decode_auth_result(_Port, Data) when is_binary(Data) ->
  case Data of
    <<ID:64, 0:8, NPerms:32, PermsBin/binary>> ->
      try decode_binary_perms(NPerms, PermsBin) of
        Perms -> {ID, {valid, Perms}}
      catch
        throw:_Reason -> {error, protocol}
      end;
    <<ID:64, 1:8, 0:8>> ->
      {ID, {invalid, denied}};
    <<ID:64, 1:8, 1:8>> ->
      {ID, {invalid, user_not_found}};
    <<ID:64, 1:8, 2:8>> ->
      {ID, {invalid, bad_password}};
    _ ->
      {error, protocol}
  end;
decode_auth_result(_Port, {eol, Line}) ->
  case grailbag_json:decode(Line) of
    {ok, Struct} ->
      try decode_json_auth_result(Struct) of
        {ID, {valid, Perms}} -> {ID, {valid, Perms}};
        {ID, {invalid, Reason}} -> {ID, {invalid, Reason}}
      catch
        _:_ -> {error, protocol}
      end;
    {error, _Reason} ->
      {error, protocol}
  end;
decode_auth_result(Port, {noeol, Part}) ->
  % JSON line is longer than port's buffer, possibly because the user has so
  % many permissions set
  try [Part | receive_until_eol(Port, ?PORT_READ_TIMEOUT)] of
    IOLine -> decode_auth_result(Port, {eol, lists:flatten(IOLine)})
  catch
    throw:timeout -> {error, timeout}
  end.

%%----------------------------------------------------------
%% decoding helpers for binary protocol {{{

%% @doc Decode permissions part of an authentication result mesage (binary
%%   protocol).
%%
%%   Function throws an error value on invalid data.

-spec decode_binary_perms(non_neg_integer(), binary()) ->
  [{grailbag:artifact_type(), [permission()]}] | no_return().

decode_binary_perms(0 = _Count, <<>> = _Perms) ->
  [];
decode_binary_perms(0 = _Count, _Perms) ->
  erlang:throw(excess_data);
decode_binary_perms(Count, <<_Fill:3, C:1,R:1,U:1,D:1,T:1,
                             TLen:16, Type:TLen/binary, Rest/binary>>) ->
  Perms = [
    P ||
    {1, P} <- [{C,create}, {R,read}, {U,update}, {D,delete}, {T,tokens}]
  ],
  [{Type, Perms} | decode_binary_perms(Count - 1, Rest)];
decode_binary_perms(_Count, _Perms) ->
  erlang:throw(bad_format).

%% }}}
%%----------------------------------------------------------
%% decoding helpers for JSON protocol {{{

%% @doc Receive all line fragments from port until EOL is encountered.
%%
%%   The function throws `timeout' atom if no data comes within `Timeout'.

-spec receive_until_eol(port(), timeout()) ->
  iolist() | no_return().

receive_until_eol(Port, Timeout) ->
  receive
    {Port, {data, {noeol, Part} }} ->
      [Part | receive_until_eol(Port, Timeout)];
    {Port, {data, {eol, Part} }} ->
      [Part]
  after Timeout ->
      erlang:throw(timeout)
  end.

%% @doc Decode authentication result to a usable data.
%%
%%   The function crashes on invalid data.

-spec decode_json_auth_result(grailbag_json:struct()) ->
  {request_id(), AuthResult}
  when AuthResult :: {valid, [Perms]}
                   | {invalid, denied | user_not_found | bad_password},
                   Perms :: {grailbag:artifact_type(), [permission()]}.

decode_json_auth_result([{_,_}|_] = Struct) ->
  AuthResultInit = {undefined, undefined, undefined, undefined},
  case lists:foldl(fun valid_json_hash_field/2, AuthResultInit, Struct) of
    {ID, true, Perms, undefined} when ID /= undefined, Perms /= undefined ->
      {ID, {valid, Perms}};
    {ID, false, undefined, undefined} when ID /= undefined ->
      {ID, {invalid, denied}};
    {ID, false, undefined, Reason} when ID /= undefined ->
      {ID, {invalid, Reason}}
  end.

%% @doc Compile data from auth reply (JSON protocol).
%%
%%   {@link lists:foldl/3} callback.
%%
%%   The function crashes on invalid data.

-spec valid_json_hash_field({binary(), grailbag_json:struct()},
                            tuple()) ->
  tuple().

valid_json_hash_field({<<"request">>, ID}, {OldID, Valid, Perms, Reason}) ->
  undefined = OldID,
  true = is_integer(ID),
  true = ID >= 0,
  {ID, Valid, Perms, Reason};
valid_json_hash_field({<<"valid">>, Valid}, {ID, OldValid, Perms, Reason}) ->
  undefined = OldValid,
  true = is_boolean(Valid),
  {ID, Valid, Perms, Reason};
valid_json_hash_field({<<"permissions">>, PermsList},
                      {ID, Valid, OldPerms, Reason}) ->
  undefined = OldPerms,
  Perms = [
    {Type, [perm_name(PN) || PN <- PermNames]} ||
    {Type, PermNames} <- PermsList
  ],
  {ID, Valid, Perms, Reason};
valid_json_hash_field({<<"error">>, Reason}, {ID, Valid, Perms, OldReason}) ->
  undefined = OldReason,
  case Reason of
    null -> {ID, Valid, Perms, denied};
    <<"user">> -> {ID, Valid, Perms, user_not_found};
    <<"password">> -> {ID, Valid, Perms, bad_password}
  end;
valid_json_hash_field({_,_}, Acc) ->
  Acc.

%% @doc Convert permission name from binary string to an atom.
%%
%%   Function crashes on unrecognized or invalid data (e.g. unknown
%%   permissions).

-spec perm_name(binary()) ->
  permission() | no_return().

perm_name(<<"create">>) -> create;
perm_name(<<"read">>)   -> read;
perm_name(<<"update">>) -> update;
perm_name(<<"delete">>) -> delete;
perm_name(<<"tokens">>) -> tokens.

%% }}}
%%----------------------------------------------------------

%% @doc Encode an authentication request for sending to auth worker.

-spec encode_auth_request(request_id(), username(), password(),
                          inet:ip_address() | undefined, json | binary) ->
  iolist().

encode_auth_request(ReqID, User, Password, IP, binary = _Protocol) ->
  Host = case IP of
    undefined -> <<0:8>>;
    {A,B,C,D} -> <<4:8, A:8,B:8,C:8,D:8>>;
    {A,B,C,D,E,F,G,H} -> <<6:8, A:16,B:16,C:16,D:16,E:16,F:16,G:16,H:16>>
  end,
  _Request = [
    <<ReqID:64>>,
    <<(iolist_size(User)):16>>, User,
    <<(iolist_size(Password)):16>>, Password,
    Host
  ];
encode_auth_request(ReqID, User, Password, IP, json = _Protocol) ->
  Host = case IP of
    undefined -> null;
    {_,_,_,_} -> grailbag:format_address(IP);
    {_,_,_,_,_,_,_,_} -> grailbag:format_address(IP)
  end,
  {ok, JSON} = grailbag_json:encode([
    {request, ReqID},
    {user, iolist_to_binary(User)},
    {password, iolist_to_binary(Password)},
    {host, Host}
  ]),
  [JSON, $\n].

%%%---------------------------------------------------------------------------
%%% workers restart timer

%% @doc Schedule a restart timer, unless there is one already active.

-spec schedule_restart(now | timeout(), #state{}) ->
  #state{}.

%schedule_restart(infinity = _Delay, State) ->
%  State;
schedule_restart(now = _Delay, State) ->
  schedule_restart(0, State);
schedule_restart(0 = _Delay, State) ->
  cleanup_restart(State),
  self() ! restart_workers,
  State#state{restart_timer = make_ref()};
schedule_restart(Delay, State = #state{restart_timer = undefined}) ->
  Timer = erlang:send_after(Delay, self(), restart_workers),
  State#state{restart_timer = Timer};
schedule_restart(_Delay, State = #state{restart_timer = Timer})
when is_reference(Timer) ->
  State.

%% @doc Cancel restart timer, in case the auth server received an out-of-band
%%   message.

-spec cleanup_restart(#state{}) ->
  ok.

cleanup_restart(_State = #state{restart_timer = undefined}) ->
  ok;
cleanup_restart(_State = #state{restart_timer = Timer})
when is_reference(Timer) ->
  erlang:cancel_timer(Timer),
  ok.

%%%---------------------------------------------------------------------------
%%% worker ports

%%----------------------------------------------------------
%% start_stop_workers() {{{

%% @doc Start or stop authentication workers, as the numbers of active and
%%   expected workers designate.

-spec start_stop_workers(#state{}) ->
  #state{}.

start_stop_workers(State = #state{active = Active, workers = Workers})
when Active == Workers ->
  cleanup_restart(State),
  State#state{restart_timer = undefined};
start_stop_workers(State = #state{active = Active, workers = Workers,
                                  ports = [Excess | Rest]})
when Active > Workers ->
  try
    port_close(Excess)
  catch
    _:_ -> ignore
  end,
  NewState = State#state{
    active = Active - 1,
    ports = Rest,
    rrports = []
  },
  start_stop_workers(NewState);
start_stop_workers(State = #state{active = Active, workers = Workers,
                                  ports = Ports,
                                  command = [Command | Args], proto = Proto})
when Active < Workers ->
  PortArgs = case Proto of
    json   -> [{args, Args}, exit_status, {line, 4096}];
    binary -> [{args, Args}, exit_status, binary, {packet, 4}]
  end,
  try open_port({spawn_executable, Command}, PortArgs) of
    Port ->
      NewState = State#state{
        active = Active + 1,
        ports = [Port | Ports],
        rrports = []
      },
      start_stop_workers(NewState)
  catch
    error:_Reason ->
      % log the error and schedule a retry
      % TODO: log this
      schedule_restart(?RESTART_DELAY, State)
  end.

%% }}}
%%----------------------------------------------------------
%% remove_worker() {{{

%% @doc Remove a port that crashed and schedule its restart.

-spec remove_worker(port(), #state{}) ->
  #state{}.

remove_worker(CrashedPort, State = #state{requests = Reqs, active = Active,
                                          ports = Ports, rrports = RRPorts}) ->
  % if the worker exited, but had some children that are still running, the
  % port stay opened
  try
    port_close(CrashedPort)
  catch
    _:_ -> ignore
  end,
  case lists:member(CrashedPort, Ports) of
    true ->
      NewPorts = lists:delete(CrashedPort, Ports),
      NewRRPorts = lists:delete(CrashedPort, RRPorts),
      NewReqs = dict:filter(
        fun
          ({_ReqID, Port}, ReplyTo) when Port == CrashedPort ->
            gen_server:reply(ReplyTo, {error, denied}),
            false;
          ({_ReqID, Port}, _) when Port /= CrashedPort ->
            true
        end,
        Reqs
      ),
      NewState = State#state{
        requests = NewReqs,
        active = Active - 1,
        ports = NewPorts,
        rrports = NewRRPorts
      },
      schedule_restart(?RESTART_DELAY, NewState);
    false ->
      State
  end.

%% }}}
%%----------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
