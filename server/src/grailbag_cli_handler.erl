%%%---------------------------------------------------------------------------
%%% @doc
%%%   Module that handles command line operations.
%%%   This includes parsing provided arguments and either starting the daemon
%%%   or sending it various administrative commands.
%%%
%%% @see grailbag_command_handler
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_cli_handler).

-behaviour(gen_indira_cli).

%% interface for daemonizing script
-export([format_error/1]).
-export([help/1]).

%% gen_indira_cli callbacks
-export([parse_arguments/2]).
-export([handle_command/2, format_request/2, handle_reply/3]).

%% interface for grailbag_command_handler
-export([reload/1]).

%%%---------------------------------------------------------------------------
%%% types {{{

-define(ADMIN_COMMAND_MODULE, grailbag_command_handler).
% XXX: `status' and `stop' commands are bound to few specific errors this
% module returns; this can't be easily moved to a config/option
-define(ADMIN_SOCKET_TYPE, indira_unix).

-type config_value() :: binary() | number() | boolean().
-type config_key() :: binary().
-type config() :: [{config_key(), config_value() | config()}].

-record(opts, {
  op :: start | status | stop | reload_config | reopen_logs
      | dist_start | dist_stop,
  admin_socket :: file:filename(),
  options :: [{atom(), term()}],
  args = [] :: [string()]
}).

%%% }}}
%%%---------------------------------------------------------------------------
%%% gen_indira_cli callbacks
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% parse_arguments() {{{

%% @private
%% @doc Parse command line arguments and decode opeartion from them.

parse_arguments(Args, [DefAdminSocket, DefConfig] = _Defaults) ->
  EmptyOptions = #opts{
    admin_socket = DefAdminSocket,
    options = [
      {config, DefConfig}
    ]
  },
  case gen_indira_cli:folds(fun cli_opt/2, EmptyOptions, Args) of
    % irregular operations
    {ok, Options = #opts{op = start,  args = []}} -> {ok, start,  Options};
    {ok, Options = #opts{op = status, args = []}} -> {ok, status, Options};
    {ok, Options = #opts{op = stop,   args = []}} -> {ok, stop,   Options};

    {ok, _Options = #opts{op = _, args = [_|_]}} ->
      {error, {help, too_many_args}};

    {ok, _Options = #opts{op = undefined}} ->
      help;

    {ok, Options = #opts{op = Command, admin_socket = AdminSocket}} ->
      {send, {?ADMIN_SOCKET_TYPE, AdminSocket}, Command, Options};

    {error, {help, _Arg}} ->
      help;

    {error, {_Reason, _Arg} = Error} ->
      {error, {help, Error}}
  end.

%% }}}
%%----------------------------------------------------------
%% handle_command() {{{

%% @private
%% @doc Execute commands more complex than "request -> reply -> print".

handle_command(start = _Command,
               _Options = #opts{admin_socket = Socket, options = CLIOpts}) ->
  ConfigFile = proplists:get_value(config, CLIOpts),
  SASLApp = case proplists:get_bool(debug, CLIOpts) of
    true -> sasl;
    false -> undefined
  end,
  PidFile = proplists:get_value(pidfile, CLIOpts),
  case config_load(ConfigFile) of
    {ok, AppEnv, IndiraOpts, ErrorLoggerLog} ->
      case install_error_logger_handler(ErrorLoggerLog) of
        ok ->
          ok = indira:set_env(grailbag, AppEnv),
          indira:daemonize(grailbag, [
            {listen, [{?ADMIN_SOCKET_TYPE, Socket}]},
            {command, {?ADMIN_COMMAND_MODULE, []}},
            {reload, {?MODULE, reload, [ConfigFile]}},
            {start_before, SASLApp},
            {pidfile, PidFile} |
            IndiraOpts
          ]);
        {error, Reason} ->
          {error, {log_file, Reason}}
      end;
    {error, Reason} ->
      {error, Reason}
  end;

handle_command(status = Command,
               Options = #opts{admin_socket = Socket, options = CLIOpts}) ->
  Timeout = proplists:get_value(timeout, CLIOpts, infinity),
  Opts = case proplists:get_bool(wait, CLIOpts) of
    true  = Wait -> [{timeout, Timeout}, retry];
    false = Wait -> [{timeout, Timeout}]
  end,
  {ok, Request} = format_request(Command, Options),
  case gen_indira_cli:send_one_command(?ADMIN_SOCKET_TYPE, Socket, Request, Opts) of
    {ok, Reply} ->
      handle_reply(Reply, Command, Options);
    {error, {socket, _, Reason}} when Reason == timeout, Wait;
                                      Reason == econnrefused;
                                      Reason == enoent ->
      % FIXME: what to do when a command got sent, but no reply was received?
      % (Reason == timeout)
      Reply = ?ADMIN_COMMAND_MODULE:hardcoded_reply(daemon_stopped),
      handle_reply(Reply, Command, Options);
    {error, Reason} ->
      {error, {send, Reason}} % mimic what `gen_indira_cli:execute()' returns
  end;

handle_command(stop = Command,
               Options = #opts{admin_socket = Socket, options = CLIOpts}) ->
  Timeout = proplists:get_value(timeout, CLIOpts, infinity),
  Opts = [{timeout, Timeout}],
  {ok, Request} = format_request(Command, Options),
  case gen_indira_cli:send_one_command(?ADMIN_SOCKET_TYPE, Socket, Request, Opts) of
    {ok, Reply} ->
      handle_reply(Reply, Command, Options);
    {error, {socket, _, Reason}} when Reason == closed;
                                      Reason == econnrefused;
                                      Reason == enoent ->
      Reply = ?ADMIN_COMMAND_MODULE:hardcoded_reply(generic_ok),
      handle_reply(Reply, Command, Options);
    {error, Reason} ->
      {error, {send, Reason}} % mimic what `gen_indira_cli:execute()' returns
  end.

%% }}}
%%----------------------------------------------------------
%% format_request() + handle_reply() {{{

%% @private
%% @doc Format a request to send to daemon.

format_request(status = _Command, _Options = #opts{options = CLIOpts}) ->
  Request = case proplists:get_bool(wait, CLIOpts) of
    true  -> ?ADMIN_COMMAND_MODULE:format_request(status_wait);
    false -> ?ADMIN_COMMAND_MODULE:format_request(status)
  end,
  {ok, Request};
format_request(Command, _Options) ->
  Request = ?ADMIN_COMMAND_MODULE:format_request(Command),
  {ok, Request}.

%% @private
%% @doc Handle a reply to a command sent to daemon.

handle_reply(Reply, status = Command, _Options) ->
  % `status' and `status_wait' have the same `Command' and replies
  case ?ADMIN_COMMAND_MODULE:parse_reply(Reply, Command) of
    running ->
      println("grailbagd is running"),
      ok;
    stopped ->
      println("grailbagd is stopped"),
      {error, 1};
    {error, Reason} ->
      {error, Reason};
    % for future changes in status detection
    Status ->
      {error, {unknown_status, Status}}
  end;

handle_reply(Reply, stop = Command, _Options = #opts{options = CLIOpts}) ->
  PrintPid = proplists:get_bool(print_pid, CLIOpts),
  case ?ADMIN_COMMAND_MODULE:parse_reply(Reply, Command) of
    {ok, Pid} when PrintPid -> println(Pid), ok;
    {ok, _Pid} when not PrintPid -> ok;
    ok -> ok;
    {error, Reason} -> {error, Reason}
  end;

handle_reply(Reply, reload_config = Command, _Options) ->
  case ?ADMIN_COMMAND_MODULE:parse_reply(Reply, Command) of
    ok -> ok;
    {error, Message} when is_binary(Message) ->
      printerr(["reload error: ", Message]),
      {error, 1};
    {error, Errors} ->
      printerr("reload errors:"),
      lists:foreach(
        % FIXME: `Error' can nominally be a JSON, and in fact it sometimes is
        % something else than binary (e.g. a list of binaries for
        % `tcp_listen'; those will be crammed into a single line)
        fun({Part, Error}) -> printerr(["  ", Part, ": ", Error]) end,
        Errors
      ),
      {error, 1}
  end;

handle_reply(Reply, Command, _Options) ->
  ?ADMIN_COMMAND_MODULE:parse_reply(Reply, Command).

%% }}}
%%----------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% interface for daemonizing script
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% format_error() {{{

%% @doc Convert an error to a printable form.

-spec format_error(Reason :: term()) ->
  iolist() | binary().

%% errors from `parse_arguments()' (own and `cli_opt()' + argument)
format_error({bad_command, Command}) ->
  ["invalid command: ", Command];
format_error({bad_option, Option}) ->
  ["invalid option: ", Option];
format_error({{bad_timeout, Value}, _Arg}) ->
  ["invalid timeout value: ", Value];
format_error(too_many_args) ->
  "too many arguments for this operation";
format_error(too_little_args) ->
  "too little arguments for this operation";

%% errors from `config_load()'
format_error({config, validate, {Section, Key, Line} = _Where}) ->
  ["config error: invalid value of ", format_toml_key(Section, Key),
    " (line ", integer_to_list(Line), ")"];
format_error({config, toml, Reason}) ->
  ["config error: ", toml:format_error(Reason)];
format_error({config, {missing, Section, Key}}) ->
  ["config error: missing required key ", format_toml_key(Section, Key)];
format_error({config, {schema, toml, Message, SchemaFile}}) ->
  ["schema file error: ", SchemaFile, ": ", Message];
format_error({config, {network, no_cert, {_,_} = AddrPort}}) ->
  ["no certificate specified for listen address ",
    format_address(AddrPort)];
format_error({config, {network, no_key, {_,_} = AddrPort}}) ->
  ["no private key specified for listen address ",
    format_address(AddrPort)];
format_error({config, {network, no_cert_key, {_,_} = AddrPort}}) ->
  ["neither certificate nor private key specified for listen address ",
    format_address(AddrPort)];
format_error({config, {network, consult, Message, OptsFile}}) ->
  ["SSL options file reading error: ", OptsFile, ": ", Message];

format_error({log_file, Error}) ->
  ["error opening log file: ", indira_disk_h:format_error(Error)];

%% errors from `gen_indira_cli:execute()'
format_error({send, Error}) ->
  ["command sending error: ", gen_indira_cli:format_error(Error)];
format_error({bad_return_value, _} = Error) ->
  gen_indira_cli:format_error(Error);

%% errors from `indira:daemonize()'
format_error({indira, _} = Error) ->
  indira:format_error(Error);

%% `handle_reply()'
format_error(Reason) when is_binary(Reason) ->
  % error message ready to print to the console
  Reason;
format_error({unknown_status, Status}) ->
  % `$SCRIPT status' returned unrecognized status
  ["unrecognized status: ", format_term(Status)];
format_error(unrecognized_reply) -> % see `?ADMIN_COMMAND_MODULE:parse_reply()'
  "unrecognized reply to the command (programmer's error)";
format_error('TODO') ->
  "command not implemented yet";

format_error(Reason) ->
  ["unrecognized error: ", format_term(Reason)].

%% @doc Serialize an arbitrary term to a single line of text.

-spec format_term(term()) ->
  iolist().

format_term(Term) ->
  io_lib:print(Term, 1, 16#ffffffff, -1).

%% @doc Format TOML path (section + key name) for error message.

-spec format_toml_key([string()], string()) ->
  iolist().

format_toml_key(Section, Key) ->
  [$", [[S, $.] || S <- Section], Key, $"].

%% @doc Format listen address for error message.

-spec format_address({any | inet:hostname(), inet:port_number()}) ->
  iolist().

format_address({any = _Addr, Port} = _AddrPort) ->
  ["*:", integer_to_list(Port)];
format_address({Addr, Port} = _AddrPort) ->
  [Addr, ":", integer_to_list(Port)].

%% }}}
%%----------------------------------------------------------
%% help() {{{

%% @doc Return a printable help message.

-spec help(string()) ->
  iolist().

help(Script) ->
  _Usage = [
    "GrailBag daemon.\n",
    "\n",
    "Daemon control:\n",
    "  ", Script, " [--socket=...] start [--debug] [--config=...] [--pidfile=...]\n",
    "  ", Script, " [--socket=...] status [--wait [--timeout=...]]\n",
    "  ", Script, " [--socket=...] stop [--timeout=...] [--print-pid]\n",
    "  ", Script, " [--socket=...] reload\n",
    "Log handling/rotation:\n",
    "  ", Script, " [--socket=...] reopen-logs\n",
    "Distributed Erlang support:\n",
    "  ", Script, " [--socket=...] dist-erl-start\n",
    "  ", Script, " [--socket=...] dist-erl-stop\n",
    ""
  ].

%% }}}
%%----------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% loading configuration
%%%---------------------------------------------------------------------------

%% @doc Install {@link error_logger} handler that writes to a file.

-spec install_error_logger_handler(file:filename() | undefined) ->
  ok | {error, file:posix() | system_limit}.

install_error_logger_handler(undefined = _ErrorLoggerLog) ->
  indira_disk_h:remove(error_logger),
  ok;
install_error_logger_handler(ErrorLoggerLog) ->
  indira_disk_h:reopen(error_logger, ErrorLoggerLog).

%% @doc Update application environment from config file and instruct
%%   application's processes to reread their parameters.

-spec reload(file:filename()) ->
  ok | {error, binary() | [{atom(), indira_json:struct()}]}.

reload(Path) ->
  case config_load(Path) of
    {ok, AppEnv, IndiraOpts, ErrorLoggerLog} ->
      case install_error_logger_handler(ErrorLoggerLog) of
        ok ->
          case indira:distributed_reconfigure(IndiraOpts) of
            ok ->
              ok = indira:set_env(grailbag, AppEnv),
              ok; % TODO: reload_subsystems();
            {error, Reason} ->
              Message = format_error(Reason),
              {error, iolist_to_binary(Message)}
          end;
        {error, Reason} ->
          Message = format_error({log_file, Reason}),
          {error, iolist_to_binary(Message)}
      end;
    {error, Reason} ->
      Message = format_error(Reason),
      {error, iolist_to_binary(Message)}
  end.

%%----------------------------------------------------------
%% reloading subsystems {{{

%%% Reload all subsystems that need to be told that config was changed.
%
%-spec reload_subsystems() ->
%  ok | {error, [{atom(), indira_json:struct()}]}.
%
%reload_subsystems() ->
%  Subsystems = [logger, tcp_listen, schema],
%  case lists:foldl(fun reload_subsystem/2, [], Subsystems) of
%    [] -> ok;
%    [_|_] = Errors -> {error, lists:reverse(Errors)}
%  end.
%
%%% Workhorse for {@link reload_subsystems/0}.
%
%-spec reload_subsystem(atom(), list()) ->
%  list().
%
%reload_subsystem(logger = Name, Errors) ->
%  {ok, LogHandlers} = application:get_env(grailbag, log_handlers),
%  case grailbag_log:reload(LogHandlers) of
%    {ok, _ReloadCandidates} ->
%      % gen_event handlers that maybe need reloading (neither
%      % `grailbag_stdout_h' nor `grailbag_syslog_h' need it, though, and
%      % they're the only supported handlers at this point)
%      Errors;
%    {error, Reason} ->
%      [{Name, iolist_to_binary(format_term(Reason))} | Errors]
%  end;
%
%reload_subsystem(tcp_listen = Name, Errors) ->
%  case grailbag_tcp_sup:reload() of
%    ok ->
%      Errors;
%    {error, TCPErrors} ->
%      % list of errors, potentially one per socket listener
%      [{Name, [iolist_to_binary(format_term(E)) || E <- TCPErrors]} | Errors]
%  end;
%
% ...

%% }}}
%%----------------------------------------------------------

%% @doc Load configuration file.

-spec config_load(file:filename()) ->
  {ok, AppEnv, IndiraOpts, ErrorLoggerLog} | {error, Reason}
  when AppEnv :: [{atom(), term()}],
       IndiraOpts :: [indira:daemon_option()],
       ErrorLoggerLog :: file:filename() | undefined,
       Reason :: {config, validate, Where :: term()}
               | {config, toml, toml:toml_error()}
               | {config, {missing, toml:section(), toml:key()}}
               | {config, {schema, toml, string(), file:filename()}}
               | {config, {network, consult, string(), file:filename()}}
               | {config, {network, Error :: atom(), AddrPort :: tuple()}}.

config_load(Path) ->
  case toml:read_file(Path, {fun config_validate/4, []}) of
    {ok, Config} ->
      case config_build(Config) of
        {ok, AppEnv, IndiraOpts, ErrorLoggerLog} ->
          {ok, AppEnv, IndiraOpts, ErrorLoggerLog};
        {error, {missing, _Section, _Key} = Reason} ->
          {error, {config, Reason}};
        {error, {schema, toml, _Error, _File} = Reason} ->
          {error, {config, Reason}};
        {error, {network, consult, _Error, _File} = Reason} ->
          {error, {config, Reason}};
        {error, {network, _Error, _AddrPort} = Reason} ->
          {error, {config, Reason}}
      end;
    {error, {validate, Where, _Reason}} ->
      % Reason :: badarg | unrecognized
      % TODO: include in the error reported
      {error, {config, validate, Where}};
    {error, Reason} ->
      {error, {config, toml, Reason}}
  end.

%%----------------------------------------------------------
%% config_build() {{{

%% @doc Build application's and Indira's configuration from values read from
%%   config file.

-spec config_build(toml:config()) ->
  {ok, AppEnv, IndiraOpts, ErrorLoggerLog} | {error, Reason}
  when AppEnv :: [{atom(), term()}],
       IndiraOpts :: [indira:daemon_option()],
       ErrorLoggerLog :: file:filename() | undefined,
       Reason :: {missing, toml:section(), toml:key()}
               | {schema, toml, string(), file:filename()}
               | {network, consult, string(), file:filename()}
               | {network, Error :: atom(), AddrPort :: tuple()}.

config_build(TOMLConfig) ->
  try build_app_env(TOMLConfig) of
    {ok, AppEnv} ->
      IndiraOpts = build_indira_opts(TOMLConfig),
      ErrorLoggerLog = proplists:get_value(error_logger_file, AppEnv),
      {ok, AppEnv, IndiraOpts, ErrorLoggerLog};
    {error, {missing, _Section, _Key} = Reason} ->
      {error, Reason}
  catch
    throw:{error, {missing, _Section, _Key} = Reason} ->
      {error, Reason};
    throw:{error, {schema, toml, _Message, _File} = Reason} ->
      {error, Reason};
    throw:{error, {network, consult, _Message, _File} = Reason} ->
      {error, Reason};
    throw:{error, {network, _Error, _AddrPort} = Reason} ->
      {error, Reason}
  end.

%% @doc Build options for Indira from TOML config.

-spec build_indira_opts(toml:config()) ->
  [{atom(), term()}].

build_indira_opts(TOMLConfig) ->
  make_proplist(TOMLConfig, [], [
    {["erlang"], "node_name", node_name},
    {["erlang"], "name_type", name_type},
    {["erlang"], "cookie_file", cookie},
    {["erlang"], "distributed_immediate", net_start}
  ]).

%% @doc Build application environment from TOML config.

-spec build_app_env(toml:config()) ->
    {ok, AppEnv :: [{atom(), term()}]}
  | {error, {missing, toml:section(), toml:key()}}.

build_app_env(TOMLConfig) ->
  {ok, DefaultEnv} = indira:default_env(grailbag),
  InitEnv = [
    {listen, build_listen_addrs(TOMLConfig)},
    {schema, build_schema(TOMLConfig)} |
    DefaultEnv
  ],
  AppEnv = make_proplist(TOMLConfig, InitEnv, [
    % XXX: [artifacts] section is handled separately by `build_schema()'
    % XXX: [network] section is handled separately by `build_listen_addrs()'
    {["users"], "auth_script",  auth_script},
    {["users"], "protocol",     auth_protocol},
    {["users"], "workers",      auth_workers},
    {["storage"], "data_dir",   data_dir},
    {["logging"], "handlers",   log_handlers},
    {["logging"], "erlang_log", error_logger_file}
  ]),
  % check required options that have no good default values (mainly, paths to
  % things)
  Mandatory = [auth_script, auth_protocol, data_dir],
  case app_env_defined(Mandatory, AppEnv) of
    ok -> {ok, AppEnv};
    % compare `make_proplist()' call above
    {missing, auth_script}   -> {error, {missing, ["users"], "auth_script"}};
    {missing, auth_protocol} -> {error, {missing, ["users"], "protocol"}};
    {missing, data_dir}      -> {error, {missing, ["storage"], "data_dir"}}
  end.

%% @doc Check if all required variables are present in an application
%%   environment that has been built from config.

-spec app_env_defined([atom()], [{atom(), term()}]) ->
  ok | {missing, atom()}.

app_env_defined([] = _Vars, _AppEnv) ->
  ok;
app_env_defined([Key | Rest] = _Vars, AppEnv) ->
  case proplists:is_defined(Key, AppEnv) of
    true -> app_env_defined(Rest, AppEnv);
    false -> {missing, Key}
  end.

%% @doc Build a proplist from values loaded from TOML config.

-spec make_proplist(toml:config(), [{atom(), term()}],
                    [{toml:section(), toml:key(), atom()}]) ->
  [{atom(), term()}].

make_proplist(TOMLConfig, Init, Keys) ->
  lists:foldr(
    fun({Section, Name, EnvKey}, Acc) ->
      case toml:get_value(Section, Name, TOMLConfig) of
        {_Type, Value} -> [{EnvKey, Value} | Acc];
        none -> Acc
      end
    end,
    Init, Keys
  ).

%% }}}
%%----------------------------------------------------------
%% build_listen_addrs() {{{

%% @doc Build a list of options for `listen' application environment
%%   parameter.

-spec build_listen_addrs(toml:config()) ->
  [Listen]
  when Listen :: {Addr :: any | inet:hostname(), Port :: inet:port_number(),
                   Opts :: [proplists:property()]}.

build_listen_addrs(TOMLConfig) ->
  {_, ListenList}  = toml:get_value(["network"], "listen",  TOMLConfig, {data, []}),
  {_, DefCertFile} = toml:get_value(["network"], "cert",    TOMLConfig, {data, none}),
  {_, DefKeyFile}  = toml:get_value(["network"], "key",     TOMLConfig, {data, none}),
  {_, DefOptsFile} = toml:get_value(["network"], "options", TOMLConfig, {data, none}),
  % options to use if nothing better was found (cert/key files set in TOML
  % have precedence over the options file)
  DefOpts = replace_props(DefCertFile, DefKeyFile, consult_file(DefOptsFile)),
  % any address without its section will get the default options
  ListenInit = lists:foldl(
    fun({Addr, Port}, Acc) -> dict:store({Addr, Port}, DefOpts, Acc) end,
    dict:new(),
    ListenList
  ),
  Listen = lists:foldr(
    fun(AddrPort, Acc) ->
      {Addr, Port} = parse_listen(AddrPort),
      {_, CertFile} = toml:get_value(["network", AddrPort], "cert", TOMLConfig, {data, none}),
      {_, KeyFile}  = toml:get_value(["network", AddrPort], "key",  TOMLConfig, {data, none}),
      case toml:get_value(["network", AddrPort], "options", TOMLConfig) of
        {string, OptsFile} ->
          Opts = replace_props(CertFile, KeyFile, consult_file(OptsFile));
        none ->
          Opts = replace_props(CertFile, KeyFile, DefOpts)
      end,
      dict:store({Addr, Port}, Opts, Acc)
    end,
    ListenInit,
    toml:sections(["network"], TOMLConfig)
  ),
  case dict:size(Listen) of
    0 ->
      [listen_entry("localhost", 3255, DefOpts, DefCertFile, DefKeyFile)];
    _ ->
      % XXX: `listen_entry()' may still need to add a cert/key file if only an
      % options file was specified in [network."addr:port"] section and it
      % didn't contain cert or key (if they were, they have the precedence
      % over the defaults from [network] section)
      lists:sort(dict:fold(
        fun({Addr, Port}, Opts, Acc) ->
          [listen_entry(Addr, Port, Opts, DefCertFile, DefKeyFile) | Acc]
        end,
        [],
        Listen
      ))
  end.

%% @doc Create an entry suitable for `listen' environment parameter.
%%
%%   If the entry doesn't have a certificate and/or private key, they're added
%%   from `DefCertFile' and `DefKeyFile' or an error value is thrown.

-spec listen_entry(any | inet:hostname(), inet:port_number(),
                   [proplists:property()], file:filename(), file:filename()) ->
  {Addr :: any | inet:hostname(), Port :: inet:port_number(),
    Opts :: [proplists:property()]}.

listen_entry(Addr, Port, Opts, DefCertFile, DefKeyFile) ->
  case has_cert_key(Opts) of
    {cert, key} ->
      {Addr, Port, Opts};

    {no_cert, key} when DefCertFile /= none ->
      {Addr, Port, [{certfile, DefCertFile} | Opts]};
    {no_cert, key} when DefCertFile == none ->
      erlang:throw({error, {network, no_cert, {Addr, Port} }});

    {cert, no_key} when DefKeyFile /= none->
      {Addr, Port, [{keyfile, DefKeyFile} | Opts]};
    {cert, no_key} when DefKeyFile == none ->
      erlang:throw({error, {network, no_key, {Addr, Port} }});

    {no_cert, no_key} when DefCertFile /= none, DefKeyFile /= none ->
      {Addr, Port, [{certfile, DefCertFile}, {keyfile, DefKeyFile} | Opts]};
    {no_cert, no_key} when DefCertFile == none, DefKeyFile /= none ->
      erlang:throw({error, {network, no_cert, {Addr, Port} }});
    {no_cert, no_key} when DefCertFile /= none, DefKeyFile == none ->
      erlang:throw({error, {network, no_key, {Addr, Port} }});
    {no_cert, no_key} when DefCertFile == none, DefKeyFile == none ->
      erlang:throw({error, {network, no_cert_key, {Addr, Port} }})
  end.

%% @doc Load SSL socket options file.

-spec consult_file(file:filename()) ->
  [proplists:property()].

consult_file(OptsFile) ->
  case file:consult(OptsFile) of
    {ok, Opts} -> Opts;
    {error, enoent} -> [];
    {error, Reason} ->
      Message = file:format_error(Reason),
      erlang:throw({error, {network, consult, Message, OptsFile}})
  end.

%% @doc Replace certificate and/or private key entries in a list of SSL socket
%%   options.

-spec replace_props(file:filename() | none, file:filename() | none,
                    [proplists:property()]) ->
  [proplists:property()].

replace_props(none = _CertFile, none = _KeyFile, Options) ->
  Options;
replace_props(none = _CertFile, KeyFile, Options) ->
  Filtered = lists:filter(
    fun
      ({key,     _}) -> false;
      ({keyfile, _}) -> false;
      (_) -> true
    end,
    Options
  ),
  [{keyfile, KeyFile} | Filtered];
replace_props(CertFile, none = _KeyFile, Options) ->
  Filtered = lists:filter(
    fun
      ({cert,     _}) -> false;
      ({certfile, _}) -> false;
      (_) -> true
    end,
    Options
  ),
  [{certfile, CertFile} | Filtered];
replace_props(CertFile, KeyFile, Options) ->
  Filtered = lists:filter(
    fun
      ({key,      _}) -> false;
      ({keyfile,  _}) -> false;
      ({cert,     _}) -> false;
      ({certfile, _}) -> false;
      (_) -> true
    end,
    Options
  ),
  [{certfile, CertFile}, {keyfile, KeyFile} | Filtered].

%% @doc Check if a list of options has certificate and private key specified,
%%   either in-line or as a path.

-spec has_cert_key([proplists:property()]) ->
  {cert | no_cert, key | no_key}.

has_cert_key(Options) ->
  lists:foldl(
    fun
      ({key,      _}, {HasCert, _HasKey}) -> {HasCert, key};
      ({keyfile,  _}, {HasCert, _HasKey}) -> {HasCert, key};
      ({cert,     _}, {_HasCert, HasKey}) -> {cert, HasKey};
      ({certfile, _}, {_HasCert, HasKey}) -> {cert, HasKey};
      (_, Acc) -> Acc
    end,
    {no_cert, no_key},
    Options
  ).

%% }}}
%%----------------------------------------------------------
%% build_schema() {{{

%% @doc Build a list of artifact schemas, appropriate for setting as
%%   application environment parameter.

-spec build_schema(toml:config()) ->
  [Schema]
  when Schema :: {ArtifactType, MandatoryTags, UniqueTags, KnownTokens},
       ArtifactType :: grailbag:artifact_type(),
       MandatoryTags :: [grailbag:tag()],
       UniqueTags :: [grailbag:tag()],
       KnownTokens :: [grailbag:token()].

build_schema(TOMLConfig) ->
  SchemaMapInit = lists:foldl(
    fun(Name, Acc) ->
      Schema = build_schema(Name, ["artifacts", Name], TOMLConfig),
      dict:store(Name, Schema, Acc)
    end,
    dict:new(),
    toml:sections(["artifacts"], TOMLConfig)
  ),
  case toml:get_value(["artifacts"], "schema", TOMLConfig) of
    {string, SchemaFile} ->
      case toml:read_file(SchemaFile, {fun schema_config_validate/4, []}) of
        {ok, SchemaTOML} ->
          SchemaMap = lists:foldl(
            fun(Name, Acc) ->
              Schema = build_schema(Name, [Name], SchemaTOML),
              dict:store(Name, Schema, Acc)
            end,
            SchemaMapInit,
            toml:sections([], SchemaTOML)
          ),
          lists:sort(dict:fold(fun(_,V,Acc) -> [V | Acc] end, [], SchemaMap));
        {error, Reason} ->
          Message = toml:format_error(Reason),
          erlang:throw({error, {schema, toml, Message, SchemaFile}})
      end;
    none ->
      lists:sort(dict:fold(fun(_,V,Acc) -> [V | Acc] end, [], SchemaMapInit))
  end.

%% @doc Build a single entry for `schema' list in application environment.
%%
%% @see build_schema/1

-spec build_schema(string(), toml:section(), toml:config()) ->
  Schema
  when Schema :: {ArtifactType, MandatoryTags, UniqueTags, KnownTokens},
       ArtifactType :: grailbag:artifact_type(),
       MandatoryTags :: [grailbag:tag()],
       UniqueTags :: [grailbag:tag()],
       KnownTokens :: [grailbag:token()].

build_schema(Name, Section, TOMLConfig) ->
  {_, MandatoryTags} = toml:get_value(Section, "mandatory_tags", TOMLConfig, {data, []}),
  {_, UniqueTags}    = toml:get_value(Section, "unique_tags",    TOMLConfig, {data, []}),
  {_, KnownTokens}   = toml:get_value(Section, "tokens",         TOMLConfig, {data, []}),
  % NOTE: tags and tokens are already binaries and are sorted
  {list_to_binary(Name), MandatoryTags, UniqueTags, KnownTokens}.

%% }}}
%%----------------------------------------------------------
%% schema_config_validate() {{{

%% @doc Validate values read from schema file (TOML; callback for
%%   {@link toml:read_file/2}).

-spec schema_config_validate(Section :: toml:section(), Key :: toml:key(),
                             Value :: toml:toml_value() | section,
                             Arg :: term()) ->
  {ok, term()} | ignore | {error, badarg}.

schema_config_validate([_ArtifactType], Name, section = _Value, _Arg) ->
  case grailbag:valid(type, Name) of
    true -> ignore;
    false -> {error, badarg}
  end;
schema_config_validate([_ArtifactType], "unique_tags", Value, _Arg) ->
  case Value of
    {array, {string, Tags}} ->
      case lists:all(fun(T) -> grailbag:valid(tag, T) end, Tags) of
        true -> {ok, lists:sort([list_to_binary(T) || T <- Tags])};
        false -> {error, badarg}
      end;
    {array, {empty, []}} ->
      {ok, []};
    _ ->
      {error, badarg}
  end;
schema_config_validate([_ArtifactType], "mandatory_tags", Value, _Arg) ->
  case Value of
    {array, {string, Tags}} ->
      case lists:all(fun(T) -> grailbag:valid(tag, T) end, Tags) of
        true -> {ok, lists:sort([list_to_binary(T) || T <- Tags])};
        false -> {error, badarg}
      end;
    {array, {empty, []}} ->
      {ok, []};
    _ ->
      {error, badarg}
  end;
schema_config_validate([_ArtifactType], "tokens", Value, _Arg) ->
  case Value of
    {array, {string, Tokens}} ->
      case lists:all(fun(T) -> grailbag:valid(token, T) end, Tokens) of
        true -> {ok, lists:sort([list_to_binary(T) || T <- Tokens])};
        false -> {error, badarg}
      end;
    {array, {empty, []}} ->
      {ok, []};
    _ ->
      {error, badarg}
  end;
schema_config_validate(_Section, _Key, _Value, _Arg) ->
  ignore.

%% }}}
%%----------------------------------------------------------
%% config_validate() {{{

%% @doc Validate values read from TOML file (callback for
%%   {@link toml:read_file/2}).

-spec config_validate(Section :: toml:section(), Key :: toml:key(),
                      Value :: toml:toml_value() | section, Arg :: term()) ->
  ok | {ok, term()} | ignore | {error, badarg | unrecognized}.

%% [artifacts], artifacts schema file and in-line schema
config_validate(["artifacts"], "schema", Value, _Arg) ->
  valid(path, Value);
config_validate(["artifacts" | Section], Key, Value, Arg) ->
  schema_config_validate(Section, Key, Value, Arg);

%% [users], authentication/authorization subsystem
config_validate(["users"], "auth_script", Value, _Arg) ->
  case Value of
    {string, [_|_] = Script} -> {ok, [Script]};
    {array, {string, [[_|_] = _Script | _] = Command}} -> {ok, Command};
    _ -> {error, badarg}
  end;
config_validate(["users"], "protocol", Value, _Arg) ->
  case Value of
    {string, "binary"} -> {ok, binary};
    {string, "json"} -> {ok, json};
    _ -> {error, badarg}
  end;
config_validate(["users"], "workers", Value, _Arg) ->
  valid(count, Value);

%% [network."addr:port"], listen addresses as subsections
config_validate(["network"], AddrSpec, section, _Arg) ->
  % NOTE: actual parsing of these will be done when Erlang app config will be
  % built
  try parse_listen(AddrSpec) of
    _Addr -> ok
  catch
    _:_ -> {error, badarg}
  end;
config_validate(["network", _ | _], _Name, section, _Arg) ->
  % further subsections are not allowed
  {error, unrecognized};

%% [network], listen addresses as a list
config_validate(["network"], "listen", Value, _Arg) ->
  case Value of
    {array, {string, AddrSpecs}} ->
      try lists:map(fun parse_listen/1, AddrSpecs) of
        Addrs -> {ok, Addrs}
      catch
        _:_ -> {error, badarg}
      end;
    {array, {empty, []}} ->
      {ok, []};
    _ ->
      {error, badarg}
  end;

%% SSL options for [network] and [network."addr:port"] sections
config_validate(["network" | _], "cert",    Value, _Arg) -> valid(path, Value);
config_validate(["network" | _], "key",     Value, _Arg) -> valid(path, Value);
config_validate(["network" | _], "options", Value, _Arg) -> valid(path, Value);

%% [storage]
config_validate(["storage"], "data_dir", Value, _Arg) ->
  valid(path, Value);

%% [logging]
config_validate(["logging"], "handlers", Value, _Arg) ->
  case Value of
    {array, {string, Names}} ->
      {ok, [{list_to_atom(N), []} || N <- Names]};
    {array, {empty, []}} ->
      {ok, []};
    _ ->
      {error, badarg}
  end;
config_validate(["logging"], "erlang_log", Value, _Arg) ->
  valid(path, Value);

%% [erlang]
config_validate(["erlang"], "node_name", Value, _Arg) ->
  case Value of
    {string, [_|_] = Name} -> {ok, list_to_atom(Name)};
    _ -> {error, badarg}
  end;
config_validate(["erlang"], "name_type", Value, _Arg) ->
  case Value of
    {string, "longnames"} -> {ok, longnames};
    {string, "shortnames"} -> {ok, shortnames};
    _ -> {error, badarg}
  end;
config_validate(["erlang"], "cookie_file", Value, _Arg) ->
  case Value of
    {string, [_|_] = Path} -> {ok, {file, Path}};
    _ -> {error, badarg}
  end;
config_validate(["erlang"], "distributed_immediate", Value, _Arg) ->
  case Value of
    {boolean, V} -> {ok, V};
    _ -> {error, badarg}
  end;

%% all the others
config_validate(_Section, _Key, _Value, _Arg) ->
  ignore.

%% @doc Validate a value from config according to its expected format.

-spec valid(Type :: path | count, Value :: toml:toml_value()) ->
  ok | {error, badarg}.

valid(path, {string, [_|_] = _Path}) -> ok;
%valid(time, {integer, T}) when T > 0 -> ok;
valid(count, {integer, T}) when T > 0 -> ok;
valid(_Type, _Value) -> {error, badarg}.

%% @doc Parse listen address.
%%   Function crashes on invalid address format.

-spec parse_listen(string()) ->
  {any | inet:hostname(), inet:port_number()} | no_return().

parse_listen(AddrPort) ->
  case string:tokens(AddrPort, ":") of
    ["*", Port] -> {any, list_to_integer(Port)};
    [Addr, Port] -> {Addr, list_to_integer(Port)}
  end.

%% }}}
%%----------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% various helpers
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% parsing command line arguments {{{

%% @doc Command line argument parser for {@link gen_indira_cli:folds/3}.

-spec cli_opt(string() | [string()], #opts{}) ->
  #opts{} | {error, help | Error}
  when Error :: bad_option | bad_command
              | {bad_timeout, string()}.

cli_opt("-h" = _Arg, _Opts) ->
  {error, help};
cli_opt("--help" = _Arg, _Opts) ->
  {error, help};

cli_opt("--socket=" ++ Socket = _Arg, Opts) ->
  cli_opt(["--socket", Socket], Opts);
cli_opt("--socket" = _Arg, _Opts) ->
  {need, 1};
cli_opt(["--socket", Socket] = _Arg, Opts) ->
  _NewOpts = Opts#opts{admin_socket = Socket};

cli_opt("--pidfile=" ++ Path = _Arg, Opts) ->
  cli_opt(["--pidfile", Path], Opts);
cli_opt("--pidfile" = _Arg, _Opts) ->
  {need, 1};
cli_opt(["--pidfile", Path] = _Arg, Opts = #opts{options = CLIOpts}) ->
  _NewOpts = Opts#opts{options = [{pidfile, Path} | CLIOpts]};

cli_opt("--config=" ++ Path = _Arg, Opts) ->
  cli_opt(["--config", Path], Opts);
cli_opt("--config" = _Arg, _Opts) ->
  {need, 1};
cli_opt(["--config", Path] = _Arg, Opts = #opts{options = CLIOpts}) ->
  _NewOpts = Opts#opts{options = [{config, Path} | CLIOpts]};

cli_opt("--timeout=" ++ Timeout = _Arg, Opts) ->
  cli_opt(["--timeout", Timeout], Opts);
cli_opt("--timeout" = _Arg, _Opts) ->
  {need, 1};
cli_opt(["--timeout", Timeout] = _Arg, Opts = #opts{options = CLIOpts}) ->
  case make_integer(Timeout) of
    {ok, Seconds} when Seconds > 0 ->
      % NOTE: we need timeout in milliseconds
      _NewOpts = Opts#opts{options = [{timeout, Seconds * 1000} | CLIOpts]};
    _ ->
      {error, {bad_timeout, Timeout}}
  end;

cli_opt("--debug" = _Arg, Opts = #opts{options = CLIOpts}) ->
  _NewOpts = Opts#opts{options = [{debug, true} | CLIOpts]};

cli_opt("--wait" = _Arg, Opts = #opts{options = CLIOpts}) ->
  _NewOpts = Opts#opts{options = [{wait, true} | CLIOpts]};

cli_opt("--print-pid" = _Arg, Opts = #opts{options = CLIOpts}) ->
  _NewOpts = Opts#opts{options = [{print_pid, true} | CLIOpts]};

cli_opt("-" ++ _ = _Arg, _Opts) ->
  {error, bad_option};

cli_opt(Arg, Opts = #opts{op = Op, args = OpArgs}) when Op /= undefined ->
  % `++' operator is a little costly, but considering how many arguments there
  % will be, the time it all takes will drown in Erlang startup
  _NewOpts = Opts#opts{args = OpArgs ++ [Arg]};

cli_opt(Arg, Opts = #opts{op = undefined}) ->
  case Arg of
    "start"  -> Opts#opts{op = start};
    "status" -> Opts#opts{op = status};
    "stop"   -> Opts#opts{op = stop};
    "reload" -> Opts#opts{op = reload_config};
    "reopen-logs" -> Opts#opts{op = reopen_logs};
    "dist-erl-start" -> Opts#opts{op = dist_start};
    "dist-erl-stop"  -> Opts#opts{op = dist_stop};
    _ -> {error, bad_command}
  end.

%% @doc Helper to convert string to integer.
%%
%%   Doesn't die on invalid argument.

-spec make_integer(string()) ->
  {ok, integer()} | {error, badarg}.

make_integer(String) ->
  try list_to_integer(String) of
    Integer -> {ok, Integer}
  catch
    error:badarg -> {error, badarg}
  end.

%% }}}
%%----------------------------------------------------------
%% printing lines to STDOUT and STDERR {{{

%% @doc Print a string to STDOUT, ending it with a new line.

-spec println(iolist() | binary()) ->
  ok.

println(Line) ->
  io:put_chars([Line, $\n]).

%% @doc Print a JSON structure to STDOUT, ending it with a new line.

-spec print_json(indira_json:struct()) ->
  ok.

print_json(Struct) ->
  {ok, JSON} = indira_json:encode(Struct),
  io:put_chars([JSON, $\n]).

%% @doc Print a string to STDERR, ending it with a new line.

-spec printerr(iolist() | binary()) ->
  ok.

printerr(Line) ->
  io:put_chars(standard_error, [Line, $\n]).

%% }}}
%%----------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
