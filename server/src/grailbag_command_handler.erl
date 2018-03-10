%%%---------------------------------------------------------------------------
%%% @doc
%%%   Administrative command handler for Indira.
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_command_handler).

-behaviour(gen_indira_command).

%% gen_indira_command callbacks
-export([handle_command/2]).

%% interface for `grailbag_cli_handler'
-export([format_request/1, parse_reply/2, hardcoded_reply/1]).

%%%---------------------------------------------------------------------------
%%% gen_indira_command callbacks
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% daemon control

%% @private
%% @doc Handle administrative commands sent to the daemon.

handle_command([{<<"command">>, <<"status">>}, {<<"wait">>, false}] = _Command,
               _Args) ->
  case indira:is_started(grailbag) of
    true  -> [{result, running}];
    false -> [{result, stopped}]
  end;
handle_command([{<<"command">>, <<"status">>}, {<<"wait">>, true}] = _Command,
               _Args) ->
  case indira:wait_for_start(grailbag_sup) of
    ok    -> [{result, running}];
    error -> [{result, stopped}]
  end;

handle_command([{<<"command">>, <<"stop">>}] = _Command, _Args) ->
  log_info(stop, "stopping GrailBag daemon", []),
  init:stop(),
  [{result, ok}, {pid, list_to_binary(os:getpid())}];

handle_command([{<<"command">>, <<"reload_config">>}] = _Command, _Args) ->
  log_info(reload, "reloading configuration", []),
  try indira:reload() of
    ok ->
      [{result, ok}];
    {error, reload_not_set} ->
      [{result, error}, {message, <<"not configured from file">>}];
    {error, reload_in_progress} ->
      log_info(reload, "another reload in progress", []),
      [{result, error}, {message, <<"reload command already in progress">>}];
    {error, Message} when is_binary(Message) ->
      log_error(reload, "reload error", [{error, Message}]),
      [{result, error}, {message, Message}];
    {error, Errors} when is_list(Errors) ->
      log_error(reload, "reload errors", [{errors, Errors}]),
      [{result, error}, {errors, Errors}]
  catch
    Type:Error ->
      % XXX: this crash should never happen and is a programming error
      log_error(reload, "reload crash", [
        {crash, Type}, {error, {term, Error}},
        {stack_trace, indira:format_stacktrace(erlang:get_stacktrace())}
      ]),
      [{result, error},
        {message, <<"reload function crashed, check logs for details">>}]
  end;

%%----------------------------------------------------------
%% Log handling/rotation

handle_command([{<<"command">>, <<"reopen_logs">>}] = _Command, _Args) ->
  log_info(reopen_logs, "reopening log files", []),
  case application:get_env(grailbag, error_logger_file) of
    {ok, Path} ->
      case indira_disk_h:reopen(error_logger, Path) of
        ok ->
          [{result, ok}];
        {error, Reason} ->
          Message = iolist_to_binary([
            "can't open ", Path, ": ", indira_disk_h:format_error(Reason)
          ]),
          log_error(reopen_logs, "error_logger reopen error",
                    [{error, Message}]),
          [{result, error}, {message, Message}]
      end;
    undefined ->
      indira_disk_h:remove(error_logger),
      [{result, ok}]
  end;

%%----------------------------------------------------------
%% Erlang networking control

handle_command([{<<"command">>, <<"dist_start">>}] = _Command, _Args) ->
  log_info(dist_start, "starting Erlang networking", []),
  case indira:distributed_start() of
    ok ->
      [{result, ok}];
    {error, Reason} ->
      ErrorDesc = iolist_to_binary(indira:format_error(Reason)),
      log_error(dist_start, "can't setup Erlang networking",
                [{error, ErrorDesc}]),
      [{result, error}, {message, <<"Erlang networking error">>}]
  end;

handle_command([{<<"command">>, <<"dist_stop">>}] = _Command, _Args) ->
  log_info(dist_stop, "stopping Erlang networking", []),
  case indira:distributed_stop() of
    ok ->
      [{result, ok}];
    {error, Reason} ->
      ErrorDesc = iolist_to_binary(indira:format_error(Reason)),
      log_error(dist_stop, "can't shutdown Erlang networking",
                [{error, ErrorDesc}]),
      [{result, error}, {message, <<"Erlang networking error">>}]
  end;

%%----------------------------------------------------------

handle_command(_Command, _Args) ->
  [{result, error}, {message, <<"unrecognized command">>}].

%%%---------------------------------------------------------------------------
%%% interface for `grailbag_cli_handler'
%%%---------------------------------------------------------------------------

%% @doc Encode administrative command as a serializable structure.

-spec format_request(gen_indira_cli:command()) ->
  gen_indira_cli:request().

format_request(status        = Command) -> [{command, Command}, {wait, false}];
format_request(status_wait   =_Command) -> [{command, status}, {wait, true}];
format_request(stop          = Command) -> [{command, Command}];
format_request(reload_config = Command) -> [{command, Command}];
format_request(reopen_logs   = Command) -> [{command, Command}];
format_request(dist_start    = Command) -> [{command, Command}];
format_request(dist_stop     = Command) -> [{command, Command}].

%% @doc Convert a reply to an administrative command to an actionable data.

-spec parse_reply(gen_indira_cli:reply(), gen_indira_cli:command()) ->
  term().

parse_reply([{<<"result">>, <<"ok">>}] = _Reply, _Command) ->
  ok;
parse_reply([{<<"message">>, Reason}, {<<"result">>, <<"error">>}] = _Reply,
            _Command) when is_binary(Reason) ->
  {error, Reason};
parse_reply([{<<"errors">>, [{_,_}|_] = Errors},
              {<<"result">>, <<"error">>}] = _Reply,
            reload_config = _Command) ->
  {error, Errors};

parse_reply([{<<"result">>, <<"running">>}] = _Reply, status = _Command) ->
  running;
parse_reply([{<<"result">>, <<"stopped">>}] = _Reply, status = _Command) ->
  stopped;

parse_reply([{<<"pid">>, Pid}, {<<"result">>, <<"ok">>}] = _Reply,
            stop = _Command) when is_binary(Pid) ->
  {ok, binary_to_list(Pid)};

parse_reply(_Reply, _Command) ->
  {error, <<"unrecognized reply from daemon">>}.

%% @doc Return a structure of specific meaning that could have been returned
%%   by the daemon.

-spec hardcoded_reply(Reply) ->
  gen_indira_cli:reply()
  when Reply :: generic_ok | daemon_stopped.

hardcoded_reply(generic_ok = _Reply) ->
  [{<<"result">>, <<"ok">>}];
hardcoded_reply(daemon_stopped = _Reply) ->
  [{<<"result">>, <<"stopped">>}].

%%%---------------------------------------------------------------------------
%%% helper functions
%%%---------------------------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% operations logging
%%%---------------------------------------------------------------------------

-spec log_info(atom(), grailbag_log:event_message(),
               grailbag_log:event_info()) ->
  ok.

log_info(Command, Message, Context) ->
  grailbag_log:info(command, Message, Context ++ [{command, {term, Command}}]).

-spec log_error(atom(), grailbag_log:event_message(),
                grailbag_log:event_info()) ->
  ok.

log_error(Command, Message, Context) ->
  grailbag_log:warn(command, Message, Context ++ [{command, {term, Command}}]).

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
