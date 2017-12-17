%%%---------------------------------------------------------------------------
%%% @doc
%%%   Syslog log handler for {@link grailbag_log}.
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_syslog_h).

-behaviour(gen_event).

%% gen_event callbacks
-export([init/1, terminate/2]).
-export([handle_event/2, handle_call/2, handle_info/2]).
-export([code_change/3]).

%%%---------------------------------------------------------------------------

-define(SYSLOG_SOCKET, "/dev/log").
-define(FACILITY, daemon).
-define(IDENT, grailbagd).

-record(state, {syslog}).

%%%---------------------------------------------------------------------------
%%% gen_event callbacks
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% initialization/termination {{{

%% @private
%% @doc Initialize event handler.

init(_Args) ->
  case indira_syslog:open_local(?SYSLOG_SOCKET) of
    {ok, Syslog} -> State = #state{syslog = Syslog};
    {error, _}   -> State = #state{}
  end,
  {ok, State}.

%% @private
%% @doc Clean up after event handler.

terminate(_Arg, _State = #state{syslog = undefined}) ->
  ok;
terminate(_Arg, _State = #state{syslog = Syslog}) ->
  indira_syslog:close(Syslog),
  ok.

%% }}}
%%----------------------------------------------------------
%% communication {{{

%% @private
%% @doc Handle {@link gen_event:notify/2}.

handle_event({log, _Pid, _Level, _Type, _Info} = Event,
             State = #state{syslog = undefined}) ->
  % try opening syslog
  case indira_syslog:open_local(?SYSLOG_SOCKET) of
    {ok, Syslog} ->
      NewState = State#state{syslog = Syslog},
      handle_event(Event, NewState);
    {error, _} ->
      % ignore and try again later
      {ok, State}
  end;

handle_event({log, Pid, Level, Type, Info} = _Event,
             State = #state{syslog = Syslog}) ->
  try indira_syslog:send(Syslog, format_event(Pid, Level, Type, Info)) of
    ok ->
      {ok, State};
    {error, _} ->
      indira_syslog:close(Syslog),
      NewState = State#state{syslog = undefined},
      {ok, NewState}
  catch
    % serialization errors
    error:_ -> ignore
  end;

%% unknown events
handle_event(_Event, State) ->
  {ok, State}.

%% @private
%% @doc Handle {@link gen_event:call/2}.

%% unknown calls
handle_call(_Request, State) ->
  {ok, {error, unknown_call}, State}.

%% @private
%% @doc Handle incoming messages.

%% unknown messages
handle_info(_Message, State) ->
  {ok, State}.

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

format_event(_Pid, Level, Type, Info) ->
  Msg = io_lib:format("~s ~s", [Type, grailbag_log:to_string(Info)]),
  Prio = case Level of
    info    -> info;
    warning -> warning;
    error   -> err
  end,
  _SyslogMsg = indira_syslog:format(?FACILITY, Prio, ?IDENT, Msg).

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
