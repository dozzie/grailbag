%%%---------------------------------------------------------------------------
%%% @private
%%% @doc
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_sup).

-behaviour(supervisor).

%% supervision tree API
-export([start_link/0]).

%% supervisor callbacks
-export([init/1]).

%%%---------------------------------------------------------------------------
%%% supervision tree API
%%%---------------------------------------------------------------------------

%% @private
%% @doc Start the supervisor process.

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%---------------------------------------------------------------------------
%%% supervisor callbacks
%%%---------------------------------------------------------------------------

%% @private
%% @doc Initialize supervisor.

init([] = _Args) ->
  {ok, LogHandlers} = application:get_env(log_handlers),
  Strategy = {one_for_one, 5, 10},
  Children = [
    {grailbag_log, {grailbag_log, start_link, [LogHandlers]},
      permanent, 5000, worker, [grailbag_log]},
    {grailbag_tcp_sup, {grailbag_tcp_sup, start_link, []},
      permanent, 5000, supervisor, [grailbag_tcp_sup]}
  ],
  {ok, {Strategy, Children}}.

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
