%%%---------------------------------------------------------------------------
%%% @private
%%% @doc
%%%   Supervisor for artifact handles.
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_artifact_sup).

-behaviour(supervisor).

%% public interface
-export([spawn_worker/5]).

%% supervision tree API
-export([start_link/0]).

%% supervisor callbacks
-export([init/1]).

%%%---------------------------------------------------------------------------
%%% public interface
%%%---------------------------------------------------------------------------

%% @doc Spawn a new worker process.

spawn_worker(Owner, ID, Path, Type, Tags) ->
  supervisor:start_child(?MODULE, [Owner, ID, Path, Type, Tags]).

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
  Strategy = {simple_one_for_one, 5, 10},
  Children = [
    {undefined, {grailbag_artifact, start_link, []},
      temporary, 1000, worker, [grailbag_artifact]}
  ],
  {ok, {Strategy, Children}}.

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
