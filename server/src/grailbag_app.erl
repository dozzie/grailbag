%%%---------------------------------------------------------------------------
%%% @private
%%% @doc
%%%   Application entry point.
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_app).

-behaviour(application).

%% application callbacks
-export([start/2, stop/1]).

%%%---------------------------------------------------------------------------
%%% application callbacks
%%%---------------------------------------------------------------------------

%% @private
%% @doc Start the application

start(_StartType, _StartArgs) ->
  grailbag_sup:start_link().

%% @private
%% @doc Terminate the application

stop(_State) ->
  ok.

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
