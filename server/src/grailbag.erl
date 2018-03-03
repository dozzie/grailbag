%%%---------------------------------------------------------------------------
%%% @doc
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag).

%% public interface
-export([timestamp/0]).

-export_type([artifact_id/0, artifact_type/0, artifact_info/0]).
-export_type([file_size/0, body_hash/0, token/0, tag/0, tag_value/0]).
-export_type([timestamp/0, ctime/0, mtime/0]).
-export_type([schema/0]).

%%%---------------------------------------------------------------------------
%%% types

-type artifact_id() :: binary().
%% UUID in its hex form. See {@link grailbag_uuid:format/1}.

-type artifact_type() :: binary().

-type artifact_info() ::
  {artifact_id(), artifact_type(),
    file_size(), body_hash(),
    ctime(), mtime(),
    [{tag(), tag_value()}], [token()],
    valid | has_errors}.

-type file_size() :: non_neg_integer().

-type body_hash() :: binary().

-type token() :: binary().

-type tag() :: binary().

-type tag_value() :: binary().

-type schema() :: term(). % TODO: define structure

-type timestamp() :: integer().

-type ctime() :: timestamp().
-type mtime() :: timestamp().

%%%---------------------------------------------------------------------------
%%% public interface
%%%---------------------------------------------------------------------------

%% @doc Return current OS time.

-spec timestamp() ->
  timestamp().

timestamp() ->
  {MSec, Sec, _USec} = os:timestamp(),
  MSec * 1000000 + Sec.

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
