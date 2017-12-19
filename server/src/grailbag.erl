%%%---------------------------------------------------------------------------
%%% @doc
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag).

-export_type([artifact_id/0, artifact_type/0, artifact_info/0]).
-export_type([file_size/0, body_hash/0, token/0, tag/0, tag_value/0]).
-export_type([schema/0]).

%%%---------------------------------------------------------------------------
%%% types

-type artifact_id() :: binary().
%% UUID in its hex form. See {@link grailbag_uuid:format/1}.

-type artifact_type() :: binary().

-type artifact_info() ::
  {artifact_id(), artifact_type(),
    file_size(), body_hash(),
    [{tag(), tag_value()}], [token()]}.

-type file_size() :: non_neg_integer().

-type body_hash() :: binary().

-type token() :: binary().

-type tag() :: binary().

-type tag_value() :: binary().

-type schema() :: term(). % TODO: define structure

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
