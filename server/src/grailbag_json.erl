%%%---------------------------------------------------------------------------
%%% @doc
%%%   JSON serializer and deserializer, jsx-style
%%%   ([http://www.erlang.org/eeps/eep-0018.html EEP-0018]).
%%%
%%%   For deserialized JSON hashes, they are compatible with functions from
%%%   {@link orddict} module.
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_json).

%% public interface
-export([decode/1, encode/1]).

-export_type([json_string/0, struct/0]).

%%%---------------------------------------------------------------------------
%%% type specification/documentation {{{

-type json_string() :: string() | binary().
%% Input formatted as JSON.

-type struct() :: indira_json:struct().

%%% }}}
%%%---------------------------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% public interface
%%%---------------------------------------------------------------------------

%% @doc Serialize jsx structure to JSON string.

-spec encode(struct()) ->
  {ok, json_string()} | {error, badarg}.

encode(Struct) ->
  indira_json:encode(Struct).

%% @doc Decode JSON binary to structure acceptable by {@link encode/1}.
%%
%%   Hashes decoded this way have keys ordered and are usable with {@link
%%   orddict} module.

-spec decode(json_string()) ->
  {ok, struct()} | {error, badarg}.

decode(JSON) ->
  indira_json:decode(JSON).

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
