%%%---------------------------------------------------------------------------
%%% @doc
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag).

%% public interface
-export([timestamp/0]).
-export([format_address/1]).

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

%% @doc Make a printable string (binary) from an IP address.

-spec format_address(inet:ip_address()) ->
  binary().

format_address({A,B,C,D} = _Addr) ->
  iolist_to_binary([
    integer_to_list(A), $., integer_to_list(B), $.,
    integer_to_list(C), $., integer_to_list(D)
  ]);
format_address({0, 0, 0, 0, 0, 16#ffff, A, B} = _Addr) ->
  iolist_to_binary([
    "::ffff:",
    integer_to_list(A div 256), $., integer_to_list(A rem 256), $.,
    integer_to_list(B div 256), $., integer_to_list(B rem 256)
  ]);
format_address({_,_,_,_,_,_,_,_} = Addr) ->
  list_to_binary(string:to_lower(format_ipv6(Addr))).

%% @doc Present IPv6 address in its shortened string format.
%%
%%   Note that the hex digits are upper case, so {@link string:to_lower/1}
%%   should be used on the returned value.

-spec format_ipv6(Addr :: inet:ip6_address()) ->
  string().

format_ipv6({0, 0, 0, 0, 0, 0, 0, 0}) -> "::";

format_ipv6({0, 0, 0, 0, 0, 0, 0, A}) -> add_colons(["", "", A]);
format_ipv6({A, 0, 0, 0, 0, 0, 0, 0}) -> add_colons([A, "", ""]);

format_ipv6({0, 0, 0, 0, 0, 0, A, B}) -> add_colons(["", "", A, B]);
format_ipv6({A, 0, 0, 0, 0, 0, 0, B}) -> add_colons([A, "", B]);
format_ipv6({A, B, 0, 0, 0, 0, 0, 0}) -> add_colons([A, B, "", ""]);

format_ipv6({0, 0, 0, 0, 0, A, B, C}) -> add_colons(["", "", A, B, C]);
format_ipv6({A, 0, 0, 0, 0, 0, B, C}) -> add_colons([A, "", B, C]);
format_ipv6({A, B, 0, 0, 0, 0, 0, C}) -> add_colons([A, B, "", C]);
format_ipv6({A, B, C, 0, 0, 0, 0, 0}) -> add_colons([A, B, C, "", ""]);

format_ipv6({0, 0, 0, 0, A, B, C, D}) -> add_colons(["", "", A, B, C, D]);
format_ipv6({A, 0, 0, 0, 0, B, C, D}) -> add_colons([A, "", B, C, D]);
format_ipv6({A, B, 0, 0, 0, 0, C, D}) -> add_colons([A, B, "", C, D]);
format_ipv6({A, B, C, 0, 0, 0, 0, D}) -> add_colons([A, B, C, "", D]);
format_ipv6({A, B, C, D, 0, 0, 0, 0}) -> add_colons([A, B, C, D, "", ""]);

format_ipv6({0, 0, 0, A, B, C, D, E}) -> add_colons(["", "", A, B, C, D, E]);
format_ipv6({A, 0, 0, 0, B, C, D, E}) -> add_colons([A, "", B, C, D, E]);
format_ipv6({A, B, 0, 0, 0, C, D, E}) -> add_colons([A, B, "", C, D, E]);
format_ipv6({A, B, C, 0, 0, 0, D, E}) -> add_colons([A, B, C, "", D, E]);
format_ipv6({A, B, C, D, 0, 0, 0, E}) -> add_colons([A, B, C, D, "", E]);
format_ipv6({A, B, C, D, E, 0, 0, 0}) -> add_colons([A, B, C, D, E, "", ""]);

format_ipv6({0, 0, A, B, C, D, E, F}) -> add_colons(["", "", A, B, C, D, E, F]);
format_ipv6({A, 0, 0, B, C, D, E, F}) -> add_colons([A, "", B, C, D, E, F]);
format_ipv6({A, B, 0, 0, C, D, E, F}) -> add_colons([A, B, "", C, D, E, F]);
format_ipv6({A, B, C, 0, 0, D, E, F}) -> add_colons([A, B, C, "", D, E, F]);
format_ipv6({A, B, C, D, 0, 0, E, F}) -> add_colons([A, B, C, D, "", E, F]);
format_ipv6({A, B, C, D, E, 0, 0, F}) -> add_colons([A, B, C, D, E, "", F]);
format_ipv6({A, B, C, D, E, F, 0, 0}) -> add_colons([A, B, C, D, E, F, "", ""]);

format_ipv6({A, B, C, D, E, F, G, H}) -> add_colons([A, B, C, D, E, F, G, H]).

%% @doc Join a list of fields with colons.

-spec add_colons(Fields :: [[] | integer()]) ->
  string().

add_colons([""]) -> "";
add_colons([F]) -> integer_to_list(F, 16);
add_colons(["" | Rest]) -> ":" ++ add_colons(Rest);
add_colons([F | Rest]) -> integer_to_list(F, 16) ++ ":" ++ add_colons(Rest).

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
