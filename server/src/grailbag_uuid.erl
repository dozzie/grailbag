%%%----------------------------------------------------------------------------
%%% @doc
%%%   UUID generating module.
%%% @end
%%%----------------------------------------------------------------------------

-module(grailbag_uuid).

-export([format/1, parse/1]).
-export([uuid/0, uuid3/2, uuid4/0, uuid5/2]).
-export([version/1]).

-export_type([uuid/0, namespace/0]).

%%%----------------------------------------------------------------------------

-type uuid() :: binary().
%% 16 bytes long binary representation of UUID.

-type namespace() :: dns | url | oid | x500 | uuid().

-define(UUID_RE, "^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$").

%%%----------------------------------------------------------------------------

%% @doc Format binary that represents UUID as a human-readable string.
%%   The string will have the form of `XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX'.

-spec format(uuid()) ->
  string().

format(UUID) when is_binary(UUID), size(UUID) == 16 ->
  <<TimeLow:4/binary, TimeMid:2/binary, TimeHiVer:2/binary,
    ClockSeq:2/binary, Node:6/binary>> = UUID,
  strhex(TimeLow) ++ "-" ++ strhex(TimeMid) ++ "-" ++ strhex(TimeHiVer) ++
    "-" ++ strhex(ClockSeq) ++ "-" ++ strhex(Node);
format(_UUID) ->
  erlang:error(badarg).

%% @doc Parse human-readable string as UUID.

-spec parse(string()) ->
  uuid().

parse("00000000-0000-0000-0000-000000000000" = _UUID) ->
  <<0:128>>;
parse(UUID) ->
  case re:run(UUID, ?UUID_RE, [dollar_endonly, {capture, none}]) of
    match ->
      % we'll treat UUID as a long integer (which it is), first parsing it
      % from human-readable string, and later encoding it as a big endian
      % binary
      UUIDNum = list_to_integer([C || C <- UUID, C /= $-], 16),
      <<UUIDNum:128>>;
    nomatch ->
      erlang:error(badarg)
  end.

%% @doc Convert byte sequence to a string with its hexadecimal representation.

-spec strhex(binary()) ->
  string().

strhex(Binary) ->
  [hex(D) || <<D:4/integer>> <= Binary].

%% @doc Convert a number to a hexadecimal digit.

-spec hex(0 .. 15) ->
  char().

hex(D) when  0 =< D, D =< 9 -> $0 + D;
hex(D) when 10 =< D         -> $a + D - 10.

%% @doc Determine version of UUID from its binary or string form.

-spec version(string() | binary()) ->
  1 .. 5 | false.

version(UUID) when is_list(UUID) ->
  case re:run(UUID, ?UUID_RE, [dollar_endonly, {capture, none}]) of
    match ->
      lists:nth(15, UUID) - $0;
    nomatch ->
      false
  end;
version(<<_:6/binary, Version:4/integer, _:76/bitstring>> = _UUID)
when Version >= 1, Version =< 5 ->
  Version;
version(UUID) when is_binary(UUID) ->
  false.

%%%----------------------------------------------------------------------------

%% @doc Generate random UUID.
%%   Obviously, this must use UUID v4.
%%
%% @equiv uuid4()

-spec uuid() ->
  uuid().

uuid() ->
  uuid4().

%% @doc Generate name-based UUID using MD5 hash.
%%
%%   UUIDs generated for the same name under the same namespace are guaranteed
%%   to be equal.
%%
%%   Note: generation function is supposed to normalize names according to
%%   rules in the namespace; this function does not do that.
%%
%%   Given the MD5 function is considered a weak digest, you should avoid
%%   UUIDs version 3 and use version 5 instead.
%%
%% @see uuid5/2

-spec uuid3(string() | atom() | binary(), namespace()) ->
  uuid().

uuid3(Name, Namespace) ->
  NamespaceID = namespace(Namespace),
  NameBin = make_binary(Name),
  Digest = crypto:md5(<<NamespaceID/binary, NameBin/binary>>),
  uuid_set_reserved_bits(Digest, uuid3).

%% @doc Generate totally random UUID.

-spec uuid4() ->
  uuid().

uuid4() ->
  Random = crypto:strong_rand_bytes(16),
  uuid_set_reserved_bits(Random, uuid4).

%% @doc Generate name-based UUID using SHA-1 hash.
%%
%%   UUIDs generated for the same name under the same namespace are guaranteed
%%   to be equal.
%%
%%   Note: generation function is supposed to normalize names according to
%%   rules in the namespace; this function does not do that.

-spec uuid5(string() | atom() | binary(), namespace()) ->
  uuid().

uuid5(Name, Namespace) ->
  NamespaceID = namespace(Namespace),
  NameBin = make_binary(Name),
  <<Digest:16/binary, _/binary>> =
    crypto:sha(<<NamespaceID/binary, NameBin/binary>>),
  uuid_set_reserved_bits(Digest, uuid5).

%%%----------------------------------------------------------------------------

%% @doc Set the bits that are reserved to what they should be.

-spec uuid_set_reserved_bits(binary(), uuid3 | uuid4 | uuid5) ->
  binary().

uuid_set_reserved_bits(Binary, Version) ->
  UUIDVersion = case Version of
    uuid3 -> <<2#0011:4/integer>>;
    uuid4 -> <<2#0100:4/integer>>;
    uuid5 -> <<2#0101:4/integer>>
  end,
  <<
    TimeLow:4/binary,
    TimeMid:2/binary, _Ver:4/bitstring, TimeHi:12/bitstring,
    _ClockSeqHi:2/bitstring, ClockSeqHi:6/bitstring, ClockSeqLow:1/binary,
    Node:6/binary
  >> = Binary,
  <<
    TimeLow/binary,
    TimeMid/binary, UUIDVersion/bitstring, TimeHi/bitstring,
    2#10:2/integer, ClockSeqHi/bitstring, ClockSeqLow/binary,
    Node/binary
  >>.

%%%----------------------------------------------------------------------------

%% @doc Convert a string-like value to binary.

-spec make_binary(string() | atom() | binary()) ->
  binary().

make_binary(String) when is_list(String) ->
  list_to_binary(String);
make_binary(String) when is_atom(String) ->
  atom_to_binary(String, unicode);
make_binary(String) when is_binary(String) ->
  String.

%%%----------------------------------------------------------------------------

%% @doc Convert a namespace into its assigned UUID.

-spec namespace(dns | url | oid | x500 | uuid()) ->
  uuid().

namespace(dns = _Namespace) ->
  <<16#6ba7b810:32, 16#9dad:16, 16#11d1:16, 16#80b4:16, 16#00c04fd430c8:48>>;
namespace(url = _Namespace) ->
  <<16#6ba7b811:32, 16#9dad:16, 16#11d1:16, 16#80b4:16, 16#00c04fd430c8:48>>;
namespace(oid = _Namespace) ->
  <<16#6ba7b812:32, 16#9dad:16, 16#11d1:16, 16#80b4:16, 16#00c04fd430c8:48>>;
namespace(x500 = _Namespace) ->
  <<16#6ba7b814:32, 16#9dad:16, 16#11d1:16, 16#80b4:16, 16#00c04fd430c8:48>>;
namespace(Namespace) when is_binary(Namespace), size(Namespace) == 16 ->
  Namespace.

%%%----------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
