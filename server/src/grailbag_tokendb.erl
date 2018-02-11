%%%---------------------------------------------------------------------------
%%% @doc
%%%   Artifact tokens database file.
%%%
%%% @todo Defend against partial writes on crash.
%%% @todo Garbage collection (compaction) for tokens deleted from disk file.
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_tokendb).

%% public interface
-export([open/1, close/1]).
-export([update/4, delete/3, id/3, tokens/1, fold/3]).

-export_type([handle/0]).

%%%---------------------------------------------------------------------------
%%% types

-define(NULL_UUID, <<0:128>>).
-define(NULL_ID, <<"00000000-0000-0000-0000-000000000000">>).

-type handle() :: {grailbag_tokens, ets:tab(), file:fd()}.

-record(token, {
  key :: {grailbag:artifact_type(), grailbag:token()},
  artifact :: grailbag:artifact_id(),
  offset :: non_neg_integer()
}).

%%%---------------------------------------------------------------------------
%%% public interface
%%%---------------------------------------------------------------------------

%% @doc Open and load a database file.

-spec open(file:filename()) ->
  {ok, handle()} | {error, Reason}
  when Reason :: file:posix().

open(Filename) ->
  case file:open(Filename, [read, write, raw, binary]) of
    {ok, FH} ->
      Table = ets:new(artifact_tokens, [set, {keypos, #token.key}]),
      case read_tokens(Table, FH, 0, <<>>) of
        ok ->
          {ok, {grailbag_tokens, Table, FH}};
        {error, Reason} ->
          file:close(FH),
          ets:delete(Table),
          {error, Reason}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%%----------------------------------------------------------
%% open() helpers {{{

%% @doc Load tokens from a file to ETS table.

-spec read_tokens(ets:tab(), file:fd(), non_neg_integer(), binary()) ->
  ok | {error, Reason}
  when Reason :: file:posix().

read_tokens(Table, FH, Offset, PrevChunk) ->
  case file:read(FH, 1024 * 1024) of
    {ok, Chunk} ->
      case parse_tokens(Table, Offset, <<PrevChunk/binary, Chunk/binary>>) of
        {ok, NewOffset, NewChunk} ->
          read_tokens(Table, FH, NewOffset, NewChunk);
        {error, Reason} ->
          {error, Reason}
      end;
    eof when byte_size(PrevChunk) == 0 ->
      ok;
    eof when byte_size(PrevChunk) > 0 ->
      % XXX: `PrevChunk' has less than a complete record
      {ok, _} = file:position(FH, Offset),
      file:truncate(FH);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Parse a chunk of database file and insert read records into ETS table.

-spec parse_tokens(ets:tab(), non_neg_integer(), binary()) ->
    {ok, Offset :: non_neg_integer(), Unparsed :: binary()}
  | {error, term()}.

parse_tokens(Table, Offset,
             <<YLen:16, Type:YLen/binary, TLen:16, Token:TLen/binary,
               UUID:128/bitstring, Rest/binary>> = _Chunk) ->
  % XXX: changes to this record format must reflect `update()' function and
  % next offset passed to recursive call
  % TODO: add another UUID field and a marker which one is used (in case of
  % a crash during saving tokens)
  % TODO: check a checksum
  Record = #token{
    key = {binary:copy(Type), binary:copy(Token)},
    artifact = list_to_binary(grailbag_uuid:format(UUID)),
    offset = Offset + 2 + YLen + 2 + TLen
  },
  ets:insert(Table, Record),
  parse_tokens(Table, Offset + 2 + YLen + 2 + TLen + 16, Rest);
parse_tokens(_Table, Offset, Chunk) ->
  {ok, Offset, Chunk}.

%% }}}
%%----------------------------------------------------------

%% @doc Close tokens database handle.

-spec close(handle()) ->
  ok.

close({grailbag_tokens, Table, FH} = _Handle) ->
  file:close(FH),
  ets:delete(Table),
  ok.

%% @doc Set a token for an artifact.

-spec update(handle(), grailbag:artifact_type(), grailbag:token(),
             grailbag:artifact_id()) ->
  ok | {error, Reason}
  when Reason :: file:posix().

update({grailbag_tokens, Table, FH} = _Handle, Type, Token, ID)
when is_binary(Type), is_binary(Token), is_binary(ID) ->
  Key = {Type, Token},
  case ets:lookup(Table, Key) of
    [#token{artifact = ID}] ->
      ok;
    [#token{artifact = OldID, offset = Offset}] when ID /= OldID ->
      UUID = grailbag_uuid:parse(binary_to_list(ID)),
      case file:pwrite(FH, Offset, UUID) of
        ok ->
          ets:update_element(Table, Key, [{#token.artifact, ID}]),
          ok;
        {error, Reason} ->
          {error, Reason}
      end;
    [] ->
      {ok, EOFOffset} = file:position(FH, eof),
      Data = [
        <<(size(Type)):16>>, Type,
        <<(size(Token)):16>>, Token,
        grailbag_uuid:parse(binary_to_list(ID))
      ],
      case file:write(FH, Data) of
        ok ->
          Record = #token{
            key = {Type, Token},
            artifact = ID,
            offset = EOFOffset + 2 + size(Type) + 2 + size(Token)
          },
          ets:insert(Table, Record),
          ok;
        {error, Reason} ->
          % FIXME: truncating may fail, leading to erroneous records; remember
          % EOFOffset in the ETS table?
          file:position(FH, EOFOffset),
          file:truncate(FH),
          {error, Reason}
      end
  end.

%% @doc Delete a token from database.

-spec delete(handle(), grailbag:artifact_type(), grailbag:token()) ->
  ok | {error, Reason}
  when Reason :: file:posix().

delete({grailbag_tokens, Table, FH} = _Handle, Type, Token) ->
  case ets:lookup(Table, {Type, Token}) of
    [#token{offset = Offset}] ->
      case file:pwrite(FH, Offset, ?NULL_UUID) of
        ok ->
          ets:update_element(Table, {Type, Token},
                             [{#token.artifact, ?NULL_ID}]),
          ok;
        {error, Reason} ->
          {error, Reason}
      end;
    [] ->
      ok
  end.

%% @doc Return the artifact identifier a token is assigned to.

-spec id(handle(), grailbag:artifact_type(), grailbag:token()) ->
  {ok, grailbag:artifact_id()} | undefined.

id({grailbag_tokens, Table, _FH} = _Handle, Type, Token) ->
  case ets:lookup(Table, {Type, Token}) of
    [#token{artifact = ?NULL_ID}] -> undefined;
    [#token{artifact = ID}] -> {ok, ID};
    [] -> undefined
  end.

%% @doc Fold over tokens assigned to artifacts.

-spec fold(Fun, term(), handle()) ->
  Acc :: term()
  when Fun :: fun((ID :: grailbag:artifact_id(),
                   Type :: grailbag:artifact_type(),
                   Token :: grailbag:token(),
                   Acc :: term()) -> NewAcc :: term()).

fold(Fun, InitAcc, {grailbag_tokens, Table, _FH} = _Handle)
when is_function(Fun, 4) ->
  ets:foldl(
    fun
      (#token{artifact = ?NULL_ID}, Acc) ->
        Acc;
      (#token{key = {Type, Token}, artifact = ID}, Acc) ->
        Fun(ID, Type, Token, Acc)
    end,
    InitAcc,
    Table
  ).

%% @doc List all tokens loaded from database.

-spec tokens(handle()) ->
  [{grailbag:artifact_id(), grailbag:artifact_type(), grailbag:token()}].

tokens(Handle) ->
  fold(fun(ID, Type, Token, Acc) -> [{ID,Type,Token} | Acc] end, [], Handle).

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
