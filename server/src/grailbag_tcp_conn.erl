%%%---------------------------------------------------------------------------
%%% @doc
%%%   Client connection worker.
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_tcp_conn).

-behaviour(gen_server).

%% public interface
-export([take_over/2]).

%% supervision tree API
-export([start/0, start_link/0]).

%% gen_server callbacks
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([code_change/3]).

%%%---------------------------------------------------------------------------
%%% types {{{

-define(READ_CHUNK_SIZE, 16384). % 16kB

-record(state, {
  socket :: ssl:sslsocket() | undefined,
  read_left :: undefined | grailbag:file_size(),
  upload_hash :: undefined | grailbag:body_hash(),
  handle :: undefined
          | {upload, grailbag:artifact_id(), grailbag_artifact:write_handle()}
          | {download, grailbag:artifact_id(), grailbag_artifact:read_handle()}
}).

%%% }}}
%%%---------------------------------------------------------------------------
%%% public interface
%%%---------------------------------------------------------------------------

%% @doc Spawn a worker process, taking over a client socket.
%%
%%   The caller must be the controlling process of the `Socket'.
%%
%%   In case of spawning error, the socket is closed. In any case, caller
%%   shouldn't bother with the socket anymore.

-spec take_over(gen_tcp:socket(), [proplists:property()]) ->
  {ok, pid()} | {error, term()}.

take_over(Socket, SSLOpts) when is_list(SSLOpts) ->
  case grailbag_tcp_conn_sup:spawn_worker() of
    {ok, Pid} ->
      ok = gen_tcp:controlling_process(Socket, Pid),
      gen_server:cast(Pid, {start, Socket, SSLOpts}),
      {ok, Pid};
    {error, Reason} ->
      gen_tcp:close(Socket),
      {error, Reason}
  end.

%%%---------------------------------------------------------------------------
%%% supervision tree API
%%%---------------------------------------------------------------------------

%% @private
%% @doc Start worker process.

start() ->
  gen_server:start(?MODULE, [], []).

%% @private
%% @doc Start worker process.

start_link() ->
  gen_server:start_link(?MODULE, [], []).

%%%---------------------------------------------------------------------------
%%% gen_server callbacks
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% initialization/termination {{{

%% @private
%% @doc Initialize event handler.

init([] = _Args) ->
  State = #state{
    socket = undefined,
    read_left = undefined,
    upload_hash = undefined,
    handle = undefined
  },
  {ok, State, 5000}.

%% @private
%% @doc Clean up after event handler.

terminate(_Arg, _State = #state{socket = Socket, handle = Handle}) ->
  case Handle of
    {upload, ID, H} ->
      grailbag_log:info("upload aborted", [{artifact, ID}]),
      grailbag_artifact:close(H);
    {download, _ID, H} ->
      grailbag_artifact:close(H);
    undefined ->
      ok
  end,
  if
    Socket /= undefined -> ssl:close(Socket);
    Socket == undefined -> ok
  end,
  ok.

%% }}}
%%----------------------------------------------------------
%% communication {{{

%% @private
%% @doc Handle {@link gen_server:call/2}.

%% unknown calls
handle_call(_Request, _From, State) ->
  {reply, {error, unknown_call}, State, 0}.

%% @private
%% @doc Handle {@link gen_server:cast/2}.

handle_cast({start, Socket, SSLOpts} = _Request,
            State = #state{socket = undefined}) ->
  {ok, {PeerAddr, PeerPort}} = inet:peername(Socket),
  {ok, {LocalAddr, LocalPort}} = inet:sockname(Socket),
  grailbag_log:set_context(connection, [
    {client, format_address(PeerAddr, PeerPort)},
    {local_address, format_address(LocalAddr, LocalPort)}
  ]),
  case ssl:ssl_accept(Socket, SSLOpts) of
    {ok, SSLConn} ->
      ssl:setopts(SSLConn, [binary, {packet, 4}, {active, once}]),
      NewState = State#state{socket = SSLConn},
      {noreply, NewState};
    {error, closed} ->
      % most probably the client rejected our certificate
      gen_tcp:close(Socket),
      {stop, normal, State};
    {error, Reason} ->
      % TODO: log the SSL error
      grailbag_log:info("SSL negotiation error", [{error, {term, Reason}}]),
      gen_tcp:close(Socket),
      {stop, normal, State}
  end;

%% unknown casts
handle_cast(_Request, State) ->
  {noreply, State, 0}.

%% @private
%% @doc Handle incoming messages.

handle_info(timeout = _Message, State = #state{socket = undefined}) ->
  % no signal to start was sent for a long time
  {stop, normal, State};

handle_info(timeout = _Message, State = #state{handle = undefined}) ->
  % break out of timeout loop
  {noreply, State};

handle_info(timeout = _Message,
            State = #state{handle = {upload, _ID, _Handle}}) ->
  % do nothing, wait for client to send another chunk
  {noreply, State};

handle_info(timeout = _Message,
            State = #state{socket = Socket, read_left = 0,
                           handle = {download, _ID, Handle}}) ->
  % artifact read and sent in the whole; go back to reading 4-byte
  % size-prefixed requests
  grailbag_artifact:close(Handle),
  ssl:setopts(Socket, [{active, once}, {packet, 4}]),
  NewState = State#state{
    read_left = undefined,
    handle = undefined
  },
  {noreply, NewState};

handle_info(timeout = _Message,
            State = #state{socket = Socket, read_left = ReadLeft,
                           handle = {download, ID, Handle}}) ->
  % timeout->read->send->timeout loop
  case grailbag_artifact:read(Handle, min(ReadLeft, 16 * 1024)) of
    {ok, Data} ->
      case ssl:send(Socket, Data) of
        ok ->
          NewState = State#state{read_left = ReadLeft - size(Data)},
          {noreply, NewState, 0};
        {error, _Reason} ->
          {stop, normal, State}
      end;
    eof ->
      grailbag_log:warn("artifact read error", [
        {operation, get},
        {error, {term, eof}},
        {artifact, ID}
      ]),
      % the client protocol has no way of sending an error, so let's terminate
      % the connection early
      {stop, normal, State};
    {error, Reason} ->
      grailbag_log:warn("artifact read error", [
        {operation, get},
        {error, {term, Reason}},
        {artifact, ID}
      ]),
      % the client protocol has no way of sending an error, so let's terminate
      % the connection early
      {stop, normal, State}
  end;

handle_info({ssl, Socket, Data} = _Message,
            State = #state{socket = Socket, upload_hash = LocalHash,
                           handle = {upload, ID, Handle}}) ->
  if
    size(Data) > ?READ_CHUNK_SIZE ->
      grailbag_log:info("client sent bigger upload chunk than allowed"),
      {stop, normal, State};
    LocalHash == undefined, size(Data) > 0 ->
      % another body chunk
      case grailbag_artifact:write(Handle, Data) of
        ok ->
          ssl:setopts(Socket, [{active, once}]),
          {noreply, State};
        {error, Reason} ->
          grailbag_log:warn("artifact write error", [
            {operation, store},
            {error, {term, Reason}},
            {artifact, ID}
          ]),
          {stop, normal, State}
      end;
    LocalHash == undefined, size(Data) == 0 ->
      % end-of-body marker
      {ok, Hash} = grailbag_artifact:finish(Handle),
      % FIXME: if the server crashes now, the unverified artifact will be kept
      % in the storage (though data corruption during transfer is a rare
      % event, so it shouldn't be a major problem)
      NewState = State#state{upload_hash = Hash},
      ssl:setopts(Socket, [{active, once}]),
      {noreply, NewState};
    is_binary(LocalHash) ->
      % hash sent after end-of-body marker
      {ok, {_, Type, Size, _Hash, CTime, MTime, Tags, [], valid}} =
        grailbag_artifact:info(Handle),
      grailbag_artifact:close(Handle),
      NewState = State#state{
        upload_hash = undefined,
        handle = undefined
      },
      Reply = case (Data == LocalHash) of
        true ->
          case grailbag_reg:store(ID, Type, Size, LocalHash, CTime, MTime, Tags) of
            ok ->
              encode_success(ID);
            %{error, duplicate_id} -> % this should never happen
            {error, unknown_type} ->
              grailbag_artifact:delete(ID), % let's hope that delete succeeds
              encode_error(unknown_type);
            {error, {schema, _, _} = Reason} ->
              grailbag_artifact:delete(ID), % let's hope that delete succeeds
              encode_error(Reason)
          end;
        false ->
          grailbag_artifact:delete(ID), % let's hope that delete succeeds
          encode_error(body_checksum_mismatch)
      end,
      case ssl:send(Socket, Reply) of
        ok ->
          ssl:setopts(Socket, [{active, once}]),
          {noreply, NewState};
        {error, _Reason} ->
          {stop, normal, NewState}
      end
  end;

handle_info({ssl, Socket, Data} = _Message,
            State = #state{socket = Socket, handle = undefined}) ->
  case decode_request(Data) of
    {store, Type, Tags} -> % long running connection
      case grailbag_reg:check_tags(Type, Tags) of
        ok ->
          case grailbag_artifact:create(Type, Tags) of
            {ok, Handle, ID} ->
              grailbag_log:info("creating a new artifact", [
                {operation, store},
                {artifact, ID}
              ]),
              % NOTE: socket is and stays in passive state and 4-byte size
              % prefix packet mode
              % XXX: maximum allowed chunk size must be at least 4kB
              Reply = encode_success(ID, ?READ_CHUNK_SIZE),
              NewState = State#state{handle = {upload, ID, Handle}};
            {error, Reason} ->
              EventID = grailbag_uuid:uuid(),
              grailbag_log:warn("can't create a new artifact", [
                {operation, store},
                {error, {term, Reason}},
                {artifact, null},
                {event_id, {uuid, EventID}}
              ]),
              Reply = encode_error({server_error, EventID}),
              NewState = State
          end;
        {error, unknown_type} ->
          Reply = encode_error(unknown_type),
          NewState = State;
        {error, {schema, _, _} = Reason} ->
          Reply = encode_error(Reason),
          NewState = State
      end,
      case ssl:send(Socket, Reply) of
        ok ->
          ssl:setopts(Socket, [{active, once}]),
          {noreply, NewState};
        {error, _} ->
          {stop, normal, NewState}
      end;
    {delete, ID} ->
      Reply = case grailbag_reg:delete(ID) of
        ok ->
          grailbag_log:info("deleted an artifact", [
            {operation, store},
            {artifact, ID}
          ]),
          encode_success(ID);
        {error, bad_id} -> encode_error(unknown_artifact);
        {error, artifact_has_tokens} -> encode_error(artifact_has_tokens);
        {error, {storage, EventID}} -> encode_error({server_error, EventID})
      end,
      case ssl:send(Socket, Reply) of
        ok ->
          ssl:setopts(Socket, [{active, once}]),
          {noreply, State};
        {error, _} ->
          {stop, normal, State}
      end;
    {update_tags, ID, Tags, UnsetTags} ->
      Reply = case grailbag_reg:update_tags(ID, Tags, UnsetTags) of
        ok ->
          grailbag_log:info("updated tags of an artifact", [
            {operation, update_tags},
            {artifact, ID},
            {tags, [{set, Tags}, {unset, UnsetTags}]}
          ]),
          encode_success(ID);
        {error, bad_id} -> encode_error(unknown_artifact);
        {error, unknown_type} -> encode_error(unknown_type);
        {error, {schema, _, _} = Reason} -> encode_error(Reason);
        {error, {storage, EventID}} -> encode_error({server_error, EventID})
      end,
      case ssl:send(Socket, Reply) of
        ok ->
          ssl:setopts(Socket, [{active, once}]),
          {noreply, State};
        {error, _} ->
          {stop, normal, State}
      end;
    {update_tokens, ID, Tokens, UnsetTokens} ->
      Reply = case grailbag_reg:update_tokens(ID, Tokens, UnsetTokens) of
        ok ->
          grailbag_log:info("updated tokens of an artifact", [
            {operation, update_tokens},
            {artifact, ID},
            {tokens, [{set, Tokens}, {unset, UnsetTokens}]}
          ]),
          encode_success(ID);
        {error, bad_id} -> encode_error(unknown_artifact);
        {error, unknown_type} -> encode_error(unknown_type);
        {error, {schema, _} = Reason} -> encode_error(Reason);
        {error, {storage, EventID}} -> encode_error({server_error, EventID})
      end,
      case ssl:send(Socket, Reply) of
        ok ->
          ssl:setopts(Socket, [{active, once}]),
          {noreply, State};
        {error, _} ->
          {stop, normal, State}
      end;
    list_types ->
      Types = grailbag_reg:list(),
      Reply = encode_types_list(Types),
      case ssl:send(Socket, Reply) of
        ok ->
          ssl:setopts(Socket, [{active, once}]),
          {noreply, State};
        {error, _} ->
          {stop, normal, State}
      end;
    {list, Type} ->
      Reply = case grailbag_reg:known_type(Type) of
        true -> encode_list(grailbag_reg:list(Type));
        false -> encode_error(unknown_type)
      end,
      case ssl:send(Socket, Reply) of
        ok ->
          ssl:setopts(Socket, [{active, once}]),
          {noreply, State};
        {error, _} ->
          {stop, normal, State}
      end;
    % TODO: `{watch, Type, ...}' (long running)
    {info, ID} ->
      Reply = case grailbag_reg:info(ID) of
        {ok, Info} -> encode_info(Info);
        undefined -> encode_error(unknown_artifact)
      end,
      case ssl:send(Socket, Reply) of
        ok ->
          ssl:setopts(Socket, [{active, once}]),
          {noreply, State};
        {error, _} ->
          {stop, normal, State}
      end;
    {get, ID} -> % potentially long running connection
      case grailbag_artifact:open(ID) of
        {ok, Handle} ->
          {ok, {_, _, FileSize, _, _, _, _, _, _} = Info} = grailbag_reg:info(ID),
          RawReply = encode_info(Info),
          % we won't be reading for a while, and sending should not add any
          % data (`{packet,4}' adds 4 bytes of packet length)
          ssl:setopts(Socket, [{packet, raw}]),
          Reply = [<<(iolist_size(RawReply)):32>>, RawReply],
          NewState = State#state{
            read_left = FileSize,
            handle = {download, ID, Handle}
          };
        {error, bad_id} ->
          ssl:setopts(Socket, [{active, once}]),
          Reply = encode_error(unknown_artifact),
          NewState = State;
        {error, Reason} ->
          ssl:setopts(Socket, [{active, once}]),
          EventID = grailbag_uuid:uuid(),
          grailbag_log:warn("can't open an artifact", [
            {operation, get},
            {error, {term, Reason}},
            {artifact, ID},
            {event_id, {uuid, EventID}}
          ]),
          Reply = encode_error({server_error, EventID}),
          NewState = State
      end,
      case ssl:send(Socket, Reply) of
        ok ->
          % go into timeout->read->send->timeout loop
          {noreply, NewState, 0};
        {error, _} ->
          {stop, normal, NewState}
      end;
    {error, badarg} ->
      % protocol error
      {stop, normal, State}
  end;

handle_info({ssl_closed, Socket} = _Message,
            State = #state{socket = Socket}) ->
  {stop, normal, State};

handle_info({ssl_error, Socket, _Reason} = _Message,
            State = #state{socket = Socket}) ->
  {stop, normal, State};

%% unknown messages
handle_info(_Message, State) ->
  {noreply, State, 0}.

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
%%% protocol handling
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% encode_success(), encode_info(), encode_list() {{{

%% @doc Encode "operation succeeded" message.

-spec encode_success(grailbag:artifact_id()) ->
  binary().

encode_success(ID) ->
  UUID = grailbag_uuid:parse(binary_to_list(ID)),
  <<0:4, 0:28, UUID/binary>>.

%% @doc Encode "operation succeeded" message.

-spec encode_success(grailbag:artifact_id(), pos_integer()) ->
  binary().

encode_success(ID, MaxChunkSize) when MaxChunkSize > 0 ->
  UUID = grailbag_uuid:parse(binary_to_list(ID)),
  <<0:4, MaxChunkSize:28, UUID/binary>>.

%% @doc Encode message with artifact metadata.

-spec encode_info(grailbag:artifact_info()) ->
  iolist().

encode_info(Info) ->
  _Result = [<<1:4, 0:28>>, encode_artifact_info(Info)].

%% @doc Encode message with list of artifacts.

-spec encode_list([grailbag:artifact_info()]) ->
  iolist().

encode_list(Artifacts) ->
  _Result = [
    <<2:4, (length(Artifacts)):28>>,
    [encode_artifact_info(Info) || Info <- Artifacts]
  ].

%% @doc Encode message with list of artifact types.

-spec encode_types_list([grailbag:artifact_type()]) ->
  iolist().

encode_types_list(Types) ->
  _Result = [
    <<3:4, (length(Types)):28>>,
    [[<<(size(T)):16>>, T] || T <- Types]
  ].

%% @doc Encode information about a single artifact.
%%
%% @see encode_info/1
%% @see encode_list/1

-spec encode_artifact_info(grailbag:artifact_info()) ->
  iolist().

encode_artifact_info({ID, Type, FileSize, Hash, CTime, MTime, Tags, Tokens, Valid} = _Info) ->
  Flags = case Valid of
    valid -> <<0:1, 0:15>>;
    has_errors -> <<1:1, 0:15>>
  end,
  _Result = [
    grailbag_uuid:parse(binary_to_list(ID)),
    <<(size(Type)):16>>, Type,
    Flags,
    <<FileSize:64>>,
    <<(size(Hash)):16>>, Hash,
    <<CTime:64, MTime:64>>,
    <<(length(Tags)):32>>,
    <<(length(Tokens)):32>>,
    [encode_tag(Tag, Value) || {Tag, Value} <- Tags],
    [encode_token(T) || T <- Tokens]
  ].

%% @doc Encode tag (its name and value).
%%
%% @see encode_artifact_info/1

-spec encode_tag(grailbag:tag(), grailbag:token()) ->
  iolist().

encode_tag(Tag, Value) ->
  [<<(size(Tag)):16>>, <<(size(Value)):32>>, Tag, Value].

%% @doc Encode token name.
%%
%% @see encode_artifact_info/1

-spec encode_token(grailbag:token()) ->
  iolist().

encode_token(Token) ->
  [<<(size(Token)):16>>, Token].

%% }}}
%%----------------------------------------------------------
%% encode_error() {{{

%% @doc Encode an error message for client.

-spec encode_error(Error) ->
  binary()
  when Error :: {server_error, grailbag_uuid:uuid()}
              | unknown_artifact
              | unknown_type
              | body_checksum_mismatch
              | artifact_has_tokens
              | {schema, [grailbag:tag()], [grailbag:tag()]}
              | {schema, [grailbag:token()]}.

encode_error({server_error, EventID}) when bit_size(EventID) == 128 ->
  <<14:4, 0:28, EventID:128/bitstring>>;
encode_error(unknown_artifact) ->
  <<15:4, 0:12, 0:16>>;
encode_error(unknown_type) ->
  <<15:4, 1:12, 0:16>>;
encode_error(body_checksum_mismatch) ->
  <<15:4, 2:12, 0:16>>;
encode_error(artifact_has_tokens) ->
  <<15:4, 3:12, 0:16>>;
encode_error({schema, TagDupNames, TagMissingNames}) ->
  NErrors = length(TagDupNames) + length(TagMissingNames),
  Errors = iolist_to_binary([
    [[<<1:8, (size(Tag)):16>>, Tag] || Tag <- TagDupNames],
    [[<<2:8, (size(Tag)):16>>, Tag] || Tag <- TagMissingNames]
  ]),
  <<15:4, 4:12, NErrors:16, Errors/binary>>;
encode_error({schema, UnknownTokenNames}) ->
  NErrors = length(UnknownTokenNames),
  Errors = iolist_to_binary([
    [<<3:8, (size(Token)):16>>, Token] ||
    Token <- UnknownTokenNames
  ]),
  <<15:4, 4:12, NErrors:16, Errors/binary>>.

%% }}}
%%----------------------------------------------------------
%% decode_request() {{{

%% @doc Decode request from a client.

-spec decode_request(Request :: binary()) ->
    {store, Type, Tags}
  | {delete, ID}
  | {update_tags, ID, Tags, UnsetTags}
  | {update_tokens, ID, Tokens, UnsetTokens}
  | list_types
  | {list, Type}
  %| {watch, Type, ...} % TODO
  | {info, ID}
  | {get, ID}
  | {error, badarg}
  when ID :: grailbag:artifact_id(),
       Type :: grailbag:artifact_type(),
       Tags :: [{grailbag:tag(), grailbag:tag_value()}],
       UnsetTags :: [grailbag:tag()],
       Tokens :: [grailbag:token()],
       UnsetTokens :: [grailbag:token()].

decode_request(<<"S", TypeLen:16, Type:TypeLen/binary,
                 NTags:32, TagsData/binary>>) ->
  case decode_tag_pairs(NTags, [], TagsData) of
    {Tags, <<>>} -> {store, Type, Tags};
    _ -> {error, badarg} % either a decode error or non-zero remaining data
  end;
decode_request(<<"D", UUID:128/bitstring>>) ->
  {delete, decode_id(UUID)};
decode_request(<<"A", UUID:128/bitstring,
                 NSTags:32, NUTags:32, TagsData/binary>>) ->
  case decode_tag_pairs(NSTags, [], TagsData) of
    {SetTags, UnsetTagsData} ->
      case decode_names(NUTags, [], UnsetTagsData) of
        {UnsetTags, <<>>} -> {update_tags, decode_id(UUID), SetTags, UnsetTags};
        _ -> {error, badarg} % either a decode error or non-zero remaining data
      end;
    error ->
      {error, badarg}
  end;
decode_request(<<"O", UUID:128/bitstring,
                 NSTokens:32, NUTokens:32, TokensData/binary>>) ->
  case decode_names(NSTokens, [], TokensData) of
    {SetTokens, UnsetTokensData} ->
      case decode_names(NUTokens, [], UnsetTokensData) of
        {UnsetTokens, <<>>} ->
          {update_tokens, decode_id(UUID), SetTokens, UnsetTokens};
        _ ->
          {error, badarg} % either a decode error or non-zero remaining data
      end;
    error ->
      {error, badarg}
  end;
decode_request(<<"T">>) ->
  list_types;
decode_request(<<"L", TypeLen:16, Type:TypeLen/binary>>) ->
  {list, Type};
%decode_request(<<"W", TypeLen:16, Type:TypeLen/binary, QueryData/binary>>) ->
%  % TODO: process `QueryData'
%  {watch, Type, ...};
decode_request(<<"I", UUID:128/bitstring>>) ->
  {info, decode_id(UUID)};
decode_request(<<"G", UUID:128/bitstring>>) ->
  {get, decode_id(UUID)};
decode_request(_) ->
  {error, badarg}.

%% }}}
%%----------------------------------------------------------
%% decode_id(), decode_tag_pairs(), decode_names() {{{

%% @doc Decode artifact ID from 128-bit value.

-spec decode_id(binary()) ->
  grailbag:artifact_id().

decode_id(UUID) when bit_size(UUID) == 128 ->
  list_to_binary(grailbag_uuid:format(UUID)).

%% @doc Decode tag-value pairs from a binary payload.
%%
%%   A pair is composed of 16-bit length of tag name, 32-bit length tag value,
%%   tag name, and tag value. Integers are in network byte order (big endian).
%%
%%   Tag names and values do not reference the `Data' binary.

-spec decode_tag_pairs(non_neg_integer(), Acc :: [TagValue], binary()) ->
  {Tags :: [TagValue], Rest :: binary()} | error
  when TagValue :: {grailbag:tag(), grailbag:tag_value()}.

decode_tag_pairs(0 = _NPairs, Pairs, Data) ->
  % reverse the list and make copy of all binaries in one go
  ReversedPairs = lists:foldl(
    fun({N,V}, Acc) -> [{binary:copy(N), binary:copy(V)} | Acc] end,
    [],
    Pairs
  ),
  {ReversedPairs, Data};
decode_tag_pairs(NPairs, Pairs,
                        <<NSize:16, VSize:32, Name:NSize/binary,
                        Value:VSize/binary, Rest/binary>> = _Data) ->
  decode_tag_pairs(NPairs - 1, [{Name, Value} | Pairs], Rest);
decode_tag_pairs(_NPairs, _Pairs, _Data) ->
  error.

%% @doc Decode tag or token names from a binary payload.
%%
%%   A name is prefixed with 16-bit length, network byte order (big endian).
%%
%%   Names do not reference the `Data' binary.

-spec decode_names(non_neg_integer(), Acc :: [Name], binary()) ->
  {Names :: [Name], Rest :: binary()} | error
  when Name :: binary().

decode_names(0 = _N, Names, Data) ->
  % reverse the list and make copy of all binaries in one go
  ReversedNames = lists:foldl(
    fun(Name, Acc) -> [binary:copy(Name) | Acc] end,
    [],
    Names
  ),
  {ReversedNames, Data};
decode_names(N, Names, <<NSize:16, Name:NSize/binary, Rest/binary>> = _Data) ->
  decode_names(N - 1, [Name | Names], Rest);
decode_names(_N, _Names, _Data) ->
  error.

%% }}}
%%----------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% helper functions
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% format_address() {{{

%% @doc Format IP address and port number for logging.

-spec format_address(inet:ip_address(), inet:port_number()) ->
  binary().

format_address({_,_,_,_} = Address, Port) ->
  iolist_to_binary([
    grailbag:format_address(Address), ":", integer_to_list(Port)
  ]);
format_address({_,_,_,_,_,_,_,_} = Address, Port) ->
  iolist_to_binary([
    "[", grailbag:format_address(Address), "]:", integer_to_list(Port)
  ]).

%% }}}
%%----------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
