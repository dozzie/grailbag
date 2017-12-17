%%%---------------------------------------------------------------------------
%%% @doc
%%%   Reading and writing artifact files in data store.
%%%
%%%   Artifact directory: sole <i>body.upload</i> file or <i>body</i>,
%%%   <i>info</i>, and <i>schema</i> files.
%%%
%%%   <i>info</i> file structure: artifact type, tags, tokens.
%%%
%%% @todo Save artifact's schema.
%%% @todo Coordinate {@link delete/1}, {@link info/1},
%%%   {@link update_tags/3}, and {@link update_tokens/3} (prevent race
%%%   conditions).
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_artifact).

-behaviour(gen_server).

%% public interface
-export([create/2, write/2, finish/1]).
-export([open/1, read/2]).
-export([update_tags/3, update_tokens/3, delete/1, info/1]).
-export([close/1]).

%% supervision tree API
-export([start/5, start_link/5]).

%% gen_server callbacks
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([code_change/3]).

-export_type([read_handle/0, write_handle/0]).

%%%---------------------------------------------------------------------------
%%% types {{{

-type read_handle() :: tuple().

-type write_handle() :: pid().

-record(state, {
  owner :: {pid(), reference()},
  path :: file:filename(),
  metadata :: {grailbag:artifact_id(), grailbag:artifact_type(),
                [{grailbag:tag(), grailbag:tag_value()}]},
  body :: file:fd() | undefined,
  hash :: binary() | undefined
}).

-define(METADATA_FILE,  "info").
-define(METADATA_UPDATE_FILE, "info.update").
-define(UPLOAD_FILE,    "body.upload").
-define(BODY_FILE,      "body").
-define(SCHEMA_FILE,    "schema").

%%% }}}
%%%---------------------------------------------------------------------------
%%% public interface
%%%---------------------------------------------------------------------------

%% @doc Create a new handle for artifact that is being uploaded.

-spec create(grailbag:artifact_type(),
             [{grailbag:tag(), grailbag:tag_value()}]) ->
  {ok, write_handle(), grailbag:artifact_id()} | {error, file:posix()}.

create(Type, Tags) ->
  {ok, DataDir} = application:get_env(grailbag, data_dir),
  case try_create(DataDir, 5) of % 5 tries should be more than enough
    {ok, Path, ID} ->
      case grailbag_artifact_sup:spawn_worker(self(), ID, Path, Type, Tags) of
        {ok, Pid} -> {ok, Pid, ID};
        {error, Reason} -> {error, Reason}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%%----------------------------------------------------------
%% try_create() {{{

%% @doc Create a directory for an artifact.
%%
%%   The ID of artifact will be generated at random.

-spec try_create(file:filename(), pos_integer()) ->
  {ok, file:filename(), grailbag:artifact_id()} | {error, file:posix()}.

try_create(DataDir, Tries) ->
  % TODO: hash the `UUID'
  UUID = grailbag_uuid:format(grailbag_uuid:uuid4()),
  Path = filename:join(DataDir, UUID),
  case file:make_dir(Path) of
    ok ->
      {ok, Path, list_to_binary(UUID)};
    {error, eexist} when Tries > 1 ->
      try_create(DataDir, Tries - 1);
    {error, Reason} ->
      {error, Reason}
  end.

%% }}}
%%----------------------------------------------------------

%% @doc Append data to artifact's body.

-spec write(write_handle(), binary()) ->
  ok | {error, file:posix()}.

write(Handle, Data) ->
  gen_server:call(Handle, {write, Data}, infinity).

%% @doc Finalize artifact's body, marking the artifact as complete.

-spec finish(write_handle()) ->
  {ok, grailbag:body_hash()} | {error, file:posix()}.

finish(Handle) ->
  gen_server:call(Handle, finish, infinity).

%% @doc Add and/or remove tags to/from specified artifact.

-spec update_tags(grailbag:artifact_id(),
                  Set :: [{grailbag:tag(), grailbag:tag_value()}],
                  Unset :: [grailbag:tag()]) ->
  ok | {error, Reason}
  when Reason :: {schema, Dup :: [grailbag:tag()], Missing :: [grailbag:tag()]}
               | term().

update_tags(_ID, _Set, _Unset) ->
  {ok, _DataDir} = application:get_env(grailbag, data_dir),
  'TODO'.

%% @doc Add and/or remove tokens to/from specified artifact.
%%
%%   <b>NOTE</b>: This function does not remove tokens from other artifacts of
%%   the same type.

-spec update_tokens(grailbag:artifact_id(),
                    Set :: [grailbag:token()],
                    Unset :: [grailbag:token()]) ->
  ok | {error, Reason}
  when Reason :: {schema, Unknown :: [grailbag:token()]}
               | term().

update_tokens(_ID, _Set, _Unset) ->
  {ok, _DataDir} = application:get_env(grailbag, data_dir),
  'TODO'.

%% @doc Open an artifact for reading its body.

-spec open(grailbag:artifact_id()) ->
  {ok, read_handle()} | {error, term()}.

open(_ID) ->
  {ok, _DataDir} = application:get_env(grailbag, data_dir),
  'TODO'.

%% @doc Read a chunk of artifact's body.

-spec read(read_handle(), pos_integer()) ->
  {ok, binary()} | eof | {error, term()}.

read(_ID, _Size) ->
  'TODO'.

%% @doc Read artifact's metadata.
%%
%% @todo Upload time.

-spec info(Object :: grailbag:artifact_id() | read_handle()) ->
  {ok, FileSize, BodyHash, Tags, Tokens} | undefined
  when FileSize :: non_neg_integer(),
       BodyHash :: grailbag:body_hash(),
       Tags :: [{grailbag:tag(), grailbag:tag_value()}],
       Tokens :: [grailbag:token()].

info(ID) when is_binary(ID) ->
  {ok, _DataDir} = application:get_env(grailbag, data_dir),
  'TODO';
info(Handle) when is_tuple(Handle) ->
  'TODO'.

%% @doc Close descriptors used for reading or writing an artifact.

-spec close(write_handle() | read_handle()) ->
  ok.

close(Handle) when is_pid(Handle) ->
  try
    gen_server:call(Handle, close, infinity)
  catch
    _:_ -> ok
  end;
close(Handle) when is_tuple(Handle) ->
  'TODO'.

%% @doc Delete an artifact.

-spec delete(grailbag:artifact_id()) ->
  ok | {error, file:posix()}.

delete(ID) ->
  {ok, DataDir} = application:get_env(grailbag, data_dir),
  Path = filename:join(DataDir, binary_to_list(ID)),
  file:delete(filename:join(Path, ?BODY_FILE)),
  file:delete(filename:join(Path, ?METADATA_FILE)),
  file:delete(filename:join(Path, ?METADATA_UPDATE_FILE)),
  file:delete(filename:join(Path, ?SCHEMA_FILE)),
  file:delete(filename:join(Path, ?UPLOAD_FILE)), % should not exist
  file:del_dir(Path).

%%%---------------------------------------------------------------------------
%%% supervision tree API
%%%---------------------------------------------------------------------------

%% @private
%% @doc Start handle process.

start(Owner, ID, Path, Type, Tags) ->
  gen_server:start(?MODULE, [Owner, ID, Path, Type, Tags], []).

%% @private
%% @doc Start handle process.

start_link(Owner, ID, Path, Type, Tags) ->
  gen_server:start_link(?MODULE, [Owner, ID, Path, Type, Tags], []).

%%%---------------------------------------------------------------------------
%%% gen_server callbacks
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% initialization/termination {{{

%% @private
%% @doc Initialize {@link gen_server} state.

init([Owner, ID, Path, Type, Tags] = _Args) ->
  case file:open(filename:join(Path, ?UPLOAD_FILE), [raw, write]) of
    {ok, FH} ->
      Ref = erlang:monitor(process, Owner),
      State = #state{
        owner = {Owner, Ref},
        path = Path,
        metadata = {ID, Type, Tags},
        body = FH,
        hash = hash_init()
      },
      {ok, State};
    {error, Reason} ->
      {stop, Reason}
  end.

%% @private
%% @doc Clean up {@link gen_server} state.

terminate(_Arg, _State = #state{body = undefined}) ->
  ok;
terminate(_Arg, _State = #state{body = FH, path = Path}) ->
  file:close(FH),
  file:delete(filename:join(Path, ?BODY_FILE)),             % should not exist
  file:delete(filename:join(Path, ?METADATA_FILE)),         % should not exist
  file:delete(filename:join(Path, ?METADATA_UPDATE_FILE)),  % should not exist
  file:delete(filename:join(Path, ?SCHEMA_FILE)),
  file:delete(filename:join(Path, ?UPLOAD_FILE)),
  file:del_dir(Path),
  ok.

%% }}}
%%----------------------------------------------------------
%% communication {{{

%% @private
%% @doc Handle {@link gen_server:call/2}.

handle_call({write, _Data} = _Request, _From,
            State = #state{body = undefined}) ->
  {reply, {error, ebadf}, State};
handle_call({write, Data} = _Request, _From,
            State = #state{body = FH, hash = HashContext}) ->
  case file:write(FH, Data) of
    ok ->
      NewHashContext = hash_update(HashContext, Data),
      NewState = State#state{hash = NewHashContext},
      {reply, ok, NewState};
    {error, Reason} ->
      file:close(FH),
      NewState = State#state{
        body = undefined,
        hash = undefined
      },
      {reply, {error, Reason}, NewState}
  end;

handle_call(finish = _Request, _From,
            State = #state{hash = undefined}) ->
  {reply, {error, ebadf}, State};
handle_call(finish = _Request, _From,
            State = #state{body = undefined, hash = Hash}) ->
  {reply, {ok, Hash}, State};
handle_call(finish = _Request, _From,
            State = #state{path = Path, body = FH, hash = HashContext,
                           metadata = {ID, Type, Tags}}) ->
  file:close(FH),
  file:rename(filename:join(Path, ?UPLOAD_FILE),
              filename:join(Path, ?BODY_FILE)),
  Hash = hash_final(HashContext),
  % TODO: write `filename:join(Path, ?SCHEMA_FILE)'
  % TODO: handle write errors
  ok = file:write_file(filename:join(Path, ?METADATA_FILE),
                       encode_info(ID, Type, Hash, Tags, [])),
  % TODO: register this artifact
  NewState = State#state{
    body = undefined,
    hash = Hash
  },
  {reply, {ok, Hash}, NewState};

handle_call(close = _Request, _From, State) ->
  {stop, normal, ok, State};

%% unknown calls
handle_call(_Request, _From, State) ->
  {reply, {error, unknown_call}, State}.

%% @private
%% @doc Handle {@link gen_server:cast/2}.

%% unknown casts
handle_cast(_Request, State) ->
  {noreply, State}.

%% @private
%% @doc Handle incoming messages.

handle_info({'DOWN', Ref, process, Pid, _Reason} = _Message,
            State = #state{owner = {Pid, Ref}}) ->
  {stop, normal, ok, State};

%% unknown messages
handle_info(_Message, State) ->
  {noreply, State}.

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
%%% calculating hash of artifact's body

%% @doc Initialize hash calculation context.

-spec hash_init() ->
  binary().

hash_init() ->
  crypto:sha_init().

%% @doc Update hash calculation context with more data.

-spec hash_update(binary(), binary()) ->
  Context :: binary().

hash_update(Context, Data) ->
  crypto:sha_update(Context, Data).

%% @doc Calculate end result (hash) from context.

-spec hash_final(binary()) ->
  Hash :: binary().

hash_final(Context) ->
  crypto:sha_final(Context).

%%%---------------------------------------------------------------------------
%%% encoding/decoding artifact metadata

%% @doc Encode metadata for writing to artifact's info file.

-spec encode_info(grailbag:artifact_id(), grailbag:artifact_type(),
                  grailbag:body_hash(),
                  [{grailbag:tag(), grailbag:tag_value()}],
                  [grailbag:token()]) ->
  iolist().

encode_info(ID, Type, Hash, Tags, Tokens) ->
  % TODO: add a checksum of this info
  % TODO: add upload time
  _Result = [
    grailbag_uuid:parse(binary_to_list(ID)),
    <<(size(Type)):16>>, Type,
    <<(size(Hash)):16>>, Hash,
    <<(length(Tags)):32>>,
    <<(length(Tokens)):32>>,
    [encode_tag(Tag, Value) || {Tag, Value} <- Tags],
    [encode_token(T) || T <- Tokens]
  ].

%% @doc Encode a tag with its value.
%%
%% @see encode_info/5

-spec encode_tag(grailbag:tag(), grailbag:tag_value()) ->
  iolist().

encode_tag(Tag, Value) ->
  % it will be easier to read 2+4 bytes once and then the tag and its value
  % with one or two reads
  [<<(size(Tag)):16>>, <<(size(Value)):32>>, Tag, Value].

%% @doc Encode a token name.
%%
%% @see encode_info/5

-spec encode_token(grailbag:token()) ->
  iolist().

encode_token(Token) ->
  [<<(size(Token)):16>>, Token].

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
