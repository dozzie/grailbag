%%%---------------------------------------------------------------------------
%%% @doc
%%%   Reading and writing artifact files in data store.
%%%
%%%   Artifact directory: sole <i>body.upload</i> file or <i>body</i>,
%%%   <i>info</i>, and <i>schema</i> files.
%%%
%%%   <i>info</i> file structure: artifact type, tags, tokens.
%%%
%%% @todo Make {@link update/2} more efficient (less decode-update-encode).
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_artifact).

-behaviour(gen_server).

%% public interface
-export([create/2, write/2, finish/1]).
-export([open/1, read/2]).
-export([update/2, delete/1, info/1, list/0]).
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

-include_lib("kernel/include/file.hrl").

-type read_handle() :: tuple().

-type write_handle() :: pid().

-record(artifact, {
  fh   :: file:fd(),
  info :: grailbag:artifact_info()
}).

-record(state, {
  owner :: {pid(), reference()},
  path :: file:filename(),
  metadata :: {grailbag:artifact_id(), grailbag:artifact_type(),
                [{grailbag:tag(), grailbag:tag_value()}]},
  time :: grailbag:timestamp(),
  body :: file:fd() | undefined,
  size :: non_neg_integer(),
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
%%
%% @see write/2
%% @see finish/1
%% @see close/1

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
%% artifact_dir() {{{

%% @doc Determine path of artifact's data directory.

-spec artifact_dir(grailbag:artifact_id()) ->
  file:filename().

artifact_dir(ID) ->
  {ok, DataDir} = application:get_env(grailbag, data_dir),
  filename:join(DataDir, binary_to_list(ID)).

%% }}}
%%----------------------------------------------------------

%% @doc Append data to artifact's body.
%%
%% @see create/2
%% @see finish/1
%% @see close/1

-spec write(write_handle(), binary()) ->
  ok | {error, file:posix()}.

write(Handle, Data) ->
  gen_server:call(Handle, {write, Data}, infinity).

%% @doc Finalize artifact's body, marking the artifact as complete.
%%
%% @see create/2
%% @see write/2
%% @see close/1

-spec finish(write_handle()) ->
  {ok, grailbag:body_hash()} | {error, file:posix()}.

finish(Handle) ->
  gen_server:call(Handle, finish, infinity).

%% @doc Update tags of specified artifact.
%%
%%   <b>NOTE</b>: This function does not check the tags against artifact's
%%   schema nor other artifacts of the same type.
%%
%%   <b>NOTE</b>: Function does not coordinate between different processes
%%   that would want to update artifact's metadata.

-spec update(grailbag:artifact_id(),
             [{grailbag:tag(), grailbag:tag_value()}]) ->
  {ok, grailbag:mtime()} | {error, Reason}
  when Reason :: {schema, Dup :: [grailbag:tag()], Missing :: [grailbag:tag()]}
               | checksum
               | term().

update(ID, Tags) ->
  Path = artifact_dir(ID),
  OldFile = filename:join(Path, ?METADATA_FILE),
  NewFile = filename:join(Path, ?METADATA_UPDATE_FILE),
  case decode_info_file(OldFile) of
    % TODO: return error on mismatching ID
    {ok, {_ID, Type, Size, Hash, CTime, _OldMTime, _OldTags, [], valid}} ->
      MTime = grailbag:timestamp(),
      case encode_info_file(NewFile, ID, Type, Size, Hash, CTime, MTime, Tags) of
        ok ->
          case file:rename(NewFile, OldFile) of
            ok -> {ok, MTime};
            {error, Reason} -> {error, Reason}
          end;
        {error, Reason} ->
          file:delete(NewFile),
          {error, Reason}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Open an artifact for reading its body.
%%
%% @see read/2
%% @see info/1
%% @see close/1

-spec open(grailbag:artifact_id()) ->
  {ok, read_handle()} | {error, bad_id | Reason}
  when Reason :: {body, file:posix()}
               | {metadata, format | checksum | file:posix()}.

open(ID) ->
  Path = artifact_dir(ID),
  case file:open(filename:join(Path, ?BODY_FILE), [raw, read, binary]) of
    {ok, FH} ->
      case decode_info_file(filename:join(Path, ?METADATA_FILE)) of
        {ok, ArtifactInfo} ->
          Handle = #artifact{
            fh = FH,
            info = ArtifactInfo
          },
          {ok, Handle};
        {error, checksum} ->
          file:close(FH),
          {error, {metadata, checksum}};
        {error, badarg} ->
          file:close(FH),
          {error, {metadata, format}};
        {error, Reason} ->
          file:close(FH),
          {error, {metadata, Reason}}
      end;
    {error, enoent} ->
      {error, bad_id};
    {error, Reason} ->
      {error, {body, Reason}}
  end.

%% @doc Read a chunk of artifact's body.
%%
%% @see open/1
%% @see close/1

-spec read(read_handle(), pos_integer()) ->
  {ok, binary()} | eof | {error, badarg | file:posix()}.

read(_Handle = #artifact{fh = FH}, Size) ->
  file:read(FH, Size).

%% @doc Read artifact's metadata.
%%
%%   Since the tokens are stored separately, tokens list is set to `[]'.
%%
%% @see open/1

-spec info(Object :: grailbag:artifact_id() | read_handle() | write_handle()) ->
  {ok, grailbag:artifact_info()} | undefined.

info(ID) when is_binary(ID) ->
  Path = artifact_dir(ID),
  case decode_info_file(filename:join(Path, ?METADATA_FILE)) of
    {ok, ArtifactInfo} -> {ok, ArtifactInfo};
    {error, _} -> undefined
  end;
info(Handle) when is_pid(Handle) ->
  gen_server:call(Handle, info, infinity);
info(_Handle = #artifact{info = ArtifactInfo}) ->
  {ok, ArtifactInfo}.

%% @doc List known artifacts.
%%
%%   Since the tokens are stored separately, tokens lists are set to `[]'.

-spec list() ->
  [grailbag:artifact_id()].

list() ->
  {ok, DataDir} = application:get_env(grailbag, data_dir),
  _Artifacts = [
    list_to_binary(Name) ||
    Name <- filelib:wildcard("????????-????-????-????-????????????", DataDir),
    valid_artifact_dir(filename:join(DataDir, Name))
  ].

%% @doc Validate artifact directory.
%%
%%   It's a quite simple validation, just presence of a few known files.

-spec valid_artifact_dir(file:filename()) ->
  boolean().

valid_artifact_dir(Path) ->
  case file:read_file_info(filename:join(Path, ?BODY_FILE)) of
    {ok, #file_info{type = regular, access = read_write}} ->
      case file:read_file_info(filename:join(Path, ?METADATA_FILE)) of
        {ok, #file_info{type = regular, access = read_write}} -> true;
        _ -> false
      end;
    _ ->
      false
  end.

%% @doc Close descriptors used for reading or writing an artifact.

-spec close(write_handle() | read_handle()) ->
  ok.

close(Handle) when is_pid(Handle) ->
  try
    gen_server:call(Handle, close, infinity)
  catch
    _:_ -> ok
  end;
close(_Handle = #artifact{fh = FH}) ->
  file:close(FH),
  ok.

%% @doc Delete an artifact.

-spec delete(grailbag:artifact_id()) ->
  ok | {error, file:posix()}.

delete(ID) ->
  Path = artifact_dir(ID),
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
        time = grailbag:timestamp(),
        body = FH,
        size = 0,
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
            State = #state{body = FH, size = Size, hash = HashContext}) ->
  case file:write(FH, Data) of
    ok ->
      NewHashContext = hash_update(HashContext, Data),
      NewState = State#state{
        size = Size + size(Data),
        hash = NewHashContext
      },
      {reply, ok, NewState};
    {error, Reason} ->
      file:close(FH),
      NewState = State#state{
        body = undefined,
        hash = undefined
      },
      {reply, {error, Reason}, NewState}
  end;

handle_call(info = _Request, _From,
            State = #state{body = undefined, size = Size, hash = Hash,
                           time = CTime = MTime, metadata = {ID, Type, Tags}})
when is_binary(Hash) ->
  ArtifactInfo = {ID, Type, Size, Hash, CTime, MTime, Tags, [], valid},
  {reply, {ok, ArtifactInfo}, State};
handle_call(info = _Request, _From, State) ->
  {reply, {error, ebadfd}, State};

handle_call(finish = _Request, _From,
            State = #state{hash = undefined}) ->
  {reply, {error, ebadf}, State};
handle_call(finish = _Request, _From,
            State = #state{body = undefined, hash = Hash}) ->
  {reply, {ok, Hash}, State};
handle_call(finish = _Request, _From,
            State = #state{path = Path, body = FH, hash = HashContext,
                           time = CTime = MTime, 
                           metadata = {ID, Type, Tags}}) ->
  {ok, FileSize} = file:position(FH, cur),
  file:close(FH),
  file:rename(filename:join(Path, ?UPLOAD_FILE),
              filename:join(Path, ?BODY_FILE)),
  Hash = hash_final(HashContext),
  % TODO: write `filename:join(Path, ?SCHEMA_FILE)'
  % TODO: handle write errors
  ok = encode_info_file(filename:join(Path, ?METADATA_FILE),
                        ID, Type, FileSize, Hash, CTime, MTime, Tags),
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

-spec hash_update(binary(), binary() | iolist()) ->
  Context :: binary().

hash_update(Context, Data) ->
  crypto:sha_update(Context, Data).

%% @doc Calculate end result (hash) from context.

-spec hash_final(binary()) ->
  Hash :: binary().

hash_final(Context) ->
  crypto:sha_final(Context).

%% @doc Calculate hash of data in one go.

-spec hash(binary() | iolist()) ->
  Hash :: binary().

hash(Data) ->
  hash_final(hash_update(hash_init(), Data)).

%%%---------------------------------------------------------------------------
%%% encoding/decoding artifact metadata

%% @doc Encode metadata and write it to artifact's info file.
%%
%% @see decode_info_file/1
%% @see encode_info/6

-spec encode_info_file(file:filename(),
                       grailbag:artifact_id(), grailbag:artifact_type(),
                       grailbag:file_size(), grailbag:body_hash(),
                       grailbag:ctime(), grailbag:mtime(),
                       [{grailbag:tag(), grailbag:tag_value()}]) ->
  ok | {error, file:posix()}.

encode_info_file(File, ID, Type, Size, Hash, CTime, MTime, Tags) ->
  Data = encode_info(ID, Type, Size, Hash, CTime, MTime, Tags),
  DataHash = hash(Data),
  file:write_file(File, [Data, DataHash, <<(size(DataHash)):16>>]).

%% @doc Encode metadata for writing to artifact's info file.
%%
%% @see decode_info/1
%% @see encode_info_file/8

-spec encode_info(grailbag:artifact_id(), grailbag:artifact_type(),
                  grailbag:file_size(), grailbag:body_hash(),
                  grailbag:ctime(), grailbag:mtime(),
                  [{grailbag:tag(), grailbag:tag_value()}]) ->
  iolist().

encode_info(ID, Type, Size, Hash, CTime, MTime, Tags) ->
  _Result = [
    grailbag_uuid:parse(binary_to_list(ID)),
    <<(size(Type)):16>>, Type,
    <<Size:64>>,
    <<(size(Hash)):16>>, Hash,
    <<CTime:64, MTime:64>>,
    <<(length(Tags)):32>>,
    [encode_tag(Tag, Value) || {Tag, Value} <- Tags]
  ].

%% @doc Encode a tag with its value.
%%
%% @see encode_info/6

-spec encode_tag(grailbag:tag(), grailbag:tag_value()) ->
  iolist().

encode_tag(Tag, Value) ->
  % it will be easier to read 2+4 bytes once and then the tag and its value
  % with one or two reads
  [<<(size(Tag)):16>>, <<(size(Value)):32>>, Tag, Value].

%% @doc Decode metadata from artifact's info file.
%%
%% @see encode_info_file/8
%% @see decode_info/1

-spec decode_info_file(file:filename()) ->
  {ok, grailbag:artifact_info()} | {error, badarg | checksum | file:posix()}.

decode_info_file(File) ->
  % the file, even accounting for tags, should be small, especially that all
  % the information is supposed to also be kept in ETS table
  case file:read_file(File) of
    {ok, HashedData} ->
      try
        <<HLen:16>> = binary:part(HashedData, size(HashedData), -2),
        DataHash = binary:part(HashedData, size(HashedData) - 2, -HLen),
        Data = binary:part(HashedData, 0, size(HashedData) - 2 - HLen),
        case (hash(Data) == DataHash) of
          true -> decode_info(Data);
          false -> {error, checksum}
        end
      catch
        _:_ -> {error, checksum}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Decode metadata from content of artifact's info file.
%%
%% @see encode_info/6
%% @see decode_info_file/1

-spec decode_info(binary()) ->
  {ok, grailbag:artifact_info()} | {error, badarg}.

decode_info(Data) ->
  case Data of
    <<UUID:16/binary, TypeLen:16, Type:TypeLen/binary,
      FileSize:64, HLen:16, Hash:HLen/binary, CTime:64, MTime:64,
      NTags:32, TagsData/binary>> ->
      ID = list_to_binary(grailbag_uuid:format(UUID)),
      try
        {Tags, <<>>} = decode_tags(NTags, [], TagsData),
        ArtifactInfo = {ID, binary:copy(Type), FileSize, binary:copy(Hash),
                         CTime, MTime, Tags, [], valid},
        {ok, ArtifactInfo}
      catch
        _:_ ->
          {error, badarg}
      end;
    _ ->
      {error, badarg}
  end.

%% @doc Decode specified number of tags and their values from binary data.

decode_tags(0 = _Count, Tags, Data) ->
  {lists:reverse(Tags), Data};
decode_tags(Count, Tags,
            <<TLen:16, VLen:32, Tag:TLen/binary, Value:VLen/binary,
              Rest/binary>> = _Data) ->
  decode_tags(Count - 1, [{binary:copy(Tag), binary:copy(Value)} | Tags], Rest).

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
