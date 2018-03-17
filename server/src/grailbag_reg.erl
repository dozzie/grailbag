%%%---------------------------------------------------------------------------
%%% @doc
%%%   Artifact in-memory registry.
%%%
%%%   The registry process is also responsible for modifying disk storage
%%%   (deleting artifacts, updating their tags and tokens).
%%%
%%% @todo Don't validate an artifact against its schema on every access, only
%%%   on updates (and on schema reloads).
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_reg).

-behaviour(gen_server).

%% public interface
-export([known_type/1, check_tags/2, store/7, delete/1]).
-export([update_tags/3, update_tokens/3]).
-export([list/0, list/1, info/1]).

%% supervision tree API
-export([start/0, start_link/0]).

%% gen_server callbacks
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([code_change/3]).

%%%---------------------------------------------------------------------------
%%% types {{{

-define(ARTIFACT_TABLE, grailbag_artifacts).
-define(TYPE_TABLE, grailbag_artifact_types).
% NOTE: mtime field in this table is only used during boot
-define(TOKEN_TABLE, grailbag_artifact_tokens).

-define(TOKEN_DB_FILE, "tokens.db").

-record(artifact, {
  id :: grailbag:artifact_id(),
  type :: grailbag:artifact_type(),
  body_size :: non_neg_integer(),
  body_hash :: grailbag:body_hash(),
  ctime :: grailbag:ctime(),
  mtime :: grailbag:mtime(),
  tags :: [{grailbag:tag(), grailbag:tag_value()}],
  tokens :: [grailbag:token()]
}).

-record(state, {
  tokens :: grailbag_tokendb:handle()
}).

%%% }}}
%%%---------------------------------------------------------------------------
%%% public interface
%%%---------------------------------------------------------------------------

%% @doc Check if an artifact type is a known one.

-spec known_type(grailbag:artifact_type()) ->
  boolean().

known_type(Type) ->
  grailbag_schema:known_type(Type).

%% @doc Check for collisions and missing tags before accepting upload of
%%   artifact's body.

-spec check_tags(grailbag:artifact_type(),
                 [{grailbag:tag(), grailbag:tag_value()}]) ->
  ok | {error, Reason}
  when Reason :: unknown_type
               | {schema, Dup :: [grailbag:tag()], Missing :: [grailbag:tag()]}.

check_tags(Type, Tags) ->
  case grailbag_schema:verify(grailbag_schema:changes(Type, Tags, [])) of
    ok ->
      ok;
    {error, [{unknown_type, _}]} ->
      {error, unknown_type};
    {error, Errors} ->
      Duplicates = lists:usort([T || {duplicate, T, _V} <- Errors]),
      Missing = lists:usort([T || {missing, T} <- Errors]),
      {error, {schema, Duplicates, Missing}}
  end.

%% @doc Record a completely uploaded artifact in registry.
%%
%%   Function intended to be called after {@link grailbag_artifact:finish/1}.
%%
%% @see grailbag_artifact:create/2
%% @see grailbag_artifact:finish/1

-spec store(grailbag:artifact_id(), grailbag:artifact_type(), non_neg_integer(),
            grailbag:body_hash(), grailbag:ctime(), grailbag:mtime(),
            [{grailbag:tag(), grailbag:tag_value()}]) ->
  ok | {error, Reason}
  when Reason :: duplicate_id
               | unknown_type
               | {schema, Dup :: [grailbag:tag()], Missing :: [grailbag:tag()]}.

store(ID, Type, BodySize, BodyHash, CTime, MTime, Tags) ->
  ArtifactInfo = {ID, Type, BodySize, BodyHash, CTime, MTime, Tags, [], valid},
  gen_server:call(?MODULE, {store, ArtifactInfo}, infinity).

%% @doc Delete an artifact from memory and from disk.

-spec delete(grailbag:artifact_id()) ->
  ok | {error, Reason}
  when Reason :: bad_id
               | artifact_has_tokens
               | {storage, EventID :: grailbag_log:event_id()}.

delete(ID) ->
  gen_server:call(?MODULE, {delete, ID}, infinity).

%% @doc Update tags of an artifact.

-spec update_tags(grailbag:artifact_id(),
                  [{grailbag:tag(), grailbag:tag_value()}],
                  [grailbag:tag()]) ->
  ok | {error, Reason}
  when Reason :: bad_id
               | unknown_type
               | {schema, Dup :: [grailbag:tag()], Missing :: [grailbag:tag()]}
               | {storage, EventID :: grailbag_log:event_id()}.

update_tags(ID, SetTags, UnsetTags) ->
  % `SetTags' is expected to be sorted by tag name
  Request = {update_tags, ID, sort_tags(SetTags), UnsetTags},
  gen_server:call(?MODULE, Request, infinity).

%%----------------------------------------------------------
%% sort_tags() {{{

%% @doc Sort list of tags, removing tag duplicates.

-spec sort_tags([{grailbag:tag(), grailbag:tag_value()}]) ->
  [{grailbag:tag(), grailbag:tag_value()}].

sort_tags([] = _Tags) ->
  [];
sort_tags(Tags) ->
  [{Tag, _} = Elem | Rest] = lists:keysort(1, Tags),
  [Elem | filter_dup_tags(Tag, Rest)].

%% @doc Remove duplicate tags from ordered list.

-spec filter_dup_tags(grailbag:tag(),
                      [{grailbag:tag(), grailbag:tag_value()}]) ->
  [{grailbag:tag(), grailbag:tag_value()}].

filter_dup_tags(OldTag, [{OldTag,_} | Rest] = _Tags) ->
  filter_dup_tags(OldTag, Rest);
filter_dup_tags(OldTag, [{Tag,_} = Elem | Rest] = _Tags) when OldTag < Tag ->
  [Elem | filter_dup_tags(Tag, Rest)];
filter_dup_tags(_OldTag, []) ->
  [].

%% }}}
%%----------------------------------------------------------

%% @doc Update tokens of an artifact.

-spec update_tokens(grailbag:artifact_id(), [grailbag:token()],
                    [grailbag:token()]) ->
  ok | {error, Reason}
  when Reason :: bad_id
               | unknown_type
               | {schema, Unknown :: [grailbag:token()]}
               | {storage, EventID :: grailbag_log:event_id()}.

update_tokens(ID, SetTokens, UnsetTokens) ->
  Request = {update_tokens, ID, SetTokens, UnsetTokens},
  gen_server:call(?MODULE, Request, infinity).

%% @doc List known artifact types.

-spec list() ->
  [grailbag:artifact_type()].

list() ->
  grailbag_schema:types().

%% @doc List metadata of all artifacts of specific type.

-spec list(grailbag:artifact_type()) ->
  [grailbag:artifact_info()].

list(Type) ->
  Records = ets:lookup(?TYPE_TABLE, Type),
  lists:keysort(1, lists:foldl(fun add_artifact_info/2, [], Records)).

%% @doc Workhorse for {@link list/1}.

add_artifact_info({_, ID}, Artifacts) ->
  case info(ID) of
    {ok, Info} -> [Info | Artifacts];
    undefined -> Artifacts
  end.

%% @doc Get artifact's metadata.

-spec info(grailbag:artifact_id()) ->
  {ok, grailbag:artifact_info()} | undefined.

info(ID) ->
  case ets:lookup(?ARTIFACT_TABLE, ID) of
    [#artifact{type = Type, body_size = Size, body_hash = Hash,
               ctime = CTime, mtime = MTime, tags = Tags, tokens = Tokens}] ->
      Valid = case grailbag_schema:verify(Type, Tags) of
        ok -> valid;
        {error, _} -> has_errors
      end,
      {ok, {ID, Type, Size, Hash, CTime, MTime, Tags, Tokens, Valid}};
    [] ->
      undefined
  end.

%%%---------------------------------------------------------------------------
%%% supervision tree API
%%%---------------------------------------------------------------------------

%% @private
%% @doc Start registry process.

start() ->
  gen_server:start({local, ?MODULE}, ?MODULE, [], []).

%% @private
%% @doc Start registry process.

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%---------------------------------------------------------------------------
%%% gen_server callbacks
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% initialization/termination {{{

%% @private
%% @doc Initialize {@link gen_server} state.

init(_Args) ->
  grailbag_log:set_context(artifacts, []),
  grailbag_schema:open(),
  ets:new(?ARTIFACT_TABLE,
          [set, named_table, protected, {keypos, #artifact.id}]),
  ets:new(?TYPE_TABLE, [bag, named_table, protected]),
  {ok, DataDir} = application:get_env(data_dir),
  case grailbag_tokendb:open(filename:join(DataDir, ?TOKEN_DB_FILE)) of
    {ok, Handle} ->
      grailbag_log:info("loading artifacts and schema"),
      lists:foreach(fun ets_add_artifact/1, grailbag_artifact:list()),
      dict:fold(fun ets_add_tokens/3, ignore, build_tokens_mapping(Handle)),
      reload_schema(),
      grailbag_log:info("starting artifact registry", [
        {artifacts, ets:info(?ARTIFACT_TABLE, size)}
      ]),
      State = #state{tokens = Handle},
      {ok, State};
    {error, Reason} ->
      {stop, {open_tokens_db, Reason}}
  end.

%% @doc Helper to add an artifact to ETS tables during boot ({@link init/1}).

-spec ets_add_artifact(grailbag:artifact_id()) ->
  any().

ets_add_artifact(ID) ->
  case grailbag_artifact:info(ID) of
    {ok, {ID, Type, _Size, _Hash, _CTime, _MTime, _Tags, _Tokens, _Valid} = Info} ->
      ets:insert(?ARTIFACT_TABLE, make_record(Info)),
      ets:insert(?TYPE_TABLE, {Type, ID});
    undefined ->
      ok
  end.

%% @doc Build a dictionary with all tokens for all artifacts.
%%
%% @see ets_add_tokens/3

build_tokens_mapping(Handle) ->
  lists:foldl(
    fun({ID, _Type, Token}, Acc) ->
      dict:update(ID, fun(TokenList) -> [Token | TokenList] end, [Token], Acc)
    end,
    dict:new(),
    grailbag_tokendb:tokens(Handle)
  ).

%% @doc Set tokens in ETS table for an artifact.
%%
%%   Suitable as {@link dict:fold/3} callback function for data returned by
%%   {@link build_tokens_mapping/1}.
%%
%% @see build_tokens_mapping/1

ets_add_tokens(ID, Tokens, _Acc) ->
  ets:update_element(?ARTIFACT_TABLE, ID,
                     {#artifact.tokens, lists:sort(Tokens)}).

%% @private
%% @doc Clean up {@link gen_server} state.

terminate(_Arg, _State = #state{tokens = Handle}) ->
  grailbag_tokendb:close(Handle),
  grailbag_schema:close(),
  ets:delete(?ARTIFACT_TABLE),
  ets:delete(?TYPE_TABLE),
  ok.

%% }}}
%%----------------------------------------------------------
%% communication {{{

%% @private
%% @doc Handle {@link gen_server:call/2}.

handle_call({store, {ID, Type, _Size, _Hash, _CTime, _MTime, Tags, [], valid} = Info} = _Request,
            _From, State) ->
  Changes = grailbag_schema:changes(Type, Tags, []),
  case grailbag_schema:verify(Changes) of
    ok ->
      case ets:insert_new(?ARTIFACT_TABLE, make_record(Info)) of
        true ->
          grailbag_schema:store(Changes),
          ets:insert(?TYPE_TABLE, {Type, ID}),
          {reply, ok, State};
        false ->
          {reply, {error, duplicate_id}, State}
      end;
    {error, [{unknown_type, _}]} ->
      {reply, {error, unknown_type}, State};
    {error, Errors} ->
      Duplicates = lists:usort([T || {duplicate, T, _V} <- Errors]),
      Missing = lists:usort([T || {missing, T} <- Errors]),
      {reply, {error, {schema, Duplicates, Missing}}, State}
  end;

handle_call({delete, ID} = _Request, _From, State) ->
  case ets:lookup(?ARTIFACT_TABLE, ID) of
    [#artifact{type = Type, tags = OldTags, tokens = []}] ->
      ets:delete_object(?TYPE_TABLE, {Type, ID}),
      ets:delete(?ARTIFACT_TABLE, ID),
      grailbag_schema:delete(Type, OldTags),
      case grailbag_artifact:delete(ID) of
        ok ->
          {reply, ok, State};
        {error, Reason} ->
          EventID = grailbag_uuid:uuid(),
          grailbag_log:warn("can't delete artifact from storage", [
            {operation, delete},
            {error, {term, Reason}},
            {artifact, ID},
            {event_id, {uuid, EventID}}
          ]),
          {reply, {error, {storage, EventID}}, State}
      end;
    [#artifact{tokens = [_|_]}] ->
      {reply, {error, artifact_has_tokens}, State};
    [] ->
      {reply, {error, bad_id}, State}
  end;

handle_call({update_tags, ID, SetTags, UnsetTags} = _Request, _From,
            State) ->
  % NOTE: `SetTags' is sorted by the tag name (see `update_tags()' function)
  case ets:lookup(?ARTIFACT_TABLE, ID) of
    [#artifact{type = Type, tags = OldTags} = Record] ->
      % NOTE: `OldTags' is sorted by the tag name (see `make_record()'
      % function)
      NewTags = merge_tags(OldTags, SetTags, sets:from_list(UnsetTags)),
      Changes = grailbag_schema:changes(Type, NewTags, OldTags),
      case grailbag_schema:verify(Changes) of
        ok ->
          case grailbag_artifact:update(ID, NewTags) of
            {ok, MTime} ->
              NewRecord = Record#artifact{
                mtime = MTime,
                tags = NewTags
              },
              ets:insert(?ARTIFACT_TABLE, NewRecord),
              grailbag_schema:store(Changes),
              {reply, ok, State};
            {error, Reason} ->
              EventID = grailbag_uuid:uuid(),
              grailbag_log:warn("can't update tags of an artifact in storage", [
                {operation, update_tags},
                {error, {term, Reason}},
                {artifact, ID},
                {event_id, {uuid, EventID}}
              ]),
              {reply, {error, {storage, EventID}}, State}
          end;
        {error, [{unknown_type, _}]} ->
          {reply, {error, unknown_type}, State};
        {error, Errors} ->
          Duplicates = lists:usort([T || {duplicate, T, _V} <- Errors]),
          Missing = lists:usort([T || {missing, T} <- Errors]),
          {reply, {error, {schema, Duplicates, Missing}}, State}
      end;
    [] ->
      {reply, {error, bad_id}, State}
  end;

handle_call({update_tokens, ID, SetTokens, UnsetTokens} = _Request, _From,
            State = #state{tokens = Handle}) ->
  case ets:lookup(?ARTIFACT_TABLE, ID) of
    [#artifact{type = Type, tokens = OldTokens}] ->
      % XXX: allow unsetting any token, but setting only a known one
      case grailbag_schema:verify_tokens(Type, SetTokens) of
        ok ->
          OldSet = sets:from_list(OldTokens),
          % only the tokens that will actually be deleted from this artifact
          DelSet = sets:intersection(OldSet, sets:from_list(UnsetTokens)),
          ets:update_element(
            ?ARTIFACT_TABLE, ID,
            {#artifact.tokens,
              lists:usort(SetTokens ++
                          sets:to_list(sets:subtract(OldSet, DelSet)))}
          ),
          case move_tokens(ID, Type, Handle, SetTokens, sets:to_list(DelSet)) of
            ok ->
              {reply, ok, State};
            {error, {storage, _EventID} = Reason} ->
              % the specific errors were already logged
              {reply, {error, Reason}, State}
          end;
        {error, [{unknown_type, _}]} ->
          {reply, {error, unknown_type}, State};
        {error, Errors} ->
          Unknown = lists:usort([T || {unknown_token, T} <- Errors]),
          {reply, {error, {schema, Unknown}}, State}
      end;
    [] ->
      {reply, {error, bad_id}, State}
  end;

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

%% @doc Move tokens that are being set from their previous owners, both
%%   on-disk and in-memory.

-spec move_tokens(grailbag:artifact_id(), grailbag:artifact_type(),
                  grailbag_tokendb:handle(),
                  [grailbag:token()], [grailbag:token()]) ->
  ok | {error, Reason}
  when Reason :: {storage, EventID :: grailbag_log:event_id()}.

move_tokens(NewID, Type, Handle, ToSet, ToDelete) ->
  ets_delete_tokens(Handle, Type, NewID, ToSet),
  % TODO: handle write errors
  lists:foreach(
    fun(T) -> grailbag_tokendb:update(Handle, Type, T, NewID) end,
    ToSet
  ),
  % TODO: handle write errors
  lists:foreach(
    fun(T) -> grailbag_tokendb:delete(Handle, Type, T) end,
    ToDelete
  ),
  % TODO: report errors
  ok.

%% @doc Delete tokens that are being set for an artifact from all previous
%%   owners.

-spec ets_delete_tokens(grailbag_tokendb:handle(), grailbag:artifact_type(),
                        grailbag:artifact_id(), [grailbag:token()]) ->
  any().

ets_delete_tokens(Handle, Type, NewID, Tokens) ->
  UpdateArtifacts = lists:usort(lists:foldl(
    fun(T, Acc) ->
      case grailbag_tokendb:id(Handle, Type, T) of
        {ok, ID} when ID /= NewID -> [ID | Acc];
        {ok, NewID} -> Acc;
        undefined -> Acc
      end
    end,
    [],
    Tokens
  )),
  FilterSet = sets:from_list(Tokens),
  FilterPred = fun(T) -> not sets:is_element(T, FilterSet) end,
  lists:foreach(
    fun(ID) ->
      case ets:lookup(?ARTIFACT_TABLE, ID) of
        [#artifact{tokens = OldTokens}] ->
          NewTokens = lists:filter(FilterPred, OldTokens),
          ets:update_element(?ARTIFACT_TABLE, ID,
                             {#artifact.tokens, NewTokens});
        [] ->
          ignore
      end
    end,
    UpdateArtifacts
  ).

%%%---------------------------------------------------------------------------

%% @doc Merge old tags list with list of tags to set, removing tags from
%%   `UnsetTags' set.
%%
%%   Function assumes that both `OldTags' and `SetTags' are sorted by tag
%%   name.

-spec merge_tags([{grailbag:tag(), grailbag:tag_value()}],
                 [{grailbag:tag(), grailbag:tag_value()}],
                 set()) ->
  [{grailbag:tag(), grailbag:tag_value()}].

merge_tags([] = _OldTags, SetTags, _UnsetTags) ->
  SetTags;
merge_tags(OldTags, [] = _SetTags, UnsetTags) ->
  lists:filter(fun({T,_}) -> not sets:is_element(T, UnsetTags) end, OldTags);

merge_tags([{OTag, _OValue} = Old | ORest] = _OldTags,
           [{STag, _SValue} | _SRest] = SetTags, UnsetTags)
when OTag < STag ->
  case sets:is_element(OTag, UnsetTags) of
    true -> merge_tags(ORest, SetTags, UnsetTags);
    false -> [Old | merge_tags(ORest, SetTags, UnsetTags)]
  end;

merge_tags([{OTag, _OValue} | ORest] = _OldTags,
           [{STag, _SValue} | _SRest] = SetTags, UnsetTags)
when OTag == STag ->
  merge_tags(ORest, SetTags, UnsetTags);

merge_tags([{OTag, _OValue} | _ORest] = OldTags,
           [{STag, _SValue} = Set | SRest] = _SetTags, UnsetTags)
when OTag > STag ->
  [Set | merge_tags(OldTags, SRest, UnsetTags)].

%%%---------------------------------------------------------------------------

%% @doc Make record suitable for ETS table from artifact info.

-spec make_record(grailbag:artifact_info()) ->
  #artifact{}.

make_record({ID, Type, Size, Hash, CTime, MTime, Tags, Tokens, _Valid} = _Info) ->
  #artifact{
    id = ID,
    type = Type,
    body_size = Size,
    body_hash = Hash,
    ctime = CTime,
    mtime = MTime,
    tags = lists:keysort(1, Tags),
    tokens = lists:sort(Tokens)
  }.

%%%---------------------------------------------------------------------------

%% @doc Rebuild schema validation counters ({@link grailbag_schema}) from
%%   artifact inventory table.

-spec reload_schema() ->
  ok | error.

reload_schema() ->
  {ok, Schema} = application:get_env(schema),
  ReloadContext = ets:foldl(
    fun(#artifact{id = ID, type = Type, tags = Tags, tokens = Tokens}, Acc) ->
      grailbag_schema:add_artifact(ID, Type, Tags, Tokens, Acc)
    end,
    grailbag_schema:reload(Schema),
    ?ARTIFACT_TABLE
  ),
  grailbag_schema:save(ReloadContext),
  case grailbag_schema:errors(ReloadContext) of
    [] ->
      ok;
    [_|_] = Errors ->
      lists:foreach(fun({ID, Error}) -> log_error(ID, Error) end, Errors),
      error
  end.

%% @doc Log a schema error.

-spec log_error(grailbag:artifact_id(), grailbag_schema:schema_error()) ->
  ok.

log_error(ID, {unknown_type, Type} = _Error) ->
  grailbag_log:info("unknown artifact type", [{artifact, ID}, {type, Type}]);
log_error(ID, {missing, Tag} = _Error) ->
  grailbag_log:info("missing mandatory tag", [{artifact, ID}, {tag, Tag}]);
log_error(ID, {duplicate, Tag, Value} = _Error) ->
  grailbag_log:info("duplicate value of a unique tag",
                    [{artifact, ID}, {tag, Tag}, {value, Value}]);
log_error(ID, {unknown_token, Token} = _Error) ->
  grailbag_log:info("unknown token", [{artifact, ID}, {token, Token}]).

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
