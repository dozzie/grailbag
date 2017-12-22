%%%---------------------------------------------------------------------------
%%% @doc
%%%   Artifact in-memory registry.
%%%
%%%   The registry process is also responsible for modifying disk storage
%%%   (deleting artifacts, updating their tags and tokens).
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_reg).

-behaviour(gen_server).

%% public interface
-export([store/7, delete/1]).
-export([update_tags/3, update_tokens/3]).
-export([list/1, info/1]).

%% supervision tree API
-export([start/0, start_link/0]).

%% gen_server callbacks
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([code_change/3]).

%%%---------------------------------------------------------------------------
%%% types {{{

-define(EVENT_ID_TODO, <<0:128>>).

-define(ARTIFACT_TABLE, grailbag_artifacts).
-define(TYPE_TABLE, grailbag_artifact_types).

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

-record(state, {}).

%%% }}}
%%%---------------------------------------------------------------------------
%%% public interface
%%%---------------------------------------------------------------------------

%% @doc Record a completely uploaded artifact in registry.
%%
%%   Function intended to be called after {@link grailbag_artifact:finish/1}.
%%
%% @see grailbag_artifact:create/2
%% @see grailbag_artifact:finish/1

-spec store(grailbag:artifact_id(), grailbag:artifact_type(), non_neg_integer(),
            grailbag:body_hash(), grailbag:ctime(), grailbag:mtime(),
            [{grailbag:tag(), grailbag:tag_value()}]) ->
  ok | {error, duplicate_id}.

store(ID, Type, BodySize, BodyHash, CTime, MTime, Tags) ->
  ArtifactInfo = {ID, Type, BodySize, BodyHash, CTime, MTime, Tags, []},
  gen_server:call(?MODULE, {store, ArtifactInfo}, infinity).

%% @doc Delete an artifact from memory and from disk.

-spec delete(grailbag:artifact_id()) ->
  ok | {error, Reason}
  when Reason :: bad_id
               | artifact_has_tokens
               | {storage, EventID :: binary()}.

delete(ID) ->
  gen_server:call(?MODULE, {delete, ID}, infinity).

%% @doc Update tags of an artifact.

-spec update_tags(grailbag:artifact_id(),
                  [{grailbag:tag(), grailbag:tag_value()}],
                  [grailbag:tag()]) ->
  ok | {error, Reason}
  when Reason :: bad_id
               | {schema, Dup :: [grailbag:tag()], Missing :: [grailbag:tag()]}
               | {storage, EventID :: binary()}.

update_tags(ID, SetTags, UnsetTags) ->
  % `SetTags' is expected to be sorted by tag name
  Request = {update_tags, ID, lists:keysort(1, SetTags), UnsetTags},
  gen_server:call(?MODULE, Request, infinity).

%% @doc Update tokens of an artifact.

-spec update_tokens(grailbag:artifact_id(), [grailbag:token()],
                    [grailbag:token()]) ->
  ok | {error, Reason}
  when Reason :: bad_id
               | {schema, Unknown :: [grailbag:token()]}
               | {storage, EventID :: binary()}.

update_tokens(ID, SetTokens, UnsetTokens) ->
  Request = {update_tokens, ID, SetTokens, UnsetTokens},
  gen_server:call(?MODULE, Request, infinity).

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
      {ok, {ID, Type, Size, Hash, CTime, MTime, Tags, Tokens}};
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
  ets:new(?ARTIFACT_TABLE,
          [set, named_table, protected, {keypos, #artifact.id}]),
  ets:new(?TYPE_TABLE, [bag, named_table, protected]),
  lists:foreach(fun ets_add_artifact/1, grailbag_artifact:list()),
  grailbag_log:info("starting artifact registry", [
    {artifacts, ets:info(?ARTIFACT_TABLE, size)}
  ]),
  State = #state{},
  {ok, State}.

ets_add_artifact(ID) ->
  case grailbag_artifact:info(ID) of
    {ok, {ID, Type, _Size, _Hash, _CTime, _MTime, _Tags, _Tokens} = Info} ->
      ets:insert(?ARTIFACT_TABLE, make_record(Info)),
      ets:insert(?TYPE_TABLE, {Type, ID});
    undefined ->
      ok
  end.

%% @private
%% @doc Clean up {@link gen_server} state.

terminate(_Arg, _State) ->
  ets:delete(?ARTIFACT_TABLE),
  ets:delete(?TYPE_TABLE),
  ok.

%% }}}
%%----------------------------------------------------------
%% communication {{{

%% @private
%% @doc Handle {@link gen_server:call/2}.

handle_call({store, {ID, Type, _Size, _Hash, _CTime, _MTime, _Tags, []} = Info} = _Request,
            _From, State) ->
  case ets:insert_new(?ARTIFACT_TABLE, make_record(Info)) of
    true ->
      ets:insert(?TYPE_TABLE, {Type, ID}),
      {reply, ok, State};
    false ->
      {reply, {error, duplicate_id}, State}
  end;

handle_call({delete, ID} = _Request, _From, State) ->
  case ets:lookup(?ARTIFACT_TABLE, ID) of
    [#artifact{type = Type, tokens = []}] ->
      ets:delete_object(?TYPE_TABLE, {Type, ID}),
      ets:delete(?ARTIFACT_TABLE, ID),
      case grailbag_artifact:delete(ID) of
        ok ->
          {reply, ok, State};
        {error, _Reason} ->
          % TODO: log this error and return correlation ID
          {reply, {error, {storage, ?EVENT_ID_TODO}}, State}
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
    [#artifact{tags = OldTags} = Record] ->
      % NOTE: `OldTags' is sorted by the tag name (see `make_record()'
      % function)
      FilterSet = sets:from_list(UnsetTags),
      OldFilteredTags = lists:filter(
        fun({T, _V}) -> not sets:is_element(T, FilterSet) end,
        OldTags
      ),
      % `lists:keymerge()' gives precedence to the first list `SetTags' for
      % keys in both lists, which is exactly what we need
      NewTags = lists:keymerge(1, SetTags, OldFilteredTags),
      % TODO: verify `NewTags' against schema
      case grailbag_artifact:update_tags(ID, NewTags) of
        ok ->
          NewRecord = Record#artifact{tags = NewTags},
          ets:insert(?ARTIFACT_TABLE, NewRecord),
          {reply, ok, State};
        {error, _Reason} ->
          % TODO: log this error and return correlation ID
          {reply, {error, {storage, ?EVENT_ID_TODO}}, State}
      end;
    [] ->
      {reply, {error, bad_id}, State}
  end;

handle_call({update_tokens, ID, SetTokens, UnsetTokens} = _Request, _From,
            State) ->
  % TODO: move tags from another artifact, if applicable
  case ets:lookup(?ARTIFACT_TABLE, ID) of
    [#artifact{tokens = OldTokens} = Record] ->
      FilterSet = sets:from_list(UnsetTokens),
      NewTokens = lists:usort(
        SetTokens ++
        lists:filter(fun(T) -> not sets:is_element(T, FilterSet) end, OldTokens)
      ),
      % TODO: verify `NewTokens' against schema
      case grailbag_artifact:update_tokens(ID, NewTokens) of
        ok ->
          NewRecord = Record#artifact{tokens = NewTokens},
          ets:insert(?ARTIFACT_TABLE, NewRecord),
          {reply, ok, State};
        {error, _Reason} ->
          % TODO: log this error and return correlation ID
          {reply, {error, {storage, ?EVENT_ID_TODO}}, State}
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

%% @doc Make record suitable for ETS table from artifact info.

-spec make_record(grailbag:artifact_info()) ->
  #artifact{}.

make_record({ID, Type, Size, Hash, CTime, MTime, Tags, Tokens} = _Info) ->
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
%%% vim:ft=erlang:foldmethod=marker
