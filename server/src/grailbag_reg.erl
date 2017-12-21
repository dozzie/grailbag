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
-export([list/1, info/1]).

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
  ok | {error, bad_id | file:posix()}.

delete(ID) ->
  gen_server:call(?MODULE, {delete, ID}, infinity).

%update_tags(ID, SetTags, UnsetTags) ->
%  gen_server:call(?MODULE, {update_tags, ID, SetTags, UnsetTags}, infinity).

%update_tokens(ID, SetTokens, UnsetTokens) ->
%  gen_server:call(?MODULE, {update_tokens, ID, SetTokens, UnsetTokens},
%                  infinity).

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
  lists:foldl(
    fun(ID, Acc) ->
      case grailbag_artifact:info(ID) of
        {ok, {ID, Type, _Size, _Hash, _CTime, _MTime, _Tags, _Tokens} = Info} ->
          ets:insert(?ARTIFACT_TABLE, make_record(Info)),
          ets:insert(?TYPE_TABLE, {Type, ID}),
          Acc;
        undefined ->
          Acc
      end
    end,
    ignore,
    grailbag_artifact:list()
  ),
  grailbag_log:info("starting artifact registry", [
    {artifacts, ets:info(?ARTIFACT_TABLE, size)}
  ]),
  State = #state{},
  {ok, State}.

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
    [#artifact{type = Type}] ->
      % TODO: check if the artifact has any tokens set
      ets:delete_object(?TYPE_TABLE, {Type, ID}),
      ets:delete(?ARTIFACT_TABLE, ID),
      Result = grailbag_artifact:delete(ID);
    [] ->
      Result = {error, bad_id}
  end,
  {reply, Result, State};

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
    tags = Tags,
    tokens = Tokens
  }.

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
