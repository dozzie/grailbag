%%%---------------------------------------------------------------------------
%%% @doc
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_schema).

%% public interface
-export([open/0, close/0]).
-export([types/0, known_type/1]).
-export([new_artifact/2, changes/3, verify/1, store/1, store/2, delete/2]).
-export([verify/2, verify_tokens/2]).
-export([reload/1, add_artifact/5, save/1, errors/1]).

-export_type([context/0, schema_diff/0, schema_error/0]).

%%%---------------------------------------------------------------------------
%%% types

-record(reload, {
  schema :: mapping(grailbag:artifact_type(), tuple()),
  errors :: [{grailbag:artifact_id(), schema_error()}]
}).

-record(schema, {
  % XXX: keep `#schema.type' at the same position as `#counter.key'
  type :: grailbag:artifact_type()  | '$1',
  mandatory :: [grailbag:tag()]     | '_',
  unique :: [grailbag:tag()]        | '_',
  tokens :: [grailbag:token()]      | '_'
}).

-record(counter, {
  % XXX: keep `#schema.type' at the same position as `#counter.key'
  key :: counter(),
  counter :: integer()
}).

-record(diff, {
  type :: grailbag:artifact_type() | {unknown_type, grailbag:artifact_type()},
  set :: [{grailbag:tag(), grailbag:tag_value()}],
  unset :: [{grailbag:tag(), grailbag:tag_value()}],
  missing :: [grailbag:tag()]
}).

-define(ETS_TABLE, grailbag_schema).

-type counter() ::
  {grailbag:artifact_type(), grailbag:tag(), grailbag:tag_value()}.

-type context() :: #reload{}.

-type schema_error() ::
    {unknown_type, grailbag:artifact_type()}
  | {missing, grailbag:tag()}
  | {duplicate, grailbag:tag(), grailbag:tag_value()}
  | {unknown_token, grailbag:token()}.

-type schema_diff() :: #diff{}.

-type mapping(_Key, _Value) :: dict().

%%%---------------------------------------------------------------------------
%%% public interface
%%%---------------------------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% creating and cleaning up data store

%% @doc Create data store for keeping the schema.

-spec open() ->
  ok.

open() ->
  ets:new(?ETS_TABLE, [set, named_table, {keypos, #counter.key}]),
  ok.

%% @doc Destroy data store that keeps the schema.

-spec close() ->
  ok.

close() ->
  ets:delete(?ETS_TABLE),
  ok.

%%%---------------------------------------------------------------------------
%%% regular access

%% @doc List known artifact types.

-spec types() ->
  [grailbag:artifact_type()].

types() ->
  lists:sort([
    Type ||
    [Type] <- ets:match(?ETS_TABLE, {schema, '$1', '_', '_', '_'})
  ]).

%% @doc Check if a type of artifact is known (has a schema).

-spec known_type(grailbag:artifact_type()) ->
  boolean().

known_type(Type) ->
  ets:member(?ETS_TABLE, Type).

%% @doc Check if a to-be-uploaded artifact conforms to its schema.
%%
%% @see changes/3
%% @see verify/1
%% @see store/1

-spec new_artifact(grailbag:artifact_type(),
                   [{grailbag:tag(), grailbag:tag_value()}]) ->
  schema_diff().

new_artifact(Type, Tags) ->
  case ets:lookup(?ETS_TABLE, Type) of
    [#schema{unique = Unique, mandatory = Mandatory}] ->
      % artificial tag values to pretend that a mandatory tag got deleted
      % (`diff_tags()' doesn't check if old tags had collisions, so we can use
      % here any value)
      OldTags = [{T, <<>>} || T <- Mandatory],
      {Set, Unset, Missing} = diff_tags(
        lists:sort(Tags), lists:sort(OldTags),
        sets:from_list(Unique), sets:from_list(Mandatory),
        [], [], []
      ),
      _Result = #diff{
        type = Type,
        set = Set,
        unset = Unset,
        missing = Missing
      };
    [] ->
      _Result = #diff{
        type = {unknown_type, Type},
        set = [],
        unset = [],
        missing = []
      }
  end.

%% @doc Calculate differences required to record a change to an artifact.
%%
%% @see new_artifact/2
%% @see verify/1
%% @see store/1

-spec changes(grailbag:artifact_type(),
              [{grailbag:tag(), grailbag:tag_value()}],
              [{grailbag:tag(), grailbag:tag_value()}]) ->
  schema_diff().

changes(Type, Tags, OldTags) ->
  case ets:lookup(?ETS_TABLE, Type) of
    [#schema{unique = Unique, mandatory = Mandatory}] ->
      {Set, Unset, Missing} = diff_tags(
        lists:sort(Tags), lists:sort(OldTags),
        sets:from_list(Unique), sets:from_list(Mandatory),
        [], [], []
      ),
      _Result = #diff{
        type = Type,
        set = Set,
        unset = Unset,
        missing = Missing
      };
    [] ->
      _Result = #diff{
        type = {unknown_type, Type},
        set = [],
        unset = [],
        missing = []
      }
  end.

%% @doc Check if recording changes to an artifact will introduce schema
%%   errors.
%%
%% @see new_artifact/2
%% @see changes/3
%% @see store/1

-spec verify(schema_diff()) ->
  ok | {error, [schema_error()]}.

verify(_Changes = #diff{type = {unknown_type, Type}}) ->
  {error, [{unknown_type, Type}]};
verify(_Changes = #diff{type = Type, set = Set, missing = Missing}) ->
  Errors = lists:foldl(
    fun({Tag, Value}, Acc) ->
      case ets:lookup(?ETS_TABLE, {Type, Tag, Value}) of
        [] -> Acc;
        [#counter{counter = 0}] -> Acc;
        [#counter{}] -> [{duplicate, Tag, Value} | Acc]
      end
    end,
    [{missing, T} || T <- Missing],
    Set
  ),
  case Errors of
    [] -> ok;
    [_|_] -> {error, Errors}
  end.

%% @doc Check correctness of schema of an already recorded artifact.
%%
%%   This is in contrast to {@link verify/1}, which checks if a change
%%   introduces errors.

-spec verify(grailbag:artifact_type(),
             [{grailbag:tag(), grailbag:tag_value()}]) ->
  ok | {error, [schema_error()]}.

verify(Type, Tags) ->
  Errors = case ets:lookup(?ETS_TABLE, Type) of
    [#schema{unique = Unique, mandatory = Mandatory}] ->
      verify(Type, lists:sort(Tags), sets:from_list(Unique), Mandatory, []);
    [] ->
      [{unknown_type, Type}]
  end,
  case Errors of
    [] -> ok;
    [_|_] -> {error, Errors}
  end.

%%----------------------------------------------------------
%% verify() workhorse {{{

%% @doc Verify all tags.
%%
%% @see verify/2

-spec verify(grailbag:artifact_type(), [{grailbag:tag(), grailbag:tag_value()}],
             term(), [grailbag:tag()], [schema_error()]) ->
  [schema_error()].

verify(_Type, [] = _Tags, _Unique, Mandatory, Errors) ->
  [{missing, MT} || MT <- Mandatory] ++ Errors;
verify(Type, [{Tag, Value} | Rest] = _Tags, Unique, Mandatory, Errors) ->
  {NewMandatory, Missing} = missing_tags(Tag, Mandatory, []),
  NewErrors = [{missing, MT} || MT <- Missing] ++ Errors,
  case sets:is_element(Tag, Unique) of
    true ->
      case ets:lookup(?ETS_TABLE, {Type, Tag, Value}) of
        [] ->
          verify(Type, Rest, Unique, NewMandatory, NewErrors);
        [#counter{counter = 0}] ->
          verify(Type, Rest, Unique, NewMandatory, NewErrors);
        [#counter{counter = 1}] ->
          verify(Type, Rest, Unique, NewMandatory, NewErrors);
        [#counter{}] ->
          Err = {duplicate, Tag, Value},
          verify(Type, Rest, Unique, NewMandatory, [Err | NewErrors])
      end;
    false ->
      verify(Type, Rest, Unique, NewMandatory, NewErrors)
  end.

%% }}}
%%----------------------------------------------------------

%% @doc Verify an artifact's tokens.

-spec verify_tokens(grailbag:artifact_type(), [grailbag:token()]) ->
  ok | {error, [schema_error()]}.

verify_tokens(Type, Tokens) ->
  Errors = case ets:lookup(?ETS_TABLE, Type) of
    [#schema{tokens = KnownTokens}] ->
      unknown_tokens(lists:sort(Tokens), KnownTokens);
    [] ->
      [{unknown_type, Type}]
  end,
  case Errors of
    [] -> ok;
    [_|_] -> {error, Errors}
  end.

%% @doc Record a new artifact with its tags in schema store.
%%
%% @see store/1
%% @see verify/1

-spec store(grailbag:artifact_type(),
            [{grailbag:tag(), grailbag:tag_value()}]) ->
  ok.

store(Type, Tags) ->
  store(changes(Type, Tags, [])).

%% @doc Record an artifact, updating its tags in schema store.
%%
%%   The artifact <em>is not</em> verified against schema.
%%
%% @see changes/3
%% @see verify/1

-spec store(schema_diff()) ->
  ok.

store(_Changes = #diff{type = {unknown_type, _Type}}) ->
  % ignore unknown types
  ok;
store(_Changes = #diff{type = Type, set = Set, unset = Unset}) ->
  lists:foreach(fun({Tag, Value}) -> ets_inc({Type, Tag, Value}) end, Set),
  lists:foreach(fun({Tag, Value}) -> ets_dec({Type, Tag, Value}) end, Unset),
  ok.

%% @doc Delete an artifact from schema store.

-spec delete(grailbag:artifact_type(),
             [{grailbag:tag(), grailbag:tag_value()}]) ->
  ok.

delete(Type, OldTags) ->
  store(changes(Type, [], OldTags)).

%%%---------------------------------------------------------------------------
%%% schema reloading

%% @doc Prepare for processing known artifacts by creating a reload context
%%   from new schema.
%%
%%   Note that errors (unknown artifact types, unique tags duplicates, missing
%%   mandatory tags) are reported at the end of process, after all artifacts
%%   were added.
%%
%% @see add_artifact/5
%% @see save/1
%% @see errors/1

-spec reload([{Type, Mandatory :: [Tag], Unique :: [Tag], [Token]}]) ->
  context()
  when Type :: grailbag:artifact_type(),
       Tag :: grailbag:tag(),
       Token :: grailbag:token().

reload(Schema) ->
  SchemaStore = lists:foldl(
    fun({Type, MandatoryTags, UniqueTags, Tokens}, Acc) ->
      dict:store(Type, {
        lists:sort(MandatoryTags),
        dict:from_list([{T, dict:new()} || T <- UniqueTags]),
        sets:from_list(Tokens)
      }, Acc)
    end,
    dict:new(),
    Schema
  ),
  _Context = #reload{
    schema = SchemaStore,
    errors = []
  }.

%% @doc Register an artifact in reload context, updating counters and
%%   list of schema errors as appropriate.
%%
%% @see reload/1
%% @see save/1
%% @see errors/1

-spec add_artifact(grailbag:artifact_id(), grailbag:artifact_type(),
                   [grailbag:tag()], [grailbag:token()], context()) ->
  context().

add_artifact(ID, Type, Tags, _Tokens,
             Context = #reload{schema = Schema, errors = Errors}) ->
  case dict:find(Type, Schema) of
    {ok, {Mandatory, Unique, KnownTokens}} ->
      % TODO: do something with tokens
      {NewUnique, NewErrors} =
        validate_tags(ID, lists:sort(Tags), Mandatory, Unique, Errors),
      NewSchema = dict:store(Type, {Mandatory, NewUnique, KnownTokens}, Schema),
      Context#reload{
        schema = NewSchema,
        errors = NewErrors
      };
    error ->
      Err = {ID, {unknown_type, Type}},
      Context#reload{errors = [Err | Errors]}
  end.

%% @doc Apply reload context so its changes are visible to {@link verify/1},
%%   {@link store/1}, and {@link delete/2}.
%%
%% @see reload/1
%% @see errors/1
%% @see open/0

-spec save(context()) ->
  ok.

save(Context) ->
  DeleteKeys = ets:foldl(
    fun
      (#schema{type = Type}, Acc) ->
        case has_type(Type, Context) of
          false -> [Type | Acc];
          true -> Acc
        end;
      (#counter{key = Key}, Acc) ->
        case has_counter(Key, Context) of
          false -> [Key | Acc];
          true -> Acc
        end
    end,
    [],
    ?ETS_TABLE
  ),
  ets:insert(?ETS_TABLE, get_counters(Context)),
  ets:insert(?ETS_TABLE, get_schema(Context)),
  lists:foreach(fun(K) -> ets:delete(?ETS_TABLE, K) end, DeleteKeys),
  ok.

%% @doc Retrieve schema errors collected in reload context.
%%
%% @see reload/1
%% @see save/1

-spec errors(context()) ->
  [{grailbag:artifact_id(), schema_error()}].

errors(_Context = #reload{errors = Errors}) ->
  Errors.

%%%---------------------------------------------------------------------------
%%% helpers

%%----------------------------------------------------------
%% diff_tags() {{{

%% @doc Calculate difference between tags to be set and unset.

-spec diff_tags([{grailbag:tag(), grailbag:tag_value()}],
                [{grailbag:tag(), grailbag:tag_value()}],
                term(), term(),
                [grailbag:tag()], [{grailbag:tag(), grailbag:tag_value()}],
                [{grailbag:tag(), grailbag:tag_value()}]) ->
  {Set :: [{grailbag:tag(), grailbag:tag_value()}],
    Unset :: [{grailbag:tag(), grailbag:tag_value()}],
    Missing :: [grailbag:tag()]}.

diff_tags([] = _Tags, [] = _OldTags,
          _Unique, _Mandatory, Missing, Set, Unset) ->
  {lists:reverse(Set), lists:reverse(Unset), lists:reverse(Missing)};
diff_tags([{NT, NV} | NRest] = _Tags, [] = OldTags,
          Unique, Mandatory, Missing, Set, Unset) ->
  % NT got set
  diff_tags(NRest, OldTags, Unique, Mandatory, Missing,
            add_if_member(NT, NV, Unique, Set), Unset);
diff_tags([] = Tags, [{OT, OV} | ORest] = _OldTags,
          Unique, Mandatory, Missing, Set, Unset) ->
  % OT got unset
  diff_tags(Tags, ORest, Unique, Mandatory,
            add_if_member(OT, Mandatory, Missing),
            Set, add_if_member(OT, OV, Unique, Unset));
diff_tags([{NT, NV} | NRest] = Tags, [{OT, OV} | ORest] = OldTags,
          Unique, Mandatory, Missing, Set, Unset) ->
  if
    NT == OT, NV == OV ->
      % tag is the same
      diff_tags(NRest, ORest, Unique, Mandatory, Missing, Set, Unset);
    NT == OT, NV /= OV ->
      % tag got changed
      diff_tags(NRest, ORest, Unique, Mandatory, Missing,
                add_if_member(NT, NV, Unique, Set),
                add_if_member(OT, OV, Unique, Unset));
    NT < OT ->
      % NT got set
      diff_tags(NRest, OldTags, Unique, Mandatory, Missing,
                add_if_member(NT, NV, Unique, Set), Unset);
    NT > OT ->
      % OT got unset
      diff_tags(Tags, ORest, Unique, Mandatory,
                add_if_member(OT, Mandatory, Missing),
                Set, add_if_member(OT, OV, Unique, Unset))
  end.

%% @doc Add tag to a list if the tag is member of a set.

-spec add_if_member(grailbag:tag(), term(), [grailbag:tag()]) ->
  [grailbag:tag()].

add_if_member(Tag, Set, List) ->
  case sets:is_element(Tag, Set) of
    true -> [Tag | List];
    false -> List
  end.

%% @doc Add tag+value to a list if the tag is member of a set.

-spec add_if_member(grailbag:tag(), grailbag:tag_value(), term(),
                    [{grailbag:tag(), grailbag:tag_value()}]) ->
  [{grailbag:tag(), grailbag:tag_value()}].

add_if_member(Tag, Value, Set, List) ->
  case sets:is_element(Tag, Set) of
    true -> [{Tag, Value} | List];
    false -> List
  end.

%% }}}
%%----------------------------------------------------------
%% validate_tags() {{{

%% @doc Check if tags of this artifact conform to artifact type's schema
%%   (unique and mandatory tags).
%%
%%   `Tags' is expected to be sorted.

-spec validate_tags(grailbag:artifact_id(),
                    [{grailbag:tag(), grailbag:tag_value()}],
                    [grailbag:tag()], term(),
                    [{grailbag:artifact_id(), schema_error()}]) ->
  {term(), [{grailbag:artifact_id(), schema_error()}]}.

validate_tags(ID, [{T, V} | Rest] = _Tags, Mandatory, Unique, Errors) ->
  {NewMandatory, Missing} = missing_tags(T, Mandatory, []),
  NewErrors1 = [{ID, {missing, MT}} || MT <- Missing] ++ Errors,
  {NewUnique, NewErrors2} = collisions(ID, T, V, Unique, NewErrors1),
  validate_tags(ID, Rest, NewMandatory, NewUnique, NewErrors2);
validate_tags(_ID, [] = _Tags, [] = _Mandatory, Unique, Errors) ->
  % no more tags and all mandatory tags were checked
  {Unique, Errors};
validate_tags(ID, [] = _Tags, Mandatory, Unique, Errors) ->
  % no more tags, but there are mandatory tags left; declare them missing
  Missing = [{ID, {missing, MT}} || MT <- Mandatory],
  {Unique, Missing ++ Errors}.

%% @doc Check if a value of a unique tag was already seen.

-spec collisions(grailbag:artifact_id(), grailbag:tag(), grailbag:tag_value(),
                 term(), [{grailbag:artifact_id(), schema_error()}]) ->
  {NewUniqueCounters :: term(),
    NewErrors :: [{grailbag:artifact_id(), schema_error()}]}.

collisions(ID, Tag, Value, UniqueCounters, Errors) ->
  case dict:find(Tag, UniqueCounters) of
    {ok, Counters} ->
      case dict:find(Value, Counters) of
        error ->
          % it's the first time we've seen this value
          NewCounters = dict:store(Value, {1, ID}, Counters),
          NewUniqueCounters = dict:store(Tag, NewCounters, UniqueCounters),
          {NewUniqueCounters, Errors};
        {ok, {1, FirstID}} ->
          % we've seen this value already, but it was not a collision at the
          % time; add duplicate schema errors for both previous and current
          % artifacts
          NewCounters = dict:store(Value, {2, FirstID}, Counters),
          NewUniqueCounters = dict:store(Tag, NewCounters, UniqueCounters),
          NewErrors = [
            {FirstID, {duplicate, Tag, Value}},
            {ID, {duplicate, Tag, Value}} |
            Errors
          ],
          {NewUniqueCounters, NewErrors};
        {ok, {N, FirstID}} when N > 1 ->
          NewCounters = dict:store(Value, {N + 1, FirstID}, Counters),
          NewUniqueCounters = dict:store(Tag, NewCounters, UniqueCounters),
          NewErrors = [{ID, {duplicate, Tag, Value}} | Errors],
          {NewUniqueCounters, NewErrors}
      end;
    error ->
      % not a unique tag, don't check it
      {UniqueCounters, Errors}
  end.

%% }}}
%%----------------------------------------------------------
%% unknown_tokens() {{{

%% @doc List unknown tokens from a given list.

-spec unknown_tokens([grailbag:token()], [grailbag:token()]) ->
  [schema_error()].

unknown_tokens([] = _Tokens, _KnownTokens) ->
  [];
unknown_tokens(Tokens, [] = _KnownTokens) ->
  [{unknown_token, T} || T <- Tokens];
unknown_tokens([T | _Rest] = Tokens, [K | KRest] = _KnownTokens) when T > K ->
  unknown_tokens(Tokens, KRest);
unknown_tokens([T | Rest] = _Tokens, [K | KRest] = _KnownTokens) when T == K ->
  unknown_tokens(Rest, KRest);
unknown_tokens([T | Rest] = _Tokens, [K | _KRest] = KnownTokens) when T < K ->
  [{unknown_token, T} | unknown_tokens(Rest, KnownTokens)].

%% }}}
%%----------------------------------------------------------
%% missing_tags() {{{

%% @doc Check if all mandatory tags smaller than current tag are present or
%%   missing.

-spec missing_tags(grailbag:tag(), [grailbag:tag()], [grailbag:tag()]) ->
  {NewMandatoryTags :: [grailbag:tag()], NewMissing :: [grailbag:tag()]}.

missing_tags(_CurTag, [] = Mandatory, Missing) ->
  % no more mandatory tags to check
  {Mandatory, Missing};
missing_tags(CurTag, [MT | _Rest] = Mandatory, Missing) when CurTag < MT ->
  % not a mandatory tag, mandatory tag was not missed yet
  {Mandatory, Missing};
missing_tags(CurTag, [MT | Rest] = _Mandatory, Missing) when CurTag == MT ->
  % this is a mandatory tag, we can now skip it in the future
  {Rest, Missing};
missing_tags(CurTag, [MT | Rest] = _Mandatory, Missing) when CurTag > MT ->
  % mandatory tag was missed, add it to the list of errors and check other
  % mandatory tags
  missing_tags(CurTag, Rest, [MT | Missing]).

%% }}}
%%----------------------------------------------------------
%% has_type(), has_counter(), get_counters(), get_schema() {{{

%% @doc Check if a reload context knows about an artifact type.

-spec has_type(grailbag:artifact_type(), context()) ->
  boolean().

has_type(Type, _Context = #reload{schema = Schema}) ->
  dict:is_key(Type, Schema).

%% @doc Check if a reload context knows about a counter

-spec has_counter(counter(), context()) ->
  boolean().

has_counter({Type, Tag, Value} = _Counter,
            _Context = #reload{schema = Schema}) ->
  case dict:find(Type, Schema) of
    {ok, {_Mandatory, Unique, _KnownTokens}} ->
      case dict:find(Tag, Unique) of
        {ok, Counters} -> dict:is_key(Value, Counters);
        error -> false
      end;
    error ->
      false
  end.

%% @doc Extract from reload context counter records ready for inserting into
%%   ETS table.

-spec get_counters(context()) ->
  [#counter{}].

get_counters(_Context = #reload{schema = Schema}) ->
  dict:fold(
    fun(Type, {_Mandatory, Unique, _KnownTokens}, Acc0) ->
      dict:fold(
        fun(Tag, Counters, Acc1) ->
          dict:fold(
            fun(Value, {N, _}, Acc) ->
              Entry = #counter{
                key = {Type, Tag, Value},
                counter = N
              },
              [Entry | Acc]
            end,
            Acc1,
            Counters
          )
        end,
        Acc0,
        Unique
      )
    end,
    [],
    Schema
  ).

%% @doc Extract from reload context type schema records ready for inserting
%%   into ETS table.

-spec get_schema(context()) ->
  [#schema{}].

get_schema(_Context = #reload{schema = Schema}) ->
  dict:fold(
    fun(Type, {Mandatory, Unique, KnownTokens}, Acc) ->
      Entry = #schema{
        type = Type,
        mandatory = Mandatory, % list is sorted already
        unique = lists:sort(dict:fetch_keys(Unique)),
        tokens = lists:sort(sets:to_list(KnownTokens))
      },
      [Entry | Acc]
    end,
    [],
    Schema
  ).

%% }}}
%%----------------------------------------------------------
%% ets_inc(), ets_dec() {{{

%% @doc Increase a counter tracked in ETS table.

-spec ets_inc(counter()) ->
  any().

ets_inc({_Type, _Tag, undefined = _Value} = _Key) ->
  ignore;
ets_inc(Key) ->
  case ets:insert_new(?ETS_TABLE, #counter{key = Key, counter = 1}) of
    true -> ok;
    false -> ets:update_counter(?ETS_TABLE, Key, {#counter.counter, 1})
  end.

%% @doc Decrease a counter tracked in ETS table.

-spec ets_dec(counter()) ->
  any().

ets_dec({_Type, _Tag, undefined = _Value} = _Key) ->
  ignore;
ets_dec(Key) ->
  try ets:update_counter(?ETS_TABLE, Key, {#counter.counter, -1}) of
    N when N > 0 -> ok;
    _ -> ets:delete(?ETS_TABLE, Key)
  catch
    % XXX: this won't normally happen, because the caller shouldn't decrement
    % a counter that was not previously added
    _:_ -> ignore
  end.

%% }}}
%%----------------------------------------------------------

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
