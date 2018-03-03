%%%---------------------------------------------------------------------------
%%% @private
%%% @doc
%%%   TCP subsystem supervisor.
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_tcp_sup).

-behaviour(supervisor).

%% supervision tree API
-export([start_link/0]).

%% config reloading
-export([reload/0]).

%% supervisor callbacks
-export([init/1]).

%%%---------------------------------------------------------------------------
%%% supervision tree API
%%%---------------------------------------------------------------------------

%% @private
%% @doc Start the supervisor process.

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%---------------------------------------------------------------------------
%%% config reloading
%%%---------------------------------------------------------------------------

%% @doc Reload configuration (start missing children, stop excessive ones,
%%   instruct all the rest to re-bind themselves).

-spec reload() ->
  ok | {error, [term()]}.

reload() ->
  {ok, NewAddrs} = application:get_env(grailbag, listen),
  Expected = lists:foldl(
    fun({Addr, Port, SSLOpts}, Acc) ->
      dict:store(child_name(Addr, Port), SSLOpts, Acc)
    end,
    dict:new(),
    NewAddrs
  ),
  Current = lists:foldl(
    fun({Name, Pid, worker, _}, Acc) -> dict:store(Name, Pid, Acc) end,
    dict:new(),
    supervisor:which_children(?MODULE)
  ),
  RebindCandidates = dict:fold(
    fun(Name, Pid, Rebind) ->
      case dict:find(Name, Expected) of
        {ok, SSLOpts} ->
          [{Name, Pid, SSLOpts} | Rebind];
        error ->
          ok = stop_child(Name, Pid),
          Rebind
      end
    end,
    [],
    Current
  ),
  StartErrors = dict:fold(
    fun(Name, SSLOpts, Errors) ->
      case dict:is_key(Name, Current) of
        true ->
          Errors;
        false ->
          case start_child(Name, SSLOpts) of
            ok -> Errors;
            Err -> [Err | Errors]
          end
      end
    end,
    [],
    Expected
  ),
  RebindErrors = lists:foldl(
    fun({Name, Pid, SSLOpts}, Errors) ->
      case reload_child(Name, Pid, SSLOpts) of
        ok -> Errors;
        Err -> [Err | Errors]
      end
    end,
    [],
    RebindCandidates
  ),
  case StartErrors ++ RebindErrors of
    [] -> ok;
    Errors -> {error, Errors}
  end.

%% @doc Start a missing child.

start_child({_, Address, Port} = Name, SSLOpts) ->
  case supervisor:start_child(?MODULE, listen_child(Address, Port, SSLOpts)) of
    {ok, _Pid} -> ok;
    {error, Reason} -> {start, Name, Reason}
  end.

%% @doc Stop an excessive child.

stop_child(Name, Pid) ->
  grailbag_tcp_listen:shutdown(Pid),
  supervisor:delete_child(?MODULE, Name),
  ok.

%% @doc Instruct the child to re-bind its listening socket.

reload_child(Name, Pid, SSLOpts) ->
  case grailbag_tcp_listen:rebind(Pid, SSLOpts) of
    ok -> ok;
    {error, Reason} -> {reload, Name, Reason}
  end.

%%%---------------------------------------------------------------------------
%%% supervisor callbacks
%%%---------------------------------------------------------------------------

%% @private
%% @doc Initialize supervisor.

init([] = _Args) ->
  {ok, Addrs} = application:get_env(listen),
  Strategy = {one_for_one, 5, 10},
  Children = [
    {grailbag_tcp_conn_sup, {grailbag_tcp_conn_sup, start_link, []},
      permanent, 1000, supervisor, [grailbag_tcp_conn_sup]} |
    [listen_child(Addr, Port, SSLOpts) || {Addr, Port, SSLOpts} <- Addrs]
  ],
  {ok, {Strategy, Children}}.

%%%---------------------------------------------------------------------------

listen_child(Address, Port, SSLOpts) ->
  {child_name(Address, Port),
    {grailbag_tcp_listen, start_link, [Address, Port, SSLOpts]},
    transient, 1000, worker, [grailbag_tcp_listen]}.

child_name(Address, Port) ->
  {grailbag_tcp_listen, Address, Port}.

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
