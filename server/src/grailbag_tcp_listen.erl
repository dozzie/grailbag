%%%---------------------------------------------------------------------------
%%% @doc
%%%   Connection acceptor process.
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_tcp_listen).

-behaviour(gen_server).

%% supervision tree API
-export([start/3, start_link/3]).

%% config reloading
-export([rebind/2, shutdown/1]).

%% gen_server callbacks
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([code_change/3]).

%%%---------------------------------------------------------------------------
%%% types {{{

-define(ACCEPT_LOOP_INTERVAL, 100).
-define(REBIND_LOOP_INTERVAL, 5000).

-type address() :: any | inet:ip_address() | inet:hostname().

-record(state, {
  socket :: gen_tcp:socket() | undefined,
  address :: address(),
  bind_address :: any | inet:ip_address(),
  port :: inet:port_number(),
  ssl_options :: [proplists:property()]
}).

%%% }}}
%%%---------------------------------------------------------------------------
%%% supervision tree API
%%%---------------------------------------------------------------------------

%% @private
%% @doc Start acceptor process.

start(Addr, Port, SSLOpts) ->
  gen_server:start(?MODULE, [Addr, Port, SSLOpts], []).

%% @private
%% @doc Start acceptor process.

start_link(Addr, Port, SSLOpts) ->
  gen_server:start_link(?MODULE, [Addr, Port, SSLOpts], []).

%%%---------------------------------------------------------------------------
%%% config reloading
%%%---------------------------------------------------------------------------

%% @doc Re-bind socket to the listen address.
%%
%%   It's mainly useful when DNS entry for the address has changed.

-spec rebind(pid(), [proplists:property()]) ->
  ok | {error, term()}.

rebind(Pid, SSLOpts) ->
  gen_server:call(Pid, {rebind, SSLOpts}, infinity).

%% @doc Shutdown the listener.

-spec shutdown(pid()) ->
  ok.

shutdown(Pid) ->
  gen_server:call(Pid, shutdown).

%%%---------------------------------------------------------------------------
%%% gen_server callbacks
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% initialization/termination {{{

%% @private
%% @doc Initialize {@link gen_server} state.

init([Addr, Port, SSLOpts] = _Args) ->
  grailbag_log:set_context(connection, [
    {address, format_address(Addr, Port)}
  ]),
  case resolve(Addr) of
    {ok, BindAddr} ->
      case listen(BindAddr, Port) of
        {ok, Socket} ->
          grailbag_log:info("listening on socket", [
            {bind_address, format_address(BindAddr)}
          ]),
          State = #state{
            socket = Socket,
            address = Addr,
            bind_address = BindAddr,
            port = Port,
            ssl_options = SSLOpts
          },
          {ok, State, 0};
        {error, Reason} ->
          grailbag_log:err("can't setup listening socket", [
            {action, setup},
            {error, {term, Reason}},
            {bind_address, format_address(BindAddr)}
          ]),
          {stop, {listen, Reason}}
      end;
    {error, Reason} ->
      grailbag_log:err("can't resolve bind address", [
        {action, setup},
        {error, {term, Reason}}
      ]),
      {stop, {resolve, Reason}}
  end.

%% @private
%% @doc Clean up {@link gen_server} state.

terminate(_Arg, _State = #state{socket = Socket}) ->
  close(Socket),
  ok.

%% }}}
%%----------------------------------------------------------
%% communication {{{

%% @private
%% @doc Handle {@link gen_server:call/2}.

handle_call({rebind, SSLOpts} = _Request, _From, State) ->
  case rebind_state(State) of
    {ok, State} ->
      % stay silent
      NewState = State#state{ssl_options = SSLOpts},
      {reply, ok, NewState, 0};
    {ok, NewState = #state{bind_address = NewBindAddr}} ->
      grailbag_log:info("rebound to a new address", [
        {bind_address, format_address(NewBindAddr)}
      ]),
      NewState1 = NewState#state{ssl_options = SSLOpts},
      {reply, ok, NewState1, 0};
    {error, Reason, NewState} ->
      case Reason of
        {resolve, E} ->
          grailbag_log:err("can't resolve bind address", [
            {action, rebind},
            {error, {term, E}}
          ]);
        {listen, E} ->
          grailbag_log:err("can't setup listening socket", [
            {action, rebind},
            {error, {term, E}},
            {bind_address, format_address(NewState#state.bind_address)}
          ])
      end,
      NewState1 = NewState#state{ssl_options = SSLOpts},
      {reply, {error, Reason}, NewState1, ?REBIND_LOOP_INTERVAL}
  end;

handle_call(shutdown = _Request, _From, State) ->
  grailbag_log:info("shutting down listening socket"),
  {stop, normal, ok, State};

%% unknown calls
handle_call(_Request, _From, State) ->
  {reply, {error, unknown_call}, State, 0}.

%% @private
%% @doc Handle {@link gen_server:cast/2}.

%% unknown casts
handle_cast(_Request, State) ->
  {noreply, State, 0}.

%% @private
%% @doc Handle incoming messages.

handle_info(timeout = _Message, State = #state{socket = undefined}) ->
  case rebind_state(State) of
    {ok, NewState = #state{bind_address = NewBindAddr}} ->
      grailbag_log:info("rebound to a new address", [
        {bind_address, format_address(NewBindAddr)}
      ]),
      {noreply, NewState, 0};
    {error, _Reason, NewState} ->
      % stay silent
      {noreply, NewState, ?REBIND_LOOP_INTERVAL}
  end;

handle_info(timeout = _Message,
            State = #state{socket = Socket, ssl_options = SSLOpts}) ->
  case gen_tcp:accept(Socket, ?ACCEPT_LOOP_INTERVAL) of
    {ok, Client} ->
      grailbag_tcp_conn:take_over(Client, SSLOpts),
      {noreply, State, 0};
    {error, timeout} ->
      % OK, no incoming connection
      {noreply, State, 0};
    {error, Reason} ->
      grailbag_log:err("accept error", [{error, {term, Reason}}]),
      {stop, {accept, Reason}, State}
  end;

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

%% @doc Re-bind a listening socket if bind address changed.

-spec rebind_state(#state{}) ->
  {ok, #state{}} | {error, Reason, #state{}}
  when Reason :: {resolve | listen, inet:posix()}.

rebind_state(State = #state{socket = Socket, address = Addr, port = Port,
                            bind_address = BindAddr}) ->
  case resolve(Addr) of
    {ok, BindAddr} when Socket /= undefined ->
      % socket present and the same as old bind address; do nothing
      {ok, State};
    {ok, NewBindAddr} ->
      % either a new bind address or the socket is closed
      close(Socket),
      case listen(NewBindAddr, Port) of
        {ok, NewSocket} ->
          NewState = State#state{
            socket = NewSocket,
            bind_address = NewBindAddr
          },
          {ok, NewState};
        {error, Reason} ->
          NewState = State#state{
            socket = undefined,
            bind_address = NewBindAddr
          },
          {error, {listen, Reason}, NewState}
      end;
    {error, Reason} ->
      close(Socket),
      NewState = State#state{socket = undefined},
      {error, {resolve, Reason}, NewState}
  end.

%%%---------------------------------------------------------------------------

%% @doc Bind to a port and (possibly) IP address.

-spec listen(any | inet:ip_address(), inet:port_number()) ->
  {ok, gen_tcp:socket()} | {error, system_limit | inet:posix()}.

listen(BindAddr, Port) ->
  Options = [
    binary, {packet, raw}, {active, false},
    {reuseaddr, true}, {keepalive, true}
  ],
  case BindAddr of
    any -> gen_tcp:listen(Port, Options);
    _   -> gen_tcp:listen(Port, [{ip, BindAddr} | Options])
  end.

%% @doc Close a TCP listening socket.

-spec close(gen_tcp:socket() | undefined) ->
  ok.

close(undefined = _Socket) ->
  ok;
close(Socket) ->
  gen_tcp:close(Socket).

%% @doc Resolve hostname to an IP address.
%%
%% @todo IPv6 support

-spec resolve(address()) ->
  {ok, any | inet:ip_address()} | {error, inet:posix()}.

resolve(any = _Address) ->
  {ok, any};
resolve({_,_,_,_} = Address) ->
  {ok, Address};
resolve({_,_,_,_,_,_,_,_} = Address) ->
  {ok, Address};
resolve(Address) when is_list(Address); is_atom(Address) ->
  inet:getaddr(Address, inet).

%%%---------------------------------------------------------------------------

%% @doc Make a printable string from an address/port pair.

-spec format_address(address(), inet:port_number()) ->
  binary().

format_address({_,_,_,_,_,_,_,_} = Addr, Port) ->
  % IPv6 needs additionally square brackets
  iolist_to_binary(["[", format_address(Addr), "]:", integer_to_list(Port)]);
format_address(Addr, Port) ->
  iolist_to_binary([format_address(Addr), $:, integer_to_list(Port)]).

%% @doc Make a printable string from an address.

-spec format_address(address()) ->
  binary().

format_address(any = _Addr) ->
  <<"*">>;
format_address(Addr) when is_atom(Addr) ->
  atom_to_binary(Addr, utf8);
format_address(Addr) when is_list(Addr) ->
  list_to_binary(Addr);
format_address({A,B,C,D} = _Addr) ->
  iolist_to_binary([
    integer_to_list(A), $., integer_to_list(B), $.,
    integer_to_list(C), $., integer_to_list(D)
  ]);
format_address({0, 0, 0, 0, 0, 16#ffff, A, B} = _Addr) ->
  iolist_to_binary([
    "::ffff:",
    integer_to_list(A div 256), $., integer_to_list(A rem 256), $.,
    integer_to_list(B div 256), $., integer_to_list(B rem 256)
  ]);
format_address({_,_,_,_,_,_,_,_} = Addr) ->
  list_to_binary(string:to_lower(format_ipv6(Addr))).

%% @doc Present IPv6 address in its shortened string format.
%%
%%   Note that the hex digits are upper case, so {@link string:to_lower/1}
%%   should be used on the returned value.

-spec format_ipv6(Addr :: inet:ip6_address()) ->
  string().

format_ipv6({0, 0, 0, 0, 0, 0, 0, 0}) -> "::";

format_ipv6({0, 0, 0, 0, 0, 0, 0, A}) -> add_colons(["", "", A]);
format_ipv6({A, 0, 0, 0, 0, 0, 0, 0}) -> add_colons([A, "", ""]);

format_ipv6({0, 0, 0, 0, 0, 0, A, B}) -> add_colons(["", "", A, B]);
format_ipv6({A, 0, 0, 0, 0, 0, 0, B}) -> add_colons([A, "", B]);
format_ipv6({A, B, 0, 0, 0, 0, 0, 0}) -> add_colons([A, B, "", ""]);

format_ipv6({0, 0, 0, 0, 0, A, B, C}) -> add_colons(["", "", A, B, C]);
format_ipv6({A, 0, 0, 0, 0, 0, B, C}) -> add_colons([A, "", B, C]);
format_ipv6({A, B, 0, 0, 0, 0, 0, C}) -> add_colons([A, B, "", C]);
format_ipv6({A, B, C, 0, 0, 0, 0, 0}) -> add_colons([A, B, C, "", ""]);

format_ipv6({0, 0, 0, 0, A, B, C, D}) -> add_colons(["", "", A, B, C, D]);
format_ipv6({A, 0, 0, 0, 0, B, C, D}) -> add_colons([A, "", B, C, D]);
format_ipv6({A, B, 0, 0, 0, 0, C, D}) -> add_colons([A, B, "", C, D]);
format_ipv6({A, B, C, 0, 0, 0, 0, D}) -> add_colons([A, B, C, "", D]);
format_ipv6({A, B, C, D, 0, 0, 0, 0}) -> add_colons([A, B, C, D, "", ""]);

format_ipv6({0, 0, 0, A, B, C, D, E}) -> add_colons(["", "", A, B, C, D, E]);
format_ipv6({A, 0, 0, 0, B, C, D, E}) -> add_colons([A, "", B, C, D, E]);
format_ipv6({A, B, 0, 0, 0, C, D, E}) -> add_colons([A, B, "", C, D, E]);
format_ipv6({A, B, C, 0, 0, 0, D, E}) -> add_colons([A, B, C, "", D, E]);
format_ipv6({A, B, C, D, 0, 0, 0, E}) -> add_colons([A, B, C, D, "", E]);
format_ipv6({A, B, C, D, E, 0, 0, 0}) -> add_colons([A, B, C, D, E, "", ""]);

format_ipv6({0, 0, A, B, C, D, E, F}) -> add_colons(["", "", A, B, C, D, E, F]);
format_ipv6({A, 0, 0, B, C, D, E, F}) -> add_colons([A, "", B, C, D, E, F]);
format_ipv6({A, B, 0, 0, C, D, E, F}) -> add_colons([A, B, "", C, D, E, F]);
format_ipv6({A, B, C, 0, 0, D, E, F}) -> add_colons([A, B, C, "", D, E, F]);
format_ipv6({A, B, C, D, 0, 0, E, F}) -> add_colons([A, B, C, D, "", E, F]);
format_ipv6({A, B, C, D, E, 0, 0, F}) -> add_colons([A, B, C, D, E, "", F]);
format_ipv6({A, B, C, D, E, F, 0, 0}) -> add_colons([A, B, C, D, E, F, "", ""]);

format_ipv6({A, B, C, D, E, F, G, H}) -> add_colons([A, B, C, D, E, F, G, H]).

%% @doc Join a list of fields with colons.

-spec add_colons(Fields :: [[] | integer()]) ->
  string().

add_colons([""]) -> "";
add_colons([F]) -> integer_to_list(F, 16);
add_colons(["" | Rest]) -> ":" ++ add_colons(Rest);
add_colons([F | Rest]) -> integer_to_list(F, 16) ++ ":" ++ add_colons(Rest).

%%%---------------------------------------------------------------------------
%%% vim:ft=erlang:foldmethod=marker
