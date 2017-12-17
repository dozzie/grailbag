%%%---------------------------------------------------------------------------
%%% @doc
%%%   Client connection worker.
%%% @end
%%%---------------------------------------------------------------------------

-module(grailbag_tcp_conn).

-behaviour(gen_server).

%% public interface
-export([take_over/1]).

%% supervision tree API
-export([start/1, start_link/1]).

%% gen_server callbacks
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([code_change/3]).

%%%---------------------------------------------------------------------------
%%% types {{{

-record(state, {
  socket :: gen_tcp:socket()
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

-spec take_over(gen_tcp:socket()) ->
  {ok, pid()} | {error, term()}.

take_over(Socket) ->
  case grailbag_tcp_conn_sup:spawn_worker(Socket) of
    {ok, Pid} ->
      ok = gen_tcp:controlling_process(Socket, Pid),
      inet:setopts(Socket, [binary, {packet, raw}, {active, once}]),
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

start(Socket) ->
  gen_server:start(?MODULE, [Socket], []).

%% @private
%% @doc Start worker process.

start_link(Socket) ->
  gen_server:start_link(?MODULE, [Socket], []).

%%%---------------------------------------------------------------------------
%%% gen_server callbacks
%%%---------------------------------------------------------------------------

%%----------------------------------------------------------
%% initialization/termination {{{

%% @private
%% @doc Initialize event handler.

init([Socket] = _Args) ->
  {ok, {PeerAddr, PeerPort}} = inet:peername(Socket),
  {ok, {LocalAddr, LocalPort}} = inet:sockname(Socket),
  grailbag_log:set_context(connection, [
    {client, {str, format_address(PeerAddr, PeerPort)}},
    {local_address, {str, format_address(LocalAddr, LocalPort)}}
  ]),
  grailbag_log:info("new connection"),
  State = #state{
    socket = Socket
  },
  {ok, State}.

%% @private
%% @doc Clean up after event handler.

terminate(_Arg, _State = #state{socket = Socket}) ->
  gen_tcp:close(Socket),
  ok.

%% }}}
%%----------------------------------------------------------
%% communication {{{

%% @private
%% @doc Handle {@link gen_server:call/2}.

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

handle_info({tcp, Socket, _Data} = _Message,
            State = #state{socket = Socket}) ->
  % TODO: process the data
  inet:setopts(Socket, [{active, once}]),
  {noreply, State};

handle_info({tcp_closed, Socket} = _Message,
            State = #state{socket = Socket}) ->
  {stop, normal, State};

handle_info({tcp_error, Socket, Reason} = _Message,
            State = #state{socket = Socket}) ->
  grailbag_log:warn("TCP socket closed abnormally", [{error, {term, Reason}}]),
  {stop, normal, State};

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
%%% helper functions
%%%---------------------------------------------------------------------------

%% @doc Format IP address and port number for logging.

-spec format_address(inet:ip_address(), inet:port_number()) ->
  string().

format_address({A,B,C,D} = _Address, Port) ->
  OctetList = [
    integer_to_list(A), integer_to_list(B),
    integer_to_list(C), integer_to_list(D)
  ],
  string:join(OctetList, ".") ++ ":" ++ integer_to_list(Port);
format_address({0, 0, 0, 0, 0, 16#ffff, A, B} = _Address, Port) ->
  OctetList = [
    integer_to_list(A div 256), integer_to_list(A rem 256),
    integer_to_list(B div 256), integer_to_list(B rem 256)
  ],
  "[::ffff:" ++ string:join(OctetList, ".") ++ "]:" ++ integer_to_list(Port);
format_address({_,_,_,_,_,_,_,_} = Address, Port) ->
  "[" ++ string:to_lower(format_ipv6(Address)) ++ "]:" ++ integer_to_list(Port).

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
