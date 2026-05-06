%
% Copyright 2026 Davide Bettio <davide@uninstall.it>
%
% Licensed under the Apache License, Version 2.0 (the "License");
% you may not use this file except in compliance with the License.
% You may obtain a copy of the License at
%
%    http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
% See the License for the specific language governing permissions and
% limitations under the License.
%
% SPDX-License-Identifier: Apache-2.0
%

%% @doc MQTT 3.1.1 client process.
%%
%% A client is started with {@link connect/1} or {@link connect/2}; the
%% returned pid receives all subsequent operations. Inbound MQTT events
%% are delivered to the owner process as messages of type
%% {@type mqtt_event()}.
%%
%% == Transports ==
%%
%% The `transport' option selects the transport. Built-in choices are
%% the atoms `gen_tcp' (default, plaintext) and `ssl'. A pre-opened
%% socket can also be passed as `{Module, Connection}'; in that case
%% `Module' must export `send/2' and `close/1', and the caller must own
%% the socket so ownership can be transferred to the gen_server.
%%
%% For active-mode transports the gen_server expects messages of the
%% form `{DataTag, Connection, Binary}', `{ClosedTag, Connection}', and
%% `{ErrorTag, Connection, Reason}'. For passive-mode transports
%% (`ssl', or any transport with `active => false') a reader process is
%% spawned that calls `recv/2' in a loop and forwards data as the same
%% messages. Tags default to `{tcp, tcp_closed, tcp_error}' for
%% `gen_tcp' and `{ssl, ssl_closed, ssl_error}' for `ssl', and can be
%% overridden via the `transport_tags' option.
%%
%% == TLS options ==
%%
%% No defaults are injected into `ssl_opts': the caller chooses the
%% security posture. On AtomVM `ssl_opts' must include
%% `{verify, verify_none}', as that is the only verification mode the
%% AtomVM `ssl' module currently supports (custom CA certificates and
%% `verify_peer' are not available). On stock OTP, callers can pass
%% `{verify, verify_peer}' together with the appropriate `cacerts'.
%%
%% == Owner messages ==
%%
%% The owner (caller of {@link connect/1} unless overridden) receives:
%% <ul>
%%   <li>`{mqtt, Pid, connack, ConnAckData}' once the broker replies to
%%       CONNECT.</li>
%%   <li>`{mqtt, Pid, publish, PublishData}' for inbound PUBLISH at any
%%       QoS. By default PUBACK / PUBREC / PUBCOMP are sent automatically;
%%       see "Manual ACK" below.</li>
%%   <li>`{mqtt, Pid, disconnected, #{}}' when the broker closes the
%%       connection.</li>
%%   <li>`{mqtt, Pid, error, #{reason := term()}}' on transport error,
%%       protocol error, or other fatal condition. The gen_server stops
%%       immediately afterwards.</li>
%% </ul>
%%
%% == Manual ACK ==
%%
%% Pass `auto_ack => false' in the connect options to opt into manual
%% acknowledgment for inbound QoS 1 / QoS 2 PUBLISH. The owner receives
%% the `{mqtt, Pid, publish, _}' event as usual, but PUBACK (QoS 1) or
%% PUBREC (QoS 2) is held back until the owner calls
%% {@link ack/2}. The QoS 2 second leg (broker's PUBREL -> our PUBCOMP)
%% remains automatic. This lets owners persist a message durably before
%% telling the broker it's been received: an owner crash between event
%% delivery and persistence will trigger a broker retransmit on
%% reconnect.
%%
%% @end
-module(amqtt_client).
-behaviour(gen_server).

-export_type([
    connect_opts/0,
    transport/0,
    transport_tags/0,
    mqtt_event/0
]).

-type qos() :: amqtt_proto:qos().
-type packet_id() :: amqtt_proto:packet_id().
-type topic() :: binary().

-type transport_module() :: gen_tcp | ssl | module().
-type connection() :: term().
-type transport() :: gen_tcp | ssl | {transport_module(), connection()}.
-type transport_tags() :: {DataTag :: atom(), ClosedTag :: atom(), ErrorTag :: atom()}.

-type connect_opts() :: #{
    host => string() | binary() | inet:ip_address(),
    port => 1..65535,
    client_id := binary(),
    keep_alive_seconds => 0..65535,
    clean_session => boolean(),
    username => binary(),
    password => binary(),
    will_topic => binary(),
    will_message => binary(),
    will_qos => qos(),
    will_retain => boolean(),
    transport => transport(),
    transport_tags => transport_tags(),
    ssl_opts => [term()],
    active => boolean(),
    owner => pid(),
    auto_ack => boolean()
}.

-type mqtt_event() ::
    {mqtt, pid(), connack, amqtt_proto:connack_data()}
    | {mqtt, pid(), publish, amqtt_proto:publish_data()}
    | {mqtt, pid(), disconnected, #{}}
    | {mqtt, pid(), error, #{reason := term()}}.

-export([
    connect/1,
    connect/2,
    publish/4,
    publish/5,
    subscribe/2,
    unsubscribe/2,
    disconnect/1,
    ping/1,
    ack/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(REQUEST_TIMEOUT, 30000).
-define(MAX_BUFFER_SIZE, (1 bsl 18)).

-type pending_ack_type() :: puback | pubrec | pubcomp | suback | unsuback.
-type pending_entry() :: {pending_ack_type(), gen_server:from(), reference()}.
-type inbound_phase() :: awaiting_ack | pubrec_sent.
-type inbound_entry() :: {inbound_phase(), qos(), amqtt_proto:publish_data()}.

-record(state, {
    transport :: transport_module() | undefined,
    connection :: connection() | undefined,
    transport_tags :: transport_tags() | undefined,
    reader :: pid() | undefined,
    pending_connect_opts :: connect_opts() | undefined,
    buffer = <<>> :: binary(),
    next_packet_id = 1 :: packet_id(),
    pending = #{} :: #{packet_id() => pending_entry()},
    inbound_qos2 = #{} :: #{packet_id() => true},
    auto_ack = true :: boolean(),
    inbound_pending_ack = #{} :: #{packet_id() => inbound_entry()},
    owner :: pid() | undefined,
    owner_monitor :: reference() | undefined,
    keep_alive_seconds :: 0..65535 | undefined,
    ping_timer :: reference() | undefined,
    pingresp_timer :: reference() | undefined,
    connected = false :: boolean()
}).

%% -------------------------------------------------------------------
%% Public API
%% -------------------------------------------------------------------

%% @equiv connect(Opts, [])
-spec connect(connect_opts()) -> {ok, pid()} | {error, term()}.
connect(Opts) ->
    connect(Opts, []).

%% @doc Start an MQTT client and send the CONNECT packet.
%%
%% Blocks until CONNECT has been written to the transport, then returns
%% `{ok, Pid}'. The CONNACK is delivered asynchronously as a
%% `{mqtt, Pid, connack, _}' message to the owner; callers should wait
%% for it before issuing {@link publish/4}, {@link subscribe/2}, or
%% {@link unsubscribe/2}, all of which return `{error, not_connected}'
%% if invoked beforehand.
%%
%% `Opts' is a {@type connect_opts()} map; `client_id' is required.
%% `GenServerOpts' is forwarded to `gen_server:start_link/3'.
%%
%% When `transport => ssl', `ssl_opts' is passed through to
%% `ssl:connect/3' as-is. On AtomVM it must include
%% `{verify, verify_none}'; see the module-level "TLS options" section.
%%
%% @param Opts MQTT and transport options.
%% @param GenServerOpts options for `gen_server:start_link/3'.
%% @returns `{ok, Pid}' on success, `{error, Reason}' on transport or
%% CONNECT-send failure.
%% @end
-spec connect(connect_opts(), list()) -> {ok, pid()} | {error, term()}.
connect(Opts, GenServerOpts) ->
    DefaultTags =
        case maps:get(transport, Opts, gen_tcp) of
            ssl -> {ssl, ssl_closed, ssl_error};
            {ssl, _} -> {ssl, ssl_closed, ssl_error};
            _ -> {tcp, tcp_closed, tcp_error}
        end,
    Opts1 = Opts#{
        owner => maps:get(owner, Opts, self()),
        transport_tags => maps:get(transport_tags, Opts, DefaultTags)
    },
    case gen_server:start_link(?MODULE, Opts1, GenServerOpts) of
        {ok, Pid} ->
            %% Ownership of a pre-opened socket must transfer before CONNECT
            %% goes out, otherwise active-mode messages reach the caller.
            case transfer_ownership(Opts1, Pid) of
                ok ->
                    case gen_server:call(Pid, send_connect_packet) of
                        ok ->
                            {ok, Pid};
                        {error, _} = Err ->
                            try
                                gen_server:stop(Pid)
                            catch
                                _:_ -> ok
                            end,
                            Err
                    end;
                {error, _} = Err ->
                    try
                        gen_server:stop(Pid)
                    catch
                        _:_ -> ok
                    end,
                    Err
            end;
        Error ->
            Error
    end.

transfer_ownership(#{transport := {Module, Connection}}, Pid) when not is_atom(Connection) ->
    case erlang:function_exported(Module, controlling_process, 2) of
        true -> Module:controlling_process(Connection, Pid);
        false -> ok
    end;
transfer_ownership(_, _) ->
    ok.

%% @equiv publish(Pid, Topic, Message, QoS, #{})
-spec publish(pid(), topic(), iodata(), qos()) -> ok | {ok, packet_id()} | {error, term()}.
publish(Pid, Topic, Message, QoS) ->
    publish(Pid, Topic, Message, QoS, #{}).

%% @doc Publish a message to `Topic'.
%%
%% Dispatch differs by QoS:
%% <ul>
%%   <li>QoS 0 -- fire-and-forget via `gen_server:cast/2'. Returns `ok'
%%       as soon as the cast is queued, regardless of whether the
%%       client process or the underlying transport actually succeeds.
%%       Cast caveats apply: a dead client process is silently dropped,
%%       there is no backpressure, and the caller does not learn about
%%       transport failure here.</li>
%%   <li>QoS 1 / 2 -- synchronous `gen_server:call/3' (with `infinity'
%%       client-side timeout, so only the server-side `?REQUEST_TIMEOUT'
%%       can fire). At QoS 1 it blocks until PUBACK; at QoS 2 it blocks
%%       through PUBREC/PUBREL/PUBCOMP. Returns `{ok, PacketId}' on
%%       success, `{error, timeout}' if the broker never acks, or other
%%       `{error, _}' tuples on transport / protocol failure. Standard
%%       `gen_server:call' caveats apply: a dead client process raises
%%       `noproc' / `timeout' / similar in the caller.</li>
%% </ul>
%%
%% `Opts' may contain `retain => boolean()' and `dup => boolean()'.
%%
%% @param Pid client returned by {@link connect/1}.
%% @param Topic publish topic.
%% @param Message payload.
%% @param QoS 0, 1, or 2.
%% @param Opts extra encoding flags.
%% @returns `ok' for QoS 0; `{ok, PacketId}' for QoS 1 or 2; `{error,
%% Reason}' on failure (`not_connected', `timeout', `no_packet_ids', or
%% a transport error).
%% @end
-spec publish(pid(), topic(), iodata(), qos(), map()) -> ok | {ok, packet_id()} | {error, term()}.
publish(Pid, Topic, Message, 0, Opts) ->
    gen_server:cast(Pid, {publish, Topic, Message, 0, Opts}),
    ok;
publish(Pid, Topic, Message, QoS, Opts) ->
    %% `infinity`: the server arms ?REQUEST_TIMEOUT and replies
    %% `{error, timeout}`; a finite call timeout would race it.
    gen_server:call(Pid, {publish, Topic, Message, QoS, Opts}, infinity).

%% @doc Subscribe to one or more topic filters.
%%
%% Blocks until the broker replies with SUBACK and returns the per-topic
%% return codes: each is the granted QoS (`0'..`2') or `16#80' if the
%% subscription was refused.
%%
%% @param Pid client returned by {@link connect/1}.
%% @param Topics list of `{TopicFilter, RequestedQoS}' pairs.
%% @returns `{ok, ReturnCodes}' or `{error, Reason}'.
%% @end
-spec subscribe(pid(), [{topic(), qos()}]) -> {ok, [qos() | 128]} | {error, term()}.
subscribe(Pid, Topics) ->
    gen_server:call(Pid, {subscribe, Topics}, infinity).

%% @doc Unsubscribe from one or more topic filters.
%%
%% Blocks until the broker replies with UNSUBACK.
%%
%% @param Pid client returned by {@link connect/1}.
%% @param Topics list of topic filters to remove.
%% @returns `ok' or `{error, Reason}'.
%% @end
-spec unsubscribe(pid(), [topic()]) -> ok | {error, term()}.
unsubscribe(Pid, Topics) ->
    gen_server:call(Pid, {unsubscribe, Topics}, infinity).

%% @doc Send DISCONNECT and stop the client.
%%
%% Best-effort: a transport error during the send is ignored; the socket
%% is closed and the gen_server terminates with reason `normal' either
%% way. Allowed before CONNACK as well.
%%
%% @param Pid client returned by {@link connect/1}.
%% @end
-spec disconnect(pid()) -> ok.
disconnect(Pid) ->
    gen_server:call(Pid, disconnect).

%% @doc Send a manual PINGREQ.
%%
%% The keep-alive timer already pings automatically; this is for
%% applications that want to probe the link explicitly. Asynchronous:
%% the PINGRESP is consumed silently by the gen_server.
%%
%% @param Pid client returned by {@link connect/1}.
%% @end
-spec ping(pid()) -> ok.
ping(Pid) ->
    gen_server:cast(Pid, ping).

%% @doc Acknowledge an inbound QoS 1 or QoS 2 PUBLISH.
%%
%% Only meaningful when the client was started with `auto_ack => false';
%% sends PUBACK (QoS 1) or PUBREC (QoS 2). For QoS 2 the broker's
%% PUBREL -> PUBCOMP second leg is auto-handled.
%%
%% Idempotent: a second `ack/2' for a packet whose PUBREC has already
%% been sent returns `ok'. Returns `{error, not_pending}' if the
%% packet_id was never received or has already been fully completed,
%% and `{error, auto_ack_enabled}' if called on an `auto_ack => true'
%% client.
%%
%% @param Pid client returned by {@link connect/1}.
%% @param PacketId the packet identifier from the inbound `publish' event.
%% @end
-spec ack(pid(), packet_id()) -> ok | {error, not_pending | auto_ack_enabled}.
ack(Pid, PacketId) ->
    gen_server:call(Pid, {ack, PacketId}, infinity).

%% -------------------------------------------------------------------
%% gen_server callbacks
%% -------------------------------------------------------------------

init(Opts) ->
    Owner = maps:get(owner, Opts),
    KeepAliveSeconds = maps:get(keep_alive_seconds, Opts, 60),
    Tags = maps:get(transport_tags, Opts),
    AutoAck = maps:get(auto_ack, Opts, true),
    OwnerRef = erlang:monitor(process, Owner),
    case open_connection(Opts) of
        {ok, Transport, Connection} ->
            Reader = maybe_start_reader(Opts, Transport, Connection, Tags),
            State = #state{
                transport = Transport,
                connection = Connection,
                transport_tags = Tags,
                reader = Reader,
                pending_connect_opts = Opts,
                owner = Owner,
                owner_monitor = OwnerRef,
                keep_alive_seconds = KeepAliveSeconds,
                auto_ack = AutoAck
            },
            {ok, State};
        {error, Reason} ->
            erlang:demonitor(OwnerRef, [flush]),
            {stop, {connect_error, Reason}}
    end.

handle_call(send_connect_packet, _From, #state{pending_connect_opts = Opts} = State) ->
    %% Drop credentials from state before the send; a failure must not leave
    %% them lingering.
    State1 = State#state{pending_connect_opts = undefined},
    Packet = amqtt_proto:encode_connect(Opts),
    case send_packet(Packet, State1) of
        {ok, State2} ->
            {reply, ok, State2};
        {error, Reason, State2} ->
            %% Stop normal, not abnormal: we are linked to the caller via
            %% start_link, and an abnormal exit would propagate a link signal
            %% that kills a non-trap_exit caller before it can observe the
            %% {error, _} reply.
            close_connection(State2),
            {stop, normal, {error, Reason}, State2#state{connection = undefined}}
    end;
handle_call(disconnect, _From, State) ->
    _ = raw_send(amqtt_proto:encode_disconnect(), State),
    close_connection(State),
    {stop, normal, ok, cancel_all_timers(State#state{connection = undefined, connected = false})};
handle_call(_Req, _From, #state{connected = false} = State) ->
    {reply, {error, not_connected}, State};
handle_call({publish, Topic, Message, QoS, Opts}, From, State) ->
    case alloc_packet_id(State) of
        {ok, PacketId, State1} ->
            Packet = amqtt_proto:encode_publish(Opts#{
                topic => Topic,
                message => Message,
                qos => QoS,
                packet_id => PacketId
            }),
            AckType =
                case QoS of
                    1 -> puback;
                    2 -> pubrec
                end,
            send_and_pend(Packet, PacketId, AckType, From, State1);
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({subscribe, Topics}, From, State) ->
    case alloc_packet_id(State) of
        {ok, PacketId, State1} ->
            Packet = amqtt_proto:encode_subscribe(#{packet_id => PacketId, topics => Topics}),
            send_and_pend(Packet, PacketId, suback, From, State1);
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({unsubscribe, Topics}, From, State) ->
    case alloc_packet_id(State) of
        {ok, PacketId, State1} ->
            Packet = amqtt_proto:encode_unsubscribe(#{packet_id => PacketId, topics => Topics}),
            send_and_pend(Packet, PacketId, unsuback, From, State1);
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({ack, _PId}, _From, #state{auto_ack = true} = State) ->
    {reply, {error, auto_ack_enabled}, State};
handle_call({ack, PacketId}, _From, #state{inbound_pending_ack = Pending} = State) ->
    case maps:find(PacketId, Pending) of
        {ok, {awaiting_ack, 1, _Data}} ->
            case send_packet(amqtt_proto:encode_puback(PacketId), State) of
                {ok, State1} ->
                    {reply, ok, State1#state{
                        inbound_pending_ack = maps:remove(PacketId, Pending)
                    }};
                {error, Reason, State1} ->
                    {reply, {error, Reason}, State1}
            end;
        {ok, {awaiting_ack, 2, Data}} ->
            case send_packet(amqtt_proto:encode_pubrec(PacketId), State) of
                {ok, State1} ->
                    {reply, ok, State1#state{
                        inbound_pending_ack = Pending#{PacketId => {pubrec_sent, 2, Data}}
                    }};
                {error, Reason, State1} ->
                    {reply, {error, Reason}, State1}
            end;
        {ok, {pubrec_sent, 2, _Data}} ->
            %% Idempotent: PUBREC already sent; do not retransmit on a
            %% re-ack from a restarted owner.
            {reply, ok, State};
        error ->
            {reply, {error, not_pending}, State}
    end.

handle_cast(_Req, #state{connected = false} = State) ->
    {noreply, State};
handle_cast({publish, Topic, Message, 0, Opts}, State) ->
    Packet = amqtt_proto:encode_publish(Opts#{topic => Topic, message => Message, qos => 0}),
    case send_packet(Packet, State) of
        {ok, State1} -> {noreply, State1};
        {error, Reason, State1} -> stop_with_error(Reason, State1)
    end;
handle_cast(ping, State) ->
    case send_packet(amqtt_proto:encode_pingreq(), State) of
        {ok, State1} -> {noreply, arm_pingresp_timer(State1)};
        {error, Reason, State1} -> stop_with_error(Reason, State1)
    end.

handle_info(
    {DataTag, _Sock, Data},
    #state{
        transport_tags = {DataTag, _, _},
        buffer = Buffer
    } = State
) ->
    NewBuffer = <<Buffer/binary, Data/binary>>,
    case byte_size(NewBuffer) > ?MAX_BUFFER_SIZE of
        true ->
            stop_with_error(buffer_overflow, State#state{buffer = NewBuffer});
        false ->
            case process_buffer(State#state{buffer = NewBuffer}) of
                {ok, State1} ->
                    {noreply, State1};
                {stop_normal, State1} ->
                    close_connection(State1),
                    {stop, normal,
                        cancel_all_timers(State1#state{connection = undefined, connected = false})};
                {error, Reason, State1} ->
                    stop_with_error(Reason, State1)
            end
    end;
handle_info(
    {ClosedTag, _Sock},
    #state{
        transport_tags = {_, ClosedTag, _},
        owner = Owner
    } = State
) ->
    Owner ! {mqtt, self(), disconnected, #{}},
    {stop, normal, cancel_all_timers(State#state{connection = undefined, connected = false})};
handle_info(
    {ErrorTag, _Sock, Reason},
    #state{
        transport_tags = {_, _, ErrorTag}
    } = State
) ->
    stop_with_error(Reason, State);
handle_info(send_ping, State) ->
    case send_packet(amqtt_proto:encode_pingreq(), State#state{ping_timer = undefined}) of
        {ok, State1} -> {noreply, arm_pingresp_timer(State1)};
        {error, Reason, State1} -> stop_with_error(Reason, State1)
    end;
handle_info(pingresp_timeout, State) ->
    stop_with_error(pingresp_timeout, State#state{pingresp_timer = undefined});
handle_info({request_timeout, PacketId}, State) ->
    case maps:get(PacketId, State#state.pending, undefined) of
        {_AckType, From, _TRef} ->
            gen_server:reply(From, {error, timeout}),
            {noreply, State#state{pending = maps:remove(PacketId, State#state.pending)}};
        _ ->
            {noreply, State}
    end;
handle_info(
    {'DOWN', Ref, process, _Owner, _Reason},
    #state{owner_monitor = Ref} = State
) ->
    _ = raw_send(amqtt_proto:encode_disconnect(), State),
    close_connection(State),
    {stop, normal, cancel_all_timers(State#state{connection = undefined, connected = false})};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{connection = undefined}) ->
    ok;
terminate(_Reason, State) ->
    close_connection(State),
    ok.

%% -------------------------------------------------------------------
%% Internal: Connection Setup
%% -------------------------------------------------------------------

open_connection(#{transport := ssl} = Opts) ->
    Host = maps:get(host, Opts),
    Port = maps:get(port, Opts, 8883),
    SslOpts = maps:get(ssl_opts, Opts, []),
    ssl:start(),
    case ssl:connect(Host, Port, [{active, false}, binary | SslOpts]) of
        {ok, SSLSocket} -> {ok, ssl, SSLSocket};
        {error, _} = Err -> Err
    end;
open_connection(#{transport := {Module, Connection}}) ->
    {ok, Module, Connection};
open_connection(Opts) ->
    Host = maps:get(host, Opts),
    Port = maps:get(port, Opts, 1883),
    Active = maps:get(active, Opts, true),
    case gen_tcp:connect(Host, Port, [{active, Active}, binary]) of
        {ok, Socket} -> {ok, gen_tcp, Socket};
        {error, _} = Err -> Err
    end.

close_connection(#state{connection = undefined}) ->
    ok;
close_connection(#state{transport = T, connection = C}) ->
    try
        T:close(C)
    catch
        _:_ -> ok
    end,
    ok.

%% -------------------------------------------------------------------
%% Internal: Reader Process for Passive-Mode Transports
%% -------------------------------------------------------------------

needs_reader(#{transport := ssl}) -> true;
needs_reader(#{transport := {ssl, _}}) -> true;
needs_reader(#{active := false}) -> true;
needs_reader(_) -> false.

maybe_start_reader(Opts, Transport, Connection, {DataTag, ClosedTag, ErrorTag}) ->
    case needs_reader(Opts) of
        true ->
            Self = self(),
            spawn_link(fun() ->
                reader_loop(Transport, Connection, Self, DataTag, ClosedTag, ErrorTag)
            end);
        false ->
            undefined
    end.

reader_loop(Transport, Connection, Owner, DataTag, ClosedTag, ErrorTag) ->
    case Transport:recv(Connection, 0) of
        {ok, Data} ->
            Owner ! {DataTag, Connection, Data},
            reader_loop(Transport, Connection, Owner, DataTag, ClosedTag, ErrorTag);
        {error, closed} ->
            Owner ! {ClosedTag, Connection};
        {error, Reason} ->
            Owner ! {ErrorTag, Connection, Reason}
    end.

%% -------------------------------------------------------------------
%% Internal: Send helpers
%% -------------------------------------------------------------------

raw_send(_Packet, #state{connection = undefined}) ->
    {error, closed};
raw_send(Packet, #state{transport = T, connection = C}) ->
    T:send(C, Packet).

send_packet(Packet, State) ->
    case raw_send(Packet, State) of
        ok -> {ok, restart_ping_timer(State)};
        {error, Reason} -> {error, Reason, State}
    end.

send_and_pend(Packet, PacketId, AckType, From, State) ->
    case send_packet(Packet, State) of
        {ok, State1} ->
            TRef = erlang:send_after(?REQUEST_TIMEOUT, self(), {request_timeout, PacketId}),
            Pending = maps:put(PacketId, {AckType, From, TRef}, State1#state.pending),
            {noreply, State1#state{pending = Pending}};
        {error, Reason, State1} ->
            gen_server:reply(From, {error, Reason}),
            stop_with_error(Reason, State1)
    end.

stop_with_error(Reason, #state{owner = Owner} = State) ->
    Owner ! {mqtt, self(), error, #{reason => Reason}},
    State1 = reply_pending_with_error(Reason, State),
    close_connection(State1),
    {stop, {transport_error, Reason},
        cancel_all_timers(State1#state{connection = undefined, connected = false})}.

reply_pending_with_error(Reason, #state{pending = Pending} = State) ->
    maps:foreach(
        fun(_PId, {_AckType, From, TRef}) ->
            erlang:cancel_timer(TRef),
            gen_server:reply(From, {error, Reason})
        end,
        Pending
    ),
    State#state{pending = #{}}.

%% -------------------------------------------------------------------
%% Internal: Packet ID allocation
%% -------------------------------------------------------------------

alloc_packet_id(State) ->
    alloc_packet_id(State, 0).

alloc_packet_id(_State, N) when N >= 65535 ->
    {error, no_packet_ids};
alloc_packet_id(#state{next_packet_id = Id, pending = Pending} = State, N) ->
    Next = (Id rem 65535) + 1,
    case maps:is_key(Id, Pending) of
        true ->
            alloc_packet_id(State#state{next_packet_id = Next}, N + 1);
        false ->
            {ok, Id, State#state{next_packet_id = Next}}
    end.

%% -------------------------------------------------------------------
%% Internal: Inbound packet processing
%% -------------------------------------------------------------------

process_buffer(#state{buffer = Buffer} = State) ->
    case amqtt_proto:decode(Buffer) of
        {ok, {Type, Data}, Rest} ->
            case handle_packet(Type, Data, State#state{buffer = Rest}) of
                {ok, State1} -> process_buffer(State1);
                {stop_normal, State1} -> {stop_normal, State1};
                {error, _, _} = Err -> Err
            end;
        {error, incomplete} ->
            {ok, State};
        {error, {protocol_error, Reason}} ->
            {error, {protocol_error, Reason}, State}
    end.

handle_packet(connack, #{return_code := 0} = Data, #state{owner = Owner} = State) ->
    Owner ! {mqtt, self(), connack, Data},
    {ok, State#state{connected = true}};
handle_packet(connack, Data, #state{owner = Owner} = State) ->
    %% MQTT 3.1.1 §3.2: a non-zero return code requires the broker to close.
    Owner ! {mqtt, self(), connack, Data},
    {stop_normal, State#state{connected = false}};
handle_packet(publish, #{qos := 0} = Data, #state{owner = Owner} = State) ->
    Owner ! {mqtt, self(), publish, Data},
    {ok, State};
handle_packet(
    publish,
    #{qos := 1, packet_id := PacketId} = Data,
    #state{owner = Owner, auto_ack = true} = State
) ->
    Owner ! {mqtt, self(), publish, Data},
    case send_packet(amqtt_proto:encode_puback(PacketId), State) of
        {ok, State1} -> {ok, State1};
        {error, Reason, State1} -> {error, Reason, State1}
    end;
handle_packet(
    publish,
    #{qos := 1, packet_id := PacketId} = Data,
    #state{owner = Owner, inbound_pending_ack = Pending} = State
) ->
    %% Manual-ack: hold PUBACK until the owner calls ack/2. Idempotent on
    %% duplicate broker retransmits before the owner has acked, only the
    %% first delivery raises an event; PUBACK is still held back.
    case maps:is_key(PacketId, Pending) of
        true ->
            {ok, State};
        false ->
            Owner ! {mqtt, self(), publish, Data},
            {ok, State#state{
                inbound_pending_ack = Pending#{PacketId => {awaiting_ack, 1, Data}}
            }}
    end;
handle_packet(
    publish,
    #{qos := 2, packet_id := PacketId} = Data,
    #state{owner = Owner, inbound_qos2 = Set, auto_ack = true} = State
) ->
    %% Deliver to the owner only the first time; PUBREC is sent in either
    %% case so the broker stops retransmitting.
    State1 =
        case maps:is_key(PacketId, Set) of
            true ->
                State;
            false ->
                Owner ! {mqtt, self(), publish, Data},
                State#state{inbound_qos2 = Set#{PacketId => true}}
        end,
    case send_packet(amqtt_proto:encode_pubrec(PacketId), State1) of
        {ok, State2} -> {ok, State2};
        {error, Reason, State2} -> {error, Reason, State2}
    end;
handle_packet(
    publish,
    #{qos := 2, packet_id := PacketId} = Data,
    #state{
        owner = Owner,
        inbound_qos2 = Set,
        inbound_pending_ack = Pending
    } = State
) ->
    %% Manual-ack: deliver once (dedup via inbound_qos2). PUBREC is held
    %% back until the owner calls ack/2, except on a duplicate retransmit
    %% after the owner has already acked, where we resend PUBREC since
    %% the broker clearly hasn't seen it.
    case maps:find(PacketId, Pending) of
        error ->
            Owner ! {mqtt, self(), publish, Data},
            {ok, State#state{
                inbound_qos2 = Set#{PacketId => true},
                inbound_pending_ack = Pending#{PacketId => {awaiting_ack, 2, Data}}
            }};
        {ok, {awaiting_ack, 2, _}} ->
            {ok, State};
        {ok, {pubrec_sent, 2, _}} ->
            case send_packet(amqtt_proto:encode_pubrec(PacketId), State) of
                {ok, State1} -> {ok, State1};
                {error, Reason, State1} -> {error, Reason, State1}
            end
    end;
handle_packet(puback, #{packet_id := PacketId}, State) ->
    {ok, complete_pending(PacketId, puback, {ok, PacketId}, State)};
handle_packet(pubrec, #{packet_id := PacketId}, State) ->
    case send_packet(amqtt_proto:encode_pubrel(PacketId), State) of
        {ok, State1} ->
            case maps:get(PacketId, State1#state.pending, undefined) of
                {pubrec, From, TRef} ->
                    Pending = maps:put(PacketId, {pubcomp, From, TRef}, State1#state.pending),
                    {ok, State1#state{pending = Pending}};
                _ ->
                    {ok, State1}
            end;
        {error, Reason, State1} ->
            {error, Reason, State1}
    end;
handle_packet(
    pubrel,
    #{packet_id := PacketId},
    #state{inbound_qos2 = Set, inbound_pending_ack = Pending} = State
) ->
    State1 = State#state{
        inbound_qos2 = maps:remove(PacketId, Set),
        inbound_pending_ack = maps:remove(PacketId, Pending)
    },
    case send_packet(amqtt_proto:encode_pubcomp(PacketId), State1) of
        {ok, State2} -> {ok, State2};
        {error, Reason, State2} -> {error, Reason, State2}
    end;
handle_packet(pubcomp, #{packet_id := PacketId}, State) ->
    {ok, complete_pending(PacketId, pubcomp, {ok, PacketId}, State)};
handle_packet(suback, #{packet_id := PacketId, return_codes := RCs}, State) ->
    {ok, complete_pending(PacketId, suback, {ok, RCs}, State)};
handle_packet(unsuback, #{packet_id := PacketId}, State) ->
    {ok, complete_pending(PacketId, unsuback, ok, State)};
handle_packet(pingresp, _Data, State) ->
    {ok, cancel_pingresp_timer(State)};
handle_packet(_Type, _Data, State) ->
    {ok, State}.

complete_pending(PacketId, ExpectedAckType, Reply, #state{pending = Pending} = State) ->
    case maps:get(PacketId, Pending, undefined) of
        {ExpectedAckType, From, TRef} ->
            erlang:cancel_timer(TRef),
            gen_server:reply(From, Reply),
            State#state{pending = maps:remove(PacketId, Pending)};
        _ ->
            State
    end.

%% -------------------------------------------------------------------
%% Internal: Timers
%% -------------------------------------------------------------------

restart_ping_timer(#state{keep_alive_seconds = 0} = State) ->
    State;
restart_ping_timer(State) ->
    State1 = cancel_ping_timer(State),
    Interval = State1#state.keep_alive_seconds * 750,
    Ref = erlang:send_after(Interval, self(), send_ping),
    State1#state{ping_timer = Ref}.

cancel_ping_timer(#state{ping_timer = undefined} = State) ->
    State;
cancel_ping_timer(#state{ping_timer = Ref} = State) ->
    erlang:cancel_timer(Ref),
    State#state{ping_timer = undefined}.

arm_pingresp_timer(#state{keep_alive_seconds = 0} = State) ->
    State;
arm_pingresp_timer(#state{pingresp_timer = OldRef, keep_alive_seconds = KA} = State) ->
    case OldRef of
        undefined -> ok;
        _ -> erlang:cancel_timer(OldRef)
    end,
    Ref = erlang:send_after(KA * 1000, self(), pingresp_timeout),
    State#state{pingresp_timer = Ref}.

cancel_pingresp_timer(#state{pingresp_timer = undefined} = State) ->
    State;
cancel_pingresp_timer(#state{pingresp_timer = Ref} = State) ->
    erlang:cancel_timer(Ref),
    State#state{pingresp_timer = undefined}.

cancel_all_timers(State) ->
    cancel_pingresp_timer(cancel_ping_timer(State)).
