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

-module(mqtt_client_tests).

-include_lib("eunit/include/eunit.hrl").

-define(RECV_TIMEOUT, 1000).
-define(TEST_TIMEOUT, 10).

-define(ASSERT_RECEIVE(Pattern, Timeout),
    receive
        Pattern -> ok
    after (Timeout) ->
        erlang:error({receive_timeout, ??Pattern})
    end
).

%% -------------------------------------------------------------------
%% Mock broker helpers
%% -------------------------------------------------------------------

start_mock_broker() ->
    {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listener),
    {Listener, Port}.

accept(Listener) ->
    {ok, Socket} = gen_tcp:accept(Listener, 5000),
    Socket.

recv_packet(Socket) ->
    {ok, Data} = gen_tcp:recv(Socket, 0, 5000),
    Data.

send_broker(Socket, Data) ->
    ok = gen_tcp:send(Socket, Data).

send_connack(Socket) -> send_connack(Socket, 0).
send_connack(Socket, ReturnCode) ->
    send_broker(Socket, <<16#20, 2, 0, ReturnCode>>).

%% Spawn an async caller that runs Fun and replies with the result to Self.
async_call(Fun) ->
    Self = self(),
    Ref = make_ref(),
    Pid = spawn(fun() -> Self ! {Ref, Fun()} end),
    {Ref, Pid}.

await({Ref, _Pid}, Timeout) ->
    receive
        {Ref, Result} -> Result
    after Timeout ->
        erlang:error(async_call_timeout)
    end.

connect_and_handshake(Port, ClientId) ->
    connect_and_handshake(Port, ClientId, 0).

connect_and_handshake(Port, ClientId, KeepAlive) ->
    {ok, Client} = mqtt_client:connect(#{
        host => "127.0.0.1",
        port => Port,
        client_id => ClientId,
        keep_alive => KeepAlive
    }),
    Client.

cleanup(Listener, Client) ->
    catch mqtt_client:disconnect(Client),
    catch gen_tcp:close(Listener),
    ok.

%% -------------------------------------------------------------------
%% Connection Tests
%% -------------------------------------------------------------------

connect_and_receive_connack_success_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"test_client">>),
        BrokerSocket = accept(Listener),
        ConnectData = recv_packet(BrokerSocket),
        {ok, {connect, ConnectInfo}, <<>>} = mqtt_proto:decode(ConnectData),
        ?assertEqual(<<"test_client">>, maps:get(client_id, ConnectInfo)),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, #{return_code := 0}}, ?RECV_TIMEOUT),
        cleanup(Listener, Client)
    end}.

connect_and_receive_connack_failure_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket, 5),
        ?ASSERT_RECEIVE({mqtt, Client, connack, #{return_code := 5}}, ?RECV_TIMEOUT),
        catch gen_tcp:close(BrokerSocket),
        catch gen_tcp:close(Listener),
        ok
    end}.

%% -------------------------------------------------------------------
%% Publish QoS 0
%% -------------------------------------------------------------------

publish_qos0_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, #{return_code := 0}}, ?RECV_TIMEOUT),

        ok = mqtt_client:publish(Client, <<"topic/a">>, <<"hello">>, 0),

        PubData = recv_packet(BrokerSocket),
        {ok, {publish, PubInfo}, <<>>} = mqtt_proto:decode(PubData),
        ?assertEqual(<<"topic/a">>, maps:get(topic, PubInfo)),
        ?assertEqual(<<"hello">>, maps:get(message, PubInfo)),
        ?assertEqual(0, maps:get(qos, PubInfo)),

        cleanup(Listener, Client)
    end}.

%% -------------------------------------------------------------------
%% Subscribe
%% -------------------------------------------------------------------

subscribe_and_receive_suback_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        SubAsync = async_call(fun() ->
            mqtt_client:subscribe(Client, [{<<"test/topic">>, 1}])
        end),

        SubData = recv_packet(BrokerSocket),
        {ok, {subscribe, SubInfo}, <<>>} = mqtt_proto:decode(SubData),
        ?assertEqual([{<<"test/topic">>, 1}], maps:get(topics, SubInfo)),
        PacketId = maps:get(packet_id, SubInfo),

        send_broker(BrokerSocket, <<16#90, 3, PacketId:16/big, 1>>),

        ?assertEqual({ok, [1]}, await(SubAsync, 2000)),

        cleanup(Listener, Client)
    end}.

%% -------------------------------------------------------------------
%% Receive Message
%% -------------------------------------------------------------------

receive_published_message_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        PublishPacket = mqtt_proto:encode_publish(#{
            topic => <<"sensor/temp">>, message => <<"22.5">>, qos => 0
        }),
        send_broker(BrokerSocket, PublishPacket),

        ?ASSERT_RECEIVE(
            {mqtt, Client, publish, #{
                topic := <<"sensor/temp">>, message := <<"22.5">>, qos := 0
            }},
            ?RECV_TIMEOUT
        ),

        cleanup(Listener, Client)
    end}.

%% -------------------------------------------------------------------
%% Disconnect
%% -------------------------------------------------------------------

disconnect_sends_packet_and_stops_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        Ref = monitor(process, Client),
        ok = mqtt_client:disconnect(Client),

        DisconnectData = recv_packet(BrokerSocket),
        ?assertMatch({ok, {disconnect, #{}}, <<>>}, mqtt_proto:decode(DisconnectData)),

        ?ASSERT_RECEIVE({'DOWN', Ref, process, Client, normal}, ?RECV_TIMEOUT),

        catch gen_tcp:close(Listener),
        ok
    end}.

%% -------------------------------------------------------------------
%% TCP Closed
%% -------------------------------------------------------------------

tcp_closed_notifies_owner_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        gen_tcp:close(BrokerSocket),

        ?ASSERT_RECEIVE({mqtt, Client, disconnected, #{}}, ?RECV_TIMEOUT),

        catch gen_tcp:close(Listener),
        ok
    end}.

%% -------------------------------------------------------------------
%% Ping
%% -------------------------------------------------------------------

automatic_ping_on_keep_alive_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        %% 1 second keep-alive -> ping at 750ms
        Client = connect_and_handshake(Port, <<"c">>, 1),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        PingData = recv_packet(BrokerSocket),
        ?assertMatch({ok, {pingreq, #{}}, <<>>}, mqtt_proto:decode(PingData)),

        send_broker(BrokerSocket, <<16#D0, 0>>),

        cleanup(Listener, Client)
    end}.

manual_ping_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        mqtt_client:ping(Client),

        PingData = recv_packet(BrokerSocket),
        ?assertMatch({ok, {pingreq, #{}}, <<>>}, mqtt_proto:decode(PingData)),

        cleanup(Listener, Client)
    end}.

%% -------------------------------------------------------------------
%% QoS 1
%% -------------------------------------------------------------------

publish_qos1_waits_for_puback_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        PubAsync = async_call(fun() ->
            mqtt_client:publish(Client, <<"t">>, <<"m">>, 1)
        end),

        PubData = recv_packet(BrokerSocket),
        {ok, {publish, PubInfo}, <<>>} = mqtt_proto:decode(PubData),
        ?assertEqual(1, maps:get(qos, PubInfo)),
        PacketId = maps:get(packet_id, PubInfo),

        send_broker(BrokerSocket, mqtt_proto:encode_puback(PacketId)),

        ?assertEqual({ok, PacketId}, await(PubAsync, 2000)),

        cleanup(Listener, Client)
    end}.

receive_publish_qos1_auto_puback_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        PublishPacket = mqtt_proto:encode_publish(#{
            topic => <<"t">>, message => <<"msg">>, qos => 1, packet_id => 7
        }),
        send_broker(BrokerSocket, PublishPacket),

        ?ASSERT_RECEIVE(
            {mqtt, Client, publish, #{topic := <<"t">>, qos := 1, packet_id := 7}},
            ?RECV_TIMEOUT
        ),

        AckData = recv_packet(BrokerSocket),
        ?assertMatch({ok, {puback, #{packet_id := 7}}, <<>>}, mqtt_proto:decode(AckData)),

        cleanup(Listener, Client)
    end}.

%% -------------------------------------------------------------------
%% QoS 2
%% -------------------------------------------------------------------

publish_qos2_full_flow_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        PubAsync = async_call(fun() ->
            mqtt_client:publish(Client, <<"t">>, <<"m">>, 2)
        end),

        PubData = recv_packet(BrokerSocket),
        {ok, {publish, PubInfo}, <<>>} = mqtt_proto:decode(PubData),
        ?assertEqual(2, maps:get(qos, PubInfo)),
        PacketId = maps:get(packet_id, PubInfo),

        send_broker(BrokerSocket, mqtt_proto:encode_pubrec(PacketId)),

        PubrelData = recv_packet(BrokerSocket),
        ?assertMatch(
            {ok, {pubrel, #{packet_id := PacketId}}, <<>>},
            mqtt_proto:decode(PubrelData)
        ),

        send_broker(BrokerSocket, mqtt_proto:encode_pubcomp(PacketId)),

        ?assertEqual({ok, PacketId}, await(PubAsync, 2000)),

        cleanup(Listener, Client)
    end}.

receive_publish_qos2_full_flow_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        PublishPacket = mqtt_proto:encode_publish(#{
            topic => <<"t">>, message => <<"qos2msg">>, qos => 2, packet_id => 10
        }),
        send_broker(BrokerSocket, PublishPacket),

        ?ASSERT_RECEIVE(
            {mqtt, Client, publish, #{topic := <<"t">>, qos := 2, packet_id := 10}},
            ?RECV_TIMEOUT
        ),

        PubrecData = recv_packet(BrokerSocket),
        ?assertMatch(
            {ok, {pubrec, #{packet_id := 10}}, <<>>}, mqtt_proto:decode(PubrecData)
        ),

        send_broker(BrokerSocket, mqtt_proto:encode_pubrel(10)),

        PubcompData = recv_packet(BrokerSocket),
        ?assertMatch(
            {ok, {pubcomp, #{packet_id := 10}}, <<>>}, mqtt_proto:decode(PubcompData)
        ),

        cleanup(Listener, Client)
    end}.

%% -------------------------------------------------------------------
%% Unsubscribe
%% -------------------------------------------------------------------

unsubscribe_and_receive_unsuback_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        UnsubAsync = async_call(fun() ->
            mqtt_client:unsubscribe(Client, [<<"test/topic">>])
        end),

        UnsubData = recv_packet(BrokerSocket),
        {ok, {unsubscribe, UnsubInfo}, <<>>} = mqtt_proto:decode(UnsubData),
        ?assertEqual([<<"test/topic">>], maps:get(topics, UnsubInfo)),
        PacketId = maps:get(packet_id, UnsubInfo),

        send_broker(BrokerSocket, <<16#B0, 2, PacketId:16/big>>),

        ?assertEqual(ok, await(UnsubAsync, 2000)),

        cleanup(Listener, Client)
    end}.

%% -------------------------------------------------------------------
%% Custom Transport
%% -------------------------------------------------------------------

connect_with_pre_opened_gen_tcp_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, true}]),

        {ok, Client} = mqtt_client:connect(#{
            transport => {gen_tcp, Socket},
            client_id => <<"custom_transport">>,
            keep_alive => 0
        }),

        BrokerSocket = accept(Listener),
        ConnectData = recv_packet(BrokerSocket),
        {ok, {connect, ConnectInfo}, <<>>} = mqtt_proto:decode(ConnectData),
        ?assertEqual(<<"custom_transport">>, maps:get(client_id, ConnectInfo)),

        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, #{return_code := 0}}, ?RECV_TIMEOUT),

        ok = mqtt_client:publish(Client, <<"t">>, <<"via-custom">>, 0),
        PubData = recv_packet(BrokerSocket),
        {ok, {publish, PubInfo}, <<>>} = mqtt_proto:decode(PubData),
        ?assertEqual(<<"via-custom">>, maps:get(message, PubInfo)),

        send_broker(
            BrokerSocket,
            mqtt_proto:encode_publish(#{topic => <<"t">>, message => <<"back">>, qos => 0})
        ),
        ?ASSERT_RECEIVE({mqtt, Client, publish, #{message := <<"back">>}}, ?RECV_TIMEOUT),

        cleanup(Listener, Client)
    end}.

connect_with_custom_tags_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, true}]),

        {ok, Client} = mqtt_client:connect(#{
            transport => {gen_tcp, Socket},
            transport_tags => {tcp, tcp_closed, tcp_error},
            client_id => <<"c">>,
            keep_alive => 0
        }),

        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, #{return_code := 0}}, ?RECV_TIMEOUT),

        cleanup(Listener, Client)
    end}.

%% -------------------------------------------------------------------
%% Passive-Mode Transport (reader process)
%% -------------------------------------------------------------------

passive_transport_publish_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

        {ok, Client} = mqtt_client:connect(#{
            transport => {gen_tcp, Socket},
            active => false,
            transport_tags => {tcp, tcp_closed, tcp_error},
            client_id => <<"passive_test">>,
            keep_alive => 0
        }),

        BrokerSocket = accept(Listener),
        ConnectData = recv_packet(BrokerSocket),
        {ok, {connect, ConnectInfo}, <<>>} = mqtt_proto:decode(ConnectData),
        ?assertEqual(<<"passive_test">>, maps:get(client_id, ConnectInfo)),

        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, #{return_code := 0}}, ?RECV_TIMEOUT),

        ok = mqtt_client:publish(Client, <<"t">>, <<"passive_msg">>, 0),
        PubData = recv_packet(BrokerSocket),
        {ok, {publish, PubInfo}, <<>>} = mqtt_proto:decode(PubData),
        ?assertEqual(<<"passive_msg">>, maps:get(message, PubInfo)),

        send_broker(
            BrokerSocket,
            mqtt_proto:encode_publish(#{topic => <<"t">>, message => <<"from_broker">>, qos => 0})
        ),
        ?ASSERT_RECEIVE({mqtt, Client, publish, #{message := <<"from_broker">>}}, ?RECV_TIMEOUT),

        cleanup(Listener, Client)
    end}.

passive_transport_subscribe_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

        {ok, Client} = mqtt_client:connect(#{
            transport => {gen_tcp, Socket},
            active => false,
            transport_tags => {tcp, tcp_closed, tcp_error},
            client_id => <<"c">>,
            keep_alive => 0
        }),

        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        SubAsync = async_call(fun() ->
            mqtt_client:subscribe(Client, [{<<"topic">>, 0}])
        end),

        SubData = recv_packet(BrokerSocket),
        {ok, {subscribe, SubInfo}, <<>>} = mqtt_proto:decode(SubData),
        PacketId = maps:get(packet_id, SubInfo),

        send_broker(BrokerSocket, <<16#90, 3, PacketId:16/big, 0>>),
        ?assertEqual({ok, [0]}, await(SubAsync, 2000)),

        cleanup(Listener, Client)
    end}.

passive_transport_closed_notifies_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),

        {ok, Client} = mqtt_client:connect(#{
            transport => {gen_tcp, Socket},
            active => false,
            transport_tags => {tcp, tcp_closed, tcp_error},
            client_id => <<"c">>,
            keep_alive => 0
        }),

        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        gen_tcp:close(BrokerSocket),

        ?ASSERT_RECEIVE({mqtt, Client, disconnected, #{}}, ?RECV_TIMEOUT),

        catch gen_tcp:close(Listener),
        ok
    end}.

%% -------------------------------------------------------------------
%% Buffer fragmentation
%% -------------------------------------------------------------------

handles_fragmented_tcp_delivery_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        PublishPacket = mqtt_proto:encode_publish(#{
            topic => <<"frag/test">>, message => <<"data">>, qos => 0
        }),
        Size = byte_size(PublishPacket),
        Half = Size div 2,
        <<Part1:Half/binary, Part2/binary>> = PublishPacket,

        send_broker(BrokerSocket, Part1),
        timer:sleep(50),
        send_broker(BrokerSocket, Part2),

        ?ASSERT_RECEIVE(
            {mqtt, Client, publish, #{topic := <<"frag/test">>, message := <<"data">>}},
            ?RECV_TIMEOUT
        ),

        cleanup(Listener, Client)
    end}.

%% -------------------------------------------------------------------
%% CONNACK gating, dedup, owner monitor, buffer cap, protocol error
%% -------------------------------------------------------------------

publish_before_connack_returns_not_connected_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),

        %% No CONNACK has been delivered yet, connected = false.
        ?assertEqual(
            {error, not_connected},
            mqtt_client:publish(Client, <<"t">>, <<"m">>, 1)
        ),
        ?assertEqual(
            {error, not_connected},
            mqtt_client:subscribe(Client, [{<<"t">>, 0}])
        ),

        cleanup(Listener, Client)
    end}.

connack_non_zero_stops_normally_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        process_flag(trap_exit, true),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket, 4),
        ?ASSERT_RECEIVE({mqtt, Client, connack, #{return_code := 4}}, ?RECV_TIMEOUT),
        ?ASSERT_RECEIVE({'EXIT', Client, normal}, ?RECV_TIMEOUT),
        process_flag(trap_exit, false),
        catch gen_tcp:close(BrokerSocket),
        catch gen_tcp:close(Listener),
        ok
    end}.

inbound_qos2_dedup_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        Publish = mqtt_proto:encode_publish(#{
            topic => <<"t">>, message => <<"m">>, qos => 2, packet_id => 99
        }),

        %% First PUBLISH: delivered, PUBREC sent.
        send_broker(BrokerSocket, Publish),
        ?ASSERT_RECEIVE(
            {mqtt, Client, publish, #{packet_id := 99, qos := 2}},
            ?RECV_TIMEOUT
        ),
        ?assertMatch(
            {ok, {pubrec, #{packet_id := 99}}, <<>>},
            mqtt_proto:decode(recv_packet(BrokerSocket))
        ),

        %% Duplicate PUBLISH: PUBREC re-sent (broker is retransmitting), but
        %% the owner must NOT receive the same message a second time.
        send_broker(BrokerSocket, Publish),
        ?assertMatch(
            {ok, {pubrec, #{packet_id := 99}}, <<>>},
            mqtt_proto:decode(recv_packet(BrokerSocket))
        ),
        receive
            {mqtt, Client, publish, _} ->
                erlang:error(duplicate_publish_delivered)
        after 200 ->
            ok
        end,

        %% PUBREL releases the packet ID; a subsequent PUBLISH with the same
        %% ID is treated as new and IS delivered.
        send_broker(BrokerSocket, mqtt_proto:encode_pubrel(99)),
        ?assertMatch(
            {ok, {pubcomp, #{packet_id := 99}}, <<>>},
            mqtt_proto:decode(recv_packet(BrokerSocket))
        ),
        send_broker(BrokerSocket, Publish),
        ?ASSERT_RECEIVE(
            {mqtt, Client, publish, #{packet_id := 99, qos := 2}},
            ?RECV_TIMEOUT
        ),

        cleanup(Listener, Client)
    end}.

owner_death_stops_client_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Self = self(),
        %% Spawn an owner whose only job is to wait for a kill signal then exit.
        Owner = spawn(fun() ->
            receive
                {connect, ConnInfo} ->
                    Self ! {owner_connected, ConnInfo},
                    receive
                        die -> ok
                    end
            end
        end),

        {ok, Client} = mqtt_client:connect(#{
            host => "127.0.0.1",
            port => Port,
            client_id => <<"owner_death">>,
            keep_alive => 0,
            owner => Owner
        }),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),

        ClientRef = monitor(process, Client),

        %% Kill the owner; the client must notice and stop.
        exit(Owner, kill),

        %% Broker should observe the DISCONNECT (or socket close) shortly.
        %% We accept either: in the kill path we send DISCONNECT best-effort,
        %% then close, so the broker sees DISCONNECT followed by close.
        ?ASSERT_RECEIVE({'DOWN', ClientRef, process, Client, normal}, ?RECV_TIMEOUT),

        catch gen_tcp:close(BrokerSocket),
        catch gen_tcp:close(Listener),
        ok
    end}.

buffer_overflow_stops_with_error_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        process_flag(trap_exit, true),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        %% Send 300 KiB of garbage that does not parse as a complete packet:
        %% start with a fixed header claiming a huge remaining-length, then
        %% never deliver the body. The buffer should overflow at 256 KiB.
        Header = <<16#30, 16#FF, 16#FF, 16#FF, 16#7F>>,
        Garbage = binary:copy(<<"X">>, 300 * 1024),
        send_broker(BrokerSocket, <<Header/binary, Garbage/binary>>),

        ?ASSERT_RECEIVE({mqtt, Client, error, #{reason := buffer_overflow}}, 3000),
        ?ASSERT_RECEIVE({'EXIT', Client, {transport_error, buffer_overflow}}, 3000),
        process_flag(trap_exit, false),

        catch gen_tcp:close(BrokerSocket),
        catch gen_tcp:close(Listener),
        ok
    end}.

protocol_error_stops_connection_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        process_flag(trap_exit, true),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        %% Send a PUBACK with packet_id 0: the hardened decoder rejects it.
        send_broker(BrokerSocket, <<16#40, 2, 0, 0>>),

        ?ASSERT_RECEIVE({mqtt, Client, error, #{reason := {protocol_error, _}}}, ?RECV_TIMEOUT),
        ?ASSERT_RECEIVE({'EXIT', Client, {transport_error, {protocol_error, _}}}, ?RECV_TIMEOUT),
        process_flag(trap_exit, false),

        catch gen_tcp:close(BrokerSocket),
        catch gen_tcp:close(Listener),
        ok
    end}.

connect_send_failure_does_not_kill_caller_test_() ->
    %% Regression: handle_call(send_connect_packet) on send failure must stop
    %% the gen_server with reason 'normal' rather than abnormal, otherwise the
    %% link from start_link propagates an exit signal that kills a non-trap_exit
    %% caller before it can observe the documented {error, _} return.
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        {ok, Socket} = gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}]),
        ok = gen_tcp:close(Socket),

        Self = self(),
        Caller = spawn(fun() ->
            Result = mqtt_client:connect(#{
                transport => {gen_tcp, Socket},
                client_id => <<"send_fail">>,
                keep_alive => 0,
                owner => Self
            }),
            Self ! {result, self(), Result},
            timer:sleep(150)
        end),
        Ref = monitor(process, Caller),

        ?ASSERT_RECEIVE({result, Caller, {error, _}}, ?RECV_TIMEOUT),
        ?ASSERT_RECEIVE({'DOWN', Ref, process, Caller, normal}, ?RECV_TIMEOUT),

        catch gen_tcp:close(Listener),
        ok
    end}.

%% -------------------------------------------------------------------
%% Manual ACK
%% -------------------------------------------------------------------

connect_manual_ack(Port, ClientId) ->
    {ok, Client} = mqtt_client:connect(#{
        host => "127.0.0.1",
        port => Port,
        client_id => ClientId,
        keep_alive => 0,
        auto_ack => false
    }),
    Client.

assert_no_broker_data(Socket, Timeout) ->
    case gen_tcp:recv(Socket, 0, Timeout) of
        {error, timeout} -> ok;
        {ok, Data} -> erlang:error({unexpected_broker_data, Data})
    end.

manual_ack_qos1_holds_puback_until_ack_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_manual_ack(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        send_broker(
            BrokerSocket,
            mqtt_proto:encode_publish(#{
                topic => <<"t">>, message => <<"m">>, qos => 1, packet_id => 11
            })
        ),

        ?ASSERT_RECEIVE(
            {mqtt, Client, publish, #{packet_id := 11, qos := 1}},
            ?RECV_TIMEOUT
        ),
        ok = assert_no_broker_data(BrokerSocket, 200),

        ok = mqtt_client:ack(Client, 11),
        AckData = recv_packet(BrokerSocket),
        ?assertMatch({ok, {puback, #{packet_id := 11}}, <<>>}, mqtt_proto:decode(AckData)),

        cleanup(Listener, Client)
    end}.

manual_ack_qos2_holds_pubrec_until_ack_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_manual_ack(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        send_broker(
            BrokerSocket,
            mqtt_proto:encode_publish(#{
                topic => <<"t">>, message => <<"m">>, qos => 2, packet_id => 22
            })
        ),

        ?ASSERT_RECEIVE(
            {mqtt, Client, publish, #{packet_id := 22, qos := 2}},
            ?RECV_TIMEOUT
        ),
        ok = assert_no_broker_data(BrokerSocket, 200),

        ok = mqtt_client:ack(Client, 22),
        ?assertMatch(
            {ok, {pubrec, #{packet_id := 22}}, <<>>},
            mqtt_proto:decode(recv_packet(BrokerSocket))
        ),

        send_broker(BrokerSocket, mqtt_proto:encode_pubrel(22)),
        ?assertMatch(
            {ok, {pubcomp, #{packet_id := 22}}, <<>>},
            mqtt_proto:decode(recv_packet(BrokerSocket))
        ),

        cleanup(Listener, Client)
    end}.

manual_ack_qos2_dup_publish_before_ack_no_pubrec_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_manual_ack(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        Pub = mqtt_proto:encode_publish(#{
            topic => <<"t">>, message => <<"m">>, qos => 2, packet_id => 33
        }),
        send_broker(BrokerSocket, Pub),
        ?ASSERT_RECEIVE({mqtt, Client, publish, #{packet_id := 33}}, ?RECV_TIMEOUT),

        %% Duplicate retransmit before owner acks: no event, no PUBREC.
        send_broker(BrokerSocket, Pub),
        receive
            {mqtt, Client, publish, _} -> erlang:error(duplicate_event)
        after 200 -> ok
        end,
        ok = assert_no_broker_data(BrokerSocket, 200),

        %% Now ack: exactly one PUBREC.
        ok = mqtt_client:ack(Client, 33),
        ?assertMatch(
            {ok, {pubrec, #{packet_id := 33}}, <<>>},
            mqtt_proto:decode(recv_packet(BrokerSocket))
        ),
        ok = assert_no_broker_data(BrokerSocket, 200),

        cleanup(Listener, Client)
    end}.

manual_ack_qos2_dup_publish_after_ack_resends_pubrec_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_manual_ack(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        Pub = mqtt_proto:encode_publish(#{
            topic => <<"t">>, message => <<"m">>, qos => 2, packet_id => 44
        }),
        send_broker(BrokerSocket, Pub),
        ?ASSERT_RECEIVE({mqtt, Client, publish, #{packet_id := 44}}, ?RECV_TIMEOUT),

        ok = mqtt_client:ack(Client, 44),
        ?assertMatch(
            {ok, {pubrec, #{packet_id := 44}}, <<>>},
            mqtt_proto:decode(recv_packet(BrokerSocket))
        ),

        %% Broker hasn't seen our PUBREC; retransmits PUBLISH. We must re-send
        %% PUBREC and not redeliver to the owner.
        send_broker(BrokerSocket, Pub),
        ?assertMatch(
            {ok, {pubrec, #{packet_id := 44}}, <<>>},
            mqtt_proto:decode(recv_packet(BrokerSocket))
        ),
        receive
            {mqtt, Client, publish, _} -> erlang:error(duplicate_event)
        after 200 -> ok
        end,

        cleanup(Listener, Client)
    end}.

manual_ack_idempotent_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_manual_ack(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        send_broker(
            BrokerSocket,
            mqtt_proto:encode_publish(#{
                topic => <<"t">>, message => <<"m">>, qos => 2, packet_id => 55
            })
        ),
        ?ASSERT_RECEIVE({mqtt, Client, publish, #{packet_id := 55}}, ?RECV_TIMEOUT),

        ok = mqtt_client:ack(Client, 55),
        ?assertMatch(
            {ok, {pubrec, #{packet_id := 55}}, <<>>},
            mqtt_proto:decode(recv_packet(BrokerSocket))
        ),

        %% Second ack on the same id is a no-op: ok return, no extra PUBREC.
        ok = mqtt_client:ack(Client, 55),
        ok = assert_no_broker_data(BrokerSocket, 200),

        cleanup(Listener, Client)
    end}.

manual_ack_unknown_packet_id_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_manual_ack(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        ?assertEqual({error, not_pending}, mqtt_client:ack(Client, 9999)),

        cleanup(Listener, Client)
    end}.

manual_ack_on_auto_ack_client_errors_test_() ->
    {timeout, ?TEST_TIMEOUT, fun() ->
        {Listener, Port} = start_mock_broker(),
        Client = connect_and_handshake(Port, <<"c">>),
        BrokerSocket = accept(Listener),
        _ConnectData = recv_packet(BrokerSocket),
        send_connack(BrokerSocket),
        ?ASSERT_RECEIVE({mqtt, Client, connack, _}, ?RECV_TIMEOUT),

        ?assertEqual({error, auto_ack_enabled}, mqtt_client:ack(Client, 1)),

        cleanup(Listener, Client)
    end}.
