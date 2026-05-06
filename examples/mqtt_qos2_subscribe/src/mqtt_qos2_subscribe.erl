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

-module(mqtt_qos2_subscribe).

-export([start/0]).

-define(BROKER, "test.mosquitto.org").
-define(TCP_PORT, 1883).
%% Stable client_id is required for the broker to recognise us as the same
%% session across reconnects when clean_session=false.
-define(CLIENT_ID, <<"amqtt_qos2_demo">>).
-define(TOPIC, <<"amqtt/amqtt_qos2_demo/#">>).
-define(MAX_MESSAGES, 10).
-define(DEADLINE_MS, 60000).
-define(RECV_TIMEOUT, 15000).

start() ->
    try run() of
        ok ->
            io:format("PASS~n"),
            ok
    catch
        Class:Reason:Stack ->
            io:format("FAIL: ~p:~p ~p~n", [Class, Reason, Stack]),
            fail
    end.

run() ->
    io:format("connecting to ~s:~p~n", [?BROKER, ?TCP_PORT]),

    {ok, Client} = amqtt_client:connect(#{
        host => ?BROKER,
        port => ?TCP_PORT,
        client_id => ?CLIENT_ID,
        clean_session => false,
        auto_ack => false,
        keep_alive_seconds => 60
    }),
    SessionPresent =
        receive
            {mqtt, Client, connack, #{return_code := 0, session_present := SP}} ->
                SP
        after ?RECV_TIMEOUT ->
            erlang:error(connack_timeout)
        end,
    io:format("connected, session_present: ~p~n", [SessionPresent]),

    {ok, _} = amqtt_client:subscribe(Client, [{?TOPIC, 2}]),
    io:format("subscribed to ~s at QoS 2 (publish here from another client)~n", [?TOPIC]),
    io:format("waiting for up to ~p messages or ~p ms...~n", [?MAX_MESSAGES, ?DEADLINE_MS]),

    Deadline = erlang:monotonic_time(millisecond) + ?DEADLINE_MS,
    Received = receive_loop(Client, 0, Deadline),

    try
        amqtt_client:disconnect(Client)
    catch
        _:_ -> ok
    end,
    io:format("processed ~p QoS 2 message(s)~n", [Received]),
    ok.

receive_loop(_Client, Count, _Deadline) when Count >= ?MAX_MESSAGES ->
    Count;
receive_loop(Client, Count, Deadline) ->
    Now = erlang:monotonic_time(millisecond),
    Remaining = Deadline - Now,
    case Remaining =< 0 of
        true ->
            Count;
        false ->
            receive
                {mqtt, Client, publish, #{
                    topic := Topic, message := Payload, packet_id := PacketId, qos := 2
                }} ->
                    io:format("[QoS2] ~s: ~p (packet_id=~p)~n", [Topic, Payload, PacketId]),
                    ok = amqtt_client:ack(Client, PacketId),
                    receive_loop(Client, Count + 1, Deadline)
            after Remaining ->
                Count
            end
    end.
