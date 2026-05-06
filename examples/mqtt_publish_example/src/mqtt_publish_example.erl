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

-module(mqtt_publish_example).

-export([start/0]).

-define(BROKER, "test.mosquitto.org").
-define(TCP_PORT, 1883).
-define(TOPIC, <<"amqtt/greet">>).
-define(MESSAGE, <<"Hello World">>).
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
    %% AtomVM's gen_tcp can hang on slow-resolving hostnames; resolve to IPv4
    %% ourselves.
    {ok, BrokerIp} = inet:getaddr(?BROKER, inet),
    io:format("connecting to ~s (~p):~p~n", [?BROKER, BrokerIp, ?TCP_PORT]),

    {ok, Client} = amqtt_client:connect(#{
        host => BrokerIp,
        port => ?TCP_PORT,
        client_id => client_id(),
        keep_alive_seconds => 60
    }),
    receive
        {mqtt, Client, connack, #{return_code := 0}} ->
            ok
    after ?RECV_TIMEOUT ->
        erlang:error(connack_timeout)
    end,
    io:format("connected~n"),

    {ok, _PacketId} = amqtt_client:publish(Client, ?TOPIC, ?MESSAGE, 1),
    io:format("published ~p to ~p~n", [?MESSAGE, ?TOPIC]),

    ok = amqtt_client:disconnect(Client),
    ok.

client_id() ->
    Suffix = integer_to_binary(erlang:system_time(millisecond)),
    <<"amqtt_pub_", Suffix/binary>>.
