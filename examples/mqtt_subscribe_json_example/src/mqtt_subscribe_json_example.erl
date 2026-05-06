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

-module(mqtt_subscribe_json_example).

-export([start/0]).

-define(BROKER, "test.mosquitto.org").
%% Port 1884 is the authenticated unencrypted listener; the `ro' user has
%% read-only access to `#' with no time limit (unlike the `wildcard' user
%% on 1883 whose `#' subscription is auto-removed after 20 s).
-define(TCP_PORT, 1884).
-define(USER, <<"ro">>).
-define(PASSWORD, <<"readonly">>).
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
    {ok, BrokerIp} = inet:getaddr(?BROKER, inet),
    io:format("connecting to ~s (~p):~p as ~s~n", [?BROKER, BrokerIp, ?TCP_PORT, ?USER]),

    {ok, Client} = amqtt_client:connect(#{
        host => BrokerIp,
        port => ?TCP_PORT,
        username => ?USER,
        password => ?PASSWORD,
        client_id => client_id(),
        keep_alive_seconds => 60
    }),
    receive
        {mqtt, Client, connack, #{return_code := 0}} -> ok
    after ?RECV_TIMEOUT ->
        erlang:error(connack_timeout)
    end,
    io:format("connected~n"),

    {ok, _} = amqtt_client:subscribe(Client, [{<<"#">>, 0}]),
    io:format("subscribed to # -- decoding JSON payloads, ignoring others~n"),

    receive_loop(Client).

receive_loop(Client) ->
    receive
        {mqtt, Client, publish, #{topic := Topic, message := Payload}} ->
            try_decode_print(Topic, Payload),
            receive_loop(Client);
        {mqtt, Client, disconnected, _} ->
            io:format("broker closed connection~n"),
            ok
    end.

try_decode_print(Topic, Payload) ->
    try json:decode(Payload) of
        Decoded ->
            io:format("~s: ~p~n", [Topic, Decoded])
    catch
        _:_ ->
            ok
    end.

client_id() ->
    Suffix = integer_to_binary(erlang:system_time(millisecond)),
    <<"amqtt_json_sub_", Suffix/binary>>.
