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

-module(mqtt_subscribe_example).

-export([start/0]).

-define(BROKER, "test.mosquitto.org").
-define(TCP_PORT, 1883).
%% Per https://test.mosquitto.org/, anonymous clients can't subscribe to the
%% literal `#` topic. Connecting with username `wildcard' (no password) lets
%% the broker accept a `#' subscription for 20 seconds before auto-removing
%% it -- intended for topic discovery.
-define(USER, <<"wildcard">>).
-define(MAX_MESSAGES, 100).
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
    {ok, BrokerIp} = inet:getaddr(?BROKER, inet),
    io:format("connecting to ~s (~p):~p as ~s~n", [?BROKER, BrokerIp, ?TCP_PORT, ?USER]),

    {ok, Client} = amqtt_client:connect(#{
        host => BrokerIp,
        port => ?TCP_PORT,
        username => ?USER,
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
    io:format(
        "subscribed to #, collecting up to ~p messages or ~p ms...~n",
        [?MAX_MESSAGES, ?DEADLINE_MS]
    ),

    Deadline = erlang:monotonic_time(millisecond) + ?DEADLINE_MS,
    Received = receive_loop(Client, 0, Deadline),

    try
        amqtt_client:disconnect(Client)
    catch
        _:_ -> ok
    end,
    io:format("received ~p message(s)~n", [Received]),
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
                {mqtt, Client, publish, #{topic := Topic, message := Payload}} ->
                    io:format("~s: ~s~n", [Topic, base64:encode(Payload)]),
                    receive_loop(Client, Count + 1, Deadline)
            after Remaining ->
                Count
            end
    end.

client_id() ->
    Suffix = integer_to_binary(erlang:system_time(millisecond)),
    <<"amqtt_sub_", Suffix/binary>>.
