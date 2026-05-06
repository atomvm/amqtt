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

-module(client_pub_sub_e2e).

-export([start/0]).

-define(TCP_PORT, 1883).
-define(TLS_PORT, 8883).
-define(RECV_TIMEOUT, 15000).
-define(ITERS_PER_QOS, 3).

broker() ->
    case os:getenv("AMQTT_E2E_BROKER") of
        false -> "127.0.0.1";
        Host -> Host
    end.

use_tls() ->
    case os:getenv("AMQTT_E2E_USE_TLS") of
        "true" -> true;
        "1" -> true;
        _ -> false
    end.

start() ->
    try run() of
        ok ->
            io:format("PASS (9/9)~n"),
            ok
    catch
        Class:Reason:Stack ->
            io:format("FAIL: ~p:~p ~p~n", [Class, Reason, Stack]),
            fail
    end.

run() ->
    Broker = broker(),
    UseTls = use_tls(),

    Suffix = binary:encode_hex(crypto:strong_rand_bytes(8), lowercase),
    Root = <<"amqtt_e2e/", Suffix/binary>>,
    PubTopic = <<Root/binary, "/payloads">>,
    ReplyTopic = <<Root/binary, "/replies">>,

    AClientId = <<"amqtt_e2e_a_", Suffix/binary>>,
    BClientId = <<"amqtt_e2e_b_", Suffix/binary>>,

    io:format("topic root: ~s~n", [Root]),

    io:format("resolving ~s ...~n", [Broker]),
    {ok, BrokerIp} = inet:getaddr(Broker, inet),
    io:format("broker ip: ~p~n", [BrokerIp]),

    io:format("connecting A (TCP) ...~n"),
    A = connect_tcp(AClientId, BrokerIp),
    wait_connack(A),
    io:format("client A (TCP) connected~n"),

    BLabel = client_b_label(UseTls),
    io:format("connecting B (~s) ...~n", [BLabel]),
    B = connect_b(UseTls, BClientId, BrokerIp, Broker),
    wait_connack(B),
    io:format("client B (~s) connected~n", [BLabel]),

    {ok, _} = amqtt_client:subscribe(A, [{ReplyTopic, 2}]),
    {ok, _} = amqtt_client:subscribe(B, [{PubTopic, 2}]),
    io:format("subscriptions installed~n"),

    try
        lists:foreach(
            fun(QoS) ->
                lists:foreach(
                    fun(Iter) ->
                        round_trip(QoS, Iter, A, B, PubTopic, ReplyTopic)
                    end,
                    lists:seq(1, ?ITERS_PER_QOS)
                )
            end,
            [0, 1, 2]
        )
    after
        try
            amqtt_client:disconnect(A)
        catch
            _:_ -> ok
        end,
        try
            amqtt_client:disconnect(B)
        catch
            _:_ -> ok
        end
    end,
    ok.

connect_tcp(ClientId, BrokerIp) ->
    {ok, Pid} = amqtt_client:connect(#{
        host => BrokerIp,
        port => ?TCP_PORT,
        client_id => ClientId,
        keep_alive_seconds => 60
    }),
    Pid.

client_b_label(true) -> "TLS";
client_b_label(false) -> "TCP".

connect_b(true, ClientId, _BrokerIp, BrokerHost) ->
    {ok, Pid} = amqtt_client:connect(#{
        host => BrokerHost,
        port => ?TLS_PORT,
        transport => ssl,
        ssl_opts => [{verify, verify_none}],
        client_id => ClientId,
        keep_alive_seconds => 60
    }),
    Pid;
connect_b(false, ClientId, BrokerIp, _BrokerHost) ->
    connect_tcp(ClientId, BrokerIp).

wait_connack(Pid) ->
    receive
        {mqtt, Pid, connack, #{return_code := 0}} ->
            ok;
        {mqtt, Pid, connack, #{return_code := RC}} ->
            erlang:error({connack_refused, RC})
    after ?RECV_TIMEOUT ->
        erlang:error({connack_timeout, Pid})
    end.

round_trip(QoS, Iter, A, B, PubTopic, ReplyTopic) ->
    Payload = crypto:strong_rand_bytes(32),
    ExpectedSha = binary:encode_hex(crypto:hash(sha256, Payload), lowercase),

    publish(A, PubTopic, Payload, QoS),

    receive
        {mqtt, B, publish, #{message := P}} when P =:= Payload ->
            Sha = binary:encode_hex(crypto:hash(sha256, P), lowercase),
            publish(B, ReplyTopic, Sha, QoS)
    after ?RECV_TIMEOUT ->
        erlang:error({b_timeout, QoS, Iter})
    end,

    receive
        {mqtt, A, publish, #{message := Reply}} ->
            case Reply of
                ExpectedSha ->
                    io:format("ok qos=~p iter=~p~n", [QoS, Iter]);
                _ ->
                    erlang:error({sha_mismatch, QoS, Iter, ExpectedSha, Reply})
            end
    after ?RECV_TIMEOUT ->
        erlang:error({a_timeout, QoS, Iter})
    end.

publish(Client, Topic, Payload, 0) ->
    ok = amqtt_client:publish(Client, Topic, Payload, 0);
publish(Client, Topic, Payload, QoS) ->
    {ok, _PacketId} = amqtt_client:publish(Client, Topic, Payload, QoS).
