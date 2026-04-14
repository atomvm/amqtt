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

-define(BROKER, "127.0.0.1").
-define(TCP_PORT, 1883).
-define(TLS_PORT, 8883).
-define(RECV_TIMEOUT, 15000).
-define(ITERS_PER_QOS, 3).

%% Set to `true' when a TLS-enabled broker is reachable on ?BROKER:?TLS_PORT.
%% While running against a plain Mosquitto on 1883 only, leave it `false'
%% and Client B will also use plain TCP (different client_id, same broker).
-define(USE_TLS, false).

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
    Suffix = hex(crypto:strong_rand_bytes(8)),
    Root = <<"amqtt_e2e/", Suffix/binary>>,
    PubTopic = <<Root/binary, "/payloads">>,
    ReplyTopic = <<Root/binary, "/replies">>,

    AClientId = <<"amqtt_e2e_a_", Suffix/binary>>,
    BClientId = <<"amqtt_e2e_b_", Suffix/binary>>,

    io:format("topic root: ~s~n", [Root]),

    %% AtomVM's default gen_tcp backend (gen_tcp_inet) hands hostnames to the
    %% underlying socket port driver, which can hang on AAAA-only or
    %% slow-resolving names. Resolve to an IPv4 tuple ourselves so the TCP
    %% client is unambiguous. AtomVM's ssl module already filters
    %% getaddrinfo results to family=inet internally, so TLS keeps the
    %% hostname (also needed for SNI).
    io:format("resolving ~s ...~n", [?BROKER]),
    {ok, BrokerIp} = inet:getaddr(?BROKER, inet),
    io:format("broker ip: ~p~n", [BrokerIp]),

    io:format("connecting A (TCP) ...~n"),
    A = connect_tcp(AClientId, BrokerIp),
    wait_connack(A),
    io:format("client A (TCP) connected~n"),

    io:format("connecting B (~s) ...~n", [client_b_label()]),
    B = connect_b(BClientId, BrokerIp),
    wait_connack(B),
    io:format("client B (~s) connected~n", [client_b_label()]),

    {ok, _} = mqtt_client:subscribe(A, [{ReplyTopic, 2}]),
    {ok, _} = mqtt_client:subscribe(B, [{PubTopic, 2}]),
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
        catch mqtt_client:disconnect(A),
        catch mqtt_client:disconnect(B)
    end,
    ok.

connect_tcp(ClientId, BrokerIp) ->
    {ok, Pid} = mqtt_client:connect(#{
        host => BrokerIp,
        port => ?TCP_PORT,
        client_id => ClientId,
        keep_alive => 60
    }),
    Pid.

-if(?USE_TLS).
client_b_label() -> "TLS".

connect_b(ClientId, _BrokerIp) ->
    {ok, Pid} = mqtt_client:connect(#{
        host => ?BROKER,
        port => ?TLS_PORT,
        transport => ssl,
        ssl_opts => [{verify, verify_none}],
        client_id => ClientId,
        keep_alive => 60
    }),
    Pid.
-else.
client_b_label() -> "TCP".

connect_b(ClientId, BrokerIp) ->
    connect_tcp(ClientId, BrokerIp).
-endif.

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
    ExpectedSha = hex(crypto:hash(sha256, Payload)),

    publish(A, PubTopic, Payload, QoS),

    receive
        {mqtt, B, publish, #{message := P}} when P =:= Payload ->
            Sha = hex(crypto:hash(sha256, P)),
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
    ok = mqtt_client:publish(Client, Topic, Payload, 0);
publish(Client, Topic, Payload, QoS) ->
    {ok, _PacketId} = mqtt_client:publish(Client, Topic, Payload, QoS).

hex(Bin) when is_binary(Bin) ->
    <<<<(nibble_hex(N1)), (nibble_hex(N2))>> || <<N1:4, N2:4>> <= Bin>>.

nibble_hex(N) when N < 10 -> $0 + N;
nibble_hex(N) -> $a + (N - 10).
