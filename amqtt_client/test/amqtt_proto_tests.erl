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

-module(amqtt_proto_tests).

-include_lib("eunit/include/eunit.hrl").

%% -------------------------------------------------------------------
%% Remaining Length Encoding/Decoding
%% -------------------------------------------------------------------

encode_remaining_length_0_test() ->
    ?assertEqual(<<0>>, amqtt_proto:encode_remaining_length(0)).

encode_remaining_length_127_test() ->
    ?assertEqual(<<127>>, amqtt_proto:encode_remaining_length(127)).

encode_remaining_length_128_test() ->
    ?assertEqual(<<128, 1>>, amqtt_proto:encode_remaining_length(128)).

encode_remaining_length_16383_test() ->
    ?assertEqual(<<16#FF, 16#7F>>, amqtt_proto:encode_remaining_length(16383)).

encode_remaining_length_16384_test() ->
    ?assertEqual(<<16#80, 16#80, 1>>, amqtt_proto:encode_remaining_length(16384)).

encode_remaining_length_2097151_test() ->
    ?assertEqual(<<16#FF, 16#FF, 16#7F>>, amqtt_proto:encode_remaining_length(2097151)).

encode_remaining_length_2097152_test() ->
    ?assertEqual(<<16#80, 16#80, 16#80, 1>>, amqtt_proto:encode_remaining_length(2097152)).

encode_remaining_length_max_test() ->
    ?assertEqual(<<16#FF, 16#FF, 16#FF, 16#7F>>, amqtt_proto:encode_remaining_length(268435455)).

decode_remaining_length_single_byte_test() ->
    ?assertEqual({ok, 0, <<>>}, amqtt_proto:decode_remaining_length(<<0>>)),
    ?assertEqual({ok, 127, <<>>}, amqtt_proto:decode_remaining_length(<<127>>)).

decode_remaining_length_two_bytes_test() ->
    ?assertEqual({ok, 128, <<>>}, amqtt_proto:decode_remaining_length(<<128, 1>>)),
    ?assertEqual({ok, 16383, <<>>}, amqtt_proto:decode_remaining_length(<<16#FF, 16#7F>>)).

decode_remaining_length_with_trailing_data_test() ->
    ?assertEqual({ok, 128, <<99>>}, amqtt_proto:decode_remaining_length(<<128, 1, 99>>)).

decode_remaining_length_incomplete_test() ->
    ?assertEqual({error, incomplete}, amqtt_proto:decode_remaining_length(<<>>)),
    ?assertEqual({error, incomplete}, amqtt_proto:decode_remaining_length(<<128>>)),
    ?assertEqual({error, incomplete}, amqtt_proto:decode_remaining_length(<<128, 128>>)).

remaining_length_roundtrip_test() ->
    Ns = [0, 1, 127, 128, 255, 16383, 16384, 2097151, 2097152, 268435455],
    lists:foreach(
        fun(N) ->
            Encoded = amqtt_proto:encode_remaining_length(N),
            ?assertEqual({ok, N, <<>>}, amqtt_proto:decode_remaining_length(Encoded))
        end,
        Ns
    ).

%% -------------------------------------------------------------------
%% CONNECT
%% -------------------------------------------------------------------

encode_minimal_connect_test() ->
    Packet = iolist_to_binary(amqtt_proto:encode_connect(#{client_id => <<"test">>})),
    {ok, {connect, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(<<"test">>, maps:get(client_id, Data)),
    ?assertEqual(60, maps:get(keep_alive_seconds, Data)),
    ?assertEqual(true, maps:get(clean_session, Data)),
    ?assertNot(maps:is_key(username, Data)),
    ?assertNot(maps:is_key(password, Data)),
    ?assertNot(maps:is_key(will_topic, Data)).

encode_connect_protocol_header_test() ->
    Packet = iolist_to_binary(amqtt_proto:encode_connect(#{client_id => <<"c">>})),
    %% Fixed header: type=1, flags=0 -> 0x10
    %% Then remaining length, then protocol name "MQTT", level 4
    <<16#10, _RL, 0, 4, Rest/binary>> = Packet,
    <<"MQTT", 4, _FlagsAndRest/binary>> = Rest,
    ok.

encode_connect_with_username_password_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_connect(#{
            client_id => <<"c">>,
            username => <<"user">>,
            password => <<"pass">>
        })
    ),
    {ok, {connect, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(<<"user">>, maps:get(username, Data)),
    ?assertEqual(<<"pass">>, maps:get(password, Data)).

encode_connect_with_will_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_connect(#{
            client_id => <<"c">>,
            will_topic => <<"/bye">>,
            will_message => <<"gone">>,
            will_qos => 1,
            will_retain => true
        })
    ),
    {ok, {connect, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(<<"/bye">>, maps:get(will_topic, Data)),
    ?assertEqual(<<"gone">>, maps:get(will_message, Data)),
    ?assertEqual(1, maps:get(will_qos, Data)),
    ?assertEqual(true, maps:get(will_retain, Data)).

encode_connect_clean_session_false_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_connect(#{client_id => <<"c">>, clean_session => false})
    ),
    {ok, {connect, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(false, maps:get(clean_session, Data)).

encode_connect_custom_keep_alive_seconds_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_connect(#{client_id => <<"c">>, keep_alive_seconds => 120})
    ),
    {ok, {connect, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(120, maps:get(keep_alive_seconds, Data)).

%% -------------------------------------------------------------------
%% CONNACK
%% -------------------------------------------------------------------

decode_connack_success_test() ->
    Packet = <<16#20, 2, 0, 0>>,
    ?assertMatch(
        {ok, {connack, #{session_present := false, return_code := 0}}, <<>>},
        amqtt_proto:decode(Packet)
    ).

decode_connack_session_present_test() ->
    Packet = <<16#20, 2, 1, 0>>,
    ?assertMatch(
        {ok, {connack, #{session_present := true, return_code := 0}}, <<>>},
        amqtt_proto:decode(Packet)
    ).

decode_connack_not_authorized_test() ->
    Packet = <<16#20, 2, 0, 5>>,
    ?assertMatch(
        {ok, {connack, #{session_present := false, return_code := 5}}, <<>>},
        amqtt_proto:decode(Packet)
    ).

%% -------------------------------------------------------------------
%% PUBLISH
%% -------------------------------------------------------------------

encode_decode_publish_qos0_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_publish(#{topic => <<"t">>, message => <<"m">>, qos => 0})
    ),
    {ok, {publish, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(<<"t">>, maps:get(topic, Data)),
    ?assertEqual(<<"m">>, maps:get(message, Data)),
    ?assertEqual(0, maps:get(qos, Data)),
    ?assertEqual(false, maps:get(dup, Data)),
    ?assertEqual(false, maps:get(retain, Data)),
    ?assertNot(maps:is_key(packet_id, Data)).

encode_decode_publish_qos1_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_publish(#{
            topic => <<"t">>, message => <<"m">>, qos => 1, packet_id => 42
        })
    ),
    {ok, {publish, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(<<"t">>, maps:get(topic, Data)),
    ?assertEqual(<<"m">>, maps:get(message, Data)),
    ?assertEqual(1, maps:get(qos, Data)),
    ?assertEqual(42, maps:get(packet_id, Data)).

encode_decode_publish_qos2_dup_retain_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_publish(#{
            topic => <<"a/b">>,
            message => <<"hello">>,
            qos => 2,
            packet_id => 1000,
            dup => true,
            retain => true
        })
    ),
    {ok, {publish, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(2, maps:get(qos, Data)),
    ?assertEqual(true, maps:get(dup, Data)),
    ?assertEqual(true, maps:get(retain, Data)),
    ?assertEqual(1000, maps:get(packet_id, Data)),
    ?assertEqual(<<"a/b">>, maps:get(topic, Data)),
    ?assertEqual(<<"hello">>, maps:get(message, Data)).

encode_decode_publish_empty_message_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_publish(#{topic => <<"t">>, message => <<>>, qos => 0})
    ),
    {ok, {publish, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(<<>>, maps:get(message, Data)).

encode_decode_publish_large_message_test() ->
    BigMsg = binary:copy(<<"x">>, 200),
    Packet = iolist_to_binary(
        amqtt_proto:encode_publish(#{topic => <<"t">>, message => BigMsg, qos => 0})
    ),
    {ok, {publish, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(BigMsg, maps:get(message, Data)).

%% -------------------------------------------------------------------
%% PUBACK / PUBREC / PUBREL / PUBCOMP
%% -------------------------------------------------------------------

encode_decode_puback_test() ->
    Packet = amqtt_proto:encode_puback(7),
    ?assertEqual(<<16#40, 2, 0, 7>>, Packet),
    ?assertMatch({ok, {puback, #{packet_id := 7}}, <<>>}, amqtt_proto:decode(Packet)).

encode_decode_pubrec_test() ->
    Packet = amqtt_proto:encode_pubrec(99),
    ?assertEqual(<<16#50, 2, 0, 99>>, Packet),
    ?assertMatch({ok, {pubrec, #{packet_id := 99}}, <<>>}, amqtt_proto:decode(Packet)).

encode_decode_pubrel_reserved_flags_test() ->
    Packet = amqtt_proto:encode_pubrel(99),
    %% PUBREL type=6, flags=2 -> 0x62
    ?assertEqual(<<16#62, 2, 0, 99>>, Packet),
    ?assertMatch({ok, {pubrel, #{packet_id := 99}}, <<>>}, amqtt_proto:decode(Packet)).

encode_decode_pubcomp_test() ->
    Packet = amqtt_proto:encode_pubcomp(99),
    ?assertEqual(<<16#70, 2, 0, 99>>, Packet),
    ?assertMatch({ok, {pubcomp, #{packet_id := 99}}, <<>>}, amqtt_proto:decode(Packet)).

%% -------------------------------------------------------------------
%% SUBSCRIBE / SUBACK
%% -------------------------------------------------------------------

encode_decode_subscribe_single_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_subscribe(#{packet_id => 1, topics => [{<<"a/b">>, 1}]})
    ),
    {ok, {subscribe, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(1, maps:get(packet_id, Data)),
    ?assertEqual([{<<"a/b">>, 1}], maps:get(topics, Data)).

encode_decode_subscribe_multiple_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_subscribe(#{
            packet_id => 5,
            topics => [{<<"a">>, 0}, {<<"b">>, 1}, {<<"c">>, 2}]
        })
    ),
    {ok, {subscribe, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(5, maps:get(packet_id, Data)),
    ?assertEqual([{<<"a">>, 0}, {<<"b">>, 1}, {<<"c">>, 2}], maps:get(topics, Data)).

subscribe_reserved_flags_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_subscribe(#{packet_id => 1, topics => [{<<"t">>, 0}]})
    ),
    %% SUBSCRIBE type=8, flags=2 -> 0x82
    <<16#82, _Rest/binary>> = Packet,
    ok.

decode_suback_test() ->
    %% SUBACK: type=9, flags=0 -> 0x90, remaining=5, packet_id=1, then 3 return codes
    Packet = <<16#90, 5, 0, 1, 0, 1, 16#80>>,
    {ok, {suback, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(1, maps:get(packet_id, Data)),
    ?assertEqual([0, 1, 16#80], maps:get(return_codes, Data)).

%% -------------------------------------------------------------------
%% UNSUBSCRIBE / UNSUBACK
%% -------------------------------------------------------------------

encode_decode_unsubscribe_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_unsubscribe(#{packet_id => 3, topics => [<<"a">>, <<"b">>]})
    ),
    {ok, {unsubscribe, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(3, maps:get(packet_id, Data)),
    ?assertEqual([<<"a">>, <<"b">>], maps:get(topics, Data)).

unsubscribe_reserved_flags_test() ->
    Packet = iolist_to_binary(
        amqtt_proto:encode_unsubscribe(#{packet_id => 1, topics => [<<"t">>]})
    ),
    <<16#A2, _Rest/binary>> = Packet,
    ok.

decode_unsuback_test() ->
    Packet = <<16#B0, 2, 0, 3>>,
    ?assertMatch({ok, {unsuback, #{packet_id := 3}}, <<>>}, amqtt_proto:decode(Packet)).

%% -------------------------------------------------------------------
%% PINGREQ / PINGRESP
%% -------------------------------------------------------------------

encode_pingreq_test() ->
    ?assertEqual(<<16#C0, 0>>, amqtt_proto:encode_pingreq()).

decode_pingresp_test() ->
    ?assertMatch({ok, {pingresp, #{}}, <<>>}, amqtt_proto:decode(<<16#D0, 0>>)).

decode_pingreq_test() ->
    ?assertMatch({ok, {pingreq, #{}}, <<>>}, amqtt_proto:decode(<<16#C0, 0>>)).

%% -------------------------------------------------------------------
%% DISCONNECT
%% -------------------------------------------------------------------

encode_disconnect_test() ->
    ?assertEqual(<<16#E0, 0>>, amqtt_proto:encode_disconnect()).

decode_disconnect_test() ->
    ?assertMatch({ok, {disconnect, #{}}, <<>>}, amqtt_proto:decode(<<16#E0, 0>>)).

%% -------------------------------------------------------------------
%% Fragmentation / Buffer Handling
%% -------------------------------------------------------------------

decode_incomplete_empty_test() ->
    ?assertEqual({error, incomplete}, amqtt_proto:decode(<<>>)).

decode_incomplete_only_fixed_header_test() ->
    ?assertEqual({error, incomplete}, amqtt_proto:decode(<<16#20>>)).

decode_incomplete_remaining_length_says_more_test() ->
    %% CONNACK header but missing payload
    ?assertEqual({error, incomplete}, amqtt_proto:decode(<<16#20, 2, 0>>)).

decode_incomplete_variable_length_test() ->
    ?assertEqual({error, incomplete}, amqtt_proto:decode(<<16#30, 128>>)).

decode_two_packets_concatenated_test() ->
    P1 = <<16#D0, 0>>,
    P2 = <<16#D0, 0>>,
    Combined = <<P1/binary, P2/binary>>,
    {ok, {pingresp, _}, Rest} = amqtt_proto:decode(Combined),
    ?assertMatch({ok, {pingresp, _}, <<>>}, amqtt_proto:decode(Rest)).

decode_connack_with_trailing_publish_test() ->
    Connack = <<16#20, 2, 0, 0>>,
    Publish = iolist_to_binary(
        amqtt_proto:encode_publish(#{topic => <<"t">>, message => <<"m">>, qos => 0})
    ),
    Combined = <<Connack/binary, Publish/binary>>,
    {ok, {connack, _}, Rest} = amqtt_proto:decode(Combined),
    {ok, {publish, Data}, <<>>} = amqtt_proto:decode(Rest),
    ?assertEqual(<<"t">>, maps:get(topic, Data)).

large_publish_roundtrip_test() ->
    %% Message > 16383 bytes requires 3-byte remaining length
    BigMsg = binary:copy(<<"A">>, 20000),
    Packet = iolist_to_binary(
        amqtt_proto:encode_publish(#{
            topic => <<"sensor/data">>,
            message => BigMsg,
            qos => 1,
            packet_id => 65535
        })
    ),
    {ok, {publish, Data}, <<>>} = amqtt_proto:decode(Packet),
    ?assertEqual(<<"sensor/data">>, maps:get(topic, Data)),
    ?assertEqual(BigMsg, maps:get(message, Data)),
    ?assertEqual(65535, maps:get(packet_id, Data)).

%% -------------------------------------------------------------------
%% Decoder negative tests: malformed flags, reserved QoS, packet ID 0
%% -------------------------------------------------------------------

decode_publish_qos3_rejected_test() ->
    %% PUBLISH (type=3) with QoS=3 (bits 2-1 == 11): flags = 0b0110 = 6
    %% Build a packet manually: <<0x36, RL, ... topic ... pid ... msg>>
    Packet = <<16#36, 5, 0, 1, "t", 0, 1>>,
    ?assertMatch({error, {protocol_error, _}}, amqtt_proto:decode(Packet)).

decode_publish_qos1_pid0_rejected_test() ->
    %% PUBLISH QoS 1 with packet_id = 0
    Packet = <<16#32, 5, 0, 1, "t", 0, 0>>,
    ?assertMatch({error, {protocol_error, _}}, amqtt_proto:decode(Packet)).

decode_puback_pid0_rejected_test() ->
    Packet = <<16#40, 2, 0, 0>>,
    ?assertMatch({error, {protocol_error, _}}, amqtt_proto:decode(Packet)).

decode_pubrel_bad_flags_test() ->
    %% PUBREL must have flags 0x02; using 0x00 must be rejected
    Packet = <<16#60, 2, 0, 7>>,
    ?assertMatch({error, {protocol_error, _}}, amqtt_proto:decode(Packet)).

decode_subscribe_bad_flags_test() ->
    %% SUBSCRIBE must have flags 0x02; using 0x00 must be rejected
    Packet = <<16#80, 6, 0, 1, 0, 1, "t", 0>>,
    ?assertMatch({error, {protocol_error, _}}, amqtt_proto:decode(Packet)).

decode_unsubscribe_bad_flags_test() ->
    %% UNSUBSCRIBE must have flags 0x02
    Packet = <<16#A0, 5, 0, 1, 0, 1, "t">>,
    ?assertMatch({error, {protocol_error, _}}, amqtt_proto:decode(Packet)).

decode_puback_bad_flags_test() ->
    %% PUBACK must have flags 0x00; non-zero must be rejected
    Packet = <<16#41, 2, 0, 1>>,
    ?assertMatch({error, {protocol_error, _}}, amqtt_proto:decode(Packet)).

decode_connack_reserved_bits_set_test() ->
    %% MQTT 3.1.1 §3.2.2.1: AckFlags bits 7..1 are reserved and must be 0.
    Packet = <<16#20, 2, 16#80, 0>>,
    ?assertMatch({error, {protocol_error, _}}, amqtt_proto:decode(Packet)).

decode_connack_invalid_return_code_test() ->
    %% Return code 6 is outside the spec range 0..5.
    Packet = <<16#20, 2, 0, 6>>,
    ?assertMatch({error, {protocol_error, _}}, amqtt_proto:decode(Packet)).

decode_connack_session_present_with_error_test() ->
    %% MQTT 3.1.1 §3.2.2.2: session_present must be 0 when return_code /= 0.
    Packet = <<16#20, 2, 1, 4>>,
    ?assertMatch(
        {error, {protocol_error, {bad_connack, session_present_with_error}}},
        amqtt_proto:decode(Packet)
    ).

decode_suback_empty_return_codes_test() ->
    %% MQTT 3.1.1 §3.9: SUBACK must contain at least one return code.
    Packet = <<16#90, 2, 0, 1>>,
    ?assertMatch({error, {protocol_error, _}}, amqtt_proto:decode(Packet)).

decode_suback_invalid_return_code_test() ->
    %% MQTT 3.1.1 §3.9.3: each return code must be 0, 1, 2, or 0x80.
    Packet = <<16#90, 3, 0, 1, 16#03>>,
    ?assertMatch(
        {error, {protocol_error, {bad_suback, return_codes}}},
        amqtt_proto:decode(Packet)
    ).
