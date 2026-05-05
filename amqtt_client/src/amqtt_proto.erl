%
% Copyright 2020-2026 Davide Bettio <davide@uninstall.it>
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

%% @doc MQTT 3.1.1 wire-format codec.
%%
%% Pure encode/decode functions for the fourteen MQTT 3.1.1 control
%% packet types. No I/O, no process state, suitable for use with any
%% transport. Encoders take a map of fields and return an `iodata'-ready
%% binary; {@link decode/1} consumes a binary buffer and returns the
%% next packet plus any trailing bytes, signalling `incomplete' when
%% more data is needed and `{protocol_error, _}' on a malformed frame.
%%
%% @end
-module(amqtt_proto).

-export_type([
    qos/0,
    packet_id/0,
    packet_type/0,
    connect_opts/0,
    publish_opts/0,
    subscribe_opts/0,
    unsubscribe_opts/0,
    connack_data/0,
    publish_data/0,
    suback_data/0,
    packet_id_data/0,
    decoded_packet/0
]).

-type qos() :: 0 | 1 | 2.
-type packet_id() :: 1..65535.
-type topic() :: binary().

-type packet_type() ::
    connect
    | connack
    | publish
    | puback
    | pubrec
    | pubrel
    | pubcomp
    | subscribe
    | suback
    | unsubscribe
    | unsuback
    | pingreq
    | pingresp
    | disconnect.

-type connect_opts() :: #{
    client_id := binary(),
    keep_alive_seconds => 0..65535,
    clean_session => boolean(),
    username => binary(),
    password => binary(),
    will_topic => binary(),
    will_message => binary(),
    will_qos => qos(),
    will_retain => boolean()
}.

-type publish_opts() :: #{
    topic := topic(),
    message := iodata(),
    qos => qos(),
    retain => boolean(),
    dup => boolean(),
    packet_id => packet_id()
}.

-type subscribe_opts() :: #{
    packet_id := packet_id(),
    topics := [{topic(), qos()}]
}.

-type unsubscribe_opts() :: #{
    packet_id := packet_id(),
    topics := [topic()]
}.

-type connack_data() :: #{
    session_present := boolean(),
    return_code := 0..5
}.

-type publish_data() :: #{
    topic := topic(),
    message := binary(),
    qos := qos(),
    dup := boolean(),
    retain := boolean(),
    packet_id => packet_id()
}.

-type suback_data() :: #{
    packet_id := packet_id(),
    return_codes := [qos() | 128]
}.

-type packet_id_data() :: #{packet_id := packet_id()}.

-type decoded_packet() ::
    {connack, connack_data()}
    | {publish, publish_data()}
    | {puback, packet_id_data()}
    | {pubrec, packet_id_data()}
    | {pubrel, packet_id_data()}
    | {pubcomp, packet_id_data()}
    | {suback, suback_data()}
    | {unsuback, packet_id_data()}
    | {pingreq, #{}}
    | {pingresp, #{}}
    | {disconnect, #{}}
    | {connect, map()}
    | {subscribe, map()}
    | {unsubscribe, map()}.

-export([
    encode_connect/1,
    encode_publish/1,
    encode_puback/1,
    encode_pubrec/1,
    encode_pubrel/1,
    encode_pubcomp/1,
    encode_subscribe/1,
    encode_unsubscribe/1,
    encode_pingreq/0,
    encode_disconnect/0,
    decode/1,
    encode_remaining_length/1,
    decode_remaining_length/1
]).

%% Packet types
-define(CONNECT, 1).
-define(CONNACK, 2).
-define(PUBLISH, 3).
-define(PUBACK, 4).
-define(PUBREC, 5).
-define(PUBREL, 6).
-define(PUBCOMP, 7).
-define(SUBSCRIBE, 8).
-define(SUBACK, 9).
-define(UNSUBSCRIBE, 10).
-define(UNSUBACK, 11).
-define(PINGREQ, 12).
-define(PINGRESP, 13).
-define(DISCONNECT, 14).

%% -------------------------------------------------------------------
%% Remaining Length Codec
%% -------------------------------------------------------------------

%% @doc Encode an MQTT variable-length integer (1 to 4 bytes).
%%
%% Used for the remaining-length field in every fixed header. `N' must
%% be in `0..268435455' (the MQTT 3.1.1 maximum).
%% @end
-spec encode_remaining_length(non_neg_integer()) -> binary().
encode_remaining_length(N) when N >= 0, N =< 268435455 ->
    encode_remaining_length_acc(N, <<>>).

encode_remaining_length_acc(N, Acc) when N < 128 ->
    <<Acc/binary, 0:1, N:7>>;
encode_remaining_length_acc(N, Acc) ->
    encode_remaining_length_acc(N bsr 7, <<Acc/binary, 1:1, N:7>>).

%% @doc Decode an MQTT variable-length integer.
%%
%% Returns `{ok, Value, Rest}' on success, `{error, incomplete}' if
%% more bytes are needed, or `{error, invalid}' if the encoding exceeds
%% four bytes.
%% @end
-spec decode_remaining_length(binary()) ->
    {ok, non_neg_integer(), binary()} | {error, incomplete | invalid}.
decode_remaining_length(Bin) ->
    decode_remaining_length(Bin, 0, 1).

decode_remaining_length(<<>>, _Value, _Multiplier) ->
    {error, incomplete};
decode_remaining_length(<<0:1, V:7, Rest/binary>>, Value, Multiplier) when Multiplier =< 2097152 ->
    {ok, Value + V * Multiplier, Rest};
decode_remaining_length(<<1:1, V:7, Rest/binary>>, Value, Multiplier) when Multiplier =< 2097152 ->
    decode_remaining_length(Rest, Value + V * Multiplier, Multiplier bsl 7);
decode_remaining_length(_Bin, _Value, _Multiplier) ->
    {error, invalid}.

%% -------------------------------------------------------------------
%% UTF-8 String Helpers
%% -------------------------------------------------------------------

encode_utf8(Bin) when is_binary(Bin) ->
    Len = byte_size(Bin),
    <<Len:16/big, Bin/binary>>.

decode_utf8(<<Len:16/big, Rest/binary>>) when byte_size(Rest) >= Len ->
    <<Str:Len/binary, Remaining/binary>> = Rest,
    {ok, Str, Remaining};
decode_utf8(_) ->
    {error, incomplete}.

%% -------------------------------------------------------------------
%% Encode Functions
%% -------------------------------------------------------------------

%% @doc Encode a CONNECT packet.
%%
%% `Opts' must include `client_id'; all other fields are optional and
%% take their MQTT 3.1.1 defaults (`keep_alive_seconds => 60',
%% `clean_session => true', no will, no credentials).
%% @end
-spec encode_connect(connect_opts()) -> iodata().
encode_connect(Opts) ->
    ClientId = maps:get(client_id, Opts),
    KeepAliveSeconds = maps:get(keep_alive_seconds, Opts, 60),
    CleanSession = maps:get(clean_session, Opts, true),
    Username = maps:get(username, Opts, undefined),
    Password = maps:get(password, Opts, undefined),
    WillTopic = maps:get(will_topic, Opts, undefined),
    WillMessage = maps:get(will_message, Opts, undefined),
    WillQoS = maps:get(will_qos, Opts, 0),
    WillRetain = maps:get(will_retain, Opts, false),

    CleanSessionBit = bool_to_bit(CleanSession),
    HasWill = WillTopic =/= undefined,
    WillFlag = bool_to_bit(HasWill),
    WillRetainBit =
        case HasWill of
            true -> bool_to_bit(WillRetain);
            false -> 0
        end,
    WillQoSBits =
        case HasWill of
            true -> WillQoS;
            false -> 0
        end,
    HasUsername = Username =/= undefined,
    UsernameBit = bool_to_bit(HasUsername),
    HasPassword = Password =/= undefined,
    PasswordBit = bool_to_bit(HasPassword),

    VarHeader =
        <<0, 4, "MQTT", 4, UsernameBit:1, PasswordBit:1, WillRetainBit:1, WillQoSBits:2, WillFlag:1,
            CleanSessionBit:1, 0:1, KeepAliveSeconds:16/big>>,

    WillIo =
        case HasWill of
            true -> [encode_utf8(WillTopic), encode_utf8(WillMessage)];
            false -> []
        end,
    UserIo =
        case HasUsername of
            true -> encode_utf8(Username);
            false -> []
        end,
    PassIo =
        case HasPassword of
            true -> encode_utf8(Password);
            false -> []
        end,

    Body = [VarHeader, encode_utf8(ClientId), WillIo, UserIo, PassIo],
    RemainingLen = encode_remaining_length(iolist_size(Body)),
    [<<?CONNECT:4, 0:4>>, RemainingLen, Body].

%% @doc Encode a PUBLISH packet.
%%
%% `topic' and `message' are required. `qos' defaults to `0'; for QoS 1
%% or 2 a `packet_id' is required. `retain' and `dup' default to
%% `false'.
%% @end
-spec encode_publish(publish_opts()) -> iodata().
encode_publish(Opts) ->
    Topic = maps:get(topic, Opts),
    Message = maps:get(message, Opts),
    QoS = maps:get(qos, Opts, 0),
    Retain = maps:get(retain, Opts, false),
    Dup = maps:get(dup, Opts, false),

    DupBit = bool_to_bit(Dup),
    RetainBit = bool_to_bit(Retain),

    TopicBin = encode_utf8(Topic),
    VarHeader =
        case QoS of
            0 ->
                TopicBin;
            _ ->
                PacketId = maps:get(packet_id, Opts),
                [TopicBin, <<PacketId:16/big>>]
        end,

    Body = [VarHeader, Message],
    RemainingLen = encode_remaining_length(iolist_size(Body)),
    [<<?PUBLISH:4, DupBit:1, QoS:2, RetainBit:1>>, RemainingLen, Body].

%% @doc Encode a PUBACK acknowledgment for `PacketId' (QoS 1).
-spec encode_puback(packet_id()) -> binary().
encode_puback(PacketId) ->
    <<?PUBACK:4, 0:4, 2, PacketId:16/big>>.

%% @doc Encode a PUBREC acknowledgment for `PacketId' (QoS 2, step 1).
-spec encode_pubrec(packet_id()) -> binary().
encode_pubrec(PacketId) ->
    <<?PUBREC:4, 0:4, 2, PacketId:16/big>>.

%% @doc Encode a PUBREL packet for `PacketId' (QoS 2, step 2).
-spec encode_pubrel(packet_id()) -> binary().
encode_pubrel(PacketId) ->
    <<?PUBREL:4, 2:4, 2, PacketId:16/big>>.

%% @doc Encode a PUBCOMP acknowledgment for `PacketId' (QoS 2, step 3).
-spec encode_pubcomp(packet_id()) -> binary().
encode_pubcomp(PacketId) ->
    <<?PUBCOMP:4, 0:4, 2, PacketId:16/big>>.

%% @doc Encode a SUBSCRIBE packet.
%%
%% `Opts' must include `packet_id' and a non-empty `topics' list of
%% `{TopicFilter, RequestedQoS}' pairs.
%% @end
-spec encode_subscribe(subscribe_opts()) -> iodata().
encode_subscribe(Opts) ->
    PacketId = maps:get(packet_id, Opts),
    Topics = maps:get(topics, Opts),

    Body = [<<PacketId:16/big>>, [[encode_utf8(Topic), QoS] || {Topic, QoS} <- Topics]],
    RemainingLen = encode_remaining_length(iolist_size(Body)),
    [<<?SUBSCRIBE:4, 2:4>>, RemainingLen, Body].

%% @doc Encode an UNSUBSCRIBE packet.
%%
%% `Opts' must include `packet_id' and a non-empty `topics' list of
%% topic filters.
%% @end
-spec encode_unsubscribe(unsubscribe_opts()) -> iodata().
encode_unsubscribe(Opts) ->
    PacketId = maps:get(packet_id, Opts),
    Topics = maps:get(topics, Opts),

    Body = [<<PacketId:16/big>>, [encode_utf8(Topic) || Topic <- Topics]],
    RemainingLen = encode_remaining_length(iolist_size(Body)),
    [<<?UNSUBSCRIBE:4, 2:4>>, RemainingLen, Body].

%% @doc Encode a PINGREQ packet.
-spec encode_pingreq() -> binary().
encode_pingreq() ->
    <<?PINGREQ:4, 0:4, 0>>.

%% @doc Encode a DISCONNECT packet.
-spec encode_disconnect() -> binary().
encode_disconnect() ->
    <<?DISCONNECT:4, 0:4, 0>>.

%% -------------------------------------------------------------------
%% Unified Decoder
%% -------------------------------------------------------------------

%% @doc Decode the next MQTT control packet from `Bin'.
%%
%% Returns `{ok, {Type, Data}, Rest}' on success, where `Type' is the
%% packet kind and `Data' is the decoded fields map. `Rest' contains
%% any bytes after the consumed packet and may be passed back to
%% `decode/1' to extract the following packet.
%%
%% Returns `{error, incomplete}' if the buffer does not yet contain a
%% full packet, or `{error, {protocol_error, Reason}}' if the bytes
%% violate MQTT 3.1.1 (reserved flag bits, reserved QoS, packet ID
%% zero, malformed UTF-8 length, etc.).
%% @end
-spec decode(binary()) ->
    {ok, decoded_packet(), Rest :: binary()}
    | {error, incomplete}
    | {error, {protocol_error, term()}}.
decode(<<>>) ->
    {error, incomplete};
decode(<<Type:4, Flags:4, Rest/binary>>) ->
    case decode_remaining_length(Rest) of
        {ok, Length, Payload} when byte_size(Payload) >= Length ->
            <<PacketData:Length/binary, Remaining/binary>> = Payload,
            case decode_packet(Type, Flags, PacketData) of
                {ok, Decoded} ->
                    {ok, Decoded, Remaining};
                {error, Reason} ->
                    {error, {protocol_error, Reason}}
            end;
        {ok, _Length, _Payload} ->
            {error, incomplete};
        {error, incomplete} ->
            {error, incomplete};
        {error, Reason} ->
            {error, {protocol_error, Reason}}
    end.

%% -------------------------------------------------------------------
%% Packet Decoders
%% -------------------------------------------------------------------

%% Per MQTT 3.1.1 §2.2.2, fixed-header flags are reserved for most types
%% (must be 0) and 0x2 for PUBREL / SUBSCRIBE / UNSUBSCRIBE. PUBLISH carries
%% DUP/QoS/RETAIN. Anything else is a malformed packet.
decode_packet(?CONNACK, 0, <<0:7, SP:1, ReturnCode>>) when ReturnCode =< 5 ->
    SessionPresent = SP =:= 1,
    if
        SessionPresent andalso ReturnCode =/= 0 ->
            {error, {bad_connack, session_present_with_error}};
        true ->
            {ok, {connack, #{session_present => SessionPresent, return_code => ReturnCode}}}
    end;
decode_packet(?PUBLISH, Flags, Data) ->
    Dup = (Flags band 8) =:= 8,
    QoS = (Flags bsr 1) band 3,
    Retain = (Flags band 1) =:= 1,
    case QoS of
        3 ->
            {error, {bad_publish, qos_reserved}};
        _ ->
            decode_publish(Dup, QoS, Retain, Data)
    end;
decode_packet(?PUBACK, 0, <<PacketId:16/big>>) when PacketId =/= 0 ->
    {ok, {puback, #{packet_id => PacketId}}};
decode_packet(?PUBREC, 0, <<PacketId:16/big>>) when PacketId =/= 0 ->
    {ok, {pubrec, #{packet_id => PacketId}}};
decode_packet(?PUBREL, 2, <<PacketId:16/big>>) when PacketId =/= 0 ->
    {ok, {pubrel, #{packet_id => PacketId}}};
decode_packet(?PUBCOMP, 0, <<PacketId:16/big>>) when PacketId =/= 0 ->
    {ok, {pubcomp, #{packet_id => PacketId}}};
decode_packet(?SUBACK, 0, <<PacketId:16/big, ReturnCodes/binary>>) when
    PacketId =/= 0, byte_size(ReturnCodes) >= 1
->
    RCs = binary_to_list(ReturnCodes),
    case lists:all(fun valid_suback_rc/1, RCs) of
        true -> {ok, {suback, #{packet_id => PacketId, return_codes => RCs}}};
        false -> {error, {bad_suback, return_codes}}
    end;
decode_packet(?UNSUBACK, 0, <<PacketId:16/big>>) when PacketId =/= 0 ->
    {ok, {unsuback, #{packet_id => PacketId}}};
decode_packet(?PINGRESP, 0, <<>>) ->
    {ok, {pingresp, #{}}};
decode_packet(?PINGREQ, 0, <<>>) ->
    {ok, {pingreq, #{}}};
decode_packet(?CONNECT, 0, Data) ->
    case decode_connect_var_header(Data) of
        {ok, Result} -> {ok, {connect, Result}};
        {error, _} = Err -> Err
    end;
decode_packet(?SUBSCRIBE, 2, <<PacketId:16/big, Payload/binary>>) when PacketId =/= 0 ->
    case decode_subscribe_topics(Payload, []) of
        {ok, Topics} ->
            {ok, {subscribe, #{packet_id => PacketId, topics => Topics}}};
        {error, _} = Err ->
            Err
    end;
decode_packet(?UNSUBSCRIBE, 2, <<PacketId:16/big, Payload/binary>>) when PacketId =/= 0 ->
    case decode_unsubscribe_topics(Payload, []) of
        {ok, Topics} ->
            {ok, {unsubscribe, #{packet_id => PacketId, topics => Topics}}};
        {error, _} = Err ->
            Err
    end;
decode_packet(?DISCONNECT, 0, <<>>) ->
    {ok, {disconnect, #{}}};
decode_packet(Type, _Flags, _Data) when
    Type < ?CONNECT; Type > ?DISCONNECT
->
    {error, {unknown_packet_type, Type}};
decode_packet(_Type, _Flags, _Data) ->
    {error, malformed_packet}.

decode_publish(Dup, 0, Retain, Data) ->
    case decode_utf8(Data) of
        {ok, Topic, Rest} ->
            {ok,
                {publish, #{
                    topic => Topic,
                    message => Rest,
                    qos => 0,
                    dup => Dup,
                    retain => Retain
                }}};
        {error, _} = Err ->
            Err
    end;
decode_publish(Dup, QoS, Retain, Data) ->
    case decode_utf8(Data) of
        {ok, Topic, Rest} when byte_size(Rest) >= 2 ->
            <<PacketId:16/big, Message/binary>> = Rest,
            case PacketId of
                0 ->
                    {error, {bad_packet_id, 0}};
                _ ->
                    {ok,
                        {publish, #{
                            topic => Topic,
                            message => Message,
                            qos => QoS,
                            dup => Dup,
                            retain => Retain,
                            packet_id => PacketId
                        }}}
            end;
        {ok, _Topic, _Rest} ->
            {error, {bad_publish, too_short}};
        {error, _} = Err ->
            Err
    end.

valid_suback_rc(0) -> true;
valid_suback_rc(1) -> true;
valid_suback_rc(2) -> true;
valid_suback_rc(16#80) -> true;
valid_suback_rc(_) -> false.

%% -------------------------------------------------------------------
%% CONNECT Decoder (for server-side reuse)
%% -------------------------------------------------------------------

decode_connect_var_header(<<
    0,
    4,
    "MQTT",
    4,
    HasUsernameBit:1,
    HasPasswordBit:1,
    WillRetainBit:1,
    WillQoS:2,
    HasWillBit:1,
    CleanSessionBit:1,
    _Reserved:1,
    KeepAliveSeconds:16/big,
    Payload/binary
>>) ->
    CleanSession = CleanSessionBit =:= 1,
    HasWill = HasWillBit =:= 1,
    WillRetain = WillRetainBit =:= 1,
    HasUsername = HasUsernameBit =:= 1,
    HasPassword = HasPasswordBit =:= 1,

    case decode_utf8(Payload) of
        {ok, ClientId, Rest0} ->
            case decode_will(HasWill, WillQoS, WillRetain, Rest0) of
                {ok, WillProps, Rest1} ->
                    case decode_credentials(HasUsername, HasPassword, Rest1) of
                        {ok, CredProps, _Rest2} ->
                            Base = #{
                                client_id => ClientId,
                                keep_alive_seconds => KeepAliveSeconds,
                                clean_session => CleanSession
                            },
                            {ok, maps:merge(maps:merge(Base, WillProps), CredProps)};
                        {error, _} = Err ->
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end;
decode_connect_var_header(_) ->
    {error, bad_connect}.

decode_will(false, _QoS, _Retain, Rest) ->
    {ok, #{}, Rest};
decode_will(true, QoS, Retain, Rest) ->
    case decode_utf8(Rest) of
        {ok, WillTopic, Rest1} ->
            case decode_utf8(Rest1) of
                {ok, WillMessage, Rest2} ->
                    {ok,
                        #{
                            will_topic => WillTopic,
                            will_message => WillMessage,
                            will_qos => QoS,
                            will_retain => Retain
                        },
                        Rest2};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

decode_credentials(false, false, Rest) ->
    {ok, #{}, Rest};
decode_credentials(true, false, Rest) ->
    case decode_utf8(Rest) of
        {ok, Username, Rest1} ->
            {ok, #{username => Username}, Rest1};
        {error, _} = Err ->
            Err
    end;
decode_credentials(true, true, Rest) ->
    case decode_utf8(Rest) of
        {ok, Username, Rest1} ->
            case decode_utf8(Rest1) of
                {ok, Password, Rest2} ->
                    {ok, #{username => Username, password => Password}, Rest2};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end;
decode_credentials(false, true, _Rest) ->
    {error, password_without_username}.

decode_subscribe_topics(<<>>, Acc) ->
    {ok, lists:reverse(Acc)};
decode_subscribe_topics(Bin, Acc) ->
    case decode_utf8(Bin) of
        {ok, Topic, <<QoS, Rest/binary>>} ->
            decode_subscribe_topics(Rest, [{Topic, QoS} | Acc]);
        _ ->
            {error, bad_subscribe_payload}
    end.

decode_unsubscribe_topics(<<>>, Acc) ->
    {ok, lists:reverse(Acc)};
decode_unsubscribe_topics(Bin, Acc) ->
    case decode_utf8(Bin) of
        {ok, Topic, Rest} ->
            decode_unsubscribe_topics(Rest, [Topic | Acc]);
        _ ->
            {error, bad_unsubscribe_payload}
    end.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

bool_to_bit(true) -> 1;
bool_to_bit(false) -> 0.
