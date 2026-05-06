<!---
  Copyright 2026 Davide Bettio <davide@uninstall.it>

  SPDX-License-Identifier: Apache-2.0
-->

# amqtt_client

An MQTT **3.1.1** client. Supports plain TCP and TLS, QoS 0 / 1 / 2 for
both publish and subscribe, persistent sessions, retain, last-will,
username/password authentication, and an optional manual-ACK mode for
end-to-end exactly-once delivery at the application boundary.

Optimized for [AtomVM](https://atomvm.org/) (an Erlang VM for embedded
devices) but runs unchanged on stock Erlang/OTP.

## Adding to your project

### rebar3

```erlang
{deps, [
    {amqtt_client,
        {git, "https://github.com/atomvm/amqtt.git",
            {branch, "main"}}}
]}.
```

### Elixir (`mix.exs`)

```elixir
defp deps do
  [
    {:amqtt_client,
     git: "https://github.com/atomvm/amqtt.git",
     branch: "main",
     sparse: "amqtt_client"}
  ]
end
```

The library lives under the `amqtt_client/` subdirectory of the repo;
`sparse:` (rebar3 git deps automatically use the matching app name) keeps
mix from building the e2e / examples projects.

## Quick usage

### Connect (plain TCP)

Erlang:

```erlang
{ok, Client} = amqtt_client:connect(#{
    host => "broker.example.com",
    port => 1883,
    client_id => <<"my_client">>,
    keep_alive_seconds => 60
}),
receive
    {mqtt, Client, connack, #{return_code := 0}} -> ok
end.
```

Elixir:

```elixir
{:ok, client} = :amqtt_client.connect(%{
  host: "broker.example.com",
  port: 1883,
  client_id: "my_client",
  keep_alive_seconds: 60
})

receive do
  {:mqtt, ^client, :connack, %{return_code: 0}} -> :ok
end
```

### Connect (TLS)

Erlang:

```erlang
{ok, Client} = amqtt_client:connect(#{
    host => "broker.example.com",
    port => 8883,
    transport => ssl,
    ssl_opts => [{verify, verify_none}],
    client_id => <<"my_client">>,
    keep_alive_seconds => 60
}).
```

Elixir:

```elixir
{:ok, client} = :amqtt_client.connect(%{
  host: "broker.example.com",
  port: 8883,
  transport: :ssl,
  ssl_opts: [{:verify, :verify_none}],
  client_id: "my_client",
  keep_alive_seconds: 60
})
```

### Subscribe

Erlang:

```erlang
{ok, _GrantedQoSs} = amqtt_client:subscribe(Client, [{<<"sensor/#">>, 1}]),
receive
    {mqtt, Client, publish, #{topic := Topic, message := Payload}} ->
        io:format("~s: ~p~n", [Topic, Payload])
end.
```

Elixir:

```elixir
{:ok, _granted_qoss} = :amqtt_client.subscribe(client, [{"sensor/#", 1}])

receive do
  {:mqtt, ^client, :publish, %{topic: topic, message: payload}} ->
    IO.puts("#{topic}: #{inspect(payload)}")
end
```

### Publish

Erlang:

```erlang
%% QoS 0 -- fire and forget
ok = amqtt_client:publish(Client, <<"sensor/temp">>, <<"22.5">>, 0).

%% QoS 1 -- blocks until PUBACK
{ok, _PacketId} = amqtt_client:publish(Client, <<"sensor/temp">>, <<"22.5">>, 1).

%% With retain
{ok, _} = amqtt_client:publish(Client, <<"status">>, <<"online">>, 1,
                               #{retain => true}).
```

Elixir:

```elixir
# QoS 0 -- fire and forget
:ok = :amqtt_client.publish(client, "sensor/temp", "22.5", 0)

# QoS 1 -- blocks until PUBACK
{:ok, _packet_id} = :amqtt_client.publish(client, "sensor/temp", "22.5", 1)

# With retain
{:ok, _} = :amqtt_client.publish(client, "status", "online", 1, %{retain: true})
```

> **Note on dispatch.** `publish/4,5` dispatches differently by QoS:
> at QoS 0 it is a `gen_server:cast/2` (fire-and-forget, returns `ok`
> as soon as the cast is queued -- no backpressure, no notification of
> transport failure, a dead client process is silently dropped). At QoS
> 1 / 2 it is a `gen_server:call/3` (synchronous, blocks until PUBACK
> or PUBCOMP, returns `{ok, PacketId}` or `{error, _}`). All the usual
> `gen_server:call` / `gen_server:cast` caveats apply -- in particular
> a dead client raises `noproc` for QoS 1/2 callers. The call uses an
> `infinity` client-side timeout so only the server-side
> `?REQUEST_TIMEOUT` (30 s) fires; the caller gets back
> `{error, timeout}` rather than an exit.

## Owner messages

The owner process (caller of `connect/1` unless overridden via the `owner`
connect option) receives these messages from the client.

Erlang:

```erlang
%% Broker replied to CONNECT.
{mqtt, Client, connack, #{
    session_present := boolean(),
    return_code     := 0..5
}}

%% Inbound PUBLISH at any QoS. `packet_id' is present only for QoS 1/2.
{mqtt, Client, publish, #{
    topic     := binary(),
    message   := binary(),
    qos       := 0 | 1 | 2,
    dup       := boolean(),
    retain    := boolean(),
    packet_id => 1..65535
}}

%% Broker closed the connection.
{mqtt, Client, disconnected, #{}}

%% Transport error, protocol error, or other fatal condition. The
%% client process stops immediately afterwards.
{mqtt, Client, error, #{reason := term()}}
```

Elixir:

```elixir
# Broker replied to CONNECT.
{:mqtt, client, :connack, %{
  session_present: boolean(),
  return_code: 0..5
}}

# Inbound PUBLISH at any QoS. `packet_id` is present only for QoS 1/2.
{:mqtt, client, :publish, %{
  topic: binary(),
  message: binary(),
  qos: 0 | 1 | 2,
  dup: boolean(),
  retain: boolean(),
  packet_id: 1..65535
}}

# Broker closed the connection.
{:mqtt, client, :disconnected, %{}}

# Transport error, protocol error, or other fatal condition. The
# client process stops immediately afterwards.
{:mqtt, client, :error, %{reason: term()}}
```

By default `PUBACK` / `PUBREC` / `PUBCOMP` are sent automatically by the
client. With `auto_ack => false` at connect time the owner must call
`amqtt_client:ack(Client, PacketId)` after handling the inbound `publish`
event; the QoS 2 `PUBREL` → `PUBCOMP` second leg stays automatic.

## Examples

See [`../examples/`](../examples/) for runnable demos (publish, TLS
publish, wildcard subscribe, QoS 2 with persistent session + manual ACK,
JSON subscribe) and [`../e2e/client_pub_sub_e2e/`](../e2e/client_pub_sub_e2e/)
for an end-to-end test you can run on AtomVM or stock OTP.
