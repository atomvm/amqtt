<!---
  Copyright 2026 Davide Bettio <davide@uninstall.it>

  SPDX-License-Identifier: Apache-2.0
-->

# amqtt

Erlang MQTT library optimized for [AtomVM](https://atomvm.org/) (an Erlang
VM for embedded devices). Also runs unchanged on stock Erlang/OTP, so the
same code can be developed and tested on a workstation and deployed on
microcontrollers.

## amqtt_client

An MQTT **3.1.1** client. Supports plain TCP and TLS, QoS 0 / 1 / 2 for
both publish and subscribe, persistent sessions, retain, last-will,
username/password authentication, and an optional manual-ACK mode for
end-to-end exactly-once delivery at the application boundary.

Quick reference (including the list of messages the client sends to its
owner process) is in [`amqtt_client/README.md`](amqtt_client/README.md).

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
  host: ~c"broker.example.com",
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
  host: ~c"broker.example.com",
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

See [`examples/`](examples/) for runnable demos (publish, TLS publish,
wildcard subscribe, QoS 2 with persistent session + manual ACK, JSON
subscribe) and [`e2e/client_pub_sub_e2e/`](e2e/client_pub_sub_e2e/) for an
end-to-end test you can run on AtomVM or stock OTP.
