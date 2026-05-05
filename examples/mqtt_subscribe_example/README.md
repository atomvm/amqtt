<!---
  Copyright 2026 Davide Bettio <davide@uninstall.it>

  SPDX-License-Identifier: Apache-2.0
-->

# mqtt_subscribe_example

Connects to `test.mosquitto.org:1883` with username `wildcard` (no password),
subscribes to `#`, and prints each incoming message as
`<topic>: <base64(payload)>` until 100 messages have been received or 60
seconds have elapsed -- whichever comes first.

The `wildcard` user is the standard test.mosquitto.org account that allows
a `#` subscription for 20 seconds for the purpose of topic discovery.

## Run on Erlang/OTP

```sh
rebar3 shell --eval "mqtt_subscribe_example:start(), init:stop(0)."
```

## Run on AtomVM

```sh
rebar3 atomvm packbeam
$ATOMVM/build/src/AtomVM \
  examples/mqtt_subscribe_example/_build/default/lib/mqtt_subscribe_example.avm \
  examples/mqtt_subscribe_example/_build/default/lib/amqtt_client.avm \
  $ATOMVM/build/libs/atomvmlib.avm
```
