<!---
  Copyright 2026 Davide Bettio <davide@uninstall.it>

  SPDX-License-Identifier: Apache-2.0
-->

# mqtt_subscribe_json_example

Connects to `test.mosquitto.org:1884` (the authenticated listener) as
`ro` / `readonly`, subscribes to `#`, and runs an infinite loop printing
each message that successfully decodes as JSON. Non-JSON payloads are
silently ignored.

Uses `json:decode/1`: present on Erlang/OTP 27+ and on AtomVM
(`AtomVM/libs/estdlib/src/json.erl`, same API).

## Run on Erlang/OTP

```sh
rebar3 shell --eval "mqtt_subscribe_json_example:start(), init:stop(0)."
```

## Run on AtomVM

```sh
rebar3 atomvm packbeam
$ATOMVM/build/src/AtomVM \
  examples/mqtt_subscribe_json_example/_build/default/lib/mqtt_subscribe_json_example.avm \
  examples/mqtt_subscribe_json_example/_build/default/lib/amqtt_client.avm \
  $ATOMVM/build/libs/atomvmlib.avm
```
