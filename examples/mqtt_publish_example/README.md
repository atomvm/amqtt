<!---
  Copyright 2026 Davide Bettio <davide@uninstall.it>

  SPDX-License-Identifier: Apache-2.0
-->

# mqtt_publish_example

Connects anonymously to `test.mosquitto.org:1883` and publishes
`"Hello World"` to topic `amqtt/greet` at QoS 1, then disconnects.

## Run on Erlang/OTP

```sh
rebar3 shell --eval "mqtt_publish_example:start(), init:stop(0)."
```

## Run on AtomVM

```sh
rebar3 atomvm packbeam
$ATOMVM/build/src/AtomVM \
  examples/mqtt_publish_example/_build/default/lib/mqtt_publish_example.avm \
  examples/mqtt_publish_example/_build/default/lib/amqtt_client.avm \
  $ATOMVM/build/libs/atomvmlib.avm
```
