<!---
  Copyright 2026 Davide Bettio <davide@uninstall.it>

  SPDX-License-Identifier: Apache-2.0
-->

# mqtt_tls_publish_example

Connects anonymously to `test.mosquitto.org:8883` over TLS and publishes
`"Hello World over TLS"` to topic `amqtt/greet` at QoS 1, then disconnects.

The TLS configuration uses `{verify, verify_none}` only: the only
verification mode AtomVM's `ssl` module supports today. SNI is
auto-populated from the hostname.

## Run on Erlang/OTP

```sh
rebar3 shell --eval "mqtt_tls_publish_example:start(), init:stop(0)."
```

## Run on AtomVM

```sh
rebar3 atomvm packbeam
$ATOMVM/build/src/AtomVM \
  examples/mqtt_tls_publish_example/_build/default/lib/mqtt_tls_publish_example.avm \
  examples/mqtt_tls_publish_example/_build/default/lib/amqtt_client.avm \
  $ATOMVM/build/libs/atomvmlib.avm
```
