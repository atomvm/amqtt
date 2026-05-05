<!---
  Copyright 2026 Davide Bettio <davide@uninstall.it>

  SPDX-License-Identifier: Apache-2.0
-->

# mqtt_qos2_subscribe

Demonstrates a QoS 2 subscriber with a **persistent session** and
**manual ACK**:

- Connects with `clean_session => false` and a stable `client_id` so the
  broker recognises the session across reconnects. On CONNACK, prints
  `session_present: true|false`.
- Subscribes to `amqtt/amqtt_qos2_demo/#` at QoS 2.
- Receives up to 10 messages (or 60 s deadline) and **manually
  acknowledges** each one via `amqtt_client:ack/2`. The broker's PUBREL ->
  PUBCOMP second leg is auto-handled by the gen_server.

To exercise it, publish from another terminal while this example is
running:

```sh
mosquitto_pub -h test.mosquitto.org -t amqtt/amqtt_qos2_demo/test -m "hello qos2" -q 2
```

## Run on Erlang/OTP

```sh
rebar3 shell --eval "mqtt_qos2_subscribe:start(), init:stop(0)."
```

## Run on AtomVM

```sh
rebar3 atomvm packbeam
$ATOMVM/build/src/AtomVM \
  examples/mqtt_qos2_subscribe/_build/default/lib/mqtt_qos2_subscribe.avm \
  examples/mqtt_qos2_subscribe/_build/default/lib/amqtt_client.avm \
  $ATOMVM/build/libs/atomvmlib.avm
```
