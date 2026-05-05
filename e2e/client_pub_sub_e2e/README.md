<!---
  Copyright 2026 Davide Bettio <davide@uninstall.it>

  SPDX-License-Identifier: Apache-2.0
-->

# client_pub_sub_e2e

End-to-end smoke test for [amqtt_client](../../amqtt_client/).

## What it does

Spins up two `amqtt_client` instances against the public broker
`test.mosquitto.org`:

- **Client A**: plain TCP, port 1883.
- **Client B**: TLS, port 8883, `ssl_opts => [{verify, verify_none}]` (the
  only verify mode AtomVM's `ssl` supports today).

They share a randomly-generated topic root and run **3 iterations at each of
QoS 0, 1, and 2** (= 9 round-trips). For each iteration A publishes a 32-byte
random payload to `<root>/payloads`; B receives it, computes the SHA-256 hex
digest, publishes that to `<root>/replies`; A verifies the digest matches.

Outputs `ok qos=… iter=…` per iteration and `PASS (9/9)` at the end (or
`FAIL: …` on any timeout / mismatch).

## Build

`amqtt_client` is wired in as a relative-path dependency
(`{path, "../../amqtt_client"}` via the `rebar3_path_deps` plugin), so a
plain `rebar3 atomvm packbeam` is enough:

```sh
rebar3 atomvm packbeam
```

This produces:
- `_build/default/lib/client_pub_sub_e2e.avm` (the entry app)
- `_build/default/lib/amqtt_client.avm` (the library)

## Run

The AtomVM binary takes the entry .avm followed by any number of library
.avms. With `$ATOMVM` pointing at your AtomVM checkout root:

```sh
$ATOMVM/build/src/AtomVM \
  e2e/client_pub_sub_e2e/_build/default/lib/client_pub_sub_e2e.avm \
  e2e/client_pub_sub_e2e/_build/default/lib/amqtt_client.avm \
  $ATOMVM/build/libs/atomvmlib.avm
```

Expected (~5–15 s with a healthy network):

```
topic root: amqtt_e2e/<random hex>
client A (TCP) connected
client B (TLS) connected
subscriptions installed
ok qos=0 iter=1
ok qos=0 iter=2
ok qos=0 iter=3
ok qos=1 iter=1
…
ok qos=2 iter=3
PASS (9/9)
```

## Network requirements

### Verifying broker reachability

```sh
nc -vz test.mosquitto.org 1883   # expect "succeeded"
nc -vz test.mosquitto.org 8883   # expect "succeeded"
```

If either fails, switch to a local broker.

### Local broker (Mosquitto)

```sh
# Plain TCP (no extra config)
mosquitto -p 1883
# In another terminal, with TLS using a self-signed cert:
mosquitto -c mosquitto-tls.conf
```

Then edit `src/client_pub_sub_e2e.erl`:

```erlang
-define(BROKER, "127.0.0.1").
```

and rebuild. The `verify_none` ssl_opt the test already uses lets you connect
to a self-signed broker without setting up CA chains.
