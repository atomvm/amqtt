<!---
  Copyright 2026 Davide Bettio <davide@uninstall.it>

  SPDX-License-Identifier: Apache-2.0
-->

# client_pub_sub_e2e

End-to-end smoke test for [amqtt_client](../../amqtt_client/) running on real
[AtomVM](https://www.atomvm.net/). It is **not** part of the library — this is
a separate rebar3 project that depends on `amqtt_client` so the library is
exercised exactly as a downstream consumer would use it.

## What it does

Spins up two `mqtt_client` instances against the public broker
`test.mosquitto.org`:

- **Client A** — plain TCP, port 1883.
- **Client B** — TLS, port 8883, `ssl_opts => [{verify, verify_none}]` (the
  only verify mode AtomVM's `ssl` supports today).

They share a randomly-generated topic root and run **3 iterations at each of
QoS 0, 1, and 2** (= 9 round-trips). For each iteration A publishes a 32-byte
random payload to `<root>/payloads`; B receives it, computes the SHA-256 hex
digest, publishes that to `<root>/replies`; A verifies the digest matches.

Outputs `ok qos=… iter=…` per iteration and `PASS (9/9)` at the end (or
`FAIL: …` on any timeout / mismatch).

## Build

The project pulls `amqtt_client` from a sibling directory via
`_checkouts/`:

```sh
mkdir -p _checkouts
ln -s ../../../amqtt_client _checkouts/amqtt_client
rebar3 atomvm packbeam
```

This produces:
- `_build/default/lib/client_pub_sub_e2e.avm` (the entry app)
- `_build/default/checkouts/amqtt_client.avm` (the library)

## Run

The AtomVM binary takes the entry .avm followed by any number of library
.avms. Using the pre-built AtomVM at `/workspace/AtomVM/`:

```sh
cd /workspace/AtomVM
./AtomVM \
  /workspace/e2e/client_pub_sub_e2e/_build/default/lib/client_pub_sub_e2e.avm \
  /workspace/e2e/client_pub_sub_e2e/_build/default/checkouts/amqtt_client.avm \
  atomvmlib.avm
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

The test connects to `test.mosquitto.org:1883` and `test.mosquitto.org:8883`,
matching the broker AtomVM's own example uses
(`/workspace/AtomVM/examples/erlang/mqtt_client.erl`). Many networks
(corporate firewalls, restricted clouds, residential ISPs) block outbound
1883 / 8883, in which case the connect calls will time out. The test wraps
the connects in a 20 s deadline and prints `FAIL: {connect_a_timeout, …}` /
`FAIL: {connect_b_timeout, …}` so a stuck broker is reported rather than
hung indefinitely.

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
