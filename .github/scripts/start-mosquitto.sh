#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Davide Bettio <davide@uninstall.it>

set -euo pipefail

WORKDIR="$(pwd)/mosquitto"
mkdir -p "$WORKDIR/config" "$WORKDIR/certs"

openssl req -x509 -nodes -newkey rsa:2048 -days 1 \
    -keyout "$WORKDIR/certs/server.key" \
    -out "$WORKDIR/certs/server.crt" \
    -subj "/CN=localhost"
chmod 644 "$WORKDIR/certs/server.key" "$WORKDIR/certs/server.crt"

cat > "$WORKDIR/config/mosquitto.conf" <<'EOF'
persistence false
allow_anonymous true

listener 1883
listener 1884

listener 8883
certfile /mosquitto/certs/server.crt
keyfile /mosquitto/certs/server.key
EOF

docker run -d --name mosquitto \
    -p 1883:1883 -p 1884:1884 -p 8883:8883 \
    -v "$WORKDIR/config:/mosquitto/config" \
    -v "$WORKDIR/certs:/mosquitto/certs" \
    eclipse-mosquitto:2.0

for i in $(seq 1 15); do
    if nc -z 127.0.0.1 1883 && nc -z 127.0.0.1 8883; then
        echo "mosquitto ready on 1883/1884/8883"
        exit 0
    fi
    sleep 1
done

echo "mosquitto did not become ready in 15s; container logs:" >&2
docker logs mosquitto >&2 || true
exit 1
