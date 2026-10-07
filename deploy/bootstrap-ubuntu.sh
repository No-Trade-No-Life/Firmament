#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install --yes ca-certificates caddy curl
id firmament >/dev/null 2>&1 || useradd --system --home-dir /var/lib/firmament --shell /usr/sbin/nologin firmament
install -d -m 0755 /opt/firmament/releases
install -d -o firmament -g firmament -m 0700 /var/lib/firmament

install -m 0644 /dev/stdin /etc/systemd/system/firmament.service <<'UNIT'
[Unit]
Description=Firmament research data warehouse
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=firmament
Group=firmament
WorkingDirectory=/var/lib/firmament
ExecStart=/opt/firmament/current/firmament
Restart=on-failure
RestartSec=5s
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/firmament

[Install]
WantedBy=multi-user.target
UNIT

install -m 0644 /dev/stdin /etc/caddy/Caddyfile <<'CADDY'
firma.ntnl.io {
    reverse_proxy 127.0.0.1:8080
}
CADDY

systemctl daemon-reload
systemctl enable firmament.service caddy.service
systemctl restart caddy.service
