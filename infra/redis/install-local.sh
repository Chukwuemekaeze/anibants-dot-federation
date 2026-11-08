#!/usr/bin/env bash
# Installs and starts this project's two Redis instances on the local machine.
# Run from the repo root: sudo bash infra/redis/install-local.sh
set -euo pipefail

cd "$(dirname "$0")"

for name in anibants-jobs anibants-cache; do
	# Data folder owned by the redis user, readable only by it.
	install -d -o redis -g redis -m 750 "/var/lib/redis/$name"
	# Config file where the redis-server@ service template expects it.
	install -o redis -g redis -m 640 "redis-$name.conf" "/etc/redis/redis-$name.conf"
	# Start now, and on every boot.
	systemctl enable --now "redis-server@$name"
	# Restart so a re-run picks up config changes.
	systemctl restart "redis-server@$name"
done

echo
systemctl --no-pager --lines=0 status redis-server@anibants-jobs redis-server@anibants-cache
