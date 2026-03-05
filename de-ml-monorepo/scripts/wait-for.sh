#!/usr/bin/env bash
# wait-for.sh: wait until a TCP host:port is reachable, then exec cmd
# Usage: ./wait-for.sh host:port [-- command args]

set -e

HOST="${1%%:*}"
PORT="${1##*:}"
shift

echo "Waiting for $HOST:$PORT..."
until nc -z "$HOST" "$PORT" 2>/dev/null; do
    sleep 1
done
echo "$HOST:$PORT is up"

# Execute optional command
if [ "$1" = "--" ]; then
    shift
    exec "$@"
fi
