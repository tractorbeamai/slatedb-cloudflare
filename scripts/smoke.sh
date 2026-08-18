#!/usr/bin/env bash
set -euo pipefail

base_url="${BASE_URL:-http://localhost:8787}"
token="${PROBE_TOKEN:?set PROBE_TOKEN to the deployment bearer token}"
database="probe-$(date +%s)-$$"
auth="Authorization: Bearer ${token}"

request() {
  curl --fail --silent --show-error --max-time 30 -H "$auth" "$@"
}

put() {
  request -H 'Content-Type: application/json' \
    -d "{\"key\":\"$1\",\"value\":\"$2\"}" \
    "${base_url}/v1/db/${database}/put"
}

put alpha one | grep -q '"ok":true'
request "${base_url}/v1/db/${database}/get?key=alpha" | grep -q '"value":"one"'
put alpha two | grep -q '"ok":true'
put prefix:a A | grep -q '"ok":true'
put prefix:b B | grep -q '"ok":true'
request "${base_url}/v1/db/${database}/scan?prefix=prefix%3A" | grep -q 'prefix:b'
request "${base_url}/v1/db/${database}/get?key=missing" | grep -q '"value":null'

for number in $(seq 1 12); do
  put "concurrent:${number}" "value:${number}" &
done
wait

for number in $(seq 1 12); do
  request "${base_url}/v1/db/${database}/get?key=concurrent:${number}" \
    | grep -q "\"value\":\"value:${number}\""
done

request -X POST "${base_url}/v1/db/${database}/admin/reopen" | grep -q '"ok":true'
request "${base_url}/v1/db/${database}/get?key=alpha" | grep -q '"value":"two"'

request -H 'Content-Type: application/json' -d '{"key":"alpha"}' \
  "${base_url}/v1/db/${database}/delete" | grep -q '"ok":true'
request "${base_url}/v1/db/${database}/get?key=alpha" | grep -q '"value":null'

printf '{"ok":true,"database":"%s"}\n' "$database"
