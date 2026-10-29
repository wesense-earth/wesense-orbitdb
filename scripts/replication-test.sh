#!/bin/bash
# Two-node replication test.
#
# Starts two OrbitDB containers on a private docker network (B bootstraps to A), writes a node
# record on each, and requires it to appear on the other. Run it with two images to test that
# stations on different versions still replicate with each other, which is what a rolling
# upgrade looks like:
#
#   scripts/replication-test.sh <image-for-A> [image-for-B]
#
# Exit status is 0 only if both directions replicate. Needs docker, curl and python3.
set -u

IMG_A=${1:?usage: replication-test.sh <image-A> [image-B]}
IMG_B=${2:-$IMG_A}
SUFFIX=$$
NET=od-repl-net-$SUFFIX
A=od-repl-a-$SUFFIX
B=od-repl-b-$SUFFIX
PORT_A=${PORT_A:-5301}
PORT_B=${PORT_B:-5302}
SETTLE_SECONDS=${SETTLE_SECONDS:-20}
REPLICATION_TIMEOUT=${REPLICATION_TIMEOUT:-120}

cleanup() {
  docker rm -f "$A" "$B" >/dev/null 2>&1
  docker network rm "$NET" >/dev/null 2>&1
}
trap cleanup EXIT

wait_http() { # port
  for _ in $(seq 1 60); do
    curl -s -m 2 "localhost:$1/health" >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo "node on port $1 did not become healthy"; return 1
}

wait_for_record() { # port id label
  local start=$SECONDS
  while [ $((SECONDS - start)) -lt "$REPLICATION_TIMEOUT" ]; do
    if curl -s -m 5 "localhost:$1/nodes" | grep -q "\"$2\""; then
      echo "$3: replicated in $((SECONDS - start))s"; return 0
    fi
    sleep 2
  done
  echo "$3: NOT replicated within ${REPLICATION_TIMEOUT}s"; return 1
}

docker network create "$NET" >/dev/null
docker run -d --name "$A" --network "$NET" -p "$PORT_A:5200" \
  -e ORBITDB_BOOTSTRAP_PEERS= -e ANNOUNCE_ADDRESS="$A" "$IMG_A" >/dev/null
wait_http "$PORT_A" || { docker logs "$A" 2>&1 | tail -20; exit 1; }

PEER_ID=$(curl -s -m 5 "localhost:$PORT_A/health" | python3 -c "import sys,json; print(json.load(sys.stdin)['libp2p_peer_id'])")
docker run -d --name "$B" --network "$NET" -p "$PORT_B:5200" \
  -e ORBITDB_BOOTSTRAP_PEERS="/dns4/$A/tcp/4002/p2p/$PEER_ID" -e ANNOUNCE_ADDRESS="$B" "$IMG_B" >/dev/null
wait_http "$PORT_B" || { docker logs "$B" 2>&1 | tail -20; exit 1; }
sleep "$SETTLE_SECONDS"

status=0
echo "A=$IMG_A  B=$IMG_B"
curl -s -m 20 -X PUT -H 'content-type: application/json' -d '{"node_name":"written-on-A"}' \
  "localhost:$PORT_A/nodes/repl-a" -o /dev/null -w "PUT on A: %{http_code} in %{time_total}s\n"
wait_for_record "$PORT_B" repl-a "A -> B" || status=1
curl -s -m 20 -X PUT -H 'content-type: application/json' -d '{"node_name":"written-on-B"}' \
  "localhost:$PORT_B/nodes/repl-b" -o /dev/null -w "PUT on B: %{http_code} in %{time_total}s\n"
wait_for_record "$PORT_A" repl-b "B -> A" || status=1

if [ $status -ne 0 ]; then
  for c in "$A" "$B"; do echo "--- $c"; docker logs "$c" 2>&1 | grep -iE "error|unhandled|TypeError" | sort | uniq -c | sort -rn | head -8; done
fi
[ $status -eq 0 ] && echo "RESULT: PASS" || echo "RESULT: FAIL"
exit $status
