#!/bin/bash
#
# Monitor both test containers for disconnect/reset events.
# Runs until Ctrl+C. Shows timestamped events from both containers
# interleaved so you can see the sequence.
#
# Run this in a terminal alongside capture.sh and docker compose up -d.

echo "Monitoring test-orbitdb-a and test-orbitdb-b for reset events..."
echo "Press Ctrl+C to stop."
echo ""

{
  docker logs -f --timestamps test-orbitdb-a 2>&1 | sed 's/^/[A] /' &
  docker logs -f --timestamps test-orbitdb-b 2>&1 | sed 's/^/[B] /' &
} | grep --line-buffered -E 'Peer (dis)?connected|stream has been reset|StreamResetError|onRemoteReset|zombie|Gossipsub streams'
