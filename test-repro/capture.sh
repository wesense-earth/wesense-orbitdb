#!/bin/bash
#
# Stream-reset packet capture
#
# Run this BEFORE or alongside `docker compose up -d` in the test-repro dir.
# Captures all TCP traffic between the two test orbitdb containers.
# Press Ctrl+C to stop capture when you've observed enough reset cycles.
#
# Output: /tmp/orbitdb-capture.pcap
#
# To analyze:
#   - Quick: tcpdump -r /tmp/orbitdb-capture.pcap -nn | less
#   - Detailed: tshark -r /tmp/orbitdb-capture.pcap -V | less
#   - GUI: copy .pcap to your Mac, open in Wireshark
#
# What to look for:
#   1. Find a TCP RST or FIN packet — that's a connection termination
#   2. Look at the timestamps of packets immediately preceding it:
#      - If there's an idle gap (no packets for N seconds) before RST
#        → some timeout fired on one side
#      - If there's active traffic right up until RST
#        → protocol-level decision to close (gossipsub prune, etc.)
#   3. Check which side sends the RST/FIN first:
#      - Source port 14002 (container A) → A closed it
#      - Source port 14003 (container B) → B closed it
#   4. Correlate the RST timestamp with docker logs -f output to
#      match the wire event to the application-level log event.

set -e

echo "Starting capture on loopback for ports 14002 and 14003..."
echo "Output: /tmp/orbitdb-capture.pcap"
echo "Press Ctrl+C to stop."
echo ""

sudo tcpdump -i lo -w /tmp/orbitdb-capture.pcap \
  'tcp port 14002 or tcp port 14003' \
  -s 0
