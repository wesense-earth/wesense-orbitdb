/**
 * OrbitDB database management.
 *
 * Opens/creates three Documents databases that auto-replicate
 * to any connected peer running the same OrbitDB address.
 *
 * Uses IPFSAccessController with open writes so the database address
 * is deterministic (derived from name + type only, not the creator's
 * identity). This means every station that opens "wesense.nodes"
 * gets the same database address — automatic replication, zero config.
 *
 * Write security is handled at the application layer via Ed25519
 * ingester signing keys, not at the OrbitDB access control layer.
 */

import { IPFSAccessController } from "@orbitdb/core";

/**
 * Open (or create) the three WeSense OrbitDB databases.
 *
 * @param {import("@orbitdb/core").OrbitDB} orbitdb
 * @returns {Promise<{nodes: object, trust: object, attestations: object}>}
 */
export async function openDatabases(orbitdb) {
  const opts = {
    type: "documents",
    AccessController: IPFSAccessController({ write: ["*"] }),
  };

  const nodes = await orbitdb.open("wesense.nodes", opts);
  const trust = await orbitdb.open("wesense.trust", opts);
  const attestations = await orbitdb.open("wesense.attestations", opts);

  return { nodes, trust, attestations };
}
