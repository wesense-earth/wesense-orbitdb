/**
 * OrbitDB database management.
 *
 * Opens/creates Documents databases that auto-replicate to any connected
 * peer running the same OrbitDB address. Only small, station-bounded
 * state belongs here — data that grows with station count, not data volume.
 *
 * Uses IPFSAccessController with open writes so the database address
 * is deterministic (derived from name + type only, not the creator's
 * identity). This means every station that opens "wesense.nodes"
 * gets the same database address — automatic replication, zero config.
 *
 * Write security is handled at the application layer via Ed25519
 * ingester signing keys, not at the OrbitDB access control layer.
 *
 * Note: wesense.attestations was removed — attestations grew unbounded
 * (4,888+ entries), causing OrbitDB sync timeouts and memory leaks.
 * Archive discovery now uses peer-to-peer path index exchange via the
 * archive replicator. See IrohPlan.md Phase 3.
 */

import { IPFSAccessController } from "@orbitdb/core";

/**
 * Open (or create) the WeSense OrbitDB databases.
 *
 * @param {import("@orbitdb/core").OrbitDB} orbitdb
 * @returns {Promise<{nodes: object, trust: object, stores: object}>}
 */
export async function openDatabases(orbitdb) {
  const opts = {
    type: "documents",
    AccessController: IPFSAccessController({ write: ["*"] }),
  };

  const nodes = await orbitdb.open("wesense.nodes", opts);
  const trust = await orbitdb.open("wesense.trust", opts);
  const stores = await orbitdb.open("wesense.stores", opts);

  return { nodes, trust, stores };
}
