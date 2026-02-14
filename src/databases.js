/**
 * OrbitDB database management.
 *
 * Opens/creates three Documents databases that auto-replicate
 * to any connected peer running the same OrbitDB address.
 */

/**
 * Open (or create) the three WeSense OrbitDB databases.
 *
 * @param {import("@orbitdb/core").OrbitDB} orbitdb
 * @returns {Promise<{nodes: object, trust: object, attestations: object}>}
 */
export async function openDatabases(orbitdb) {
  const nodes = await orbitdb.open("wesense.nodes", {
    type: "documents",
    indexBy: "ingester_id",
  });

  const trust = await orbitdb.open("wesense.trust", {
    type: "documents",
    indexBy: "ingester_id",
  });

  const attestations = await orbitdb.open("wesense.attestations", {
    type: "documents",
    indexBy: "manifest_hash",
  });

  return { nodes, trust, attestations };
}
