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
  const nodes = await orbitdb.open("wesense.nodes", { type: "documents" });
  const trust = await orbitdb.open("wesense.trust", { type: "documents" });
  const attestations = await orbitdb.open("wesense.attestations", { type: "documents" });

  return { nodes, trust, attestations };
}
