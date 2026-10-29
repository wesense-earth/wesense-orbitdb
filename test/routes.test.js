import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import express from "express";

import { createNodesRouter } from "../src/routes/nodes.js";
import { createTrustRouter } from "../src/routes/trust.js";
import { createStoresRouter } from "../src/routes/stores.js";
import { createAttestationsRouter } from "../src/routes/attestations.js";

// In-memory stand-in with the same shapes as an OrbitDB Documents database:
// get(key) -> { key, value, hash } | undefined, all() -> [{ key, value, hash }].
function fakeDocsDb() {
  const docs = new Map();
  return {
    async put(doc) {
      docs.set(doc._id, doc);
    },
    async get(key) {
      return docs.has(key) ? { key, value: docs.get(key), hash: "h" } : undefined;
    },
    async del(key) {
      docs.delete(key);
    },
    async all() {
      return [...docs].map(([key, value]) => ({ key, value, hash: "h" }));
    },
  };
}

let server;
let base;

before(async () => {
  const app = express();
  app.use(express.json({ limit: "100kb" }));
  app.use("/nodes", createNodesRouter(fakeDocsDb()));
  app.use("/trust", createTrustRouter(fakeDocsDb()));
  app.use("/stores", createStoresRouter(fakeDocsDb()));
  app.use("/attestations", createAttestationsRouter(fakeDocsDb()));
  await new Promise((resolve) => {
    server = app.listen(0, "127.0.0.1", resolve);
  });
  base = `http://127.0.0.1:${server.address().port}`;
});

after(() => server.close());

async function call(method, path, body) {
  const res = await fetch(base + path, {
    method,
    headers: body === undefined ? {} : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, json: await res.json() };
}

test("trust: PUT then GET returns the entry", async () => {
  assert.equal((await call("PUT", "/trust/wsi_a", { public_key: "k1", key_version: 1 })).status, 200);
  const got = await call("GET", "/trust/wsi_a");
  assert.equal(got.status, 200);
  assert.equal(got.json.versions["1"].public_key, "k1");
});

test("trust: key rotation keeps the earlier version", async () => {
  await call("PUT", "/trust/wsi_rot", { public_key: "k1", key_version: 1 });
  await call("PUT", "/trust/wsi_rot", { public_key: "k2", key_version: 2 });
  const got = await call("GET", "/trust/wsi_rot");
  assert.deepEqual(Object.keys(got.json.versions).sort(), ["1", "2"]);
  assert.equal(got.json.versions["1"].public_key, "k1");
  assert.equal(got.json.versions["2"].public_key, "k2");
  const list = await call("GET", "/trust");
  assert.deepEqual(Object.keys(list.json.keys.wsi_rot).sort(), ["1", "2"]);
});

test("trust: DELETE revokes every version", async () => {
  await call("PUT", "/trust/wsi_rev", { public_key: "k1", key_version: 1 });
  await call("PUT", "/trust/wsi_rev", { public_key: "k2", key_version: 2 });
  assert.equal((await call("DELETE", "/trust/wsi_rev")).status, 200);
  const got = await call("GET", "/trust/wsi_rev");
  assert.equal(got.json.versions["1"].status, "revoked");
  assert.equal(got.json.versions["2"].status, "revoked");
});

test("trust: unknown id is 404 for GET and DELETE", async () => {
  assert.equal((await call("GET", "/trust/nope")).status, 404);
  assert.equal((await call("DELETE", "/trust/nope")).status, 404);
});

test("nodes: PUT, GET by id, DELETE", async () => {
  await call("PUT", "/nodes/n1", { node_name: "one" });
  const got = await call("GET", "/nodes/n1");
  assert.equal(got.status, 200);
  assert.equal(got.json.node_name, "one");
  assert.equal((await call("DELETE", "/nodes/n1")).status, 200);
  assert.equal((await call("GET", "/nodes/n1")).status, 404);
});

test("stores: PUT and GET by id", async () => {
  await call("PUT", "/stores/s1", { store_scope: ["nz/*"] });
  const got = await call("GET", "/stores/s1");
  assert.equal(got.status, 200);
  assert.deepEqual(got.json.store_scope, ["nz/*"]);
  assert.equal((await call("GET", "/stores/none")).status, 404);
});

test("attestations: second attester is added, not replacing the first", async () => {
  await call("PUT", "/attestations/m1", { ingester_id: "a", signature: "s", path: "p" });
  const second = await call("PUT", "/attestations/m1", { ingester_id: "b" });
  assert.equal(second.json.attestation_count, 2);
  const got = await call("GET", "/attestations/m1");
  assert.equal(got.json.path, "p");
  const consensus = await call("GET", "/attestations/m1/consensus");
  assert.equal(consensus.status, 200);
  assert.equal(consensus.json.consensus_reached, true);
  assert.equal((await call("GET", "/attestations/none/consensus")).status, 404);
});

test("PUT without a JSON object body is a 400", async () => {
  for (const path of ["/nodes/x", "/trust/x", "/stores/x", "/attestations/x"]) {
    const res = await fetch(base + path, { method: "PUT" });
    assert.equal(res.status, 400, path);
  }
});
