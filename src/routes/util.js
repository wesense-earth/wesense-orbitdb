/**
 * Shared helpers for the route modules.
 */

/**
 * Express middleware for routes that take a JSON object body.
 * Express 5 leaves req.body undefined when no body is sent (or it is not JSON),
 * so reject that here instead of letting the handler throw a 500.
 */
export function requireJsonBody(req, res, next) {
  const body = req.body;
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return res.status(400).json({ error: "JSON object body required" });
  }
  next();
}

/**
 * Look up one document by key.
 * OrbitDB Documents `get(key)` resolves to a single `{ key, value, hash }`
 * entry, or undefined when the key does not exist.
 * @returns {Promise<object|undefined>} the stored document value
 */
export async function getValue(db, key) {
  const entry = await db.get(key);
  return entry ? entry.value : undefined;
}
