import type { IncomingMessage } from "node:http";
import { sha256 } from "./crypto.js";
import { db, type ClientRow } from "./db.js";

export function extractToken(req: IncomingMessage, url: URL): string | undefined {
  const auth = req.headers.authorization;
  if (auth?.toLowerCase().startsWith("bearer ")) return auth.slice(7).trim();
  const x = req.headers["x-api-key"];
  if (typeof x === "string") return x;
  return url.searchParams.get("key") ?? url.searchParams.get("apiKey") ?? url.searchParams.get("api_key") ?? undefined;
}

export function findClient(req: IncomingMessage, url: URL): ClientRow | undefined {
  const token = extractToken(req, url);
  return token
    ? (db.prepare("SELECT * FROM client_keys WHERE token_hash = ? AND enabled = 1").get(sha256(token)) as unknown as ClientRow | undefined)
    : undefined;
}
