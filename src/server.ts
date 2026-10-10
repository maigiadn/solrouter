import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { handleAdmin } from "./admin.js";
import { config } from "./config.js";
import { sha256 } from "./crypto.js";
import { db, type ClientRow } from "./db.js";
import { handleProxy } from "./proxy.js";

const page = readFileSync(fileURLToPath(new URL("../public/index.html", import.meta.url)));

function extractToken(req: import("node:http").IncomingMessage, url: URL): string | undefined {
  const auth = req.headers.authorization;
  if (auth?.toLowerCase().startsWith("bearer ")) return auth.slice(7).trim();
  const x = req.headers["x-api-key"];
  if (typeof x === "string") return x;
  return url.searchParams.get("key") ?? url.searchParams.get("apiKey") ?? url.searchParams.get("api_key") ?? undefined;
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url ?? "/", "http://localhost");
  try {
    if (url.pathname === "/healthz") { res.writeHead(200).end("ok"); return; }
    if (url.pathname === "/admin" || url.pathname === "/admin/") {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" }).end(page);
      return;
    }
    if (url.pathname.startsWith("/admin/api/")) return await handleAdmin(req, res, url);

    const token = extractToken(req, url);
    const client = token
      ? (db.prepare("SELECT * FROM client_keys WHERE token_hash = ? AND enabled = 1").get(sha256(token)) as unknown as ClientRow | undefined)
      : undefined;
    if (!client) {
      res.writeHead(401, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "invalid_router_key", message: "Thiếu hoặc sai router API key." }));
      return;
    }
    await handleProxy(req, res, url, client.id);
  } catch (e) {
    console.error(e);
    if (!res.headersSent) res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "internal_error" }));
  }
});

server.listen(config.port, () => console.log(`solrouter lắng nghe :${config.port} → ${config.upstreamUrl}`));
