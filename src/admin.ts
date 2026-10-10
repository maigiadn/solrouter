import type { IncomingMessage, ServerResponse } from "node:http";
import { config } from "./config.js";
import { encrypt, maskKey, newClientToken, safeEqual, sha256, signSession, verifySession } from "./crypto.js";
import { db, type UpstreamRow } from "./db.js";
import { readBody } from "./proxy.js";
import * as pool from "./pool.js";

const SESSION_TTL = 7 * 24 * 3600_000;
const COOKIE = "sr_session";
const failures = new Map<string, { n: number; until: number }>();

function json(res: ServerResponse, status: number, body: unknown, extra: Record<string, string> = {}) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", ...extra });
  res.end(JSON.stringify(body));
}

function cookieOf(req: IncomingMessage, name: string): string | undefined {
  for (const part of (req.headers.cookie ?? "").split(";")) {
    const [k, ...v] = part.trim().split("=");
    if (k === name) return v.join("=");
  }
}

export const isAdmin = (req: IncomingMessage) => verifySession(cookieOf(req, COOKIE));

async function parse(req: IncomingMessage): Promise<Record<string, unknown>> {
  const raw = await readBody(req, 64 * 1024);
  if (raw.length === 0) return {};
  try { return JSON.parse(raw.toString("utf8")) as Record<string, unknown>; } catch { throw Object.assign(new Error("invalid JSON"), { status: 400 }); }
}

const upstreamView = (r: UpstreamRow) => ({
  id: r.id, label: r.label, mask: r.key_mask, enabled: !!r.enabled, status: r.status,
  cooling: r.cooldown_until > Date.now(), creditsRemaining: r.credits_remaining,
  requests: r.requests, errors: r.errors, lastUsedAt: r.last_used_at, lastError: r.last_error,
});

async function refreshUpstream(row: UpstreamRow) {
  const r = await fetch(config.upstreamUrl + "/v2/me", {
    headers: { authorization: `Bearer ${pool.secretOf(row)}` },
    signal: AbortSignal.timeout(config.upstreamTimeoutMs),
  });
  if (r.status === 401) { pool.markInvalid(row.id, "401 invalid api key"); return; }
  const remaining = r.headers.get("x-credits-remaining");
  if (remaining !== null && /^\d+$/.test(remaining)) {
    db.prepare("UPDATE upstream_keys SET credits_remaining = ?, last_error = NULL, status = CASE WHEN ? > 0 THEN 'active' ELSE status END WHERE id = ?")
      .run(Number(remaining), Number(remaining), row.id);
  } else if (r.status === 402) {
    pool.markExhausted(row.id, "402 credits_exhausted");
  }
}

export async function handleAdmin(req: IncomingMessage, res: ServerResponse, url: URL) {
  const path = url.pathname.replace(/^\/admin\/api/, "");
  const method = req.method ?? "GET";
  try {
    if (path === "/login" && method === "POST") {
      const ip = req.socket.remoteAddress ?? "?";
      const f = failures.get(ip);
      if (f && f.n >= 5 && f.until > Date.now()) return json(res, 429, { error: "Thử lại sau vài phút" });
      const { password } = await parse(req);
      if (typeof password !== "string" || !safeEqual(password, config.adminPassword)) {
        failures.set(ip, { n: (f?.n ?? 0) + 1, until: Date.now() + 5 * 60_000 });
        return json(res, 401, { error: "Sai mật khẩu" });
      }
      failures.delete(ip);
      const secure = req.headers["x-forwarded-proto"] === "https" ? "; Secure" : "";
      return json(res, 200, { ok: true }, {
        "set-cookie": `${COOKIE}=${signSession(SESSION_TTL)}; HttpOnly; SameSite=Strict; Path=/admin; Max-Age=${SESSION_TTL / 1000}${secure}`,
      });
    }
    if (path === "/logout" && method === "POST") {
      return json(res, 200, { ok: true }, { "set-cookie": `${COOKIE}=; HttpOnly; SameSite=Strict; Path=/admin; Max-Age=0` });
    }
    if (!isAdmin(req)) return json(res, 401, { error: "unauthorized" });

    if (path === "/state" && method === "GET") {
      const upstream = (db.prepare("SELECT * FROM upstream_keys ORDER BY id").all() as unknown as UpstreamRow[]).map(upstreamView);
      const clients = db.prepare("SELECT id, label, token_prefix AS prefix, enabled, requests, credits_used AS creditsUsed, last_used_at AS lastUsedAt FROM client_keys ORDER BY id").all();
      const logs = db.prepare(
        `SELECT l.ts, l.method, l.path, l.status, l.credits, l.attempts, l.ms, c.label AS client, u.label AS upstream
         FROM request_log l LEFT JOIN client_keys c ON c.id = l.client_id LEFT JOIN upstream_keys u ON u.id = l.upstream_id
         ORDER BY l.id DESC LIMIT 100`,
      ).all();
      return json(res, 200, { upstream, clients, logs });
    }

    if (path === "/upstream" && method === "POST") {
      const { label, key } = await parse(req);
      if (typeof key !== "string" || key.trim().length < 8) return json(res, 400, { error: "key không hợp lệ" });
      const k = key.trim();
      const info = db.prepare("INSERT INTO upstream_keys (label, key_enc, key_mask, created_at) VALUES (?,?,?,?)")
        .run(typeof label === "string" && label.trim() ? label.trim() : maskKey(k), encrypt(k), maskKey(k), Date.now());
      return json(res, 201, { id: Number(info.lastInsertRowid) });
    }

    const up = /^\/upstream\/(\d+)(\/refresh)?$/.exec(path);
    if (up) {
      const id = Number(up[1]);
      const row = db.prepare("SELECT * FROM upstream_keys WHERE id = ?").get(id) as unknown as UpstreamRow | undefined;
      if (!row) return json(res, 404, { error: "not found" });
      if (up[2] && method === "POST") { await refreshUpstream(row); return json(res, 200, { ok: true }); }
      if (method === "DELETE") { db.prepare("DELETE FROM upstream_keys WHERE id = ?").run(id); return json(res, 200, { ok: true }); }
      if (method === "PATCH") {
        const b = await parse(req);
        if (typeof b.enabled === "boolean") db.prepare("UPDATE upstream_keys SET enabled = ? WHERE id = ?").run(b.enabled ? 1 : 0, id);
        if (typeof b.label === "string" && b.label.trim()) db.prepare("UPDATE upstream_keys SET label = ? WHERE id = ?").run(b.label.trim(), id);
        if (b.reset === true) db.prepare("UPDATE upstream_keys SET status = 'active', cooldown_until = 0, retry_at = 0, last_error = NULL WHERE id = ?").run(id);
        return json(res, 200, { ok: true });
      }
    }

    if (path === "/clients" && method === "POST") {
      const { label } = await parse(req);
      if (typeof label !== "string" || !label.trim()) return json(res, 400, { error: "label bắt buộc" });
      const token = newClientToken();
      db.prepare("INSERT INTO client_keys (label, token_hash, token_prefix, created_at) VALUES (?,?,?,?)")
        .run(label.trim(), sha256(token), token.slice(0, 8), Date.now());
      return json(res, 201, { token }); // chỉ hiển thị một lần
    }

    const cl = /^\/clients\/(\d+)$/.exec(path);
    if (cl) {
      const id = Number(cl[1]);
      if (method === "DELETE") { db.prepare("DELETE FROM client_keys WHERE id = ?").run(id); return json(res, 200, { ok: true }); }
      if (method === "PATCH") {
        const b = await parse(req);
        if (typeof b.enabled === "boolean") db.prepare("UPDATE client_keys SET enabled = ? WHERE id = ?").run(b.enabled ? 1 : 0, id);
        return json(res, 200, { ok: true });
      }
    }
    return json(res, 404, { error: "not found" });
  } catch (e) {
    return json(res, (e as { status?: number }).status ?? 500, { error: (e as Error).message });
  }
}
