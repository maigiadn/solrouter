import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { config } from "./config.js";
import { db } from "./db.js";
import * as pool from "./pool.js";

const STRIP_REQ = new Set(["host", "connection", "authorization", "x-api-key", "content-length", "cookie", "transfer-encoding", "upgrade", "keep-alive"]);
const STRIP_RES = new Set(["content-encoding", "content-length", "transfer-encoding", "connection", "keep-alive"]);
const KEY_PARAMS = ["key", "apiKey", "api_key"];

export async function readBody(req: IncomingMessage, limit = config.maxBodyBytes): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += (chunk as Buffer).length;
    if (size > limit) throw Object.assign(new Error("body too large"), { status: 413 });
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks);
}

let logCount = 0;
export function logRequest(e: { clientId: number; upstreamId: number | null; method: string; path: string; status: number; credits: number | null; attempts: number; ms: number }) {
  db.prepare("INSERT INTO request_log (ts, client_id, upstream_id, method, path, status, credits, attempts, ms) VALUES (?,?,?,?,?,?,?,?,?)")
    .run(Date.now(), e.clientId, e.upstreamId, e.method, e.path, e.status, e.credits, e.attempts, e.ms);
  db.prepare("UPDATE client_keys SET requests = requests + 1, credits_used = credits_used + ?, last_used_at = ? WHERE id = ?")
    .run(e.credits ?? 0, Date.now(), e.clientId);
  if (++logCount % 200 === 0) db.exec("DELETE FROM request_log WHERE id <= (SELECT MAX(id) - 5000 FROM request_log)");
}

const intHeader = (h: Headers, name: string) => {
  const v = h.get(name);
  return v !== null && /^\d+$/.test(v) ? Number(v) : null;
};

function sendJson(res: ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(body));
}

export async function handleProxy(req: IncomingMessage, res: ServerResponse, url: URL, clientId: number) {
  const started = Date.now();
  const method = req.method ?? "GET";
  let body: Buffer | undefined;
  try {
    body = method === "GET" || method === "HEAD" ? undefined : await readBody(req);
  } catch (e) {
    return sendJson(res, (e as { status?: number }).status ?? 400, { error: "bad_request", message: (e as Error).message });
  }

  for (const p of KEY_PARAMS) url.searchParams.delete(p);
  const target = config.upstreamUrl + url.pathname + url.search;
  const headers: Record<string, string> = {};
  for (const [k, v] of Object.entries(req.headers)) {
    if (!STRIP_REQ.has(k) && typeof v === "string") headers[k] = v;
  }

  const candidates = pool.orderedCandidates().slice(0, config.maxAttempts);
  let attempts = 0;
  let sawExhausted = false;

  for (const row of candidates) {
    attempts++;
    let up: Response;
    try {
      up = await fetch(target, {
        method,
        headers: { ...headers, authorization: `Bearer ${pool.secretOf(row)}` },
        body: body ? new Uint8Array(body) : undefined,
        redirect: "manual",
        signal: AbortSignal.timeout(config.upstreamTimeoutMs),
      });
    } catch (e) {
      pool.markTransient(row.id, `network: ${(e as Error).message}`);
      continue;
    }

    const remaining = intHeader(up.headers, "x-credits-remaining");
    const cost = intHeader(up.headers, "x-credits-cost");

    if (up.status === 402) {
      const text = await up.text().catch(() => "");
      let left: number | null = null;
      try { left = (JSON.parse(text) as { remaining?: number }).remaining ?? null; } catch { /* body không phải JSON */ }
      sawExhausted = true;
      // Còn credit nhưng không đủ cho call đắt này → không đánh dấu hết quota
      if (left !== null && left > 0) pool.setRemaining(row.id, left);
      else pool.markExhausted(row.id, "402 credits_exhausted");
      continue;
    }
    if (up.status === 401) {
      await up.arrayBuffer().catch(() => null);
      pool.markInvalid(row.id, "401 invalid api key");
      continue;
    }
    if (up.status === 429 || up.status === 502 || up.status === 503 || up.status === 504) {
      await up.arrayBuffer().catch(() => null);
      pool.markTransient(row.id, `upstream ${up.status}`);
      continue;
    }

    pool.markUsed(row.id, remaining);
    const outHeaders: Record<string, string> = {};
    up.headers.forEach((v, k) => { if (!STRIP_RES.has(k)) outHeaders[k] = v; });
    res.writeHead(up.status, outHeaders);
    logRequest({ clientId, upstreamId: row.id, method, path: url.pathname, status: up.status, credits: cost, attempts, ms: Date.now() - started });
    if (!up.body || method === "HEAD") return res.end();
    const stream = Readable.fromWeb(up.body as import("node:stream/web").ReadableStream);
    stream.on("error", () => res.destroy());
    res.on("close", () => stream.destroy());
    stream.pipe(res);
    return;
  }

  const status = sawExhausted ? 402 : 503;
  logRequest({ clientId, upstreamId: null, method, path: url.pathname, status, credits: null, attempts, ms: Date.now() - started });
  sendJson(res, status, {
    error: sawExhausted ? "all_keys_exhausted" : "no_upstream_keys_available",
    message: sawExhausted
      ? "Mọi API key khả dụng đều hết credits cho request này."
      : "Không có API key nào khả dụng (chưa thêm key, bị tắt, lỗi hoặc đang cooldown).",
    attempts,
  });
}
