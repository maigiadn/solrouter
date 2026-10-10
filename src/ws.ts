import { request as httpRequest, type IncomingMessage } from "node:http";
import { request as httpsRequest } from "node:https";
import type { Socket } from "node:net";
import type { Duplex } from "node:stream";
import { findClient } from "./auth.js";
import { config } from "./config.js";
import * as pool from "./pool.js";
import { logRequest } from "./proxy.js";
import type { UpstreamRow } from "./db.js";

const STRIP_REQ = new Set(["host", "authorization", "x-api-key", "cookie", "content-length", "transfer-encoding"]);
const KEY_PARAMS = ["key", "apiKey", "api_key"];

type Attempt =
  | { kind: "open"; res: IncomingMessage; socket: Duplex; head: Buffer }
  | { kind: "reject"; status: number; body: string }
  | { kind: "network"; error: string };

function replyAndClose(socket: Duplex, status: number, reason: string, body: unknown) {
  const payload = JSON.stringify(body);
  socket.end(
    `HTTP/1.1 ${status} ${reason}\r\ncontent-type: application/json; charset=utf-8\r\nconnection: close\r\ncontent-length: ${Buffer.byteLength(payload)}\r\n\r\n${payload}`,
  );
}

/** Mở handshake WebSocket tới upstream bằng một API key; không đọc frame, chỉ trả về socket thô khi nhận 101. */
function openUpstream(row: UpstreamRow, path: string, search: URLSearchParams, headers: Record<string, string>): Promise<Attempt> {
  const secret = pool.secretOf(row);
  const target = new URL(config.upstreamUrl + path);
  for (const [k, v] of search) target.searchParams.append(k, v);
  target.searchParams.set("key", secret);
  const doRequest = target.protocol === "https:" ? httpsRequest : httpRequest;

  return new Promise((resolve) => {
    const req = doRequest(target, {
      method: "GET",
      headers: { ...headers, authorization: `Bearer ${secret}`, connection: "Upgrade", upgrade: "websocket" },
      signal: AbortSignal.timeout(config.upstreamTimeoutMs),
    });
    req.on("upgrade", (res, socket, head) => resolve({ kind: "open", res, socket, head }));
    req.on("response", (res) => {
      const chunks: Buffer[] = [];
      let size = 0;
      res.on("data", (c: Buffer) => { if ((size += c.length) <= 4096) chunks.push(c); });
      res.on("end", () => resolve({ kind: "reject", status: res.statusCode ?? 502, body: Buffer.concat(chunks).toString() }));
      res.on("error", () => resolve({ kind: "reject", status: res.statusCode ?? 502, body: "" }));
    });
    req.on("error", (e) => resolve({ kind: "network", error: e.message }));
    req.end();
  });
}

const intHeader = (v: string | string[] | undefined) => {
  const s = Array.isArray(v) ? v[0] : v;
  return s !== undefined && /^\d+$/.test(s) ? Number(s) : null;
};

/** Proxy WebSocket: xác thực router key, chọn upstream key round-robin, failover khi handshake bị từ chối. */
export async function handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer) {
  const started = Date.now();
  let clientGone = false;
  socket.on("error", () => { clientGone = true; });
  socket.on("close", () => { clientGone = true; });

  try {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (String(req.headers.upgrade).toLowerCase() !== "websocket") {
      return replyAndClose(socket, 400, "Bad Request", { error: "bad_request", message: "Chỉ hỗ trợ Upgrade: websocket." });
    }
    if (url.pathname.startsWith("/admin")) {
      return replyAndClose(socket, 404, "Not Found", { error: "not_found" });
    }
    const client = findClient(req, url);
    if (!client) {
      return replyAndClose(socket, 401, "Unauthorized", { error: "invalid_router_key", message: "Thiếu hoặc sai router API key." });
    }

    for (const p of KEY_PARAMS) url.searchParams.delete(p);
    const headers: Record<string, string> = {};
    for (const [k, v] of Object.entries(req.headers)) {
      if (!STRIP_REQ.has(k) && typeof v === "string") headers[k] = v;
    }

    const candidates = pool.orderedCandidates().slice(0, config.maxAttempts);
    let attempts = 0;
    let sawExhausted = false;

    for (const row of candidates) {
      if (clientGone) return;
      attempts++;
      const r = await openUpstream(row, url.pathname, url.searchParams, headers);

      if (r.kind === "network") { pool.markTransient(row.id, `network: ${r.error}`); continue; }

      if (r.kind === "reject") {
        if (r.status === 402) {
          let left: number | null = null;
          try { left = (JSON.parse(r.body) as { remaining?: number }).remaining ?? null; } catch { /* body không phải JSON */ }
          sawExhausted = true;
          if (left !== null && left > 0) pool.setRemaining(row.id, left);
          else pool.markExhausted(row.id, "402 credits_exhausted");
        } else if (r.status === 401) {
          pool.markInvalid(row.id, "401 invalid api key");
        } else if (r.status === 429 || r.status === 502 || r.status === 503 || r.status === 504) {
          pool.markTransient(row.id, `upstream ${r.status}`);
        } else {
          // lỗi khác (vd 400/404 do path sai) → trả nguyên cho client, không đổi trạng thái key
          logRequest({ clientId: client.id, upstreamId: row.id, method: "WS", path: url.pathname, status: r.status, credits: null, attempts, ms: Date.now() - started });
          return replyAndClose(socket, r.status, "Upstream Error", safeJson(r.body));
        }
        continue;
      }

      // 101 Switching Protocols
      const { res, socket: up, head: upHead } = r;
      if (clientGone) { up.destroy(); return; }
      pool.markUsed(row.id, intHeader(res.headers["x-credits-remaining"]));
      logRequest({ clientId: client.id, upstreamId: row.id, method: "WS", path: url.pathname, status: 101, credits: intHeader(res.headers["x-credits-cost"]), attempts, ms: Date.now() - started });

      let out = `HTTP/1.1 101 ${res.statusMessage || "Switching Protocols"}\r\n`;
      for (let i = 0; i < res.rawHeaders.length; i += 2) out += `${res.rawHeaders[i]}: ${res.rawHeaders[i + 1]}\r\n`;
      socket.write(out + "\r\n");
      if (upHead.length) socket.write(upHead);
      if (head.length) up.write(head);

      (socket as Socket).setNoDelay?.(true);
      (up as Socket).setNoDelay?.(true);
      const closeBoth = () => { socket.destroy(); up.destroy(); };
      socket.on("error", closeBoth);
      up.on("error", closeBoth);
      socket.on("close", closeBoth);
      up.on("close", closeBoth);
      up.pipe(socket);
      socket.pipe(up);
      return;
    }

    if (clientGone) return;
    const status = sawExhausted ? 402 : 503;
    logRequest({ clientId: client.id, upstreamId: null, method: "WS", path: url.pathname, status, credits: null, attempts, ms: Date.now() - started });
    replyAndClose(socket, status, sawExhausted ? "Payment Required" : "Service Unavailable", {
      error: sawExhausted ? "all_keys_exhausted" : "no_upstream_keys_available",
      message: sawExhausted
        ? "Mọi API key khả dụng đều hết credits cho request này."
        : "Không có API key nào khả dụng (chưa thêm key, bị tắt, lỗi hoặc đang cooldown).",
      attempts,
    });
  } catch (e) {
    console.error(e);
    if (!socket.destroyed) replyAndClose(socket, 500, "Internal Server Error", { error: "internal_error" });
  }
}

function safeJson(text: string): unknown {
  try { return JSON.parse(text); } catch { return { error: "upstream_error", message: text.slice(0, 200) }; }
}
