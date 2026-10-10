import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { createServer, request as httpRequest, type Server } from "node:http";
import { createServer as createNetServer } from "node:net";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { createHash } from "node:crypto";
import type { Socket } from "node:net";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

// ---------- fake upstream (fomoapi.io) ----------
interface Reply { status: number; headers?: Record<string, string>; body?: unknown }
interface Call { key: string; url: string; method: string; body: string }

class FakeUpstream {
  calls: Call[] = [];
  behavior = new Map<string, () => Reply>();
  server!: Server;
  url = "";

  async start() {
    this.server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c: Buffer) => chunks.push(c));
      req.on("end", () => {
        const key = (req.headers.authorization ?? "").replace(/^Bearer /, "");
        this.calls.push({ key, url: req.url ?? "", method: req.method ?? "", body: Buffer.concat(chunks).toString() });
        const r = this.behavior.get(key)?.() ?? { status: 200, body: { ok: true } };
        res.writeHead(r.status, { "content-type": "application/json", ...r.headers });
        res.end(JSON.stringify(r.body ?? {}));
      });
    });
    this.server.on("upgrade", (req, socket: Socket) => {
      const u = new URL(req.url ?? "/", "http://x");
      const key = u.searchParams.get("key") ?? (req.headers.authorization ?? "").replace(/^Bearer /, "");
      this.calls.push({ key, url: req.url ?? "", method: "WS", body: "" });
      const r = this.behavior.get(key)?.() ?? { status: 200 };
      if (r.status !== 200) {
        const body = JSON.stringify(r.body ?? {});
        socket.end(`HTTP/1.1 ${r.status} X\r\ncontent-type: application/json\r\ncontent-length: ${body.length}\r\nconnection: close\r\n\r\n${body}`);
        return;
      }
      const accept = createHash("sha1").update(String(req.headers["sec-websocket-key"]) + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
      socket.write(`HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: ${accept}\r\n\r\n`);
      const send = (text: string) => socket.write(Buffer.concat([Buffer.from([0x81, text.length]), Buffer.from(text)]));
      send(`hello:${key}`);
      socket.on("data", (buf: Buffer) => { // echo frame text có mask từ client
        if (((buf[0] ?? 0) & 0x0f) !== 1) return;
        const len = (buf[1] ?? 0) & 0x7f, mask = buf.subarray(2, 6);
        send(Buffer.from(buf.subarray(6, 6 + len).map((b, i) => b ^ (mask[i % 4] ?? 0))).toString());
      });
      socket.on("error", () => {});
    });
    await new Promise<void>((r) => this.server.listen(0, "127.0.0.1", r));
    this.url = `http://127.0.0.1:${(this.server.address() as { port: number }).port}`;
  }
  stop() { this.server.close(); this.server.closeAllConnections(); }
  keysCalled() { return this.calls.map((c) => c.key); }
  reset() { this.calls = []; this.behavior.clear(); }
}

// ---------- router thật chạy trong child process ----------
const freePort = () => new Promise<number>((resolve) => {
  const s = createNetServer().listen(0, () => { const p = (s.address() as { port: number }).port; s.close(() => resolve(p)); });
});

class Router {
  base = "";
  dataDir = mkdtempSync(join(tmpdir(), "solrouter-test-"));
  proc!: ChildProcess;
  cookie = "";
  password = "test-admin-pw";

  async start(upstreamUrl: string) {
    const port = await freePort();
    this.base = `http://127.0.0.1:${port}`;
    this.proc = spawn(process.execPath, [fileURLToPath(new URL("./server.js", import.meta.url))], {
      env: { ...process.env, ADMIN_PASSWORD: this.password, PORT: String(port), DATA_DIR: this.dataDir, UPSTREAM_URL: upstreamUrl, APP_SECRET: "" },
      stdio: "ignore",
    });
    for (let i = 0; i < 100; i++) {
      try { if ((await fetch(`${this.base}/healthz`)).ok) return; } catch { /* chưa lên */ }
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error("router không khởi động được");
  }
  stop() { this.proc.kill(); rmSync(this.dataDir, { recursive: true, force: true }); }

  async login(password = this.password) {
    const r = await fetch(`${this.base}/admin/api/login`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ password }) });
    if (r.ok) this.cookie = (r.headers.get("set-cookie") ?? "").split(";")[0] ?? "";
    return r;
  }
  admin(method: string, path: string, body?: unknown) {
    return fetch(`${this.base}/admin/api${path}`, {
      method, headers: { "content-type": "application/json", cookie: this.cookie }, body: body === undefined ? undefined : JSON.stringify(body),
    });
  }
  async addUpstream(label: string, key: string) {
    const r = await this.admin("POST", "/upstream", { label, key });
    assert.equal(r.status, 201);
    return ((await r.json()) as { id: number }).id;
  }
  async newClient(label: string) {
    const r = await this.admin("POST", "/clients", { label });
    assert.equal(r.status, 201);
    return ((await r.json()) as { token: string }).token;
  }
  call(token: string, path = "/v2/me", init: RequestInit = {}) {
    return fetch(`${this.base}${path}`, { ...init, headers: { authorization: `Bearer ${token}`, ...(init.headers as Record<string, string>) } });
  }
  db() {
    const d = new DatabaseSync(join(this.dataDir, "solrouter.db"));
    d.exec("PRAGMA busy_timeout = 3000");
    return d;
  }
  upstreamRow(id: number) {
    const d = this.db();
    try { return d.prepare("SELECT * FROM upstream_keys WHERE id = ?").get(id) as Record<string, any>; } finally { d.close(); }
  }
  async state() { return (await (await this.admin("GET", "/state")).json()) as { upstream: any[]; clients: any[]; logs: any[] }; }
  async clearUpstream() { for (const u of (await this.state()).upstream) await this.admin("DELETE", `/upstream/${u.id}`); }
}

const fake = new FakeUpstream();
const router = new Router();
let token = "";
before(async () => {
  await fake.start();
  await router.start(fake.url);
  await router.login();
  token = await router.newClient("test");
});
after(() => { router.stop(); fake.stop(); });

// mỗi test bắt đầu với pool trống + fake upstream sạch
const fresh = async () => { fake.reset(); await router.clearUpstream(); };

describe("xác thực router key", () => {
  test("thiếu / sai key → 401, không gọi upstream", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    assert.equal((await fetch(`${router.base}/v2/me`)).status, 401);
    assert.equal((await router.call("sr_sai")).status, 401);
    assert.equal(fake.calls.length, 0);
  });

  test("chấp nhận Bearer, x-api-key và ?key=; ?key= bị gỡ khỏi URL upstream", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    assert.equal((await router.call(token)).status, 200);
    assert.equal((await fetch(`${router.base}/v2/me`, { headers: { "x-api-key": token } })).status, 200);
    assert.equal((await fetch(`${router.base}/v2/me?foo=1&key=${token}`)).status, 200);
    assert.equal(fake.calls.length, 3);
    assert.equal(fake.calls[2]?.url, "/v2/me?foo=1");
  });

  test("upstream nhận API key thật, không nhận router key; body POST được chuyển nguyên vẹn", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    const r = await router.call(token, "/v2/echo", { method: "POST", body: '{"x":1}', headers: { "content-type": "application/json" } });
    assert.equal(r.status, 200);
    assert.equal(fake.calls[0]?.key, "key-A-aaaaaaaa");
    assert.equal(fake.calls[0]?.body, '{"x":1}');
  });

  test("router key bị tắt → 401; bật lại → 200", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    const t = await router.newClient("tạm");
    const id = (await router.state()).clients.find((c) => c.label === "tạm").id;
    await router.admin("PATCH", `/clients/${id}`, { enabled: false });
    assert.equal((await router.call(t)).status, 401);
    await router.admin("PATCH", `/clients/${id}`, { enabled: true });
    assert.equal((await router.call(t)).status, 200);
  });
});

describe("round-robin", () => {
  test("xoay vòng đều qua 3 key", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    await router.addUpstream("B", "key-B-bbbbbbbb");
    await router.addUpstream("C", "key-C-cccccccc");
    for (let i = 0; i < 6; i++) assert.equal((await router.call(token)).status, 200);
    const seq = fake.keysCalled();
    // cùng một chu kỳ lặp lại, mỗi key đúng 2 lần, không key nào liên tiếp
    assert.deepEqual(seq.slice(0, 3), seq.slice(3, 6));
    assert.equal(new Set(seq.slice(0, 3)).size, 3);
  });

  test("key bị tắt (enabled=false) không được dùng", async () => {
    await fresh();
    const a = await router.addUpstream("A", "key-A-aaaaaaaa");
    await router.addUpstream("B", "key-B-bbbbbbbb");
    await router.admin("PATCH", `/upstream/${a}`, { enabled: false });
    for (let i = 0; i < 4; i++) await router.call(token);
    assert.deepEqual(new Set(fake.keysCalled()), new Set(["key-B-bbbbbbbb"]));
  });
});

describe("402 — hết credits", () => {
  test("402 remaining=0 → thử key kế tiếp, key đầu thành exhausted, retry_at ≈ +30 phút", async () => {
    await fresh();
    const a = await router.addUpstream("A", "key-A-aaaaaaaa");
    await router.addUpstream("B", "key-B-bbbbbbbb");
    fake.behavior.set("key-A-aaaaaaaa", () => ({ status: 402, body: { error: "credits_exhausted", remaining: 0 } }));
    const r = await router.call(token);
    assert.equal(r.status, 200, "client vẫn nhận 200 nhờ failover");
    const row = router.upstreamRow(a);
    assert.equal(row.status, "exhausted");
    assert.equal(row.credits_remaining, 0);
    const delta = row.retry_at - Date.now();
    assert.ok(delta > 29 * 60_000 && delta <= 30 * 60_000, `retry_at lệch: ${delta}`);
    // log ghi attempts = 2
    assert.equal((await router.state()).logs[0].attempts, 2);
    // các request sau không đụng vào key A nữa
    fake.calls = [];
    await router.call(token); await router.call(token);
    assert.ok(!fake.keysCalled().includes("key-A-aaaaaaaa"));
  });

  test("402 nhưng remaining>0 (call quá đắt) → key KHÔNG bị đánh dấu exhausted", async () => {
    await fresh();
    const a = await router.addUpstream("A", "key-A-aaaaaaaa");
    await router.addUpstream("B", "key-B-bbbbbbbb");
    fake.behavior.set("key-A-aaaaaaaa", () => ({ status: 402, body: { error: "insufficient", remaining: 7 } }));
    assert.equal((await router.call(token)).status, 200);
    const row = router.upstreamRow(a);
    assert.equal(row.status, "active");
    assert.equal(row.credits_remaining, 7);
  });

  test("mọi key đều 402 → client nhận 402 all_keys_exhausted", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    await router.addUpstream("B", "key-B-bbbbbbbb");
    for (const k of ["key-A-aaaaaaaa", "key-B-bbbbbbbb"]) fake.behavior.set(k, () => ({ status: 402, body: { remaining: 0 } }));
    const r = await router.call(token);
    assert.equal(r.status, 402);
    assert.equal(((await r.json()) as any).error, "all_keys_exhausted");
  });

  test("key exhausted đến hạn retry được thử lại (xếp cuối) và hồi phục về active", async () => {
    await fresh();
    const a = await router.addUpstream("A", "key-A-aaaaaaaa");
    fake.behavior.set("key-A-aaaaaaaa", () => ({ status: 402, body: { remaining: 0 } }));
    assert.equal((await router.call(token)).status, 402);
    // chưa đến hạn: không gọi upstream, trả 503 vì không còn key khả dụng
    fake.calls = [];
    assert.equal((await router.call(token)).status, 503);
    assert.equal(fake.calls.length, 0);
    // giả lập đã qua 30 phút + upstream đã nạp credits
    const d = router.db(); d.prepare("UPDATE upstream_keys SET retry_at = 1 WHERE id = ?").run(a); d.close();
    fake.behavior.clear();
    assert.equal((await router.call(token)).status, 200);
    assert.equal(router.upstreamRow(a).status, "active");
  });
});

describe("lỗi khác", () => {
  test("401 → key invalid, failover sang key khác; Reset đưa về active", async () => {
    await fresh();
    const a = await router.addUpstream("A", "key-A-aaaaaaaa");
    await router.addUpstream("B", "key-B-bbbbbbbb");
    fake.behavior.set("key-A-aaaaaaaa", () => ({ status: 401, body: { error: "invalid" } }));
    assert.equal((await router.call(token)).status, 200);
    assert.equal(router.upstreamRow(a).status, "invalid");
    fake.calls = [];
    for (let i = 0; i < 3; i++) await router.call(token);
    assert.ok(!fake.keysCalled().includes("key-A-aaaaaaaa"), "key invalid không được dùng lại");
    fake.behavior.clear();
    await router.admin("PATCH", `/upstream/${a}`, { reset: true });
    assert.equal(router.upstreamRow(a).status, "active");
    fake.calls = [];
    for (let i = 0; i < 4; i++) await router.call(token);
    assert.ok(fake.keysCalled().includes("key-A-aaaaaaaa"), "sau Reset key A được dùng lại");
  });

  for (const code of [429, 502, 503, 504]) {
    test(`${code} → cooldown ~30s, vẫn active, failover sang key khác`, async () => {
      await fresh();
      const a = await router.addUpstream("A", "key-A-aaaaaaaa");
      await router.addUpstream("B", "key-B-bbbbbbbb");
      fake.behavior.set("key-A-aaaaaaaa", () => ({ status: code }));
      assert.equal((await router.call(token)).status, 200);
      const row = router.upstreamRow(a);
      assert.equal(row.status, "active");
      const delta = row.cooldown_until - Date.now();
      assert.ok(delta > 25_000 && delta <= 30_000, `cooldown lệch: ${delta}`);
      fake.calls = [];
      for (let i = 0; i < 3; i++) await router.call(token);
      assert.ok(!fake.keysCalled().includes("key-A-aaaaaaaa"), "key đang cooldown bị bỏ qua");
    });
  }

  test("lỗi mạng (upstream không kết nối được) → cooldown, rồi 503 khi hết key", async () => {
    await fresh();
    const dead = new Router(); // router thứ hai trỏ vào cổng không có ai nghe
    const deadPort = await freePort();
    await dead.start(`http://127.0.0.1:${deadPort}`);
    try {
      await dead.login();
      const id = await dead.addUpstream("A", "key-A-aaaaaaaa");
      const t = await dead.newClient("t");
      const r = await dead.call(t);
      assert.equal(r.status, 503);
      assert.equal(((await r.json()) as any).error, "no_upstream_keys_available");
      assert.match(dead.upstreamRow(id).last_error, /^network:/);
      assert.ok(dead.upstreamRow(id).cooldown_until > Date.now());
    } finally { dead.stop(); }
  });

  test("không có upstream key nào → 503", async () => {
    await fresh();
    const r = await router.call(token);
    assert.equal(r.status, 503);
    assert.equal(((await r.json()) as any).error, "no_upstream_keys_available");
  });

  test("lỗi 4xx/5xx khác (vd 404, 500) được trả nguyên cho client, không đổi trạng thái key", async () => {
    await fresh();
    const a = await router.addUpstream("A", "key-A-aaaaaaaa");
    fake.behavior.set("key-A-aaaaaaaa", () => ({ status: 404, body: { error: "not_found" } }));
    const r = await router.call(token, "/v2/nope");
    assert.equal(r.status, 404);
    assert.equal(router.upstreamRow(a).status, "active");
  });
});

describe("credits & log", () => {
  test("x-credits-remaining cập nhật credits_remaining của key", async () => {
    await fresh();
    const a = await router.addUpstream("A", "key-A-aaaaaaaa");
    fake.behavior.set("key-A-aaaaaaaa", () => ({ status: 200, headers: { "x-credits-remaining": "1234" } }));
    await router.call(token);
    assert.equal(router.upstreamRow(a).credits_remaining, 1234);
  });

  test("x-credits-cost cộng vào credits_used của client và ghi vào log", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    fake.behavior.set("key-A-aaaaaaaa", () => ({ status: 200, headers: { "x-credits-cost": "5" } }));
    const before = (await router.state()).clients.find((c) => c.label === "test").creditsUsed;
    await router.call(token, "/v2/costly");
    await router.call(token, "/v2/costly");
    const st = await router.state();
    assert.equal(st.clients.find((c) => c.label === "test").creditsUsed, before + 10);
    assert.equal(st.logs[0].credits, 5);
    assert.equal(st.logs[0].path, "/v2/costly");
    assert.equal(st.logs[0].client, "test");
    assert.equal(st.logs[0].upstream, "A");
  });

  test("request xuất hiện trong dashboard (state): log + bộ đếm requests", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    const before = (await router.state()).clients.find((c) => c.label === "test").requests;
    await router.call(token, "/v2/marker");
    const st = await router.state();
    assert.equal(st.clients.find((c) => c.label === "test").requests, before + 1);
    assert.equal(st.logs[0].path, "/v2/marker");
    assert.equal(st.logs[0].status, 200);
  });
});

describe("bảo mật lưu trữ", () => {
  test("API key upstream được mã hoá; router key chỉ lưu SHA-256", async () => {
    await fresh();
    const secretKey = "fapi_SUPER_SECRET_PLAINTEXT_123456";
    await router.addUpstream("S", secretKey);
    const t = await router.newClient("hash-check");
    // quét mọi file trong DATA_DIR (db, wal, shm) tìm plaintext
    for (const f of readdirSync(router.dataDir)) {
      if (f === "secret.key") continue;
      const bytes = readFileSync(join(router.dataDir, f));
      assert.ok(!bytes.includes(secretKey), `plaintext upstream key lộ trong ${f}`);
      assert.ok(!bytes.includes(t), `router key plaintext lộ trong ${f}`);
    }
    const d = router.db();
    const row = d.prepare("SELECT token_hash FROM client_keys WHERE label = 'hash-check'").get() as { token_hash: string };
    d.close();
    assert.equal(row.token_hash, createHash("sha256").update(t).digest("hex"));
  });

  test("dashboard không trả API key plaintext qua /state", async () => {
    await fresh();
    await router.addUpstream("S", "fapi_SUPER_SECRET_PLAINTEXT_123456");
    const raw = await (await router.admin("GET", "/state")).text();
    assert.ok(!raw.includes("SUPER_SECRET_PLAINTEXT"));
  });
});

describe("admin & đăng nhập", () => {
  test("admin API cần cookie phiên", async () => {
    const r = await fetch(`${router.base}/admin/api/state`);
    assert.equal(r.status, 401);
  });

  test("sai mật khẩu 5 lần → khoá (429), kể cả khi sau đó nhập đúng", async () => {
    const r2 = new Router();
    await r2.start(fake.url);
    try {
      for (let i = 0; i < 5; i++) assert.equal((await r2.login("sai")).status, 401);
      assert.equal((await r2.login("sai")).status, 429);
      assert.equal((await r2.login()).status, 429, "đúng mật khẩu vẫn bị chặn trong thời gian khoá");
    } finally { r2.stop(); }
  });

  test("đăng nhập đúng → 200 + cookie HttpOnly", async () => {
    const r = await router.login();
    assert.equal(r.status, 200);
    assert.match(r.headers.get("set-cookie") ?? "", /sr_session=.+HttpOnly/);
  });
});

// ---------- WebSocket proxy ----------
/** Mở WebSocket qua router (đúng kiểu bot: ?key=), trả về tin đầu tiên + echo. */
function wsOpen(tok: string, path = "/ws/alerts") {
  return new Promise<{ first: string; echo: (t: string) => Promise<string>; close: () => void }>((resolve, reject) => {
    const ws = new WebSocket(`${router.base.replace("http", "ws")}${path}?key=${tok}&type=buy`);
    const msgs: string[] = [];
    let waiter: ((m: string) => void) | undefined;
    ws.onmessage = (e) => { const m = String(e.data); if (waiter) { const w = waiter; waiter = undefined; w(m); } else msgs.push(m); };
    const next = () => new Promise<string>((r) => { const m = msgs.shift(); if (m !== undefined) r(m); else waiter = r; });
    ws.onerror = () => reject(new Error("ws error"));
    ws.onopen = async () => resolve({
      first: await next(),
      echo: async (t) => { ws.send(t); return next(); },
      close: () => ws.close(),
    });
  });
}

/** Gửi handshake thô để đọc mã trạng thái khi router từ chối. */
function wsRaw(tok: string | null, path = "/ws/alerts") {
  return new Promise<{ status: number; body: any }>((resolve, reject) => {
    const req = httpRequest(`${router.base}${path}${tok ? `?key=${tok}` : ""}`, {
      headers: { connection: "Upgrade", upgrade: "websocket", "sec-websocket-version": "13", "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==" },
    });
    req.on("response", (res) => {
      let b = ""; res.on("data", (c) => (b += c));
      res.on("end", () => resolve({ status: res.statusCode ?? 0, body: b ? JSON.parse(b) : null }));
    });
    req.on("upgrade", (res, sock) => { sock.destroy(); resolve({ status: res.statusCode ?? 0, body: null }); });
    req.on("error", reject);
    req.end();
  });
}

describe("WebSocket proxy", () => {
  test("kết nối qua router: upstream nhận key thật (không phải router key), tin nhắn đi hai chiều", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    const c = await wsOpen(token);
    assert.equal(c.first, "hello:key-A-aaaaaaaa");
    assert.equal(await c.echo("ping"), "ping");
    assert.ok(!fake.calls[0]?.url.includes(token), "router key không được lọt lên upstream");
    assert.match(fake.calls[0]?.url ?? "", /type=buy/, "query khác được giữ nguyên");
    c.close();
  });

  test("request WS xuất hiện trong dashboard (method WS, status 101) và tăng bộ đếm", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    const before = (await router.state()).clients.find((c) => c.label === "test").requests;
    (await wsOpen(token, "/ws/trades")).close();
    const st = await router.state();
    assert.equal(st.logs[0].method, "WS");
    assert.equal(st.logs[0].status, 101);
    assert.equal(st.logs[0].path, "/ws/trades");
    assert.equal(st.clients.find((c) => c.label === "test").requests, before + 1);
  });

  test("round-robin giữa các kết nối WS", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    await router.addUpstream("B", "key-B-bbbbbbbb");
    const conns = [await wsOpen(token), await wsOpen(token), await wsOpen(token), await wsOpen(token)];
    const used = conns.map((c) => c.first);
    conns.forEach((c) => c.close());
    assert.equal(new Set(used).size, 2, `phải dùng cả 2 key: ${used}`);
    assert.notEqual(used[0], used[1]);
  });

  test("thiếu / sai router key → 401, không gọi upstream", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    assert.equal((await wsRaw(null)).status, 401);
    const r = await wsRaw("sr_sai");
    assert.equal(r.status, 401);
    assert.equal(r.body.error, "invalid_router_key");
    assert.equal(fake.calls.length, 0);
  });

  test("402 ở handshake → failover sang key khác, key đầu thành exhausted", async () => {
    await fresh();
    const a = await router.addUpstream("A", "key-A-aaaaaaaa");
    await router.addUpstream("B", "key-B-bbbbbbbb");
    fake.behavior.set("key-A-aaaaaaaa", () => ({ status: 402, body: { remaining: 0 } }));
    const c = await wsOpen(token);
    assert.equal(c.first, "hello:key-B-bbbbbbbb");
    assert.equal(router.upstreamRow(a).status, "exhausted");
    assert.equal((await router.state()).logs[0].attempts, 2);
    c.close();
  });

  test("401 ở handshake → key invalid + failover", async () => {
    await fresh();
    const a = await router.addUpstream("A", "key-A-aaaaaaaa");
    await router.addUpstream("B", "key-B-bbbbbbbb");
    fake.behavior.set("key-A-aaaaaaaa", () => ({ status: 401, body: {} }));
    const c = await wsOpen(token);
    assert.equal(c.first, "hello:key-B-bbbbbbbb");
    assert.equal(router.upstreamRow(a).status, "invalid");
    c.close();
  });

  test("429 ở handshake → cooldown + failover", async () => {
    await fresh();
    const a = await router.addUpstream("A", "key-A-aaaaaaaa");
    await router.addUpstream("B", "key-B-bbbbbbbb");
    fake.behavior.set("key-A-aaaaaaaa", () => ({ status: 429 }));
    const c = await wsOpen(token);
    assert.equal(c.first, "hello:key-B-bbbbbbbb");
    assert.ok(router.upstreamRow(a).cooldown_until > Date.now());
    c.close();
  });

  test("mọi key đều 402 → 402 all_keys_exhausted; không có key → 503", async () => {
    await fresh();
    await router.addUpstream("A", "key-A-aaaaaaaa");
    fake.behavior.set("key-A-aaaaaaaa", () => ({ status: 402, body: { remaining: 0 } }));
    const r = await wsRaw(token);
    assert.equal(r.status, 402);
    assert.equal(r.body.error, "all_keys_exhausted");
    await fresh();
    const r2 = await wsRaw(token);
    assert.equal(r2.status, 503);
    assert.equal(r2.body.error, "no_upstream_keys_available");
  });

  test("không cho nâng cấp WS vào /admin", async () => {
    assert.equal((await wsRaw(token, "/admin/api/state")).status, 404);
  });
});

describe("chống thêm trùng upstream key", () => {
  test("thêm cùng một key lần 2 → 409 báo trùng, kèm tên key đã có; không tạo bản ghi thứ hai", async () => {
    await fresh();
    await router.addUpstream("gốc", "key-DUP-xxxxxxxx");
    const r = await router.admin("POST", "/upstream", { label: "bản sao", key: "key-DUP-xxxxxxxx" });
    assert.equal(r.status, 409);
    const body = (await r.json()) as any;
    assert.equal(body.duplicate, true);
    assert.match(body.error, /đã tồn tại \(gốc\)/);
    assert.ok(!body.error.includes("key-DUP-xxxxxxxx"), "thông báo không được lộ key đầy đủ");
    assert.equal((await router.state()).upstream.length, 1);
  });

  test("khoảng trắng thừa vẫn bị coi là trùng; key khác thì thêm được", async () => {
    await fresh();
    await router.addUpstream("A", "key-DUP-xxxxxxxx");
    assert.equal((await router.admin("POST", "/upstream", { key: "  key-DUP-xxxxxxxx \n" })).status, 409);
    assert.equal((await router.admin("POST", "/upstream", { key: "key-OTHER-yyyyyyyy" })).status, 201);
  });

  test("xoá key rồi thêm lại cùng key thì được phép", async () => {
    await fresh();
    const id = await router.addUpstream("A", "key-DUP-xxxxxxxx");
    await router.admin("DELETE", `/upstream/${id}`);
    assert.equal((await router.admin("POST", "/upstream", { key: "key-DUP-xxxxxxxx" })).status, 201);
  });

  test("DB cũ chưa có key_hash: khởi động lại tự điền và vẫn phát hiện trùng với key đã lưu", async () => {
    const r2 = new Router();
    await r2.start(fake.url);
    try {
      await r2.login();
      await r2.addUpstream("cũ", "key-LEGACY-zzzzzzzz");
      // giả lập DB từ phiên bản trước: xoá cột hash của hàng
      const d = r2.db(); d.exec("DROP INDEX idx_upstream_key_hash"); d.exec("UPDATE upstream_keys SET key_hash = NULL"); d.close();
      r2.proc.kill(); await new Promise((r) => r2.proc.once("exit", r));
      const port = await freePort();
      r2.base = `http://127.0.0.1:${port}`;
      r2.proc = spawn(process.execPath, [fileURLToPath(new URL("./server.js", import.meta.url))], {
        env: { ...process.env, ADMIN_PASSWORD: r2.password, PORT: String(port), DATA_DIR: r2.dataDir, UPSTREAM_URL: fake.url, APP_SECRET: "" }, stdio: "ignore",
      });
      for (let i = 0; i < 100; i++) { try { if ((await fetch(`${r2.base}/healthz`)).ok) break; } catch { /* chưa lên */ } await new Promise((r) => setTimeout(r, 50)); }
      await r2.login();
      assert.equal((await r2.admin("POST", "/upstream", { key: "key-LEGACY-zzzzzzzz" })).status, 409);
    } finally { r2.stop(); }
  });
});
