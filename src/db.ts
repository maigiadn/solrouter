import { DatabaseSync } from "node:sqlite";
import { join } from "node:path";
import { config } from "./config.js";
import { decrypt, fingerprint } from "./crypto.js";

export const db = new DatabaseSync(join(config.dataDir, "solrouter.db"));
db.exec(`
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS upstream_keys (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  label TEXT NOT NULL,
  key_enc TEXT NOT NULL,
  key_mask TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'active',      -- active | exhausted | invalid
  cooldown_until INTEGER NOT NULL DEFAULT 0,  -- ms epoch, lỗi tạm thời
  retry_at INTEGER NOT NULL DEFAULT 0,        -- ms epoch, thử lại key exhausted
  credits_remaining INTEGER,
  requests INTEGER NOT NULL DEFAULT 0,
  errors INTEGER NOT NULL DEFAULT 0,
  last_used_at INTEGER,
  last_error TEXT,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS client_keys (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  label TEXT NOT NULL,
  token_hash TEXT NOT NULL UNIQUE,
  token_prefix TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  requests INTEGER NOT NULL DEFAULT 0,
  credits_used INTEGER NOT NULL DEFAULT 0,
  last_used_at INTEGER,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS request_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  client_id INTEGER,
  upstream_id INTEGER,
  method TEXT NOT NULL,
  path TEXT NOT NULL,
  status INTEGER NOT NULL,
  credits INTEGER,
  attempts INTEGER NOT NULL DEFAULT 1,
  ms INTEGER NOT NULL
);
`);

// Migration: cột key_hash để phát hiện key trùng (key cũ được điền lại bằng cách giải mã một lần)
if (!(db.prepare("PRAGMA table_info(upstream_keys)").all() as { name: string }[]).some((c) => c.name === "key_hash")) {
  db.exec("ALTER TABLE upstream_keys ADD COLUMN key_hash TEXT");
}
for (const r of db.prepare("SELECT id, key_enc FROM upstream_keys WHERE key_hash IS NULL").all() as { id: number; key_enc: string }[]) {
  try { db.prepare("UPDATE upstream_keys SET key_hash = ? WHERE id = ?").run(fingerprint(decrypt(r.key_enc)), r.id); }
  catch { console.error(`Không giải mã được upstream key #${r.id} (APP_SECRET đã đổi?)`); }
}
// Nếu DB cũ đã có sẵn key trùng thì bỏ qua index, vẫn chặn thêm trùng ở tầng API
try { db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_upstream_key_hash ON upstream_keys(key_hash)"); }
catch { console.error("DB đang có API key trùng nhau — hãy xoá bớt; chưa tạo được unique index"); }

export interface UpstreamRow {
  id: number;
  label: string;
  key_enc: string;
  key_mask: string;
  enabled: number;
  status: "active" | "exhausted" | "invalid";
  cooldown_until: number;
  retry_at: number;
  credits_remaining: number | null;
  requests: number;
  errors: number;
  last_used_at: number | null;
  last_error: string | null;
  created_at: number;
}

export interface ClientRow {
  id: number;
  label: string;
  token_hash: string;
  token_prefix: string;
  enabled: number;
  requests: number;
  credits_used: number;
  last_used_at: number | null;
  created_at: number;
}
