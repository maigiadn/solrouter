import { DatabaseSync } from "node:sqlite";
import { join } from "node:path";
import { config } from "./config.js";

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
