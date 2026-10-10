import { config } from "./config.js";
import { decrypt } from "./crypto.js";
import { db, type UpstreamRow } from "./db.js";

let lastId = 0;

/**
 * Trả về các key khả dụng theo thứ tự round-robin (bắt đầu từ key kế tiếp
 * sau key được chọn lần trước). Key `exhausted` đến hạn thử lại xếp cuối.
 */
export function orderedCandidates(now = Date.now()): UpstreamRow[] {
  const rows = db
    .prepare("SELECT * FROM upstream_keys WHERE enabled = 1 AND status != 'invalid' AND cooldown_until <= ? ORDER BY id")
    .all(now) as unknown as UpstreamRow[];
  const active = rows.filter((r) => r.status === "active");
  const retry = rows.filter((r) => r.status === "exhausted" && r.retry_at <= now);
  const start = active.findIndex((r) => r.id > lastId);
  const rotated = start <= 0 ? active : [...active.slice(start), ...active.slice(0, start)];
  const first = rotated[0];
  if (first) lastId = first.id;
  return [...rotated, ...retry];
}

export const secretOf = (row: UpstreamRow) => decrypt(row.key_enc);

export function markUsed(id: number, remaining: number | null) {
  db.prepare(
    `UPDATE upstream_keys SET requests = requests + 1, last_used_at = ?, status = 'active', last_error = NULL,
       credits_remaining = COALESCE(?, credits_remaining) WHERE id = ?`,
  ).run(Date.now(), remaining, id);
}

export function markExhausted(id: number, reason: string) {
  db.prepare("UPDATE upstream_keys SET status = 'exhausted', credits_remaining = 0, retry_at = ?, errors = errors + 1, last_error = ? WHERE id = ?")
    .run(Date.now() + config.exhaustedRetryMs, reason, id);
}

export function markInvalid(id: number, reason: string) {
  db.prepare("UPDATE upstream_keys SET status = 'invalid', errors = errors + 1, last_error = ? WHERE id = ?").run(reason, id);
}

export function markTransient(id: number, reason: string) {
  db.prepare("UPDATE upstream_keys SET cooldown_until = ?, errors = errors + 1, last_error = ? WHERE id = ?")
    .run(Date.now() + config.transientCooldownMs, reason, id);
}

export function setRemaining(id: number, remaining: number) {
  db.prepare("UPDATE upstream_keys SET credits_remaining = ? WHERE id = ?").run(remaining, id);
}
