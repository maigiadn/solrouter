import { mkdirSync } from "node:fs";

const adminPassword = process.env.ADMIN_PASSWORD;
if (!adminPassword) {
  console.error("Thiếu biến môi trường ADMIN_PASSWORD");
  process.exit(1);
}

export const config = {
  port: Number(process.env.PORT ?? 3000),
  dataDir: process.env.DATA_DIR ?? "./data",
  upstreamUrl: (process.env.UPSTREAM_URL ?? "https://api.fomoapi.io").replace(/\/+$/, ""),
  adminPassword,
  appSecret: process.env.APP_SECRET,
  maxBodyBytes: 10 * 1024 * 1024,
  upstreamTimeoutMs: 30_000,
  maxAttempts: 10,
  transientCooldownMs: 30_000,
  exhaustedRetryMs: 30 * 60_000,
};

mkdirSync(config.dataDir, { recursive: true });
