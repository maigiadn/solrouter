import { createCipheriv, createDecipheriv, createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { config } from "./config.js";

function loadSecret(): string {
  if (config.appSecret) return config.appSecret;
  const file = join(config.dataDir, "secret.key");
  if (existsSync(file)) return readFileSync(file, "utf8").trim();
  const secret = randomBytes(32).toString("hex");
  writeFileSync(file, secret, { mode: 0o600 });
  return secret;
}

const secret = loadSecret();
const aesKey = createHash("sha256").update("enc:" + secret).digest();
const hmacKey = createHash("sha256").update("hmac:" + secret).digest();

export function encrypt(plain: string): string {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", aesKey, iv);
  const ct = Buffer.concat([cipher.update(plain, "utf8"), cipher.final()]);
  return Buffer.concat([iv, cipher.getAuthTag(), ct]).toString("base64");
}

export function decrypt(blob: string): string {
  const buf = Buffer.from(blob, "base64");
  const decipher = createDecipheriv("aes-256-gcm", aesKey, buf.subarray(0, 12));
  decipher.setAuthTag(buf.subarray(12, 28));
  return Buffer.concat([decipher.update(buf.subarray(28)), decipher.final()]).toString("utf8");
}

/** Dấu vân tay của API key (HMAC theo secret) để phát hiện key trùng mà không cần giải mã từng dòng. */
export const fingerprint = (key: string) => createHmac("sha256", hmacKey).update("key:" + key).digest("hex");

export const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");

export function safeEqual(a: string, b: string): boolean {
  return timingSafeEqual(createHash("sha256").update(a).digest(), createHash("sha256").update(b).digest());
}

export function signSession(ttlMs: number): string {
  const exp = String(Date.now() + ttlMs);
  return `${exp}.${createHmac("sha256", hmacKey).update(exp).digest("hex")}`;
}

export function verifySession(token: string | undefined): boolean {
  if (!token) return false;
  const [exp, sig] = token.split(".");
  if (!exp || !sig || Number(exp) < Date.now()) return false;
  const expected = createHmac("sha256", hmacKey).update(exp).digest("hex");
  return sig.length === expected.length && timingSafeEqual(Buffer.from(sig), Buffer.from(expected));
}

export const newClientToken = () => "sr_" + randomBytes(24).toString("hex");

export function maskKey(key: string): string {
  return key.length <= 12 ? "****" : `${key.slice(0, 6)}…${key.slice(-4)}`;
}
