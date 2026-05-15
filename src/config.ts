import 'dotenv/config';
import { z } from 'zod';

const boolish = z
  .string()
  .optional()
  .transform((value) => value === 'true' || value === '1');

const optionalInt = z
  .string()
  .optional()
  .transform((value) => (value == null || value === '' ? undefined : Number.parseInt(value, 10)));

const envSchema = z.object({
  HOST: z.string().default('127.0.0.1'),
  PORT: z.coerce.number().int().positive().default(3000),
  CACHE_DIR: z.string().default('./data/cache'),
  DATABASE_PATH: z.string().default('./data/cache.sqlite'),
  MAX_CACHE_BYTES: z.coerce.number().int().positive().default(10 * 1024 * 1024 * 1024),
  CHUNK_SIZE_BYTES: z.coerce.number().int().positive().default(4 * 1024 * 1024),
  ALLOWLIST_HOSTS: z.string().optional().default(''),
  AUTH_TOKEN: z.string().optional().default(''),
  RATE_LIMIT_MAX: z.coerce.number().int().positive().default(120),
  RATE_LIMIT_WINDOW: z.string().default('1 minute'),
  REQUEST_TIMEOUT_MS: z.coerce.number().int().positive().default(120_000),
  MAX_UPSTREAM_REDIRECTS: z.coerce.number().int().min(0).default(3),
  MIN_CACHEABLE_BYTES: optionalInt,
  MAX_CACHEABLE_BYTES: optionalInt,
  TRUST_PROXY: boolish,
  LOG_LEVEL: z.string().default('info'),
  AIOSTREAMS_ADDON_URL: z.string().optional().default(''),
  ADDON_PUBLIC_BASE_URL: z.string().optional().default(''),
  ADDON_NAME: z.string().optional().default('Proxy Cache Wrapper'),
  ADDON_ID: z.string().optional().default('community.proxy-cache-wrapper'),
  ADDON_AUTH_TOKEN: z.string().optional().default('')
});

const env = envSchema.parse(process.env);

export type AppConfig = {
  host: string;
  port: number;
  cacheDir: string;
  databasePath: string;
  maxCacheBytes: number;
  chunkSizeBytes: number;
  allowlistHosts: string[];
  authToken?: string;
  rateLimitMax: number;
  rateLimitWindow: string;
  requestTimeoutMs: number;
  maxUpstreamRedirects: number;
  minCacheableBytes?: number;
  maxCacheableBytes?: number;
  trustProxy: boolean;
  logLevel: string;
  allowPrivateUpstreamsForTesting?: boolean;
  aiostreamsAddonUrl?: string;
  addonPublicBaseUrl?: string;
  addonName: string;
  addonId: string;
  addonAuthToken?: string;
};

export const config: AppConfig = {
  host: env.HOST,
  port: env.PORT,
  cacheDir: env.CACHE_DIR,
  databasePath: env.DATABASE_PATH,
  maxCacheBytes: env.MAX_CACHE_BYTES,
  chunkSizeBytes: env.CHUNK_SIZE_BYTES,
  allowlistHosts: env.ALLOWLIST_HOSTS.split(',').map((host) => host.trim().toLowerCase()).filter(Boolean),
  authToken: env.AUTH_TOKEN || undefined,
  rateLimitMax: env.RATE_LIMIT_MAX,
  rateLimitWindow: env.RATE_LIMIT_WINDOW,
  requestTimeoutMs: env.REQUEST_TIMEOUT_MS,
  maxUpstreamRedirects: env.MAX_UPSTREAM_REDIRECTS,
  minCacheableBytes: env.MIN_CACHEABLE_BYTES,
  maxCacheableBytes: env.MAX_CACHEABLE_BYTES,
  trustProxy: env.TRUST_PROXY,
  logLevel: env.LOG_LEVEL,
  allowPrivateUpstreamsForTesting: false,
  aiostreamsAddonUrl: env.AIOSTREAMS_ADDON_URL || undefined,
  addonPublicBaseUrl: env.ADDON_PUBLIC_BASE_URL || undefined,
  addonName: env.ADDON_NAME,
  addonId: env.ADDON_ID,
  addonAuthToken: env.ADDON_AUTH_TOKEN || undefined
};

export function redactUrl(rawUrl: string): string {
  try {
    const url = new URL(rawUrl);
    url.search = url.search ? '?redacted' : '';
    return url.toString();
  } catch {
    return '[invalid-url]';
  }
}
