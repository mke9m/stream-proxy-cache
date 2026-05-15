import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import http from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { AddressInfo } from 'node:net';
import { buildServer } from '../src/routes.js';
import { AppConfig } from '../src/config.js';

const media = Buffer.alloc(256 * 1024, 0).map((_, index) => index % 251);

function createRangeServer(delayMs = 0) {
  const requests: string[] = [];
  const server = http.createServer((req, res) => {
    if (req.url === '/redirect.bin') {
      res.writeHead(302, { location: `http://${req.headers.host}/media.bin` }).end();
      return;
    }
    if (req.url !== '/media.bin') {
      res.writeHead(404).end();
      return;
    }
    requests.push(req.headers.range ?? 'full');
    const contentType = 'application/octet-stream';
    if (req.method === 'HEAD') {
      res.writeHead(200, {
        'content-length': media.length,
        'content-type': contentType,
        'accept-ranges': 'bytes'
      }).end();
      return;
    }
    const range = req.headers.range;
    let start = 0;
    let end = media.length - 1;
    let status = 200;
    if (range) {
      const match = /^bytes=(\d+)-(\d*)$/.exec(range);
      if (!match || Number.parseInt(match[1], 10) >= media.length) {
        res.writeHead(416, { 'content-range': `bytes */${media.length}` }).end();
        return;
      }
      start = Number.parseInt(match[1], 10);
      end = match[2] ? Math.min(Number.parseInt(match[2], 10), media.length - 1) : media.length - 1;
      status = 206;
    }
    res.writeHead(status, {
      'content-length': end - start + 1,
      'content-type': contentType,
      'accept-ranges': 'bytes',
      ...(status === 206 ? { 'content-range': `bytes ${start}-${end}/${media.length}` } : {})
    });
    const first = media.subarray(start, Math.min(start + 8192, end + 1));
    res.write(first);
    setTimeout(() => res.end(media.subarray(start + first.length, end + 1)), delayMs);
  });
  return { server, requests };
}

describe('stream proxy integration', () => {
  let tempDir: string;
  let upstream: http.Server;
  let upstreamUrl: string;
  let app: Awaited<ReturnType<typeof buildServer>>;
  let requests: string[];

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'stream-cache-test-'));
    const created = createRangeServer(150);
    upstream = created.server;
    requests = created.requests;
    await new Promise<void>((resolve) => upstream.listen(0, '127.0.0.1', resolve));
    const port = (upstream.address() as AddressInfo).port;
    upstreamUrl = `http://127.0.0.1:${port}/media.bin`;

    const config: AppConfig = {
      host: '127.0.0.1',
      port: 0,
      cacheDir: join(tempDir, 'cache'),
      databasePath: join(tempDir, 'cache.sqlite'),
      maxCacheBytes: 1024 * 1024 * 100,
      chunkSizeBytes: 64 * 1024,
      allowlistHosts: [],
      rateLimitMax: 1000,
      rateLimitWindow: '1 minute',
      requestTimeoutMs: 5000,
      maxUpstreamRedirects: 3,
      trustProxy: false,
      logLevel: 'silent',
      allowPrivateUpstreamsForTesting: true,
      addonName: 'Test Addon',
      addonId: 'test.proxy-cache'
    };
    app = await buildServer(config);
  });

  afterEach(async () => {
    await app.close();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
    await rm(tempDir, { recursive: true, force: true });
  });

  it('streams first bytes before the upstream response completes', async () => {
    const started = Date.now();
    const response = await app.inject({
      method: 'GET',
      url: `/stream?url=${encodeURIComponent(upstreamUrl)}`,
      headers: { range: 'bytes=0-65535' }
    });
    expect(response.statusCode).toBe(206);
    expect(response.body.length).toBe(65536);
    expect(Date.now() - started).toBeLessThan(1000);
    expect(response.headers['content-range']).toBe(`bytes 0-65535/${media.length}`);
  });

  it('serves repeated ranges from cache', async () => {
    const target = `/stream?url=${encodeURIComponent(upstreamUrl)}`;
    const first = await app.inject({ method: 'GET', url: target, headers: { range: 'bytes=0-1023' } });
    expect(first.statusCode).toBe(206);
    const upstreamAfterFirst = requests.length;
    const second = await app.inject({ method: 'GET', url: target, headers: { range: 'bytes=0-1023' } });
    expect(second.statusCode).toBe(206);
    expect(Buffer.compare(second.rawPayload, media.subarray(0, 1024))).toBe(0);
    expect(requests.length).toBe(upstreamAfterFirst);
  });

  it('fetches only a sought uncached chunk', async () => {
    const target = `/stream?url=${encodeURIComponent(upstreamUrl)}`;
    await app.inject({ method: 'GET', url: target, headers: { range: 'bytes=0-1023' } });
    const upstreamAfterFirst = requests.length;
    const seek = await app.inject({ method: 'GET', url: target, headers: { range: 'bytes=131072-132095' } });
    expect(seek.statusCode).toBe(206);
    expect(Buffer.compare(seek.rawPayload, media.subarray(131072, 132096))).toBe(0);
    expect(requests.length).toBe(upstreamAfterFirst + 1);
    expect(requests.at(-1)).toBe('bytes=131072-196607');
  });

  it('returns 416 for unsatisfiable ranges', async () => {
    const response = await app.inject({
      method: 'GET',
      url: `/stream?url=${encodeURIComponent(upstreamUrl)}`,
      headers: { range: `bytes=${media.length}-` }
    });
    expect(response.statusCode).toBe(416);
    expect(response.headers['content-range']).toBe(`bytes */${media.length}`);
  });

  it('follows configured upstream redirects', async () => {
    const redirectUrl = upstreamUrl.replace('/media.bin', '/redirect.bin');
    const response = await app.inject({
      method: 'GET',
      url: `/stream?url=${encodeURIComponent(redirectUrl)}`,
      headers: { range: 'bytes=0-1023' }
    });
    expect(response.statusCode).toBe(206);
    expect(Buffer.compare(response.rawPayload, media.subarray(0, 1024))).toBe(0);
  });
});
