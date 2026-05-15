import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import http from 'node:http';
import { AddressInfo } from 'node:net';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildServer } from '../src/routes.js';
import { AppConfig } from '../src/config.js';
import { rewriteStream } from '../src/addon/stremioAddon.js';

describe('Stremio addon wrapper', () => {
  let tempDir: string;
  let upstream: http.Server;
  let upstreamBaseUrl: string;
  let app: Awaited<ReturnType<typeof buildServer>>;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), 'stremio-addon-test-'));
    upstream = http.createServer((req, res) => {
      if (req.url === '/manifest.json') {
        res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({
          id: 'aiostreams.test',
          version: '1.2.3',
          name: 'AIOStreams',
          resources: ['stream'],
          types: ['movie', 'series'],
          catalogs: []
        }));
        return;
      }
      if (req.url === '/stream/movie/tt1234567.json') {
        res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({
          streams: [
            {
              name: 'TorBox',
              title: 'TorBox 1080p',
              url: 'https://cdn.torbox.example/video.mp4?token=secret',
              behaviorHints: { filename: 'video.mp4' }
            },
            {
              name: 'Magnet fallback',
              infoHash: 'abc123'
            }
          ]
        }));
        return;
      }
      res.writeHead(404).end();
    });
    await new Promise<void>((resolve) => upstream.listen(0, '127.0.0.1', resolve));
    upstreamBaseUrl = `http://127.0.0.1:${(upstream.address() as AddressInfo).port}`;

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
      maxUpstreamRedirects: 0,
      trustProxy: false,
      logLevel: 'silent',
      allowPrivateUpstreamsForTesting: true,
      aiostreamsAddonUrl: upstreamBaseUrl,
      addonPublicBaseUrl: 'http://proxy.local:3000',
      addonName: 'AIOStreams Proxy Cache',
      addonId: 'test.aiostreams.proxy',
      prefetchEnabled: false,
      prefetchConcurrency: 2,
      prefetchStartAheadChunks: 2,
      prefetchRetryAfterMs: 600000
    };
    app = await buildServer(config);
  });

  afterEach(async () => {
    await app.close();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
    await rm(tempDir, { recursive: true, force: true });
  });

  it('wraps the upstream manifest', async () => {
    const response = await app.inject({ method: 'GET', url: '/manifest.json' });
    expect(response.statusCode).toBe(200);
    const body = response.json();
    expect(body.id).toBe('test.aiostreams.proxy');
    expect(body.name).toBe('AIOStreams Proxy Cache');
    expect(body.resources).toContain('stream');
  });

  it('rewrites AIOStreams direct URLs through the proxy cache', async () => {
    const response = await app.inject({ method: 'GET', url: '/stream/movie/tt1234567.json' });
    expect(response.statusCode).toBe(200);
    const body = response.json();
    expect(body.streams[0].name).toBe('TorBox + Cache');
    expect(body.streams[0].url).toBe(
      'http://proxy.local:3000/stream?url=https%3A%2F%2Fcdn.torbox.example%2Fvideo.mp4%3Ftoken%3Dsecret'
    );
    expect(body.streams[0].behaviorHints.notWebReady).toBe(true);
    expect(body.streams[1].infoHash).toBe('abc123');
    expect(body.streams[1].url).toBeUndefined();
  });
});

describe('rewriteStream', () => {
  it('adds proxy auth headers when AUTH_TOKEN is set', () => {
    const stream = rewriteStream({ url: 'https://example.com/file.mp4' }, 'http://localhost:3000', 'secret-token');
    expect(stream.url).toBe('http://localhost:3000/stream?url=https%3A%2F%2Fexample.com%2Ffile.mp4');
    expect(stream.behaviorHints?.proxyHeaders).toEqual({
      request: {
        Authorization: 'Bearer secret-token'
      }
    });
  });
});
