import Fastify, { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import rateLimit from '@fastify/rate-limit';
import { AppConfig } from './config.js';
import { CacheStore } from './cache/cacheStore.js';
import { FileCache } from './cache/fileCache.js';
import { StreamProxy } from './proxy/streamProxy.js';
import { proxyStats } from './proxy/stats.js';
import { registerStremioAddonRoutes } from './addon/stremioAddon.js';

export async function buildServer(config: AppConfig): Promise<FastifyInstance> {
  const store = new CacheStore(config.databasePath);
  const fileCache = new FileCache(config.cacheDir, config.chunkSizeBytes, store);
  await fileCache.ensureDirs();
  const streamProxy = new StreamProxy(config, store, fileCache);

  const server = Fastify({
    logger: { level: config.logLevel, redact: ['req.query.url'] },
    trustProxy: config.trustProxy
  });

  await server.register(rateLimit, {
    max: config.rateLimitMax,
    timeWindow: config.rateLimitWindow
  });

  server.addHook('onRequest', async (request, reply) => {
    if (!config.authToken) return;
    if (request.url.startsWith('/health')) return;
    if (request.url.startsWith('/manifest.json')) return;
    if (request.url.startsWith('/stream/')) return;
    const auth = request.headers.authorization;
    if (auth !== `Bearer ${config.authToken}`) {
      return reply.code(401).send({ error: 'Unauthorized' });
    }
  });

  server.get('/health', async () => ({ ok: true }));
  registerStremioAddonRoutes(server, config);

  server.route({
    method: ['GET', 'HEAD'],
    url: '/stream',
    handler: (request: FastifyRequest<{ Querystring: { url?: string } }>, reply: FastifyReply) => streamProxy.handleStream(request, reply)
  });

  server.get('/cache/stats', async () => ({
    ...proxyStats,
    ...store.getStats()
  }));

  server.get('/cache/items', async () =>
    store.listItems().map((item) => ({
      ...item,
      url: '[redacted]'
    }))
  );

  server.post('/cache/cleanup', async () => streamProxy.cleanup());

  server.addHook('onClose', async () => {
    store.close();
  });

  return server;
}
