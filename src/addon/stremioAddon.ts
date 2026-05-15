import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { request } from 'undici';
import { AppConfig, redactUrl } from '../config.js';

type StremioManifest = {
  id: string;
  version: string;
  name: string;
  description?: string;
  resources?: Array<string | { name: string; types?: string[]; idPrefixes?: string[] }>;
  types?: string[];
  catalogs?: unknown[];
  idPrefixes?: string[];
  behaviorHints?: Record<string, unknown>;
};

type StremioStream = {
  name?: string;
  title?: string;
  url?: string;
  externalUrl?: string;
  infoHash?: string;
  fileIdx?: number;
  behaviorHints?: Record<string, unknown>;
  [key: string]: unknown;
};

type StreamResponse = {
  streams?: StremioStream[];
  [key: string]: unknown;
};

type RouteParams = {
  type: string;
  id: string;
  extra?: string;
};

export function registerStremioAddonRoutes(server: FastifyInstance, config: AppConfig): void {
  const addon = new StremioAddon(config);

  server.get('/manifest.json', async (request, reply) => addon.manifest(request, reply));
  server.get('/stream/:type/:id.json', async (request: FastifyRequest<{ Params: RouteParams }>, reply) => addon.stream(request, reply));
  server.get('/stream/:type/:id/:extra.json', async (request: FastifyRequest<{ Params: RouteParams }>, reply) => addon.stream(request, reply));
}

export class StremioAddon {
  constructor(private readonly config: AppConfig) {}

  async manifest(request: FastifyRequest, reply: FastifyReply): Promise<FastifyReply> {
    const upstream = this.config.aiostreamsAddonUrl;
    let manifest: StremioManifest | undefined;
    if (upstream) {
      try {
        manifest = await fetchJson<StremioManifest>(joinAddonUrl(upstream, 'manifest.json'), this.config.requestTimeoutMs);
      } catch (error) {
        request.log.warn({ err: error, upstream: redactUrl(upstream) }, 'Failed to fetch upstream Stremio manifest');
      }
    }

    const response: StremioManifest = {
      ...(manifest ?? {}),
      id: this.config.addonId,
      name: this.config.addonName,
      version: manifest?.version ?? '0.1.0',
      description: `Streams from ${manifest?.name ?? 'AIOStreams'} through the local proxy cache.`,
      resources: normalizeResources(manifest?.resources),
      types: manifest?.types ?? ['movie', 'series'],
      catalogs: manifest?.catalogs ?? [],
      behaviorHints: {
        ...(manifest?.behaviorHints ?? {}),
        configurable: false,
        configurationRequired: !upstream
      }
    };

    return reply.header('access-control-allow-origin', '*').send(response);
  }

  async stream(request: FastifyRequest<{ Params: RouteParams }>, reply: FastifyReply): Promise<FastifyReply> {
    if (!this.config.aiostreamsAddonUrl) {
      return reply.code(503).send({ streams: [], error: 'AIOSTREAMS_ADDON_URL is not configured' });
    }

    const upstreamPath = buildStreamPath(request.params);
    const upstreamUrl = joinAddonUrl(this.config.aiostreamsAddonUrl, upstreamPath);
    let upstreamResponse: StreamResponse;
    try {
      upstreamResponse = await fetchJson<StreamResponse>(upstreamUrl, this.config.requestTimeoutMs);
    } catch (error) {
      request.log.warn({ err: error, upstream: redactUrl(upstreamUrl) }, 'Failed to fetch upstream Stremio streams');
      return reply.code(502).send({ streams: [] });
    }

    const publicBaseUrl = this.publicBaseUrl(request);
    const streams = (upstreamResponse.streams ?? []).map((stream) => rewriteStream(stream, publicBaseUrl, this.config.authToken));
    return reply.header('access-control-allow-origin', '*').send({
      ...upstreamResponse,
      streams
    });
  }

  private publicBaseUrl(request: FastifyRequest): string {
    if (this.config.addonPublicBaseUrl) return trimTrailingSlash(this.config.addonPublicBaseUrl);
    const proto = request.headers['x-forwarded-proto']?.toString().split(',')[0] ?? 'http';
    return `${proto}://${request.headers.host}`;
  }
}

export function rewriteStream(stream: StremioStream, publicBaseUrl: string, authToken?: string): StremioStream {
  if (!stream.url || !isHttpUrl(stream.url)) return stream;

  const proxied = new URL('/stream', `${trimTrailingSlash(publicBaseUrl)}/`);
  proxied.searchParams.set('url', stream.url);
  const behaviorHints: Record<string, unknown> = {
    ...(stream.behaviorHints ?? {}),
    notWebReady: true
  };

  if (authToken) {
    const existingProxyHeaders = behaviorHints.proxyHeaders as { request?: Record<string, string> } | undefined;
    behaviorHints.proxyHeaders = {
      ...(existingProxyHeaders ?? {}),
      request: {
        ...(existingProxyHeaders?.request ?? {}),
        Authorization: `Bearer ${authToken}`
      }
    };
  }

  return {
    ...stream,
    name: stream.name ? `${stream.name} + Cache` : 'Proxy Cache',
    url: proxied.toString(),
    behaviorHints
  };
}

function normalizeResources(resources?: StremioManifest['resources']): StremioManifest['resources'] {
  const hasStream = resources?.some((resource) => (typeof resource === 'string' ? resource === 'stream' : resource.name === 'stream'));
  if (resources && hasStream) return resources;
  return [...(resources ?? []), 'stream'];
}

function buildStreamPath(params: RouteParams): string {
  const id = params.id.endsWith('.json') ? params.id : `${params.id}.json`;
  if (!params.extra) return `stream/${encodeURIComponent(params.type)}/${id}`;
  const extra = params.extra.endsWith('.json') ? params.extra : `${params.extra}.json`;
  return `stream/${encodeURIComponent(params.type)}/${encodeURIComponent(params.id)}/${extra}`;
}

function joinAddonUrl(base: string, path: string): string {
  return `${trimTrailingSlash(base)}/${path.replace(/^\/+/, '')}`;
}

function trimTrailingSlash(value: string): string {
  return value.replace(/\/+$/, '');
}

function isHttpUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

async function fetchJson<T>(url: string, timeoutMs: number): Promise<T> {
  const response = await request(url, {
    method: 'GET',
    headersTimeout: timeoutMs,
    bodyTimeout: timeoutMs
  });
  if (response.statusCode < 200 || response.statusCode >= 300) {
    await discardBody(response.body);
    throw new Error(`Upstream addon returned ${response.statusCode}`);
  }
  return (await response.body.json()) as T;
}

async function discardBody(body: { dump: (opts?: { limit: number }) => Promise<void> }): Promise<void> {
  try {
    await body.dump({ limit: 1024 * 1024 });
  } catch {
    // Best-effort cleanup for failed addon responses.
  }
}
