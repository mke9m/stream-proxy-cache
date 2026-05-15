import { FastifyReply, FastifyRequest } from 'fastify';
import { createWriteStream } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { Readable } from 'node:stream';
import { request } from 'undici';
import { AppConfig, redactUrl } from '../config.js';
import { CacheItem, CacheStore } from '../cache/cacheStore.js';
import { FileCache, cacheKeyForUrl } from '../cache/fileCache.js';
import { parseRangeHeader, resolveRange, ByteRange } from '../cache/rangeMap.js';
import { assertUrlAllowed } from '../security/urlPolicy.js';
import { proxyStats } from './stats.js';

type StreamQuery = {
  url?: string;
};

type UpstreamMetadata = {
  contentLength?: number;
  contentType?: string;
  etag?: string;
  lastModified?: string;
};

export class StreamProxy {
  private readonly inflightChunks = new Map<string, Promise<void>>();

  constructor(
    private readonly config: AppConfig,
    private readonly store: CacheStore,
    private readonly fileCache: FileCache
  ) {}

  async handleStream(request: FastifyRequest<{ Querystring: StreamQuery }>, reply: FastifyReply): Promise<FastifyReply | void> {
    const rawUrl = request.query.url;
    if (!rawUrl) {
      return reply.code(400).send({ error: 'Missing url query parameter' });
    }

    let upstreamUrl: URL;
    try {
      upstreamUrl = await assertUrlAllowed(rawUrl, {
        allowlistHosts: this.config.allowlistHosts,
        allowPrivateUpstreamsForTesting: this.config.allowPrivateUpstreamsForTesting
      });
    } catch (error) {
      request.log.warn({ err: error, url: redactUrl(rawUrl) }, 'Blocked upstream URL');
      return reply.code(400).send({ error: error instanceof Error ? error.message : 'Invalid URL' });
    }

    const normalizedUrl = upstreamUrl.toString();
    const { urlHash, cacheKey } = cacheKeyForUrl(normalizedUrl);
    const item = this.store.upsertItem({ urlHash, cacheKey, url: normalizedUrl });
    this.store.incrementActive(item.id);
    proxyStats.activeStreams += 1;

    try {
      const freshItem = await this.prepareMetadata(item, normalizedUrl, request);
      const parsed = parseRangeHeader(headerString(request.headers.range));
      if (freshItem.contentLength === undefined && (parsed.kind === 'none' || (parsed.kind === 'valid' && parsed.end === undefined))) {
        const status = parsed.kind === 'none' ? 200 : 206;
        const start = parsed.kind === 'valid' ? parsed.start : 0;
        reply.code(status).headers({
          'accept-ranges': 'bytes',
          'content-type': freshItem.contentType ?? 'application/octet-stream',
          'cache-control': 'no-store'
        });
        if (request.method === 'HEAD') {
          this.finishStream(item.id, request);
          return reply.send();
        }
        const stream = Readable.from(this.streamWithAccounting(item.id, this.streamDirectOpen(normalizedUrl, start, request), request));
        return reply.send(stream);
      }
      const resolved = resolveRange(parsed, freshItem.contentLength);

      if (!resolved) {
        const contentRange = freshItem.contentLength !== undefined ? `bytes */${freshItem.contentLength}` : 'bytes */*';
        this.finishStream(item.id, request);
        return reply.code(416).headers({ 'accept-ranges': 'bytes', 'content-range': contentRange }).send();
      }

      const isRangeRequest = parsed.kind !== 'none';
      const responseLength = resolved.end - resolved.start + 1;
      const status = isRangeRequest ? 206 : 200;
      const headers: Record<string, string | number> = {
        'accept-ranges': 'bytes',
        'content-type': freshItem.contentType ?? 'application/octet-stream',
        'cache-control': 'no-store',
        'content-length': responseLength
      };
      if (isRangeRequest && freshItem.contentLength !== undefined) {
        headers['content-range'] = `bytes ${resolved.start}-${resolved.end}/${freshItem.contentLength}`;
      }
      if (freshItem.etag) headers.etag = freshItem.etag;
      if (freshItem.lastModified) headers['last-modified'] = freshItem.lastModified;

      reply.code(status).headers(headers);
      if (request.method === 'HEAD') {
        this.finishStream(item.id, request);
        return reply.send();
      }

      const source = this.shouldCache(freshItem)
        ? this.streamRange(freshItem, normalizedUrl, resolved, request)
        : this.streamDirect(normalizedUrl, resolved, request);
      const stream = Readable.from(this.streamWithAccounting(freshItem.id, source, request));
      return reply.send(stream);
    } catch (error) {
      this.finishStream(item.id, request);
      throw error;
    }
  }

  async cleanup(): Promise<{ removed: number }> {
    const before = this.store.listItems().length;
    await this.fileCache.enforceMaxSize(0);
    return { removed: before - this.store.listItems().length };
  }

  private async prepareMetadata(item: CacheItem, upstreamUrl: string, request: FastifyRequest): Promise<CacheItem> {
    if (item.contentLength !== undefined && item.contentType) return item;
    const metadata = await this.fetchMetadata(upstreamUrl, request);
    this.store.updateMetadata(item.id, metadata);
    return this.store.getById(item.id) ?? item;
  }

  private async fetchMetadata(upstreamUrl: string, request: FastifyRequest): Promise<UpstreamMetadata> {
    try {
      const response = await requestUpstream(upstreamUrl, {
        method: 'HEAD',
        timeoutMs: this.config.requestTimeoutMs,
        maxRedirections: this.config.maxUpstreamRedirects
      });
      if (response.statusCode >= 200 && response.statusCode < 400) {
        return metadataFromHeaders(response.headers);
      }
    } catch (error) {
      request.log.debug({ err: error, url: redactUrl(upstreamUrl) }, 'HEAD metadata lookup failed');
    }

    const response = await requestUpstream(upstreamUrl, {
      method: 'GET',
      range: { start: 0, end: 0 },
      timeoutMs: this.config.requestTimeoutMs,
      maxRedirections: this.config.maxUpstreamRedirects
    });
    const metadata = metadataFromHeaders(response.headers);
    const contentRange = response.headers['content-range'];
    if (typeof contentRange === 'string') {
      const match = /^bytes \d+-\d+\/(\d+|\*)$/.exec(contentRange);
      if (match?.[1] && match[1] !== '*') metadata.contentLength = Number.parseInt(match[1], 10);
    }
    response.body.destroy();
    return metadata;
  }

  private async *streamRange(item: CacheItem, upstreamUrl: string, range: ByteRange, request: FastifyRequest): AsyncGenerator<Buffer> {
    for (const chunkIndex of this.fileCache.chunkIndexesForRange(range)) {
      const bounds = this.fileCache.chunkBounds(chunkIndex, item.contentLength);
      const desired: ByteRange = {
        start: Math.max(range.start, bounds.start),
        end: Math.min(range.end, bounds.end)
      };
      if (this.fileCache.hasChunk(item, chunkIndex)) {
        proxyStats.cacheHits += 1;
        const stream = await this.fileCache.readChunkSlice(item, chunkIndex, desired);
        for await (const chunk of stream) {
          const buffer = Buffer.from(chunk);
          proxyStats.bytesServedFromCache += buffer.length;
          yield buffer;
        }
        continue;
      }

      proxyStats.cacheMisses += 1;
      const key = `${item.id}:${chunkIndex}`;
      const inflight = this.inflightChunks.get(key);
      if (inflight) {
        await inflight;
        if (this.fileCache.hasChunk(item, chunkIndex)) {
          const stream = await this.fileCache.readChunkSlice(item, chunkIndex, desired);
          for await (const chunk of stream) {
            const buffer = Buffer.from(chunk);
            proxyStats.bytesServedFromCache += buffer.length;
            yield buffer;
          }
          continue;
        }
      }
      yield* this.fetchStreamAndCacheChunk(item, upstreamUrl, chunkIndex, desired, request);
    }
  }

  private async *streamDirect(upstreamUrl: string, range: ByteRange, request: FastifyRequest): AsyncGenerator<Buffer> {
    const response = await requestUpstream(upstreamUrl, {
      method: 'GET',
      range,
      timeoutMs: this.config.requestTimeoutMs,
      maxRedirections: this.config.maxUpstreamRedirects
    });
    if (![200, 206].includes(response.statusCode)) {
      throw new Error(`Unexpected upstream status ${response.statusCode}`);
    }
    request.log.debug({ url: redactUrl(upstreamUrl) }, 'Streaming without cache due cache size policy');
    for await (const chunk of response.body) {
      const buffer = Buffer.from(chunk);
      proxyStats.bytesFetchedFromUpstream += buffer.length;
      yield buffer;
    }
  }

  private async *streamDirectOpen(upstreamUrl: string, start: number, request: FastifyRequest): AsyncGenerator<Buffer> {
    const response = await requestUpstream(upstreamUrl, {
      method: 'GET',
      range: start > 0 ? { start } : undefined,
      timeoutMs: this.config.requestTimeoutMs,
      maxRedirections: this.config.maxUpstreamRedirects
    });
    if (![200, 206].includes(response.statusCode)) {
      throw new Error(`Unexpected upstream status ${response.statusCode}`);
    }
    request.log.debug({ url: redactUrl(upstreamUrl) }, 'Streaming upstream with unknown content length');
    for await (const chunk of response.body) {
      const buffer = Buffer.from(chunk);
      proxyStats.bytesFetchedFromUpstream += buffer.length;
      yield buffer;
    }
  }

  private async *fetchStreamAndCacheChunk(
    item: CacheItem,
    upstreamUrl: string,
    chunkIndex: number,
    desired: ByteRange,
    fastifyRequest: FastifyRequest
  ): AsyncGenerator<Buffer> {
    const bounds = this.fileCache.chunkBounds(chunkIndex, item.contentLength);
    const path = this.fileCache.chunkPath(item, chunkIndex);
    await mkdir(dirname(path), { recursive: true });
    const file = createWriteStream(path);
    let written = 0;
    let offset = bounds.start;
    const key = `${item.id}:${chunkIndex}`;
    let resolveInflight!: () => void;
    this.inflightChunks.set(
      key,
      new Promise<void>((resolve) => {
        resolveInflight = resolve;
      })
    );

    try {
      const response = await requestUpstream(upstreamUrl, {
        method: 'GET',
        range: bounds,
        timeoutMs: this.config.requestTimeoutMs,
        maxRedirections: this.config.maxUpstreamRedirects
      });
      if (![200, 206].includes(response.statusCode)) {
        throw new Error(`Unexpected upstream status ${response.statusCode}`);
      }
      for await (const chunk of response.body) {
        const buffer = Buffer.from(chunk);
        file.write(buffer);
        written += buffer.length;
        proxyStats.bytesFetchedFromUpstream += buffer.length;

        const chunkStart = offset;
        const chunkEnd = offset + buffer.length - 1;
        offset += buffer.length;
        if (chunkEnd < desired.start || chunkStart > desired.end) continue;
        const sliceStart = Math.max(0, desired.start - chunkStart);
        const sliceEnd = Math.min(buffer.length, desired.end - chunkStart + 1);
        yield buffer.subarray(sliceStart, sliceEnd);
      }
    } catch (error) {
      fastifyRequest.log.warn({ err: error, url: redactUrl(upstreamUrl) }, 'Upstream streaming failed');
      throw error;
    } finally {
      try {
        await new Promise<void>((resolve, reject) => {
          file.end((error?: Error | null) => (error ? reject(error) : resolve()));
        });
        if (written > 0) {
          this.fileCache.recordChunk(item, chunkIndex, written);
        }
      } finally {
        resolveInflight();
        this.inflightChunks.delete(key);
      }
    }
  }

  private async *streamWithAccounting(itemId: number, source: AsyncGenerator<Buffer>, request: FastifyRequest): AsyncGenerator<Buffer> {
    try {
      yield* source;
    } finally {
      this.finishStream(itemId, request);
    }
  }

  private finishStream(itemId: number, request: FastifyRequest): void {
    this.store.decrementActive(itemId);
    proxyStats.activeStreams = Math.max(0, proxyStats.activeStreams - 1);
    void this.fileCache.enforceMaxSize(this.config.maxCacheBytes).catch((error) => request.log.warn({ err: error }, 'Cache eviction failed'));
  }

  private shouldCache(item: CacheItem): boolean {
    if (item.contentLength === undefined) return true;
    if (this.config.minCacheableBytes !== undefined && item.contentLength < this.config.minCacheableBytes) return false;
    if (this.config.maxCacheableBytes !== undefined && item.contentLength > this.config.maxCacheableBytes) return false;
    return true;
  }
}

function metadataFromHeaders(headers: Record<string, string | string[] | undefined>): UpstreamMetadata {
  const contentLength = headerString(headers['content-length']);
  return {
    contentLength: contentLength ? Number.parseInt(contentLength, 10) : undefined,
    contentType: headerString(headers['content-type']),
    etag: headerString(headers.etag),
    lastModified: headerString(headers['last-modified'])
  };
}

function headerString(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

async function requestUpstream(
  url: string,
  options: { method: 'HEAD' | 'GET'; range?: { start: number; end?: number }; timeoutMs: number; maxRedirections: number }
) {
  const headers: Record<string, string> = {};
  if (options.range) headers.range = `bytes=${options.range.start}-${options.range.end ?? ''}`;
  return request(url, {
    method: options.method,
    headers,
    bodyTimeout: options.timeoutMs,
    headersTimeout: options.timeoutMs,
    maxRedirections: options.maxRedirections
  } as Parameters<typeof request>[1] & { maxRedirections: number });
}
