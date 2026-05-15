import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, rm, stat } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createHash } from 'node:crypto';
import { CacheItem, CacheStore } from './cacheStore.js';
import { ByteRange, rangesToChunkIndexes } from './rangeMap.js';

export function cacheKeyForUrl(url: string): { urlHash: string; cacheKey: string } {
  const urlHash = createHash('sha256').update(url).digest('hex');
  return { urlHash, cacheKey: urlHash.slice(0, 32) };
}

export class FileCache {
  private activeFetches = new Map<string, Promise<void>>();

  constructor(
    private readonly cacheDir: string,
    private readonly chunkSize: number,
    private readonly store: CacheStore
  ) {}

  async ensureDirs(): Promise<void> {
    await mkdir(this.cacheDir, { recursive: true });
  }

  chunkPath(item: CacheItem, chunkIndex: number): string {
    return join(this.cacheDir, item.cacheKey, `${chunkIndex}.chunk`);
  }

  itemDir(item: CacheItem): string {
    return join(this.cacheDir, item.cacheKey);
  }

  chunkBounds(chunkIndex: number, contentLength?: number): ByteRange {
    const start = chunkIndex * this.chunkSize;
    const naturalEnd = start + this.chunkSize - 1;
    return { start, end: contentLength === undefined ? naturalEnd : Math.min(naturalEnd, contentLength - 1) };
  }

  cachedRanges(item: CacheItem): ByteRange[] {
    return this.store.getChunks(item.id).map((chunk) => ({ start: chunk.startByte, end: chunk.endByte }));
  }

  hasChunk(item: CacheItem, chunkIndex: number): boolean {
    return this.store.getChunks(item.id).some((chunk) => chunk.chunkIndex === chunkIndex);
  }

  recordChunk(item: CacheItem, chunkIndex: number, size: number): void {
    const bounds = this.chunkBounds(chunkIndex, item.contentLength);
    this.store.recordChunk({
      itemId: item.id,
      chunkIndex,
      startByte: bounds.start,
      endByte: bounds.start + size - 1,
      size
    });
  }

  async readChunkSlice(item: CacheItem, chunkIndex: number, desired: ByteRange): Promise<Readable> {
    const bounds = this.chunkBounds(chunkIndex, item.contentLength);
    const start = Math.max(desired.start, bounds.start) - bounds.start;
    const end = Math.min(desired.end, bounds.end) - bounds.start;
    return createReadStream(this.chunkPath(item, chunkIndex), { start, end });
  }

  async fetchChunkOnce(
    item: CacheItem,
    chunkIndex: number,
    fetcher: (range: ByteRange, destination: string) => Promise<void>
  ): Promise<void> {
    if (this.hasChunk(item, chunkIndex)) return;
    const key = `${item.id}:${chunkIndex}`;
    const existing = this.activeFetches.get(key);
    if (existing) {
      await existing;
      return;
    }

    const promise = this.fetchChunk(item, chunkIndex, fetcher).finally(() => {
      this.activeFetches.delete(key);
    });
    this.activeFetches.set(key, promise);
    await promise;
  }

  async removeItem(item: CacheItem): Promise<void> {
    await rm(this.itemDir(item), { recursive: true, force: true });
    this.store.deleteItem(item.id);
  }

  async enforceMaxSize(maxBytes: number): Promise<void> {
    const stats = this.store.getStats();
    let currentSize = stats.cacheSize;
    if (currentSize <= maxBytes) return;
    for (const item of this.store.evictionCandidates()) {
      if (currentSize <= maxBytes) break;
      currentSize -= item.totalBytesCached;
      await this.removeItem(item);
    }
  }

  chunkIndexesForRange(range: ByteRange): number[] {
    return rangesToChunkIndexes(range, this.chunkSize);
  }

  private async fetchChunk(
    item: CacheItem,
    chunkIndex: number,
    fetcher: (range: ByteRange, destination: string) => Promise<void>
  ): Promise<void> {
    const bounds = this.chunkBounds(chunkIndex, item.contentLength);
    const path = this.chunkPath(item, chunkIndex);
    await mkdir(dirname(path), { recursive: true });
    await fetcher(bounds, path);
    const fileStats = await stat(path);
    this.store.recordChunk({
      itemId: item.id,
      chunkIndex,
      startByte: bounds.start,
      endByte: bounds.start + fileStats.size - 1,
      size: fileStats.size
    });
  }
}

export async function writeReadableToFile(readable: Readable, destination: string): Promise<void> {
  await mkdir(dirname(destination), { recursive: true });
  await pipeline(readable, createWriteStream(destination));
}
