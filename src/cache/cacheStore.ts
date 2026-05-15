import Database from 'better-sqlite3';
import { dirname } from 'node:path';
import { mkdirSync } from 'node:fs';

export type CacheItem = {
  id: number;
  urlHash: string;
  cacheKey: string;
  url: string;
  contentLength?: number;
  contentType?: string;
  etag?: string;
  lastModified?: string;
  createdAt: number;
  lastAccessedAt: number;
  totalBytesCached: number;
  completed: boolean;
  activeStreams: number;
};

export type CachedChunk = {
  itemId: number;
  chunkIndex: number;
  startByte: number;
  endByte: number;
  size: number;
};

type CacheItemRow = Omit<CacheItem, 'completed'> & { completed: 0 | 1 };

export class CacheStore {
  private db: Database.Database;

  constructor(databasePath: string) {
    mkdirSync(dirname(databasePath), { recursive: true });
    this.db = new Database(databasePath);
    this.db.pragma('journal_mode = WAL');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS cache_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url_hash TEXT NOT NULL UNIQUE,
        cache_key TEXT NOT NULL UNIQUE,
        url TEXT NOT NULL,
        content_length INTEGER,
        content_type TEXT,
        etag TEXT,
        last_modified TEXT,
        created_at INTEGER NOT NULL,
        last_accessed_at INTEGER NOT NULL,
        total_bytes_cached INTEGER NOT NULL DEFAULT 0,
        completed INTEGER NOT NULL DEFAULT 0,
        active_streams INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS cached_chunks (
        item_id INTEGER NOT NULL,
        chunk_index INTEGER NOT NULL,
        start_byte INTEGER NOT NULL,
        end_byte INTEGER NOT NULL,
        size INTEGER NOT NULL,
        PRIMARY KEY (item_id, chunk_index),
        FOREIGN KEY (item_id) REFERENCES cache_items(id) ON DELETE CASCADE
      );
      CREATE INDEX IF NOT EXISTS idx_cache_items_accessed ON cache_items(last_accessed_at);
    `);
  }

  upsertItem(input: { urlHash: string; cacheKey: string; url: string }): CacheItem {
    const now = Date.now();
    this.db.prepare(`
      INSERT INTO cache_items (url_hash, cache_key, url, created_at, last_accessed_at)
      VALUES (@urlHash, @cacheKey, @url, @now, @now)
      ON CONFLICT(url_hash) DO UPDATE SET last_accessed_at = excluded.last_accessed_at
    `).run({ ...input, now });
    return this.getByHash(input.urlHash)!;
  }

  getByHash(urlHash: string): CacheItem | undefined {
    return this.rowToItem(this.db.prepare(cacheItemSelect('WHERE url_hash = ?')).get(urlHash) as CacheItemRow | undefined);
  }

  getById(id: number): CacheItem | undefined {
    return this.rowToItem(this.db.prepare(cacheItemSelect('WHERE id = ?')).get(id) as CacheItemRow | undefined);
  }

  updateMetadata(id: number, metadata: Partial<Pick<CacheItem, 'contentLength' | 'contentType' | 'etag' | 'lastModified' | 'completed'>>): void {
    const current = this.getById(id);
    if (!current) return;
    this.db.prepare(`
      UPDATE cache_items SET
        content_length = @contentLength,
        content_type = @contentType,
        etag = @etag,
        last_modified = @lastModified,
        completed = @completed
      WHERE id = @id
    `).run({
      id,
      contentLength: metadata.contentLength ?? current.contentLength ?? null,
      contentType: metadata.contentType ?? current.contentType ?? null,
      etag: metadata.etag ?? current.etag ?? null,
      lastModified: metadata.lastModified ?? current.lastModified ?? null,
      completed: (metadata.completed ?? current.completed) ? 1 : 0
    });
  }

  touch(id: number): void {
    this.db.prepare('UPDATE cache_items SET last_accessed_at = ? WHERE id = ?').run(Date.now(), id);
  }

  incrementActive(id: number): void {
    this.db.prepare('UPDATE cache_items SET active_streams = active_streams + 1 WHERE id = ?').run(id);
  }

  decrementActive(id: number): void {
    this.db.prepare('UPDATE cache_items SET active_streams = MAX(active_streams - 1, 0) WHERE id = ?').run(id);
  }

  recordChunk(chunk: CachedChunk): void {
    const tx = this.db.transaction(() => {
      const previous = this.db.prepare('SELECT size FROM cached_chunks WHERE item_id = ? AND chunk_index = ?').get(chunk.itemId, chunk.chunkIndex) as { size: number } | undefined;
      this.db.prepare(`
        INSERT INTO cached_chunks (item_id, chunk_index, start_byte, end_byte, size)
        VALUES (@itemId, @chunkIndex, @startByte, @endByte, @size)
        ON CONFLICT(item_id, chunk_index) DO UPDATE SET
          start_byte = excluded.start_byte,
          end_byte = excluded.end_byte,
          size = excluded.size
      `).run(chunk);
      const delta = chunk.size - (previous?.size ?? 0);
      this.db.prepare('UPDATE cache_items SET total_bytes_cached = total_bytes_cached + ? WHERE id = ?').run(delta, chunk.itemId);
      const item = this.getById(chunk.itemId);
      if (item?.contentLength !== undefined && item.totalBytesCached >= item.contentLength) {
        this.db.prepare('UPDATE cache_items SET completed = 1 WHERE id = ?').run(chunk.itemId);
      }
    });
    tx();
  }

  getChunks(itemId: number): CachedChunk[] {
    return this.db.prepare('SELECT item_id as itemId, chunk_index as chunkIndex, start_byte as startByte, end_byte as endByte, size FROM cached_chunks WHERE item_id = ? ORDER BY chunk_index').all(itemId) as CachedChunk[];
  }

  listItems(): CacheItem[] {
    const rows = this.db.prepare(cacheItemSelect('ORDER BY last_accessed_at DESC')).all() as CacheItemRow[];
    return rows.map((row) => this.rowToItem(row)!);
  }

  getStats(): { itemCount: number; cacheSize: number; completedCount: number } {
    return this.db.prepare('SELECT COUNT(*) as itemCount, COALESCE(SUM(total_bytes_cached), 0) as cacheSize, SUM(completed) as completedCount FROM cache_items').get() as {
      itemCount: number;
      cacheSize: number;
      completedCount: number;
    };
  }

  evictionCandidates(): CacheItem[] {
    const rows = this.db.prepare(cacheItemSelect('WHERE active_streams = 0 ORDER BY last_accessed_at ASC')).all() as CacheItemRow[];
    return rows.map((row) => this.rowToItem(row)!);
  }

  deleteItem(id: number): void {
    this.db.prepare('DELETE FROM cache_items WHERE id = ? AND active_streams = 0').run(id);
  }

  close(): void {
    this.db.close();
  }

  private rowToItem(row?: CacheItemRow): CacheItem | undefined {
    if (!row) return undefined;
    return {
      id: row.id,
      urlHash: row.urlHash,
      cacheKey: row.cacheKey,
      url: row.url,
      contentLength: row.contentLength ?? undefined,
      contentType: row.contentType ?? undefined,
      etag: row.etag ?? undefined,
      lastModified: row.lastModified ?? undefined,
      createdAt: row.createdAt,
      lastAccessedAt: row.lastAccessedAt,
      totalBytesCached: row.totalBytesCached,
      completed: Boolean(row.completed),
      activeStreams: row.activeStreams
    };
  }
}

function cacheItemSelect(suffix: string): string {
  return `
    SELECT
      id,
      url_hash AS urlHash,
      cache_key AS cacheKey,
      url,
      content_length AS contentLength,
      content_type AS contentType,
      etag,
      last_modified AS lastModified,
      created_at AS createdAt,
      last_accessed_at AS lastAccessedAt,
      total_bytes_cached AS totalBytesCached,
      completed,
      active_streams AS activeStreams
    FROM cache_items
    ${suffix}
  `;
}
