# Self-hosted HTTP Streaming Proxy Cache

This service accepts an allowed media URL, streams it to the client immediately, and stores the bytes in a local filesystem cache at the same time. It is intended for lawful personal media sources where you have the right to access and cache the content.

Do not use this to bypass access controls, redistribute copyrighted media, or expose signed private URLs to other people.

## Architecture

```text
Video player
  |
  | GET /stream?url=<encoded upstream URL>
  v
Fastify proxy
  |
  +-- URL policy: allowlist, SSRF protection, auth, rate limit
  |
  +-- SQLite metadata: URL hash, content headers, chunk map, LRU data
  |
  +-- Filesystem cache: <cacheKey>/<chunkIndex>.chunk
  |
  +-- Upstream HTTP media source, using Range requests for missing chunks
```

## Setup

```bash
npm install
npm run dev
```

The service defaults to `127.0.0.1:3000`.

With Docker Compose:

```bash
docker compose up --build
```

Compose binds to `127.0.0.1:3000` so the service is not publicly exposed by default.

## Environment Variables

Copy `.env.example` to `.env` and adjust:

| Variable | Default | Purpose |
| --- | --- | --- |
| `HOST` | `127.0.0.1` | Bind host |
| `PORT` | `3000` | Bind port |
| `CACHE_DIR` | `./data/cache` | Chunk file directory |
| `DATABASE_PATH` | `./data/cache.sqlite` | SQLite metadata path |
| `MAX_CACHE_BYTES` | `10737418240` | LRU cache size limit |
| `CHUNK_SIZE_BYTES` | `4194304` | Chunk size, default 4 MiB |
| `ALLOWLIST_HOSTS` | empty | Comma-separated host allowlist, supports `*.example.com` |
| `AUTH_TOKEN` | empty | Optional bearer token for non-health endpoints |
| `RATE_LIMIT_MAX` | `120` | Requests per rate window |
| `RATE_LIMIT_WINDOW` | `1 minute` | Fastify rate-limit window |
| `REQUEST_TIMEOUT_MS` | `120000` | Upstream inactivity timeout |
| `MAX_UPSTREAM_REDIRECTS` | `3` | Redirect limit |
| `MIN_CACHEABLE_BYTES` | empty | Stream without caching when content is smaller |
| `MAX_CACHEABLE_BYTES` | empty | Stream without caching when content is larger |
| `PREFETCH_ENABLED` | `false` | Download missing chunks ahead of playback in the background |
| `PREFETCH_CONCURRENCY` | `2` | Number of background chunk downloads per active item |
| `PREFETCH_START_AHEAD_CHUNKS` | `2` | How many chunks ahead of the current playback chunk to begin |
| `PREFETCH_MAX_BYTES` | empty | Optional cap on bytes to prefetch after playback starts |
| `LOG_LEVEL` | `info` | Structured log level |

## API Examples

Health:

```bash
curl http://127.0.0.1:3000/health
```

Stream a media URL:

```bash
curl -v \
  -H "Range: bytes=0-1048575" \
  "http://127.0.0.1:3000/stream?url=$(node -p 'encodeURIComponent("https://media.example.com/video.mp4")')"
```

With auth:

```bash
curl -H "Authorization: Bearer $AUTH_TOKEN" \
  "http://127.0.0.1:3000/cache/stats"
```

Stats and items:

```bash
curl http://127.0.0.1:3000/cache/stats
curl http://127.0.0.1:3000/cache/items
curl -X POST http://127.0.0.1:3000/cache/cleanup
```

Example Stremio-compatible URL shape:

```text
http://127.0.0.1:3000/stream?url=https%3A%2F%2Fmedia.example.com%2Fmovie.mp4%3Ftoken%3D...
```

The proxy endpoint is still plain HTTP, so another addon or client can also reference it directly.

## AIOStreams + Stremio Wrapper Addon

This service can also act as a small Stremio addon that wraps an existing AIOStreams addon. The flow is:

```text
Stremio
  -> this addon's /stream/movie/tt...json route
  -> AIOStreams stream route
  -> AIOStreams returns TorBox direct media URLs
  -> this addon rewrites those URLs to /stream?url=<TorBox URL>
  -> Stremio plays through the progressive proxy cache
```

Configure:

```env
AIOSTREAMS_ADDON_URL=https://your-aiostreams-addon-base-url
ADDON_PUBLIC_BASE_URL=http://127.0.0.1:3000
ADDON_NAME=AIOStreams Proxy Cache
ADDON_ID=community.aiostreams.proxy-cache
```

`AIOSTREAMS_ADDON_URL` should be the base URL of your already-configured AIOStreams addon, without `/manifest.json` at the end. For example, if AIOStreams gives you:

```text
https://example.aiostreams.app/abc123/manifest.json
```

set:

```env
AIOSTREAMS_ADDON_URL=https://example.aiostreams.app/abc123
```

Then install this wrapper in Stremio:

```text
http://127.0.0.1:3000/manifest.json
```

For another device on your LAN, set `ADDON_PUBLIC_BASE_URL` to the LAN URL that the Stremio device can reach, for example:

```env
HOST=0.0.0.0
ADDON_PUBLIC_BASE_URL=http://192.168.1.50:3000
```

Only do this on a trusted private network. Keep Docker Compose or your firewall from exposing the service publicly.

If `AUTH_TOKEN` is set, the wrapper adds Stremio `behaviorHints.proxyHeaders` to the rewritten streams:

```json
{
  "Authorization": "Bearer your-token"
}
```

Client support for those headers can vary, so for local Stremio testing the simplest setup is often to leave `AUTH_TOKEN` empty and bind the service to localhost.

## Progressive Caching

The proxy stores media as fixed-size chunk files. When a client asks for `Range: bytes=0-`, the proxy serves any cached chunks directly from disk and fetches missing chunks from upstream with HTTP `Range` requests. Missing upstream bytes are written to the cache and yielded to the client as they arrive, so playback can begin before the whole file or chunk has downloaded.

Seeking works the same way: a request for a later byte range maps to the corresponding chunk or chunks. Cached chunks are served locally; uncached chunks are fetched from upstream.

The proxy preserves the normal video-player headers:

- `Range`
- `Content-Range`
- `Accept-Ranges`
- `Content-Length`
- `Content-Type`
- `206 Partial Content`
- `416 Range Not Satisfiable`

## Security Notes

- Only `http` and `https` upstream URLs are accepted.
- Localhost, private IP ranges, link-local, multicast, reserved ranges, and metadata-service style internal addresses are blocked by DNS/IP checks.
- Use `ALLOWLIST_HOSTS` to restrict upstream sources.
- Signed URL query strings are redacted in logs.
- Set `AUTH_TOKEN` before exposing the service to any shared network.
- Docker Compose binds only to loopback by default.

## Tests

```bash
npm test
```

The integration tests run a local HTTP server with byte-range support and verify first playback, replay from cache, seeking to uncached ranges, and `416` behavior.

## Large Stream Tuning

For very large, high-bitrate files, use a longer upstream inactivity timeout:

```env
REQUEST_TIMEOUT_MS=120000
```

The proxy streams continuous multi-chunk playback through one upstream range request and writes cache chunks as bytes pass through. Small seek requests still fill individual cache chunks for replay.

To download ahead of playback as quickly as the upstream and your server allow, enable background prefetch:

```env
PREFETCH_ENABLED=true
PREFETCH_CONCURRENCY=4
PREFETCH_START_AHEAD_CHUNKS=1
PREFETCH_MAX_BYTES=
```

With this enabled, starting a stream begins background downloads of missing chunks ahead of the current playback position. Leaving `PREFETCH_MAX_BYTES` empty allows prefetch to continue toward the end of the file. Be careful with disk space: a single 100 GB movie can become a 100 GB cache item if playback starts and prefetch is allowed to finish.
