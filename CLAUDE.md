# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm install       # Install dependencies
npm start         # Run the bot (production)
npm run dev       # Run with --watch for auto-reload on file changes
```

There are no configured test or lint scripts.

**Required setup:**
- Create a `.env` file with `BOT_TOKEN=<telegram_bot_token>`
- Optionally set `YTDLP_TEMP_DIR` (defaults to OS temp dir)
- Optionally place a `cookies.txt` file in the project root for authenticated TikTok requests. It must be in Netscape format, because only yt-dlp reads it
- Optionally set `INTAKE_PORT`, `INTAKE_SECRET` and `INTAKE_CHAT_ID` (plus `INTAKE_HOST`, default `0.0.0.0`) to enable the HTTP intake used by the TikTok floating-button APK

**External dependency:** `yt-dlp` must be installed on the system. For video and audio it is the *primary* download path (best quality), not just a fallback — the bot still works without it, but every video is then served by the JS API instead.

## Architecture

The entire bot lives in `bot.js` (~1800 lines), organized into clearly-delineated sections.

### Download Strategy (Cascade)

Step 0 always runs first: a JS API metadata fetch (v1, 1 attempt → v2, 2 attempts with exponential backoff). It decides whether the post is a video or a slideshow, and doubles as the fallback content source. From there the path splits by content type:

```
URL received
  → Step 0: JS API metadata (v1 → v2)
  │
  ├─ Slideshow (images)
  │    → JS API only — yt-dlp cannot handle slideshows
  │
  └─ Video / audio (quality-first)
       → yt-dlp without cookies
       → yt-dlp with cookies (only if cookies.txt exists)
       → JS API stream (no-watermark), re-fetching metadata if step 0 failed
       → Error message to user
```

Note the ordering: for video, **yt-dlp runs before the JS API stream**, because it yields better quality. The JS API is the safety net, not the first choice. yt-dlp is also rejected when it returns an audio-only file for a video request (a slideshow soundtrack, or a post that exposes no video format), which sends the cascade on to the JS API.

### Key Sections in `bot.js`

Each section opens with a `// ──────` banner comment, so `grep -n '^//  ' bot.js` lists them in order with current line numbers. Listed here by banner name rather than line range, because line numbers go stale on every edit.

| Banner | Purpose |
|--------|---------|
| `Configuration` | The `CONFIG` object — all tuneable constants (timeouts, retry counts, rate limits, file size cap) |
| `Validation` / `Logger` | Startup token check; levelled logger, plus `logDebug` gated behind `LOG_DEBUG=1` |
| `Cookies` | Checks for `cookies.txt` (passed to yt-dlp), supports `/reload_cookies` |
| `Metrics` | Tracks processed/failed counts, fallback rates |
| `Throttling` | Bottleneck limiters (`apiLimiter`, `downloadLimiter`, `ytdlpLimiter`) and per-user job counters |
| `HTTP helpers` | `downloadStream()` — streams with an early oversize reject |
| `TikTok URL matching` | `TIKTOK_URL_RE` plus trailing-punctuation stripping and dedupe |
| `PRIMARY: TikTok JS API` | `fetchTikTokAPI()` — v1 → v2 with retry logic |
| `FALLBACK: yt-dlp (Python)` | Binary probe, subprocess spawner, stderr parser, temp-dir cleanup |
| `Data extractors (JS API)` | `getVideoUrl`, `getImageUrls`, `getAudioUrl` — handles v1/v2 API diffs |
| `Caption builders` | HTML captions with author/timestamp/description, plus filename sanitising |
| `Telegram helpers` | Group detection, permission checks, status-message edit/delete |
| `Telegram send helper with retry` | `withTelegramRetry()` — retries 429s and transient network errors |
| `Content senders (JS API)` | Stream video/audio to Telegram, batch images (max 10/group) |
| `Content sender (yt-dlp — primary, max quality)` | Downloads via subprocess, then uploads the temp file |
| `Main processor — cascade logic` | `sendByCascade()` — the cascade itself; `processUrl()` — runs it, falls back to the app's media, group auto-delete, error reporting |
| `Bot setup` | Commands: `/start`, `/stats`, `/audio`, `/reload_cookies` |
| `HTTP intake (TikTok floating-button APK)` | `POST /submit` with an HMAC-signed body, `GET /status/<job>`, `GET /health`; builds a context for a fixed chat and runs `processUrl()` |
| `Periodic cleanup for yt-dlp temp files` | Removes temp dirs older than 1h, runs every 30min |
| `Startup` | `main()`, graceful shutdown, process-level error handlers |

### Important Patterns

- **Streaming-first:** On the JS API path, video and audio are piped directly from HTTP → Telegram without writing to disk. The yt-dlp path is the only one that writes temp files.
- **TikTok API v1 vs v2:** Response schemas differ. `getVideoUrl()`, `getImageUrls()`, and `getAudioUrl()` handle both formats.
- **yt-dlp needs a browser User-Agent:** TikTok's WAF answers yt-dlp's default UA with a JS challenge page instead of the video page, so extraction fails before any download ("Unexpected response from webpage request"). `ytdlpDownload()` passes `--user-agent` from `DEFAULT_HEADERS`. Do not remove it — no yt-dlp version fixes this, and the challenge is still intermittent even with the UA set.
- **Cookies stay with yt-dlp:** `downloadStream()` must never send them. Its URLs come from the API and can point at third-party hosts (v2 goes through SSSTik), and a session cookie sent there is a stolen login.
- **Rate limiting at two levels:** Per-user rate limiter (20 requests / 10s via `@grammyjs/ratelimiter`, plus a 40-job queue cap since one message can fan out into several URLs) + global concurrency control (Bottleneck).
- **Captions use HTML parse mode,** not Markdown, and run through `escHtml()`. Only the `/start` and `/stats` command replies use Markdown.
- **Group behavior:** In group chats, the bot auto-deletes status messages and the user's original message (2s delay) if it has `can_delete_messages` permission. This does not apply in private chats.
- **HTTP intake:** the APK signs `${timestamp}.${rawBody}` with HMAC-SHA256 using `INTAKE_SECRET` and sends `X-Timestamp` / `X-Signature`; the secret never goes over the wire, so plain HTTP is acceptable. Requests outside a 5-minute window or with an already-seen signature are rejected. The endpoint answers 202 as soon as the job is queued, with a job id; results go to `INTAKE_CHAT_ID` like a hand-sent link. A failed job's error names the link and stays in the chat, because no user message holds it. All intake jobs share one per-user job budget (key `intake`). `processUrl()` is given a minimal context from `makeChatContext()`, so any new `ctx.*` use in the senders must be added there too.
- **Job progress:** `GET /status/<job>` signs `${timestamp}.status:<job>` and has no replay cache, since polls repeat. Its `detail` comes from `editStatus()` through `ctx.onStatus`, so the text the chat shows is the text the phone shows. `processUrl()` resolves with what the sender reported (`kind`, `size`, `count`, `failed`, `method`) or with `{ ok: false, error }`; senders must keep returning that. Finished jobs stay readable for `jobTtlMs`.
- **Story media:** the APK sends a story's direct media as `media` next to the link. The cascade still runs first because yt-dlp gives better quality, and `parseAppMedia()` turns `media` into API-shaped data for the JS API senders only when the cascade fails. Its host check is what stops a leaked secret from turning the server into a fetch-anything proxy, so keep it.
- **User-facing text is in Indonesian (Bahasa Indonesia).** Log/comment language is English.
