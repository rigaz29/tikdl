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
- Optionally place a `cookies.txt` file in the project root for authenticated TikTok requests

**External dependency:** `yt-dlp` must be installed on the system for the fallback download path to work.

## Architecture

The entire bot lives in `bot.js` (~1000 lines), organized into clearly-delineated sections:

### Download Strategy (Cascade)

```
URL received
  → JS API v1 (1 attempt)
  → JS API v2 (2 attempts, with exponential backoff)
  → yt-dlp subprocess (without cookies)
  → yt-dlp subprocess (with cookies)
  → Error message to user
```

### Key Sections in `bot.js`

| Lines | Section | Purpose |
|-------|---------|---------|
| 15–67 | `CONFIG` | All tuneable constants (timeouts, retry counts, rate limits) |
| 100–121 | Cookie management | Loads `cookies.txt`, supports `/reload_cookies` |
| 127–137 | Metrics | Tracks processed/failed counts, fallback rates |
| 142–144 | Throttling | `apiLimiter` (Bottleneck) and `downloadLimiter` for concurrency |
| 150–195 | HTTP helpers | Stream downloads with automatic cookie-retry fallback |
| 212–248 | `fetchTikTokAPI()` | JS API caller: v1 → v2 with retry logic |
| 254–394 | yt-dlp layer | Subprocess spawner, metadata parser, file size enforcement |
| 399–419 | Data extractors | `getVideoUrl`, `getImageUrls`, `getAudioUrl` — handles v1/v2 API diffs |
| 425–500 | Caption builders | Markdown captions with author/timestamp/description |
| 540–682 | Content senders | Stream video/audio to Telegram, batch images (max 10/group) |
| 693–772 | Main processor | Full cascade logic, group auto-delete, error reporting |
| 778–897 | Bot setup | Commands: `/start`, `/stats`, `/audio`, `/reload_cookies` |
| 903–929 | Cleanup loop | Removes yt-dlp temp dirs older than 1h, runs every 30min |

### Important Patterns

- **Streaming-first:** Video and audio are piped directly from HTTP → Telegram without writing to disk. The yt-dlp fallback is the only path that writes temp files.
- **TikTok API v1 vs v2:** Response schemas differ. `getVideoUrl()`, `getImageUrls()`, and `getAudioUrl()` handle both formats.
- **Rate limiting at two levels:** Per-user rate limiter (5 req/10s via `@grammyjs/ratelimiter`) + global concurrency control (Bottleneck).
- **Group behavior:** In group chats, the bot auto-deletes status messages and the user's original message (2s delay) if it has `can_delete_messages` permission. This does not apply in private chats.
- **User-facing text is in Indonesian (Bahasa Indonesia).** Log/comment language is English.
