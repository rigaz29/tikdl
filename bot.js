require("dotenv").config();

const { Bot, InputFile, GrammyError, HttpError } = require("grammy");
const { run } = require("@grammyjs/runner");
const { limit } = require("@grammyjs/ratelimiter");
const Tiktok = require("@tobyg74/tiktok-api-dl");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const Bottleneck = require("bottleneck");
const { PassThrough } = require("stream");
const { spawn, execSync } = require("child_process");
const os = require("os");

// ──────────────────────────────────────────────
//  Configuration
// ──────────────────────────────────────────────

const CONFIG = {
  bot: {
    token: process.env.BOT_TOKEN,
    allowedUpdates: ["message", "callback_query"],
  },

  rateLimit: {
    window: 10_000,
    maxRequests: 5,
  },

  api: {
    v1Retries: 1,
    v2Retries: 2,
    retryBackoffMs: 1_000,
  },

  download: {
    timeout: 30_000,
    maxRedirects: 10,
    maxUrlsPerMessage: 3,
    maxFileSizeMB: 50,
  },

  ytdlp: {
    timeout: 120_000,       // 2 minutes for yt-dlp
    tempDir: process.env.YTDLP_TEMP_DIR || path.join(os.tmpdir(), "tiktok-bot"),
  },

  autoDelete: {
    enabled: true,
    delayMs: 2_000,
    groupsOnly: true,
  },

  throttle: {
    api: {
      minTime: 100,
      maxConcurrent: 10,
      reservoir: 30,
      reservoirRefreshAmount: 30,
      reservoirRefreshInterval: 60_000,
    },
    download: {
      maxConcurrent: 15,
      minTime: 50,
    },
    process: {
      maxConcurrent: 20,
    },
    ytdlp: {
      maxConcurrent: 3,
    },
  },
};

// ──────────────────────────────────────────────
//  Validation
// ──────────────────────────────────────────────

if (!CONFIG.bot.token) {
  console.error("❌ BOT_TOKEN tidak ditemukan di file .env!");
  console.error("💡 Buat file .env dan tambahkan: BOT_TOKEN=your_bot_token_here");
  process.exit(1);
}

// ──────────────────────────────────────────────
//  Logger
// ──────────────────────────────────────────────

const LOG_COLORS = {
  INFO: "\x1b[36m",
  WARN: "\x1b[33m",
  ERROR: "\x1b[31m",
  OK: "\x1b[32m",
  RESET: "\x1b[0m",
};

function log(level, message) {
  const ts = new Date().toISOString();
  const color = LOG_COLORS[level] || LOG_COLORS.RESET;
  console.log(`${color}[${ts}] [${level}] ${message}${LOG_COLORS.RESET}`);
}

// ──────────────────────────────────────────────
//  Cookies
// ──────────────────────────────────────────────

const COOKIES_PATH = path.join(__dirname, "cookies.txt");
let cookieString = null;
let hasCookiesFile = false;

function loadCookies() {
  try {
    if (fs.existsSync(COOKIES_PATH)) {
      cookieString = fs.readFileSync(COOKIES_PATH, "utf8").trim();
      hasCookiesFile = !!cookieString;
      if (hasCookiesFile) {
        log("OK", "Cookies loaded from cookies.txt");
      }
    } else {
      log("WARN", "cookies.txt not found — running without cookies");
      hasCookiesFile = false;
    }
  } catch (err) {
    log("ERROR", `Failed to load cookies: ${err.message}`);
    hasCookiesFile = false;
  }
}

// ──────────────────────────────────────────────
//  Metrics
// ──────────────────────────────────────────────

const metrics = {
  processed: 0,
  failed: 0,
  videos: 0,
  images: 0,
  audio: 0,
  ytdlpFallbacks: 0,
  cookieFallbacks: 0,
  activeJobs: 0,
  startedAt: Date.now(),
};

// ──────────────────────────────────────────────
//  Throttling
// ──────────────────────────────────────────────

const apiLimiter = new Bottleneck(CONFIG.throttle.api);
const downloadLimiter = new Bottleneck(CONFIG.throttle.download);
const processingLimiter = new Bottleneck(CONFIG.throttle.process);
const ytdlpLimiter = new Bottleneck(CONFIG.throttle.ytdlp);

// ──────────────────────────────────────────────
//  HTTP helpers
// ──────────────────────────────────────────────

const DEFAULT_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "*/*",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  Connection: "keep-alive",
  Referer: "https://www.tiktok.com/",
};

function buildAxiosConfig(useCookies = true) {
  const headers = { ...DEFAULT_HEADERS };
  if (useCookies && cookieString) {
    headers.Cookie = cookieString;
  }
  return {
    timeout: CONFIG.download.timeout,
    maxRedirects: CONFIG.download.maxRedirects,
    headers,
  };
}

async function downloadStream(url, maxBytes = 0) {
  const tryDownload = async (useCookies) => {
    const resp = await axios({
      ...buildAxiosConfig(useCookies),
      method: "GET",
      url,
      responseType: "stream",
    });

    // Reject oversized content early (before streaming the whole body) so the
    // cascade can fall back to yt-dlp, which can select a smaller format.
    if (maxBytes > 0) {
      const len = Number(resp.headers["content-length"]);
      if (Number.isFinite(len) && len > maxBytes) {
        resp.data.destroy();
        const e = new Error(`File too large: ${formatBytes(len)} (max ${formatBytes(maxBytes)})`);
        e.code = "FILE_TOO_LARGE";
        throw e;
      }
    }

    const pass = new PassThrough();
    pass.on("error", (err) => log("WARN", `Stream error: ${err.message}`));
    resp.data.on("error", (err) => pass.destroy(err));
    resp.data.pipe(pass);
    return pass;
  };

  try {
    return await tryDownload(true);
  } catch (err) {
    // Oversize is not an auth problem — don't waste a second request retrying.
    if (err.code === "FILE_TOO_LARGE") throw err;
    if (cookieString) {
      log("WARN", `Download retry without cookies: ${err.message}`);
      return tryDownload(false);
    }
    throw err;
  }
}

// ──────────────────────────────────────────────
//  TikTok URL matching
// ──────────────────────────────────────────────

const TIKTOK_URL_RE =
  /https?:\/\/(?:[\w-]+\.)?tiktok\.com\/\S+/gi;

function extractTikTokUrls(text) {
  return (text.match(TIKTOK_URL_RE) || []).map((u) => u.replace(/[)\]}>]+$/, ""));
}

// ──────────────────────────────────────────────
//  PRIMARY: TikTok JS API
// ──────────────────────────────────────────────

/**
 * Try JS API with v1→v2 fallback.
 * @param {string} url
 * @param {boolean} useCookies - whether to pass cookies header
 * @returns {{ data, apiVersion }} or throws
 */
async function fetchTikTokAPI(url) {
  const versions = [
    { version: "v1", retries: CONFIG.api.v1Retries },
    { version: "v2", retries: CONFIG.api.v2Retries },
  ];

  let lastError;

  for (const { version, retries } of versions) {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const result = await Tiktok.Downloader(url, { version });

        if (result?.status === "success" && result.result) {
          return { data: result.result, apiVersion: version };
        }

        throw new Error(result?.message || `${version} returned non-success`);
      } catch (err) {
        lastError = err;
        log("WARN", `JS API ${version} attempt ${attempt}/${retries}: ${err.message}`);

        if (attempt < retries) {
          await sleep(CONFIG.api.retryBackoffMs * 2 ** (attempt - 1));
        }
      }
    }
  }

  throw new Error(`JS API failed — ${lastError?.message || "unknown"}`);
}

// ──────────────────────────────────────────────
//  FALLBACK: yt-dlp (Python)
// ──────────────────────────────────────────────

/**
 * Check if yt-dlp is installed.
 */
let ytdlpAvailable = false;

function checkYtdlp() {
  try {
    const ver = execSync("yt-dlp --version", { timeout: 5000 }).toString().trim();
    ytdlpAvailable = true;
    log("OK", `yt-dlp found: ${ver}`);
  } catch {
    ytdlpAvailable = false;
    log("WARN", "yt-dlp not found — fallback disabled. Install: pip install yt-dlp");
  }
}

/**
 * Download via yt-dlp, returning the file path.
 * @param {string} url
 * @param {boolean} useCookies
 * @returns {{ filepath: string, metadata: object }}
 */
function ytdlpDownload(url, useCookies = false, mode = "auto") {
  return new Promise((resolve, reject) => {
    const tempDir = fs.mkdtempSync(path.join(CONFIG.ytdlp.tempDir, "dl-"));
    const outputTpl = path.join(tempDir, "%(title).100B.%(ext)s");
    const maxSize = CONFIG.download.maxFileSizeMB;

    // For /audio fallback, grab an audio-only stream instead of the full video.
    const format =
      mode === "audio"
        ? `bestaudio[filesize<${maxSize}M]/bestaudio/best[filesize<${maxSize}M]/best`
        : `best[filesize<${maxSize}M]/best`;

    const args = [
      "--no-playlist",
      "--format", format,
      "--max-filesize", `${maxSize}M`,
      "--socket-timeout", "30",
      "--retries", "3",
      "--no-check-certificates",
      "--no-warnings",
      "--print-json",
      "-o", outputTpl,
    ];

    if (useCookies && hasCookiesFile) {
      args.push("--cookies", COOKIES_PATH);
    }

    args.push(url);

    const proc = spawn("yt-dlp", args, {
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });

    let stdout = "";
    let stderr = "";

    proc.stdout.on("data", (d) => { stdout += d.toString(); });
    proc.stderr.on("data", (d) => { stderr += d.toString(); });

    const timer = setTimeout(() => {
      proc.kill("SIGKILL");
      cleanup(tempDir);
      reject(new Error("yt-dlp timeout"));
    }, CONFIG.ytdlp.timeout);

    proc.on("close", (code) => {
      clearTimeout(timer);

      if (code !== 0) {
        cleanup(tempDir);
        const errMsg = parseYtdlpError(stderr) || `yt-dlp exit code ${code}`;
        return reject(new Error(errMsg));
      }

      // Find the downloaded file
      try {
        const files = fs.readdirSync(tempDir).filter((f) => !f.startsWith("."));

        // Pick the largest file: yt-dlp may leave small leftovers (e.g. partial
        // fragments) alongside the real output, and readdir order is unspecified.
        let filepath = null;
        let biggest = -1;
        for (const f of files) {
          const p = path.join(tempDir, f);
          try {
            const s = fs.statSync(p);
            if (s.isFile() && s.size > biggest) {
              biggest = s.size;
              filepath = p;
            }
          } catch {
            // ignore unreadable entries
          }
        }

        if (!filepath) {
          cleanup(tempDir);
          return reject(new Error("yt-dlp produced no output"));
        }

        const stat = fs.statSync(filepath);

        if (stat.size === 0) {
          cleanup(tempDir);
          return reject(new Error("yt-dlp output is empty"));
        }

        if (stat.size > maxSize * 1024 * 1024) {
          cleanup(tempDir);
          return reject(new Error(`File too large: ${formatBytes(stat.size)}`));
        }

        // Try to parse JSON metadata from stdout
        let metadata = {};
        try {
          // yt-dlp --print-json outputs JSON per line
          const lines = stdout.trim().split("\n");
          metadata = JSON.parse(lines[lines.length - 1]);
        } catch {
          // Metadata parsing is optional
        }

        resolve({ filepath, tempDir, metadata, fileSize: stat.size });
      } catch (err) {
        cleanup(tempDir);
        reject(err);
      }
    });

    proc.on("error", (err) => {
      clearTimeout(timer);
      cleanup(tempDir);
      reject(err);
    });
  });
}

function parseYtdlpError(stderr) {
  const keywords = ["error:", "failed:", "unable to", "not found", "forbidden", "unavailable"];
  const lines = stderr.split("\n").reverse();
  for (const line of lines) {
    const lower = line.toLowerCase();
    if (keywords.some((k) => lower.includes(k))) {
      return line.trim().slice(0, 200);
    }
  }
  if (stderr.toLowerCase().includes("private video")) return "Video is private";
  if (stderr.includes("404")) return "Video not found (404)";
  return null;
}

function cleanup(dir) {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {
    // ignore
  }
}

// ──────────────────────────────────────────────
//  Data extractors (JS API)
// ──────────────────────────────────────────────

/** The API returns some URL fields as either a string or an array of strings. */
function firstUrl(value) {
  if (Array.isArray(value)) return value.find(Boolean) || null;
  return value || null;
}

function getVideoUrl(data, apiVersion) {
  const v = data?.video;
  if (!v) return null;

  if (apiVersion === "v2") {
    return firstUrl(v.playAddr);
  }

  return (
    firstUrl(v.noWaterMark) ||
    firstUrl(v.playAddr) ||
    firstUrl(v.play) ||
    firstUrl(v.downloadAddr) ||
    firstUrl(v.watermark) ||
    null
  );
}

function getImageUrls(data) {
  const imgs = data?.images || [];
  // Some schemas return [{ url: "..." }] instead of ["..."].
  return imgs
    .map((img) => (typeof img === "string" ? img : firstUrl(img?.url || img?.urls)))
    .filter(Boolean);
}

function getAudioUrl(data) {
  const m = data?.music;
  if (!m) return null;
  return firstUrl(m.playUrl) || firstUrl(m.play_url) || null;
}

// ──────────────────────────────────────────────
//  Caption builders
// ──────────────────────────────────────────────

/** Escape special characters for Telegram legacy Markdown mode */
function escMd(text) {
  return String(text).replace(/[_*`[]/g, "\\$&");
}

/** Build a safe .mp4 filename so Telegram treats the document as a video. */
function buildVideoFilename(data) {
  const author = data.author || {};
  const username = author.uniqueId || author.username || author.nickname || "tiktok";
  const videoId = data.id || data.aweme_id || Date.now();
  const base = `${username}_${videoId}`.replace(/[^\w.-]+/g, "_").slice(0, 100);
  return `${base}.mp4`;
}

function buildCaption(data, type, apiVersion, method = "API") {
  const author = data.author || {};
  const username = author.uniqueId || author.username || author.nickname || "Unknown";
  const uid = author.uid || author.id || "–";
  const videoId = data.id || data.aweme_id || "";
  const originalUrl =
    data.url ||
    data.webVideoUrl ||
    (videoId ? `https://www.tiktok.com/@${username}/${type === "image" ? "photo" : "video"}/${videoId}` : `https://www.tiktok.com/@${username}`);

  const lines = [
    `📅 ${formatTimestamp(data.createTime || data.create_time)}`,
    `👤 [${escMd(username)}](${originalUrl}) (UID: ${escMd(uid)})`,
    `🔧 ${escMd(method)} ${escMd(apiVersion.toUpperCase())}`,
    `🔗 [Link TikTok](${originalUrl})`,
  ];

  const desc = (data.desc || data.description || "").trim();
  if (desc) {
    const short = desc.length > 300 ? `${desc.slice(0, 300)}…` : desc;
    lines.push("", `📝 "${escMd(short)}"`);
  }

  return lines.join("\n");
}

function buildYtdlpCaption(metadata, url) {
  const title = metadata.title || metadata.fulltitle || "TikTok Video";
  const uploader = metadata.uploader || metadata.uploader_id || "Unknown";
  const uploaderId = metadata.uploader_id || "";
  const duration = metadata.duration ? `${Math.round(metadata.duration)}s` : "–";
  const webpage = metadata.webpage_url || url;

  const lines = [
    `👤 [${escMd(uploader)}](${webpage})`,
    `🔧 yt-dlp fallback`,
    `⏱️ ${escMd(duration)}`,
    `🔗 [Link TikTok](${webpage})`,
  ];

  if (title && title !== "TikTok Video") {
    const short = title.length > 200 ? `${title.slice(0, 200)}…` : title;
    lines.push("", `📝 "${escMd(short)}"`);
  }

  return lines.join("\n");
}

function formatTimestamp(ts) {
  if (!ts) return "–";
  try {
    const d = new Date(Number(ts) * 1000);
    if (Number.isNaN(d.getTime())) return "–";
    return d.toLocaleDateString("id-ID", {
      year: "numeric",
      month: "long",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "Asia/Jakarta",
    });
  } catch {
    return "–";
  }
}

function formatBytes(n) {
  if (n < 1024) return `${n}B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)}KB`;
  return `${(n / (1024 * 1024)).toFixed(2)}MB`;
}

// ──────────────────────────────────────────────
//  Telegram helpers
// ──────────────────────────────────────────────

function isGroup(ctx) {
  const t = ctx.chat?.type;
  return t === "group" || t === "supergroup";
}

const deletePermCache = new Map();

async function canDeleteMessages(ctx) {
  const chatId = ctx.chat.id;
  if (deletePermCache.has(chatId)) return deletePermCache.get(chatId);
  try {
    const me = await ctx.api.getChatMember(chatId, ctx.me.id);
    const result = me.status === "creator" || me.can_delete_messages === true;
    deletePermCache.set(chatId, result);
    setTimeout(() => deletePermCache.delete(chatId), 10 * 60_000); // 10 min TTL
    return result;
  } catch {
    return false;
  }
}

async function tryDelete(ctx, messageId) {
  try {
    await ctx.api.deleteMessage(ctx.chat.id, messageId);
  } catch {
    // silent
  }
}

async function editStatus(ctx, msg, text) {
  try {
    await ctx.api.editMessageText(msg.chat.id, msg.message_id, text);
  } catch {
    // silent
  }
}

// ──────────────────────────────────────────────
//  Telegram send helper with 429 retry
// ──────────────────────────────────────────────

async function withTelegramRetry(fn, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (err instanceof GrammyError && err.error_code === 429) {
        const waitMs = ((err.parameters?.retry_after ?? 5) + 1) * 1000;
        log("WARN", `Telegram flood limit (429), retry in ${waitMs}ms (attempt ${attempt}/${maxRetries})`);
        if (attempt < maxRetries) {
          await sleep(waitMs);
        } else {
          throw err;
        }
      } else {
        throw err;
      }
    }
  }
}

// ──────────────────────────────────────────────
//  Content senders (JS API)
// ──────────────────────────────────────────────

async function sendVideoStream(ctx, data, apiVersion, statusMsg) {
  const videoUrl = getVideoUrl(data, apiVersion);
  if (!videoUrl) throw new Error("No video URL in API response");

  await editStatus(ctx, statusMsg, "📺 Streaming video…");

  const maxBytes = CONFIG.download.maxFileSizeMB * 1024 * 1024;
  const stream = await downloadLimiter.schedule(() => downloadStream(videoUrl, maxBytes));
  const caption = buildCaption(data, "video", apiVersion, "API");

  // Send as a document (uncompressed) — Telegram re-encodes playable videos.
  // disable_content_type_detection keeps it a plain file instead of letting
  // Telegram auto-render the .mp4 as an inline playable video.
  const filename = buildVideoFilename(data);

  await withTelegramRetry(() =>
    ctx.replyWithDocument(new InputFile(stream, filename), {
      caption,
      parse_mode: "Markdown",
      disable_content_type_detection: true,
    })
  );

  metrics.videos++;
}

async function sendImages(ctx, data, apiVersion, statusMsg) {
  const urls = getImageUrls(data);
  if (urls.length === 0) throw new Error("No images in slideshow");

  await editStatus(ctx, statusMsg, `🖼️ Streaming ${urls.length} gambar…`);

  const results = await Promise.allSettled(
    urls.map((url) => downloadLimiter.schedule(() => downloadStream(url)))
  );

  const streams = results
    .filter((r) => r.status === "fulfilled")
    .map((r) => r.value);

  if (streams.length === 0) throw new Error("Failed to download all images");

  if (streams.length < urls.length) {
    await ctx.reply(`⚠️ ${urls.length - streams.length} dari ${urls.length} gambar gagal diunduh.`);
  }

  const caption = buildCaption(data, "image", apiVersion, "API");

  const BATCH_SIZE = 10;
  for (let i = 0; i < streams.length; i += BATCH_SIZE) {
    const batch = streams.slice(i, i + BATCH_SIZE);

    const mediaGroup = batch.map((stream, j) => ({
      type: "photo",
      media: new InputFile(stream),
      ...(i === 0 && j === 0 ? { caption, parse_mode: "Markdown" } : {}),
    }));

    await withTelegramRetry(() => ctx.replyWithMediaGroup(mediaGroup));
  }

  metrics.images += streams.length;
}

async function sendAudioStream(ctx, data, apiVersion, statusMsg) {
  const audioUrl = getAudioUrl(data);
  if (!audioUrl) throw new Error("No audio URL in content");

  await editStatus(ctx, statusMsg, "🎵 Streaming audio…");

  const maxBytes = CONFIG.download.maxFileSizeMB * 1024 * 1024;
  const stream = await downloadLimiter.schedule(() => downloadStream(audioUrl, maxBytes));
  const musicTitle = data.music?.title || data.music?.musicTitle || "TikTok Audio";
  const artist = data.music?.author || data.author?.nickname || "Unknown";

  await withTelegramRetry(() =>
    ctx.replyWithAudio(new InputFile(stream), {
      title: musicTitle,
      performer: artist,
      caption: buildCaption(data, "audio", apiVersion, "API"),
      parse_mode: "Markdown",
    })
  );

  metrics.audio++;
}

// ──────────────────────────────────────────────
//  Content sender (yt-dlp fallback)
// ──────────────────────────────────────────────

async function sendViaYtdlp(ctx, url, mode, statusMsg) {
  if (!ytdlpAvailable) {
    throw new Error("yt-dlp not installed — fallback unavailable");
  }

  await editStatus(ctx, statusMsg, "🔄 Fallback: downloading via yt-dlp…");

  let result;
  let usedCookies = false;

  // Step 1: try without cookies
  try {
    result = await ytdlpLimiter.schedule(() => ytdlpDownload(url, false, mode));
  } catch (err1) {
    log("WARN", `yt-dlp (no cookies): ${err1.message}`);

    // Step 2: try with cookies
    if (hasCookiesFile) {
      try {
        await editStatus(ctx, statusMsg, "🍪 yt-dlp retry with cookies…");
        result = await ytdlpLimiter.schedule(() => ytdlpDownload(url, true, mode));
        usedCookies = true;
        metrics.cookieFallbacks++;
      } catch (err2) {
        throw new Error(`yt-dlp failed: ${err2.message}`);
      }
    } else {
      throw new Error(`yt-dlp failed: ${err1.message}`);
    }
  }

  metrics.ytdlpFallbacks++;

  try {
    await editStatus(ctx, statusMsg, "📤 Uploading via yt-dlp…");

    const ext = path.extname(result.filepath).toLowerCase();
    // Decide by the actual file we got. In audio mode yt-dlp is asked for an
    // audio-only stream; if only a video format exists we still send the video
    // rather than mislabel an .mp4 as audio (which Telegram would reject).
    const isAudio = [".mp3", ".m4a", ".wav", ".flac", ".aac", ".ogg", ".opus"].includes(ext);
    const caption = buildYtdlpCaption(result.metadata, url);

    const fileStream = fs.createReadStream(result.filepath);
    const filename = path.basename(result.filepath);

    if (isAudio) {
      const title = result.metadata?.title || "TikTok Audio";
      const artist = result.metadata?.uploader || "Unknown";

      await withTelegramRetry(() =>
        ctx.replyWithAudio(new InputFile(fileStream, filename), {
          title,
          performer: artist,
          caption,
          parse_mode: "Markdown",
        })
      );
      metrics.audio++;
    } else {
      // Send as a document (uncompressed) — Telegram re-encodes playable videos.
      // disable_content_type_detection keeps it a plain file instead of an
      // inline playable video.
      await withTelegramRetry(() =>
        ctx.replyWithDocument(new InputFile(fileStream, filename), {
          caption,
          parse_mode: "Markdown",
          disable_content_type_detection: true,
        })
      );
      metrics.videos++;
    }
  } finally {
    // Always clean up temp directory
    if (result.tempDir) cleanup(result.tempDir);
  }
}

// ──────────────────────────────────────────────
//  Main processor — cascade logic
//
//  1. JS API (v1→v2) tanpa cookies
//  2. JS API (v1→v2) dengan cookies (jika ada)
//  3. yt-dlp tanpa cookies
//  4. yt-dlp dengan cookies (jika ada)
// ──────────────────────────────────────────────

async function processUrl(ctx, url, mode = "auto") {
  const username = ctx.from?.username || ctx.from?.first_name || "Unknown";
  const userId = ctx.from?.id;
  const inGroup = isGroup(ctx);
  const originalMsgId = ctx.message?.message_id;

  log("INFO", `Processing [${mode}] from ${username} (${userId}): ${url}`);

  metrics.processed++;
  metrics.activeJobs++;

  let statusMsg = null;
  let deletable = false;

  try {
    statusMsg = await ctx.reply("⚡ Memproses link TikTok…");

    if (inGroup && CONFIG.autoDelete.enabled) {
      deletable = await canDeleteMessages(ctx);
    }

    // ── Step 1: JS API ───────────────────────
    let jsSuccess = false;
    let jsErrorMsg = "";

    try {
      await editStatus(ctx, statusMsg, "📡 Fetching via API…");
      const { data, apiVersion } = await apiLimiter.schedule(() => fetchTikTokAPI(url));

      log("INFO", `JS API OK via ${apiVersion}: type=${data.type || "unknown"}`);

      const isImage = data.type === "image" || (data.images?.length > 0);

      if (mode === "audio") {
        await sendAudioStream(ctx, data, apiVersion, statusMsg);
      } else if (isImage) {
        await sendImages(ctx, data, apiVersion, statusMsg);
      } else {
        await sendVideoStream(ctx, data, apiVersion, statusMsg);
      }

      jsSuccess = true;
    } catch (jsErr) {
      jsErrorMsg = jsErr.message;
      log("WARN", `JS API failed for ${username}: ${jsErrorMsg}`);
      // Continue to yt-dlp fallback
    }

    // ── Step 2: yt-dlp fallback ──────────────
    if (!jsSuccess) {
      try {
        await sendViaYtdlp(ctx, url, mode, statusMsg);
      } catch (ytErr) {
        throw new Error(`Semua metode gagal.\nAPI: ${jsErrorMsg}\nyt-dlp: ${ytErr.message}`);
      }
    }

    // ── Cleanup ──────────────────────────────
    if (statusMsg) await tryDelete(ctx, statusMsg.message_id);

    if (inGroup && CONFIG.autoDelete.enabled && deletable) {
      setTimeout(() => tryDelete(ctx, originalMsgId), CONFIG.autoDelete.delayMs);
    }

    log("OK", `Done for ${username}`);
  } catch (err) {
    metrics.failed++;
    log("ERROR", `Failed for ${username}: ${err.message}`);

    const errorText = `❌ Gagal mengunduh:\n${err.message}`;
    if (statusMsg) {
      await editStatus(ctx, statusMsg, errorText);
      setTimeout(() => tryDelete(ctx, statusMsg.message_id), 8_000);
    } else {
      await ctx.reply(errorText).catch(() => {});
    }
  } finally {
    metrics.activeJobs--;
  }
}

// ──────────────────────────────────────────────
//  Bot setup
// ──────────────────────────────────────────────

function createBot() {
  const bot = new Bot(CONFIG.bot.token);

  // ── Middleware ────────────────────────────

  bot.use(
    limit({
      timeFrame: CONFIG.rateLimit.window,
      limit: CONFIG.rateLimit.maxRequests,
      keyGenerator: (ctx) => String(ctx.from?.id ?? "anon"),
      onLimitExceeded: (ctx) =>
        ctx.reply("⏰ Tunggu sebentar, terlalu banyak request!"),
    })
  );

  // ── Commands ─────────────────────────────

  bot.command("start", (ctx) => {
    const text = [
      "🤖 *TikTok Downloader Bot*\n",
      "Kirim link TikTok dan bot akan otomatis mengunduh kontennya.\n",
      "📝 *Perintah:*",
      "/start — Pesan ini",
      "/audio <link> — Unduh audio/musik saja",
      "/stats — Statistik bot",
      "/reload\\_cookies — Muat ulang cookies\n",
      "⚡ *Fitur:*",
      "• Video, slideshow, audio",
      "• Streaming tanpa disk (primary)",
      "• yt-dlp fallback jika API gagal",
      "• Auto-retry dengan cookies",
      "• Auto-delete di grup",
    ].join("\n");

    log("INFO", `User started: ${ctx.from?.username || ctx.from?.id}`);
    return ctx.reply(text, { parse_mode: "Markdown" });
  });

  bot.command("stats", (ctx) => {
    const uptimeSec = Math.floor((Date.now() - metrics.startedAt) / 1000);
    const h = Math.floor(uptimeSec / 3600);
    const m = Math.floor((uptimeSec % 3600) / 60);
    const s = uptimeSec % 60;
    const uptime = `${h}h ${m}m ${s}s`;

    const total = metrics.processed;
    const success = total - metrics.failed;
    const rate = total > 0 ? ((success / total) * 100).toFixed(1) : "–";
    const mem = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);

    const text = [
      "📊 *Statistik Bot*\n",
      `⏱️ Uptime: ${uptime}`,
      `🔢 Total: ${total}`,
      `✅ Sukses: ${rate}%`,
      `🎥 Video: ${metrics.videos}`,
      `🖼️ Gambar: ${metrics.images}`,
      `🎵 Audio: ${metrics.audio}`,
      `🔄 yt-dlp fallbacks: ${metrics.ytdlpFallbacks}`,
      `🍪 Cookie fallbacks: ${metrics.cookieFallbacks}`,
      `⚡ Aktif: ${metrics.activeJobs}`,
      `🧠 Memory: ${mem} MB`,
      `🍪 Cookies: ${hasCookiesFile ? "✅" : "❌"}`,
      `🐍 yt-dlp: ${ytdlpAvailable ? "✅" : "❌"}`,
    ].join("\n");

    return ctx.reply(text, { parse_mode: "Markdown" });
  });

  bot.command("audio", async (ctx) => {
    const text = ctx.message?.text || "";
    const urls = extractTikTokUrls(text);

    if (urls.length === 0) {
      return ctx.reply("🎵 Kirim: /audio <link TikTok>");
    }

    await processingLimiter.schedule(() => processUrl(ctx, urls[0], "audio"));
  });

  bot.command("reload_cookies", (ctx) => {
    loadCookies();
    return ctx.reply(`🍪 Cookies: ${hasCookiesFile ? "✅ Loaded" : "❌ Tidak ditemukan"}`);
  });

  // ── URL handler ──────────────────────────

  bot.on("message:text", async (ctx) => {
    const urls = extractTikTokUrls(ctx.message.text);
    if (urls.length === 0) return;

    const batch = urls.slice(0, CONFIG.download.maxUrlsPerMessage);

    if (urls.length > CONFIG.download.maxUrlsPerMessage) {
      await ctx.reply(
        `⚠️ Maksimal ${CONFIG.download.maxUrlsPerMessage} link per pesan. Memproses ${CONFIG.download.maxUrlsPerMessage} pertama.`
      );
    }

    await Promise.allSettled(batch.map((url) => processingLimiter.schedule(() => processUrl(ctx, url))));
  });

  // ── Error boundary ───────────────────────

  bot.catch((err) => {
    const e = err.error;

    if (e instanceof GrammyError) {
      log("ERROR", `Telegram API error: ${e.description}`);
    } else if (e instanceof HttpError) {
      log("ERROR", `Network error: ${e.message}`);
    } else {
      log("ERROR", `Unhandled: ${e?.message || e}`);
    }
  });

  return bot;
}

// ──────────────────────────────────────────────
//  Periodic cleanup for yt-dlp temp files
// ──────────────────────────────────────────────

function startCleanupInterval() {
  setInterval(() => {
    try {
      const tempBase = CONFIG.ytdlp.tempDir;
      if (!fs.existsSync(tempBase)) return;

      const cutoff = Date.now() - 3600_000; // 1 hour
      const entries = fs.readdirSync(tempBase, { withFileTypes: true });

      for (const entry of entries) {
        if (!entry.isDirectory()) continue;
        const fullPath = path.join(tempBase, entry.name);
        try {
          const stat = fs.statSync(fullPath);
          if (stat.mtimeMs < cutoff) {
            fs.rmSync(fullPath, { recursive: true, force: true });
            log("INFO", `Cleaned up stale temp dir: ${entry.name}`);
          }
        } catch {
          // ignore
        }
      }
    } catch (err) {
      log("WARN", `Cleanup error: ${err.message}`);
    }
  }, 30 * 60_000); // every 30 minutes
}

// ──────────────────────────────────────────────
//  Startup
// ──────────────────────────────────────────────

async function main() {
  // Ensure yt-dlp temp dir exists
  fs.mkdirSync(CONFIG.ytdlp.tempDir, { recursive: true });

  loadCookies();
  checkYtdlp();

  const bot = createBot();

  // Init with retries
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      await bot.init();
      log("OK", `Bot initialized: @${bot.botInfo.username}`);
      break;
    } catch (err) {
      if (attempt === 3) {
        log("ERROR", `Init failed after 3 attempts: ${err.message}`);
        process.exit(1);
      }
      log("WARN", `Init attempt ${attempt}/3 failed, retrying…`);
      await sleep(2_000);
    }
  }

  const runner = run(bot, {
    runner: {
      fetch: {
        allowed_updates: CONFIG.bot.allowedUpdates,
      },
      retryInterval: "exponential", // backoff between failed getUpdates calls
      maxRetryTime: 30 * 60_000,    // keep retrying getUpdates for up to 30 min
    },
  });

  startCleanupInterval();

  log("OK", "Bot is running! Press Ctrl+C to stop.");
  log("INFO", `Cookies: ${hasCookiesFile ? "✅" : "❌"}  |  yt-dlp: ${ytdlpAvailable ? "✅" : "❌"}`);

  const shutdown = () => {
    log("WARN", "Shutting down…");
    if (runner.isRunning()) runner.stop();

    // Cleanup temp dir
    try {
      fs.rmSync(CONFIG.ytdlp.tempDir, { recursive: true, force: true });
    } catch {
      // ignore
    }

    process.exit(0);
  };

  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

process.on("uncaughtException", (err) => {
  log("ERROR", `Uncaught: ${err.message}`);
});

process.on("unhandledRejection", (err) => {
  log("ERROR", `Unhandled rejection: ${err?.message || err}`);
});

main().catch((err) => {
  log("ERROR", `Fatal: ${err.message}`);
  process.exit(1);
});
