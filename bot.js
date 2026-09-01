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
const { spawn } = require("child_process");
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
    maxRequests: 20,
    // Cap on queued+running jobs per user — the middleware limit counts
    // messages, but one message can fan out into maxUrlsPerMessage jobs.
    maxQueuedJobsPerUser: 40,
  },

  api: {
    v1Retries: 1,
    v2Retries: 2,
    retryBackoffMs: 1_000,
  },

  download: {
    timeout: 30_000,
    maxRedirects: 10,
    maxUrlsPerMessage: 20,
    maxFileSizeMB: 50,
  },

  ytdlp: {
    timeout: 120_000,       // 2 minutes for yt-dlp
    recheckIntervalMs: 10 * 60_000, // re-probe the binary so runtime (un)installs are noticed
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
      reservoir: 100,
      reservoirRefreshAmount: 100,
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
  DEBUG: "\x1b[90m",
  RESET: "\x1b[0m",
};

const DEBUG_ENABLED = ["1", "true"].includes(String(process.env.LOG_DEBUG).toLowerCase());

function log(level, message) {
  const ts = new Date().toISOString();
  const color = LOG_COLORS[level] || LOG_COLORS.RESET;
  console.log(`${color}[${ts}] [${level}] ${message}${LOG_COLORS.RESET}`);
}

/** For intentionally-swallowed errors, so they stay diagnosable. Enable with LOG_DEBUG=1. */
function logDebug(message) {
  if (DEBUG_ENABLED) log("DEBUG", message);
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

// Per-user cap on queued+running jobs — without this, a user staying under
// the message rate limit could still queue hundreds of jobs (20 msg/10s ×
// maxUrlsPerMessage links each) and grow Bottleneck's queue unboundedly.
const userJobCounts = new Map();

function acquireUserJobs(userId, requested) {
  const current = userJobCounts.get(userId) || 0;
  const granted = Math.max(0, Math.min(requested, CONFIG.rateLimit.maxQueuedJobsPerUser - current));
  if (granted > 0) userJobCounts.set(userId, current + granted);
  return granted;
}

function releaseUserJob(userId) {
  const current = userJobCounts.get(userId) || 0;
  if (current <= 1) userJobCounts.delete(userId);
  else userJobCounts.set(userId, current - 1);
}

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
    // If the consumer destroys the PassThrough (e.g. a failed upload), stop
    // the underlying CDN download too instead of letting the socket linger.
    pass.on("close", () => {
      if (!resp.data.destroyed) resp.data.destroy();
    });
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

// Telegram caps message text at 4096 chars, but don't rely on that upstream
// guarantee: bound the scanned length ourselves.
const MAX_URL_SCAN_LENGTH = 8_192;

function extractTikTokUrls(text) {
  if (typeof text !== "string") return [];
  const bounded = text.length > MAX_URL_SCAN_LENGTH ? text.slice(0, MAX_URL_SCAN_LENGTH) : text;
  // Strip trailing punctuation the \S+ match drags along ("link." / "link,"):
  // canonical TikTok URLs never end in these. Dedupe so a URL pasted twice in
  // one message isn't downloaded and sent twice.
  const urls = (bounded.match(TIKTOK_URL_RE) || []).map((u) =>
    u.replace(/[)\]}>.,;:!?'"]+$/, "")
  );
  return [...new Set(urls)];
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
 * yt-dlp availability — probed at startup, re-probed on an interval, and
 * flipped off immediately on ENOENT so a runtime uninstall/install is noticed
 * without restarting the bot.
 */
let ytdlpAvailable = false;
let ytdlpEverChecked = false;

function setYtdlpStatus(available, version) {
  if (available !== ytdlpAvailable || !ytdlpEverChecked) {
    if (available) {
      log("OK", `yt-dlp found: ${version || "unknown version"}`);
    } else {
      log("WARN", "yt-dlp not found — fallback disabled. Install: pip install yt-dlp");
    }
  }
  ytdlpAvailable = available;
  ytdlpEverChecked = true;
}

function checkYtdlp() {
  // Async spawn instead of execSync: this also runs on an interval at runtime,
  // where blocking the event loop for up to 5s is not acceptable.
  return new Promise((resolve) => {
    let settled = false;
    let out = "";
    const finish = (available, version) => {
      if (settled) return;
      settled = true;
      setYtdlpStatus(available, version);
      resolve(available);
    };

    const proc = spawn("yt-dlp", ["--version"], {
      stdio: ["ignore", "pipe", "ignore"],
      windowsHide: true,
    });
    const timer = setTimeout(() => {
      proc.kill("SIGKILL");
      finish(false);
    }, 5000);

    proc.stdout.on("data", (d) => { out += d.toString(); });
    proc.on("error", () => { clearTimeout(timer); finish(false); });
    proc.on("close", (code) => { clearTimeout(timer); finish(code === 0, out.trim()); });
  });
}

/**
 * Download via yt-dlp, returning the file path.
 * @param {string} url
 * @param {boolean} useCookies
 * @returns {{ filepath: string, metadata: object }}
 */
function ytdlpDownload(url, useCookies = false, mode = "auto") {
  return new Promise((resolve, reject) => {
    // Re-create the base dir each time: OS tmp cleaners (systemd-tmpfiles,
    // tmpwatch) can remove it while the bot runs, which would otherwise make
    // every mkdtemp fail with ENOENT until restart.
    fs.mkdirSync(CONFIG.ytdlp.tempDir, { recursive: true });
    const tempDir = fs.mkdtempSync(path.join(CONFIG.ytdlp.tempDir, "dl-"));
    activeTempDirs.add(tempDir);
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
      // TikTok's WAF serves a JS challenge page instead of the video page to
      // yt-dlp's default User-Agent, so extraction fails before any download
      // ("Unexpected response from webpage request"). A browser UA gets the
      // real page.
      "--user-agent", DEFAULT_HEADERS["User-Agent"],
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
        // Fall back to the last stderr line so the user never sees just a
        // bare exit code for failures parseYtdlpError doesn't recognize.
        const tail = stderr.trim().split("\n").filter(Boolean).pop();
        const errMsg =
          parseYtdlpError(stderr) ||
          (tail ? `yt-dlp: ${tail.trim().slice(0, 200)}` : `yt-dlp exit code ${code}`);
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
          // --max-filesize makes yt-dlp skip the download but still exit 0,
          // which would otherwise surface as an unhelpful "no output".
          if (/larger than.*max-filesize/i.test(stdout + stderr)) {
            return reject(new Error(`Video melebihi batas ${maxSize}MB`));
          }
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
        } catch (err) {
          logDebug(`yt-dlp metadata parse failed: ${err.message}`);
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
      // Binary disappeared at runtime — stop advertising the yt-dlp path
      // until the periodic re-check sees it again.
      if (err.code === "ENOENT") setYtdlpStatus(false);
      reject(err);
    });
  });
}

function parseYtdlpError(stderr) {
  const keywords = [
    "error:", "failed:", "unable to", "not found", "not available", "forbidden",
    "unavailable", "unsupported url", "sign in", "login required", "private",
    "timed out", "http error",
  ];
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

/** Temp dirs of jobs still downloading or uploading — the periodic sweep must skip these. */
const activeTempDirs = new Set();

function cleanup(dir) {
  activeTempDirs.delete(dir);
  try {
    // maxRetries: on Windows rm can hit transient EBUSY/EPERM right after a
    // file handle is closed.
    fs.rmSync(dir, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
  } catch (err) {
    logDebug(`cleanup(${dir}) failed: ${err.message}`);
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

  if (apiVersion === "v2") {
    // SSSTik sometimes returns only a top-level `direct` URL without video.playAddr.
    return firstUrl(v?.playAddr) || firstUrl(data?.direct);
  }

  if (!v) return null;

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

/** Escape text for Telegram HTML parse mode. */
function escHtml(text) {
  return String(text).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
}

/** Escape a URL for use inside an HTML href="" attribute. */
function escAttr(url) {
  return escHtml(url).replace(/"/g, "&quot;");
}

/**
 * Sanitize a filename base. The \w whitelist already removes slashes (so no
 * path traversal is possible); edge dots are stripped so an all-junk title
 * can't collapse into "..", ".mp4", or an empty name.
 */
function sanitizeFilenameBase(base) {
  const cleaned = String(base)
    .replace(/[^\w.-]+/g, "_")
    .replace(/^[.\s]+|[.\s]+$/g, "")
    .slice(0, 100);
  return cleaned || "tiktok";
}

/** Build a safe .mp4 filename so Telegram treats the document as a video. */
function buildVideoFilename(data) {
  const author = data.author || {};
  const username = author.uniqueId || author.username || author.nickname || "tiktok";
  const videoId = data.id || data.aweme_id || Date.now();
  return `${sanitizeFilenameBase(`${username}_${videoId}`)}.mp4`;
}

/**
 * Build "<username>_<videoid>.<ext>" for a yt-dlp download from its metadata,
 * keeping the real file extension. Falls back to yt-dlp's own filename when
 * neither username nor id is available.
 */
function buildYtdlpFilename(metadata, filepath) {
  const username = metadata?.uploader || metadata?.uploader_id || metadata?.creator || "";
  const videoId = metadata?.id || metadata?.display_id || "";

  if (!username && !videoId) {
    return path.basename(filepath);
  }

  const ext = path.extname(filepath) || (metadata?.ext ? `.${metadata.ext}` : ".mp4");
  const base = sanitizeFilenameBase([username || "tiktok", videoId].filter(Boolean).join("_"));
  return `${base}${ext}`;
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
    `📅 ${escHtml(formatTimestamp(data.createTime || data.create_time))}`,
    `👤 <a href="${escAttr(originalUrl)}">${escHtml(username)}</a> (UID: ${escHtml(uid)})`,
    `🔧 ${escHtml(method)} ${escHtml(String(apiVersion).toUpperCase())}`,
    `🔗 <a href="${escAttr(originalUrl)}">Link TikTok</a>`,
  ];

  const desc = (data.desc || data.description || "").trim();
  if (desc) {
    const short = desc.length > 300 ? `${desc.slice(0, 300)}…` : desc;
    lines.push("", `📝 "${escHtml(short)}"`);
  }

  return lines.join("\n");
}

function buildYtdlpCaption(metadata, url) {
  const title = metadata.title || metadata.fulltitle || "TikTok Video";
  const uploader = metadata.uploader || metadata.uploader_id || "Unknown";
  const duration = metadata.duration ? `${Math.round(metadata.duration)}s` : "–";
  const webpage = metadata.webpage_url || url;

  const lines = [
    `👤 <a href="${escAttr(webpage)}">${escHtml(uploader)}</a>`,
    `🔧 yt-dlp`,
    `⏱️ ${escHtml(duration)}`,
    `🔗 <a href="${escAttr(webpage)}">Link TikTok</a>`,
  ];

  if (title && title !== "TikTok Video") {
    const short = title.length > 200 ? `${title.slice(0, 200)}…` : title;
    lines.push("", `📝 "${escHtml(short)}"`);
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

// chatId -> { value, expiresAt }. Timestamp expiry instead of setTimeout:
// concurrent cache misses in one chat would stack timers whose earliest firing
// cuts short the TTL of a freshly cached entry. Expired entries are dropped on
// read; the periodic cleanup sweeps entries of chats that went quiet. The TTL
// also bounds how long a stale answer survives an admin change mid-runtime.
const deletePermCache = new Map();
const DELETE_PERM_TTL_MS = 10 * 60_000;

async function canDeleteMessages(ctx) {
  const chatId = ctx.chat.id;
  const cached = deletePermCache.get(chatId);
  if (cached && cached.expiresAt > Date.now()) return cached.value;
  deletePermCache.delete(chatId);
  try {
    const me = await ctx.api.getChatMember(chatId, ctx.me.id);
    const value = me.status === "creator" || me.can_delete_messages === true;
    deletePermCache.set(chatId, { value, expiresAt: Date.now() + DELETE_PERM_TTL_MS });
    return value;
  } catch (err) {
    logDebug(`getChatMember(${chatId}) failed: ${err.message}`);
    return false;
  }
}

async function tryDelete(ctx, messageId) {
  try {
    await ctx.api.deleteMessage(ctx.chat.id, messageId);
  } catch (err) {
    logDebug(`deleteMessage(${messageId}) failed: ${err.message}`);
  }
}

async function editStatus(ctx, msg, text) {
  try {
    await ctx.api.editMessageText(msg.chat.id, msg.message_id, text);
  } catch (err) {
    logDebug(`editMessageText failed: ${err.message}`);
  }
}

// ──────────────────────────────────────────────
//  Telegram send helper with retry (429 + transient network errors)
// ──────────────────────────────────────────────

const TRANSIENT_NET_CODES = new Set([
  "ECONNRESET", "ECONNABORTED", "ECONNREFUSED", "ETIMEDOUT", "EPIPE",
  "EAI_AGAIN", "ENETUNREACH", "UND_ERR_SOCKET", "UND_ERR_CONNECT_TIMEOUT",
  "UND_ERR_HEADERS_TIMEOUT", "UND_ERR_BODY_TIMEOUT",
]);

function isTransientTelegramError(err) {
  // grammY wraps all network-level failures (timeouts, resets) in HttpError.
  if (err instanceof HttpError) return true;
  // Telegram-side transient failures: flood limit and 5xx gateway hiccups.
  // Other GrammyErrors (400 bad request, etc.) are permanent — never retry.
  if (err instanceof GrammyError) {
    return err.error_code === 429 || err.error_code >= 500;
  }
  const code = err?.code || err?.cause?.code;
  if (code && TRANSIENT_NET_CODES.has(code)) return true;
  return /socket hang up|fetch failed|network|terminated/i.test(err?.message || "");
}

/**
 * Retry a Telegram send. `makeAttempt` runs once per attempt and MUST create
 * any streams it uploads inside itself — a stream consumed by a failed
 * attempt cannot be replayed, so reusing one would send an empty/corrupt file.
 */
async function withTelegramRetry(makeAttempt, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await makeAttempt();
    } catch (err) {
      if (attempt >= maxRetries || !isTransientTelegramError(err)) throw err;

      let waitMs;
      if (err instanceof GrammyError && err.error_code === 429) {
        waitMs = ((err.parameters?.retry_after ?? 5) + 1) * 1000;
        log("WARN", `Telegram flood limit (429), retry in ${waitMs}ms (attempt ${attempt}/${maxRetries})`);
      } else {
        waitMs = 1000 * 2 ** (attempt - 1);
        log("WARN", `Transient send error: ${err.message} — retry in ${waitMs}ms (attempt ${attempt}/${maxRetries})`);
      }
      await sleep(waitMs);
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
  const caption = buildCaption(data, "video", apiVersion, "API");
  const filename = buildVideoFilename(data);

  // Download inside the retry attempt: a consumed stream can't be replayed,
  // so each retry needs a fresh one.
  await withTelegramRetry(async () => {
    const stream = await downloadLimiter.schedule(() => downloadStream(videoUrl, maxBytes));
    try {
      // Send as a document (uncompressed) — Telegram re-encodes playable videos.
      // disable_content_type_detection keeps it a plain file instead of letting
      // Telegram auto-render the .mp4 as an inline playable video.
      return await ctx.replyWithDocument(new InputFile(stream, filename), {
        caption,
        parse_mode: "HTML",
        disable_content_type_detection: true,
      });
    } catch (err) {
      stream.destroy(); // release the CDN socket if the upload failed midway
      throw err;
    }
  });

  metrics.videos++;
}

async function sendImages(ctx, data, apiVersion, statusMsg) {
  const urls = getImageUrls(data);
  if (urls.length === 0) throw new Error("No images in slideshow");

  await editStatus(ctx, statusMsg, `🖼️ Streaming ${urls.length} gambar…`);

  const caption = buildCaption(data, "image", apiVersion, "API");

  const BATCH_SIZE = 10;
  let sentCount = 0;
  let failedCount = 0;
  let captionPending = true;

  for (let i = 0; i < urls.length; i += BATCH_SIZE) {
    const batchUrls = urls.slice(i, i + BATCH_SIZE);
    const wantCaption = captionPending;

    // Download inside the retry attempt: a consumed stream can't be replayed,
    // so each retry needs fresh ones. On a failed send every stream of the
    // batch is destroyed so the CDN sockets don't linger.
    const batchResult = await withTelegramRetry(async () => {
      const settled = await Promise.allSettled(
        batchUrls.map((url) => downloadLimiter.schedule(() => downloadStream(url)))
      );
      const streams = settled
        .filter((r) => r.status === "fulfilled")
        .map((r) => r.value);

      if (streams.length === 0) {
        return { sent: 0, failed: batchUrls.length };
      }

      try {
        if (streams.length === 1) {
          // Telegram requires 2–10 items per media group — a lone leftover
          // (e.g. image 11 of an 11-image slideshow, or a batch where only
          // one download succeeded) must go as a single photo instead.
          await ctx.replyWithPhoto(
            new InputFile(streams[0]),
            wantCaption ? { caption, parse_mode: "HTML" } : {}
          );
        } else {
          const mediaGroup = streams.map((stream, j) => ({
            type: "photo",
            media: new InputFile(stream),
            ...(wantCaption && j === 0 ? { caption, parse_mode: "HTML" } : {}),
          }));
          await ctx.replyWithMediaGroup(mediaGroup);
        }
      } catch (err) {
        for (const s of streams) s.destroy();
        throw err;
      }

      return { sent: streams.length, failed: batchUrls.length - streams.length };
    });

    if (batchResult.sent > 0) captionPending = false;
    sentCount += batchResult.sent;
    failedCount += batchResult.failed;
  }

  if (sentCount === 0) throw new Error("Failed to download all images");

  if (failedCount > 0) {
    await ctx
      .reply(`⚠️ ${failedCount} dari ${urls.length} gambar gagal diunduh.`)
      .catch(() => {});
  }

  metrics.images += sentCount;
}

async function sendAudioStream(ctx, data, apiVersion, statusMsg) {
  const audioUrl = getAudioUrl(data);
  if (!audioUrl) throw new Error("No audio URL in content");

  await editStatus(ctx, statusMsg, "🎵 Streaming audio…");

  const maxBytes = CONFIG.download.maxFileSizeMB * 1024 * 1024;
  const musicTitle = data.music?.title || data.music?.musicTitle || "TikTok Audio";
  const artist = data.music?.author || data.author?.nickname || "Unknown";
  const caption = buildCaption(data, "audio", apiVersion, "API");

  // Download inside the retry attempt: a consumed stream can't be replayed.
  await withTelegramRetry(async () => {
    const stream = await downloadLimiter.schedule(() => downloadStream(audioUrl, maxBytes));
    try {
      return await ctx.replyWithAudio(new InputFile(stream), {
        title: musicTitle,
        performer: artist,
        caption,
        parse_mode: "HTML",
      });
    } catch (err) {
      stream.destroy(); // release the CDN socket if the upload failed midway
      throw err;
    }
  });

  metrics.audio++;
}

// ──────────────────────────────────────────────
//  Content sender (yt-dlp — primary, max quality)
// ──────────────────────────────────────────────

async function sendViaYtdlp(ctx, url, mode, statusMsg) {
  if (!ytdlpAvailable) {
    throw new Error("yt-dlp not installed");
  }

  await editStatus(ctx, statusMsg, "⬇️ Mengunduh kualitas terbaik (yt-dlp)…");

  let result;
  let usedCookies = false;

  // Step 1: try without cookies
  try {
    result = await ytdlpLimiter.schedule(() => ytdlpDownload(url, false, mode));
  } catch (err1) {
    log("WARN", `yt-dlp (no cookies): ${err1.message}`);

    // A size-cap failure is not an auth problem — cookies won't shrink the
    // video, so a retry would only burn up to another 2-minute timeout.
    const isSizeError = /melebihi batas|too large|max-filesize/i.test(err1.message);

    // Step 2: try with cookies
    if (hasCookiesFile && !isSizeError) {
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

    // The reverse mislabel: in video/auto mode an audio-only file means
    // yt-dlp couldn't get actual video — TikTok slideshow posts, for one,
    // download as just their soundtrack. Throw so the cascade falls back to
    // the JS API instead of replying with the wrong content type.
    if (isAudio && mode !== "audio") {
      throw new Error("yt-dlp returned audio-only output for a video request");
    }

    const caption = buildYtdlpCaption(result.metadata, url);
    const filename = buildYtdlpFilename(result.metadata, result.filepath);

    // Open the file inside each retry attempt: a consumed stream can't be
    // replayed, and a leftover open fd would make the rm in cleanup() fail
    // on Windows and orphan the temp dir.
    const sendFile = async (send) => {
      const fileStream = fs.createReadStream(result.filepath);
      try {
        return await send(fileStream);
      } catch (err) {
        fileStream.destroy();
        throw err;
      }
    };

    if (isAudio) {
      const title = result.metadata?.title || "TikTok Audio";
      const artist = result.metadata?.uploader || "Unknown";

      await withTelegramRetry(() =>
        sendFile((fileStream) =>
          ctx.replyWithAudio(new InputFile(fileStream, filename), {
            title,
            performer: artist,
            caption,
            parse_mode: "HTML",
          })
        )
      );
      metrics.audio++;
    } else {
      // Send as a document (uncompressed) — Telegram re-encodes playable videos.
      // disable_content_type_detection keeps it a plain file instead of an
      // inline playable video.
      await withTelegramRetry(() =>
        sendFile((fileStream) =>
          ctx.replyWithDocument(new InputFile(fileStream, filename), {
            caption,
            parse_mode: "HTML",
            disable_content_type_detection: true,
          })
        )
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
//  0. JS API metadata fetch (detects image vs video; also a fallback source)
//  Images (slideshow):
//    → JS API only (yt-dlp can't handle slideshows)
//  Video / audio (quality-first):
//    1. yt-dlp (best quality ≤50MB) without cookies → with cookies
//    2. JS API stream (no-watermark) as fallback if yt-dlp fails or returns
//       an audio-only file for a video request (e.g. slideshow soundtrack);
//       re-fetches metadata if step 0 failed and re-detects slideshows
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

    // ── Step 0: JS API metadata ──────────────
    // Detects image vs video and doubles as the fallback source if yt-dlp fails.
    let apiData = null;
    let apiVersion = null;
    let apiErrorMsg = "";

    try {
      await editStatus(ctx, statusMsg, "📡 Mengambil info…");
      const res = await apiLimiter.schedule(() => fetchTikTokAPI(url));
      apiData = res.data;
      apiVersion = res.apiVersion;
      log("INFO", `JS API OK via ${apiVersion}: type=${apiData.type || "unknown"}`);
    } catch (jsErr) {
      apiErrorMsg = jsErr.message;
      log("WARN", `JS API metadata failed for ${username}: ${apiErrorMsg}`);
    }

    const isImage = !!apiData && (apiData.type === "image" || apiData.images?.length > 0);

    // ── Send ─────────────────────────────────
    if (isImage && mode !== "audio") {
      // Slideshows: only the JS API path handles these.
      await sendImages(ctx, apiData, apiVersion, statusMsg);
    } else {
      // Video/audio: prefer yt-dlp for maximum quality, fall back to JS API stream.
      let sent = false;
      let ytErrorMsg = "";

      if (ytdlpAvailable) {
        try {
          await sendViaYtdlp(ctx, url, mode, statusMsg);
          sent = true;
        } catch (ytErr) {
          ytErrorMsg = ytErr.message;
          log("WARN", `yt-dlp failed for ${username}: ${ytErrorMsg}`);
        }
      }

      if (!sent) {
        // Step 0 may have failed transiently and yt-dlp ran for up to two
        // minutes since — the API is the last remaining source, so give the
        // metadata fetch (v1 → v2) one more chance before giving up.
        if (!apiData) {
          try {
            await editStatus(ctx, statusMsg, "📡 Fallback ke API…");
            const res = await apiLimiter.schedule(() => fetchTikTokAPI(url));
            apiData = res.data;
            apiVersion = res.apiVersion;
            log("INFO", `JS API retry OK via ${apiVersion}: type=${apiData.type || "unknown"}`);
          } catch (jsErr) {
            apiErrorMsg = jsErr.message;
          }
        }

        if (!apiData) {
          throw new Error(
            `Semua metode gagal.\nyt-dlp: ${ytErrorMsg || "tidak tersedia"}\nAPI: ${apiErrorMsg || "tidak ada data"}`
          );
        }

        await editStatus(ctx, statusMsg, "📡 Fallback ke API…");
        if (mode === "audio") {
          await sendAudioStream(ctx, apiData, apiVersion, statusMsg);
        } else if (apiData.type === "image" || apiData.images?.length > 0) {
          // A slideshow step 0 couldn't identify (its metadata fetch failed
          // back then): yt-dlp only grabs the soundtrack for these — which is
          // why it was rejected above — so send the actual images.
          await sendImages(ctx, apiData, apiVersion, statusMsg);
        } else {
          await sendVideoStream(ctx, apiData, apiVersion, statusMsg);
        }
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
      "• Video kualitas terbaik via yt-dlp (dikirim sebagai file)",
      "• Fallback ke API jika yt-dlp gagal",
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
      `⬇️ yt-dlp downloads: ${metrics.ytdlpFallbacks}`,
      `🍪 Cookie retries: ${metrics.cookieFallbacks}`,
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

    const userKey = String(ctx.from?.id ?? "anon");
    if (acquireUserJobs(userKey, 1) === 0) {
      return ctx.reply("⏰ Antrean kamu masih penuh — tunggu beberapa unduhan selesai dulu.").catch(() => {});
    }

    await processingLimiter
      .schedule(() => processUrl(ctx, urls[0], "audio"))
      .finally(() => releaseUserJob(userKey));
  });

  bot.command("reload_cookies", (ctx) => {
    loadCookies();
    return ctx.reply(`🍪 Cookies: ${hasCookiesFile ? "✅ Loaded" : "❌ Tidak ditemukan"}`);
  });

  // ── URL handler ──────────────────────────

  bot.on("message:text", async (ctx) => {
    const urls = extractTikTokUrls(ctx.message.text);
    if (urls.length === 0) return;

    let batch = urls.slice(0, CONFIG.download.maxUrlsPerMessage);

    if (urls.length > CONFIG.download.maxUrlsPerMessage) {
      await ctx
        .reply(
          `⚠️ Maksimal ${CONFIG.download.maxUrlsPerMessage} link per pesan. Memproses ${CONFIG.download.maxUrlsPerMessage} pertama.`
        )
        .catch(() => {});
    }

    const userKey = String(ctx.from?.id ?? "anon");
    const granted = acquireUserJobs(userKey, batch.length);

    if (granted === 0) {
      return ctx.reply("⏰ Antrean kamu masih penuh — tunggu beberapa unduhan selesai dulu.").catch(() => {});
    }
    if (granted < batch.length) {
      await ctx
        .reply(`⚠️ Antrean hampir penuh: hanya ${granted} dari ${batch.length} link yang diproses.`)
        .catch(() => {});
      batch = batch.slice(0, granted);
    }

    await Promise.allSettled(
      batch.map((url) =>
        processingLimiter
          .schedule(() => processUrl(ctx, url))
          .finally(() => releaseUserJob(userKey))
      )
    );
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
    // Sweep expired delete-permission cache entries of chats that went quiet
    // (active chats already drop theirs lazily on read).
    const now = Date.now();
    for (const [chatId, entry] of deletePermCache) {
      if (entry.expiresAt <= now) deletePermCache.delete(chatId);
    }

    try {
      const tempBase = CONFIG.ytdlp.tempDir;
      if (!fs.existsSync(tempBase)) return;

      const cutoff = Date.now() - 3600_000; // 1 hour
      const entries = fs.readdirSync(tempBase, { withFileTypes: true });

      for (const entry of entries) {
        if (!entry.isDirectory()) continue;
        const fullPath = path.join(tempBase, entry.name);
        // Never sweep a dir a running job still uses — mtime alone can't tell,
        // since the upload to Telegram can outlive the 1h cutoff.
        if (activeTempDirs.has(fullPath)) continue;
        try {
          const stat = fs.statSync(fullPath);
          if (stat.mtimeMs < cutoff) {
            fs.rmSync(fullPath, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
            log("INFO", `Cleaned up stale temp dir: ${entry.name}`);
          }
        } catch (err) {
          logDebug(`Cleanup skipped ${entry.name}: ${err.message}`);
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
  await checkYtdlp();

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

  // When getUpdates keeps failing past maxRetryTime the runner stops silently
  // and the process would stay alive but deaf. Exit non-zero instead so a
  // supervisor (pm2/systemd/NSSM) can restart the bot.
  let shuttingDown = false;
  runner.task()?.then(
    () => {
      if (!shuttingDown) {
        log("ERROR", "Update polling stopped unexpectedly (maxRetryTime exceeded?) — exiting");
        process.exit(1);
      }
    },
    (err) => {
      log("ERROR", `Update polling crashed: ${err?.message || err} — exiting`);
      process.exit(1);
    }
  );

  startCleanupInterval();

  // Re-probe yt-dlp so an uninstall/install during runtime is picked up.
  setInterval(() => { checkYtdlp(); }, CONFIG.ytdlp.recheckIntervalMs);

  log("OK", "Bot is running! Press Ctrl+C to stop.");
  log("INFO", `Cookies: ${hasCookiesFile ? "✅" : "❌"}  |  yt-dlp: ${ytdlpAvailable ? "✅" : "❌"}`);

  const shutdown = () => {
    shuttingDown = true;
    log("WARN", "Shutting down…");
    if (runner.isRunning()) runner.stop();

    // Only remove our own dl-* dirs: YTDLP_TEMP_DIR may point at a directory
    // shared with other software.
    try {
      for (const entry of fs.readdirSync(CONFIG.ytdlp.tempDir, { withFileTypes: true })) {
        if (entry.isDirectory() && entry.name.startsWith("dl-")) {
          fs.rmSync(path.join(CONFIG.ytdlp.tempDir, entry.name), { recursive: true, force: true });
        }
      }
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
  log("ERROR", `Uncaught: ${err?.stack || err?.message || err}`);
});

process.on("unhandledRejection", (err) => {
  log("ERROR", `Unhandled rejection: ${err?.stack || err?.message || err}`);
});

main().catch((err) => {
  log("ERROR", `Fatal: ${err.message}`);
  process.exit(1);
});
