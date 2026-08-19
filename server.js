const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
require("dotenv").config();
const { verifyFloSignature } = require("./flo-auth");
const {
  verifyFloPayment,
  sendFloPayment,
  MARKETPLACE_FLO_ADDRESS,
} = require("./flo-chain");

// Fail fast if the DB isn't configured
if (!process.env.DATABASE_URL) {
  console.error("FATAL: DATABASE_URL is not set. Exiting.");
  process.exit(1);
}

const app = express();
const port = process.env.PORT || 3000;
const sunoCache = new Map();
const flowCache = new Map();
const CACHE_DURATION = 5 * 60 * 1000; // 5 minutes
const MAX_CACHE_ENTRIES = 2000; // cap cache size to avoid unbounded growth

const cacheCleanupInterval = setInterval(() => {
  const now = Date.now();

  for (const [key, entry] of sunoCache.entries()) {
    if (now - entry.timestamp > CACHE_DURATION) {
      sunoCache.delete(key);
    }
  }

  for (const [key, entry] of flowCache.entries()) {
    if (now - entry.timestamp > CACHE_DURATION) {
      flowCache.delete(key);
    }
  }
}, 60 * 1000);

function cacheSet(cache, key, value) {
  if (cache.size >= MAX_CACHE_ENTRIES) {
    // evict oldest entry (Maps preserve insertion order)
    const oldestKey = cache.keys().next().value;
    cache.delete(oldestKey);
  }
  cache.set(key, value);
}

// Restrict CORS via ALLOWED_ORIGINS (comma-separated). Falls back to
// allow-all if unset.
const allowedOrigins = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(",").map((o) => o.trim())
  : null;

app.use(
  cors(
    allowedOrigins
      ? {
          origin: (origin, callback) => {
            if (!origin || allowedOrigins.includes(origin)) {
              callback(null, true);
            } else {
              callback(new Error("Not allowed by CORS"));
            }
          },
        }
      : undefined,
  ),
);
app.use(express.json({ limit: "100kb" }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

// Log and prevent crashes on idle client errors (e.g. DB connection drops)
pool.on("error", (err) => {
  console.error("Unexpected error on idle PostgreSQL client:", err);
});

let dbReady = false;

pool
  .connect()
  .then(async (client) => {
    console.log("Connected to Neon PostgreSQL");
    client.release();
    await ensureMarketplaceSchema();
    dbReady = true;
  })
  .catch((err) => {
    console.error("Database connection failed:", err);
  });

// Fetch with a hard timeout - external scrape targets (Suno / Flow Music)
// can stall indefinitely, which would otherwise tie up connections forever.
async function fetchWithTimeout(url, options = {}, timeoutMs = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

// Helper: Scrape Suno Plays
async function fetchSunoPlayCount(inputUrl) {
  try {
    const response = await fetchWithTimeout(inputUrl, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      },
    });
    if (!response.ok) {
      throw new Error(`Failed to fetch from Suno. Status: ${response.status}`);
    }

    const html = await response.text();
    let playCount = null;

    // Extract play count using the known regex pattern
    const playCountMatch = html.match(/play_count\\?["']?\s*:\s*(\d+)/i);
    if (playCountMatch) {
      playCount = parseInt(playCountMatch[1], 10);
    } else {
      console.warn("Could not find play_count in HTML");
    }

    return playCount;
  } catch (e) {
    console.error("Suno scrape error:", e);
    return null;
  }
}

// Helper: Scrape Google Flow Play Count
async function fetchGoogleFlowPlayCount(inputUrl) {
  try {
    const response = await fetchWithTimeout(inputUrl, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      },
    });

    if (!response.ok) {
      throw new Error(
        `Failed to fetch Google Flow page. Status: ${response.status}`,
      );
    }

    const html = await response.text();

    // Flow embeds the song data inside the page's
    // Next.js serialized data.
    //
    // Try the common play_count formats used by Flow.
    const playCountMatch =
      html.match(/"play_count"\s*:\s*(\d+)/i) ||
      html.match(/"playCount"\s*:\s*(\d+)/i);

    if (!playCountMatch) {
      console.warn("Could not find play_count in Google Flow HTML");

      return null;
    }

    const playCount = parseInt(playCountMatch[1], 10);

    console.log(`Google Flow play count: ${playCount}`);

    return playCount;
  } catch (error) {
    console.error("Google Flow play count error:", error);

    return null;
  }
}

// Health Check Endpoint
app.get("/health", async (req, res) => {
  let dbOk = false;
  try {
    await pool.query("SELECT 1");
    dbOk = true;
  } catch (err) {
    console.error("Health check DB query failed:", err);
  }

  const healthy = dbReady && dbOk;

  res.status(healthy ? 200 : 503).json({
    success: healthy,
    status: healthy ? "ok" : "degraded",
    db: dbOk ? "connected" : "unreachable",
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
  });
});

// 1. GET /api/suno-plays
app.get("/api/suno-plays", async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) {
    return res.status(400).json({
      success: false,
      error: "Missing url parameter",
    });
  }

  const cached = sunoCache.get(targetUrl);

  if (cached && Date.now() - cached.timestamp < CACHE_DURATION) {
    console.log(`Serving cached Suno plays for ${targetUrl}`);

    return res.json({
      success: true,
      playCount: cached.playCount,
      cached: true,
    });
  }

  console.log(`Scraping Suno URL: ${targetUrl}`);
  const playCount = await fetchSunoPlayCount(targetUrl);

  if (playCount !== null) {
    cacheSet(sunoCache, targetUrl, {
      playCount,
      timestamp: Date.now(),
    });

    return res.json({
      success: true,
      playCount,
      cached: false,
    });
  } else {
    res.status(500).json({
      success: false,
      error: "Failed to extract play count",
    });
  }
});

// 2. GET /api/platform-plays
app.get("/api/platform-plays", async (req, res) => {
  const trackId = req.query.id;

  if (!trackId || !trackId.trim()) {
    return res.status(400).json({
      success: false,
      error: "Missing track id",
    });
  }

  try {
    const result = await pool.query(
      "SELECT play_count FROM plays WHERE track_id = $1",
      [trackId],
    );

    res.json({
      success: true,
      playCount: result.rows.length ? result.rows[0].play_count : 0,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      success: false,
      error: "Database error",
    });
  }
});

// 3. GET /api/likes
app.get("/api/likes", async (req, res) => {
  const trackId = req.query.id;
  const userId = req.query.user;

  if (!trackId || !userId) {
    return res.status(400).json({
      success: false,
      error: "Missing track id or user id",
    });
  }

  try {
    const countResult = await pool.query(
      "SELECT COUNT(*)::int AS like_count FROM likes WHERE track_id = $1",
      [trackId],
    );

    const likedResult = await pool.query(
      "SELECT 1 FROM likes WHERE track_id = $1 AND user_id = $2",
      [trackId, userId],
    );

    res.json({
      success: true,
      likeCount: countResult.rows[0].like_count,
      liked: likedResult.rows.length > 0,
    });
  } catch (err) {
    console.error(err);

    res.status(500).json({
      success: false,
      error: "Database error",
    });
  }
});

// 4. GET /api/liked-tracks
app.get("/api/liked-tracks", async (req, res) => {
  const userId = req.query.user;

  if (!userId || !userId.trim()) {
    return res.status(400).json({
      success: false,
      error: "Missing user id",
    });
  }

  try {
    const result = await pool.query(
      "SELECT track_id FROM likes WHERE user_id = $1 ORDER BY track_id",
      [userId],
    );

    res.json({
      success: true,
      trackIds: result.rows.map((row) => row.track_id),
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      success: false,
      error: "Database error",
    });
  }
});

// 5. POST /api/platform-plays
app.post("/api/platform-plays", async (req, res) => {
  const trackId = req.query.id;

  if (!trackId || !trackId.trim()) {
    return res.status(400).json({
      success: false,
      error: "Missing track id",
    });
  }

  try {
    const result = await pool.query(
      `
      INSERT INTO plays (track_id, play_count)
      VALUES ($1, 1)
      ON CONFLICT (track_id)
      DO UPDATE SET play_count = plays.play_count + 1
      RETURNING play_count
      `,
      [trackId],
    );

    res.json({
      success: true,
      playCount: result.rows[0].play_count,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      success: false,
      error: "Database error",
    });
  }
});

// 6. POST /api/likes
app.post("/api/likes", async (req, res) => {
  const trackId = req.query.id;
  const userId = req.query.user;

  if (!trackId || !userId) {
    return res.status(400).json({
      success: false,
      error: "Missing track id or user id",
    });
  }

  try {
    const existing = await pool.query(
      "SELECT 1 FROM likes WHERE track_id = $1 AND user_id = $2",
      [trackId, userId],
    );

    let liked;

    if (existing.rows.length) {
      await pool.query(
        "DELETE FROM likes WHERE track_id = $1 AND user_id = $2",
        [trackId, userId],
      );
      liked = false;
    } else {
      await pool.query(
        "INSERT INTO likes (track_id, user_id) VALUES ($1, $2)",
        [trackId, userId],
      );
      liked = true;
    }

    const countResult = await pool.query(
      "SELECT COUNT(*)::int AS like_count FROM likes WHERE track_id = $1",
      [trackId],
    );

    res.json({
      success: true,
      liked,
      likeCount: countResult.rows[0].like_count,
    });
  } catch (err) {
    console.error(err);

    res.status(500).json({
      success: false,
      error: "Database error",
    });
  }
});

// 7. GET /api/user-stats
// Aggregates total plays and total likes across a set of track ids in one
app.get("/api/user-stats", async (req, res) => {
  const idsParam = req.query.ids;

  if (!idsParam || !idsParam.trim()) {
    return res.status(400).json({
      success: false,
      error: "Missing ids parameter",
    });
  }

  const trackIds = idsParam
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  if (!trackIds.length) {
    return res.json({
      success: true,
      totalPlays: 0,
      totalLikes: 0,
    });
  }

  try {
    const playsResult = await pool.query(
      "SELECT COALESCE(SUM(play_count), 0)::int AS total FROM plays WHERE track_id = ANY($1)",
      [trackIds],
    );

    const likesResult = await pool.query(
      "SELECT COUNT(*)::int AS total FROM likes WHERE track_id = ANY($1)",
      [trackIds],
    );

    res.json({
      success: true,
      totalPlays: playsResult.rows[0].total,
      totalLikes: likesResult.rows[0].total,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      success: false,
      error: "Database error",
    });
  }
});

// 8. GET /api/google-flow-plays
app.get("/api/google-flow-plays", async (req, res) => {
  const targetUrl = req.query.url;

  if (!targetUrl) {
    return res.status(400).json({
      success: false,
      error: "Missing url parameter",
    });
  }

  // Make sure this is actually a Google Flow URL
  if (!targetUrl.includes("flowmusic.app")) {
    return res.status(400).json({
      success: false,
      error: "Invalid Google Flow URL",
    });
  }

  const cached = flowCache.get(targetUrl);

  if (cached && Date.now() - cached.timestamp < CACHE_DURATION) {
    console.log(`Serving cached Google Flow plays for ${targetUrl}`);

    return res.json({
      success: true,
      playCount: cached.playCount,
      cached: true,
    });
  }

  console.log(`Fetching Google Flow plays: ${targetUrl}`);

  const playCount = await fetchGoogleFlowPlayCount(targetUrl);

  if (playCount !== null) {
    cacheSet(flowCache, targetUrl, {
      playCount,
      timestamp: Date.now(),
    });

    return res.json({
      success: true,
      playCount,
      cached: false,
    });
  }

  return res.status(500).json({
    success: false,
    error: "Failed to extract Google Flow play count",
  });
});

// =====================================================================
// MARKETPLACE
// Admin-curated Property bundles containing zero-or-many creative
// components, financing positions, and people - plus scarcity/utility
// scoring, algorithmic pricing, and the ranking pipeline.
// =====================================================================

// Financing is handled separately in financing_positions.
// It is not part of the property component ranking.
const PROPERTY_COMPONENT_TYPES = ["lyrics", "music", "vocals", "marketing"];
// financing is deliberately excluded here - it's never a tracks_component;
// it only ever lives in financing_positions (see /api/properties/:id/financing
// and the two /api/financing/* endpoints below).
const ALL_COMPONENT_TYPES = [...PROPERTY_COMPONENT_TYPES];

// consecutive high-scarcity pipeline runs required before minting a new slot
const SLOT_RELEASE_SUSTAINED_RUNS = 3;
const SLOT_RELEASE_SCARCITY_THRESHOLD = 2.0;

// Price model weights.
const PRICE_ALPHA = 0.15;
const PRICE_BETA = 0.15;
const BASE_PROPERTY_PRICE = 1;

// ---------------------------------------------------------------------
// Schema setup - runs on boot, idempotent (safe to re-run on every deploy)
// ---------------------------------------------------------------------
async function ensureMarketplaceSchema() {
  // plays/likes belong to the original track-library feature
  await pool.query(`
    CREATE TABLE IF NOT EXISTS plays (
      track_id    TEXT PRIMARY KEY,
      play_count  INT DEFAULT 0
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS likes (
      track_id    TEXT NOT NULL,
      user_id     TEXT NOT NULL,
      PRIMARY KEY (track_id, user_id)
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS categories (
      id            SERIAL PRIMARY KEY,
      slug          TEXT UNIQUE NOT NULL,
      name          TEXT NOT NULL,
      created_at    TIMESTAMPTZ DEFAULT now()
    );
  `);

  // Starts false for every category: initially all categories share one
  // global top-100 properties pool. An admin flips this once a category
  // has enough of its own momentum to get an independent top-100 pool.
  await pool.query(`
    ALTER TABLE categories ADD COLUMN IF NOT EXISTS independent_ranking BOOLEAN DEFAULT false;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS people (
      flo_id        TEXT PRIMARY KEY,
      work_profile  TEXT NOT NULL,
      experience    TEXT,
      name          TEXT,
      cv_url        TEXT,
      created_at    TIMESTAMPTZ DEFAULT now(),
      updated_at    TIMESTAMPTZ DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS tracks_components (
      id                  SERIAL PRIMARY KEY,
      track_id            TEXT NOT NULL,
      category_id         INT REFERENCES categories(id),
      component_type      TEXT NOT NULL,
      contributor_flo_id  TEXT NOT NULL,
      metadata            JSONB,
      created_at          TIMESTAMPTZ DEFAULT now()
    );
  `);

  // A property is an admin-curated bundle containing components,
  // people, and financing. Ranking fields are updated by the
  // marketplace pipeline.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS properties (
      id                SERIAL PRIMARY KEY,
      category_id       INT REFERENCES categories(id),
      component_type    TEXT,
      status            TEXT DEFAULT 'active',
      total_slots       INT DEFAULT 5,
      scarcity_score    NUMERIC DEFAULT 0,
      utility_score     NUMERIC DEFAULT 0,
      current_price     NUMERIC DEFAULT 0,
      high_scarcity_streak INT DEFAULT 0,
      created_at        TIMESTAMPTZ DEFAULT now(),
      updated_at        TIMESTAMPTZ DEFAULT now()
    );
  `);
  await pool.query(`
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS name TEXT;
  `);
  await pool.query(`
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS created_by_flo_id TEXT;
  `);
  await pool.query(`
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS category_rank INT;
  `);
  await pool.query(`
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS in_top_100 BOOLEAN DEFAULT false;
  `);
  // Kept for compatibility with older property records.
  // New bundle properties do not use component_type.
  await pool.query(`
    ALTER TABLE properties ALTER COLUMN component_type DROP NOT NULL;
  `);

  await pool.query(`
    ALTER TABLE properties DROP CONSTRAINT IF EXISTS properties_category_id_component_type_key;
  `);

  // Kept for backwards compatibility with existing historical data.
  // New marketplace operations use property_components instead.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_members (
      property_id     INT REFERENCES properties(id),
      component_id    INT REFERENCES tracks_components(id),
      rank            INT NOT NULL,
      snapshot_at     TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (property_id, component_id, snapshot_at)
    );
  `);

  // Membership of a bundle: which lyrics/music/marketing/vocals
  // components an admin has attached to it. A component can be attached
  // to more than one property (e.g. a lyric reused across bundles).
  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_components (
      property_id     INT REFERENCES properties(id),
      component_id    INT REFERENCES tracks_components(id),
      added_by_flo_id TEXT,
      added_at        TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (property_id, component_id)
    );
  `);

  // People attached to a bundle. Each person has a FLO address,
  // work profile, and optional name/CV. Role is stored as an empty
  // string when no role is provided.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_people (
      property_id     INT REFERENCES properties(id),
      person_flo_id   TEXT REFERENCES people(flo_id),
      role            TEXT NOT NULL DEFAULT '',
      added_by_flo_id TEXT,
      added_at        TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (property_id, person_flo_id, role)
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_slots (
      id              SERIAL PRIMARY KEY,
      property_id     INT REFERENCES properties(id),
      slot_index      INT NOT NULL,
      owner_flo_id    TEXT,
      acquired_at     TIMESTAMPTZ,
      acquired_price  NUMERIC,
      eligible_to_sell_at TIMESTAMPTZ,
      UNIQUE (property_id, slot_index)
    );
  `);
  // Set true while a sell's on-chain payout is in flight (between the
  // validate-and-commit step and the pay-then-finalize step below) so a
  // second concurrent sell request on the same slot can't also trigger a
  // payout before the first one finalizes.
  await pool.query(`
    ALTER TABLE property_slots ADD COLUMN IF NOT EXISTS pending_payout BOOLEAN DEFAULT false;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_transactions (
      id            SERIAL PRIMARY KEY,
      property_id   INT REFERENCES properties(id),
      slot_id       INT REFERENCES property_slots(id),
      type          TEXT NOT NULL,
      flo_id        TEXT NOT NULL,
      price         NUMERIC NOT NULL,
      flo_txid      TEXT,
      created_at    TIMESTAMPTZ DEFAULT now()
    );
  `);
  // Prevent the same FLO transaction from being used more than once.
  // Ignore NULL txids so older transactions remain valid.
  try {
    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS property_transactions_flo_txid_unique
      ON property_transactions (flo_txid) WHERE flo_txid IS NOT NULL;
    `);
  } catch (err) {
    console.error(
      "WARNING: could not create unique index on property_transactions.flo_txid " +
        "(likely pre-existing duplicates) - txid replay protection is NOT active:",
      err,
    );
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_interest (
      id            SERIAL PRIMARY KEY,
      property_id   INT REFERENCES properties(id),
      flo_id        TEXT NOT NULL,
      intent        TEXT NOT NULL,
      created_at    TIMESTAMPTZ DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_usage_events (
      id                    SERIAL PRIMARY KEY,
      property_id           INT REFERENCES properties(id),
      component_id          INT REFERENCES tracks_components(id),
      usage_type            TEXT NOT NULL,
      actor_flo_id          TEXT,
      rights_duration_days  INT,
      value_type            TEXT,
      value_amount          NUMERIC,
      value_description     TEXT,
      metadata              JSONB,
      created_at            TIMESTAMPTZ DEFAULT now()
    );
  `);

  // Minimal task board - just enough for financing_positions.task_id to
  // reference. Full task board UI is a separate feature.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tasks (
      id                  SERIAL PRIMARY KEY,
      requester_flo_id    TEXT NOT NULL,
      brief               TEXT NOT NULL,
      budget              NUMERIC,
      status              TEXT DEFAULT 'open',
      fulfilled_by_flo_id TEXT,
      track_id            TEXT,
      created_at          TIMESTAMPTZ DEFAULT now()
    );
  `);

  await pool.query(`
  CREATE TABLE IF NOT EXISTS property_tasks (
    property_id INT REFERENCES properties(id),
    task_id     INT REFERENCES tasks(id),
    added_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (property_id, task_id)
  );
`);

  // component_type identifies the type of creative work requested.
  // claimed_at and completed_at track the task lifecycle.
  await pool.query(`
    ALTER TABLE tasks ADD COLUMN IF NOT EXISTS component_type TEXT;
  `);
  await pool.query(`
    ALTER TABLE tasks ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
  `);
  await pool.query(`
    ALTER TABLE tasks ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS financing_positions (
      id                SERIAL PRIMARY KEY,
      track_id          TEXT,
      task_id           INT REFERENCES tasks(id),
      financier_flo_id  TEXT NOT NULL,
      stage             TEXT NOT NULL,
      amount            NUMERIC NOT NULL,
      revenue_share_pct NUMERIC,
      status            TEXT DEFAULT 'active',
      created_at        TIMESTAMPTZ DEFAULT now()
    );
  `);
  // Allow financing to be attached directly to a bundle property.
  await pool.query(`
    ALTER TABLE financing_positions ADD COLUMN IF NOT EXISTS property_id INT REFERENCES properties(id);
  `);

  console.log("Marketplace v3 schema ready (bundle properties)");
}

// ---------------------------------------------------------------------
// Scarcity / Utility / Price
// ---------------------------------------------------------------------

// Minimal in-memory rate limiter (no extra dependency) for the endpoints
// that can influence scarcity/utility scores or move ownership - these
// are the ones worth protecting from spam even before real auth exists.
// Not distributed-safe (per-process only) - fine for a single instance,
// revisit if this ever runs behind multiple server processes.
const rateLimitBuckets = new Map();
function rateLimit({ windowMs = 60 * 1000, max = 20 } = {}) {
  return (req, res, next) => {
    const key = `${req.ip}:${req.path}`;
    const now = Date.now();
    const bucket = rateLimitBuckets.get(key) || [];
    const recent = bucket.filter((t) => now - t < windowMs);

    if (recent.length >= max) {
      return res.status(429).json({
        success: false,
        error: "Too many requests, slow down",
      });
    }

    recent.push(now);
    rateLimitBuckets.set(key, recent);
    next();
  };
}

// Admin allowlist for marketplace admin actions (creating categories,
// logging usage events, manually triggering the pipeline). Configurable
// via ADMIN_FLO_IDS (comma-separated) with a hardcoded default so this
// works out of the box
const ADMIN_FLO_IDS = (
  process.env.ADMIN_FLO_IDS || "FSLjdS5mtMzfZ3BRHMyqueshFSRxNkuJeN"
)
  .split(",")
  .map((id) => id.trim())
  .filter(Boolean);

// Must run after verifyFloSignature - it trusts req.body[floIdField]
// because the signature middleware already proved that ID signed this
// request. Doesn't do its own signature check.
function requireAdmin(floIdField = "floId") {
  return (req, res, next) => {
    const floId = req.body[floIdField];
    if (!floId || !ADMIN_FLO_IDS.includes(floId)) {
      return res
        .status(403)
        .json({ success: false, error: "Admin access required" });
    }
    next();
  };
}

// trailing 7-day want_to_buy / want_to_sell ratio, clamped 0.1-10
async function computeScarcityScore(propertyId) {
  const result = await pool.query(
    `
    SELECT
      COUNT(DISTINCT flo_id) FILTER (WHERE intent = 'want_to_buy')  AS buy_count,
      COUNT(DISTINCT flo_id) FILTER (WHERE intent = 'want_to_sell') AS sell_count
    FROM property_interest
    WHERE property_id = $1
      AND created_at > now() - INTERVAL '7 days'
    `,
    [propertyId],
  );

  const { buy_count, sell_count } = result.rows[0];
  const raw = Number(buy_count) / Math.max(Number(sell_count), 1);
  return Math.min(Math.max(raw, 0.1), 10); // clamp 0.1-10
}

// Combine usage events with an engagement score.
async function computeUtilityScore(propertyId) {
  const usageResult = await pool.query(
    `
    SELECT usage_type, COUNT(*)::int AS count
    FROM property_usage_events
    WHERE property_id = $1
    GROUP BY usage_type
    `,
    [propertyId],
  );

  const weights = {
    licensed_media: 3,
    public_infrastructure: 4,
    product_integration: 4,
    rights_lease_as_payment: 3,
    embed: 1,
    external_play: 0.5,
  };

  let usageUtility = 0;
  for (const row of usageResult.rows) {
    const w = weights[row.usage_type] ?? 1;
    usageUtility += w * row.count;
  }

  // Use total plays as a temporary engagement score.
  // TODO: switch to 7-day play growth once plays have timestamps.
  const engagementResult = await pool.query(
    `
    WITH members AS (
      SELECT DISTINCT tc.track_id
      FROM property_components pc
      JOIN tracks_components tc ON tc.id = pc.component_id
      WHERE pc.property_id = $1
    )
    SELECT COALESCE(SUM(p.play_count), 0)::int AS total_plays
    FROM plays p
    JOIN members m ON m.track_id = p.track_id
    `,
    [propertyId],
  );

  const engagementProxy =
    Math.log10(1 + Number(engagementResult.rows[0]?.total_plays || 0)) * 0.5;

  return usageUtility + engagementProxy;
}

function computePrice(scarcityScore, utilityScore) {
  return (
    BASE_PROPERTY_PRICE *
    (1 + PRICE_ALPHA * scarcityScore) *
    (1 + PRICE_BETA * utilityScore)
  );
}

// ---------------------------------------------------------------------
// Ranking pipeline
//
// Rank populated properties, update their scores and prices,
// release new slots when demand stays high, and maintain the Top 100.
// Categories with independent ranking get their own Top 100;
// the rest share the global Top 100.
// ---------------------------------------------------------------------
async function runMarketplacePipeline() {
  console.log("Running marketplace pipeline...");

  try {
    const populated = await pool.query(`
      SELECT DISTINCT p.id, p.category_id, p.high_scarcity_streak, p.total_slots
      FROM properties p
      WHERE p.status = 'active'
        AND (
          EXISTS (SELECT 1 FROM property_components pc WHERE pc.property_id = p.id)
          OR EXISTS (SELECT 1 FROM property_people pp WHERE pp.property_id = p.id)
          OR EXISTS (SELECT 1 FROM financing_positions fp WHERE fp.property_id = p.id)
          OR EXISTS (SELECT 1 FROM property_tasks pt WHERE pt.property_id = p.id)
        )
    `);

    // 1 & 2: recompute scores/price, mint slots on sustained scarcity
    const scored = [];
    for (const property of populated.rows) {
      const scarcity = await computeScarcityScore(property.id);
      const utility = await computeUtilityScore(property.id);
      const price = computePrice(scarcity, utility);

      const highScarcity = scarcity > SLOT_RELEASE_SCARCITY_THRESHOLD;
      const newStreak = highScarcity
        ? (property.high_scarcity_streak || 0) + 1
        : 0;

      let newTotalSlots = property.total_slots;
      if (newStreak >= SLOT_RELEASE_SUSTAINED_RUNS) {
        newTotalSlots += 1;
        await pool.query(
          `INSERT INTO property_slots (property_id, slot_index)
           VALUES ($1, $2) ON CONFLICT DO NOTHING`,
          [property.id, newTotalSlots],
        );
      }

      await pool.query(
        `
        UPDATE properties
        SET scarcity_score = $1,
            utility_score = $2,
            current_price = $3,
            total_slots = $4,
            high_scarcity_streak = $5,
            updated_at = now()
        WHERE id = $6
        `,
        [
          scarcity,
          utility,
          price,
          newTotalSlots,
          newStreak >= SLOT_RELEASE_SUSTAINED_RUNS ? 0 : newStreak,
          property.id,
        ],
      );

      scored.push({
        id: property.id,
        category_id: property.category_id,
        price,
      });
    }

    // 3: rank into top 100, respecting each category's ranking mode
    const categories = await pool.query(
      "SELECT id, independent_ranking FROM categories",
    );
    const independentCategoryIds = new Set(
      categories.rows.filter((c) => c.independent_ranking).map((c) => c.id),
    );

    // Reset first, so properties that fall out of a top 100 lose the flag.
    await pool.query(
      "UPDATE properties SET category_rank = NULL, in_top_100 = false WHERE id = ANY($1)",
      [scored.map((s) => s.id)],
    );

    async function rankAndFlag(list) {
      const ranked = [...list].sort((a, b) => b.price - a.price).slice(0, 100);
      for (let i = 0; i < ranked.length; i++) {
        await pool.query(
          `UPDATE properties SET category_rank = $1, in_top_100 = true WHERE id = $2`,
          [i + 1, ranked[i].id],
        );
      }
    }

    // Global pool: every scored property in a non-independent category.
    await rankAndFlag(
      scored.filter((s) => !independentCategoryIds.has(s.category_id)),
    );

    // Independent pools: each graduated category ranks only within itself.
    for (const categoryId of independentCategoryIds) {
      await rankAndFlag(scored.filter((s) => s.category_id === categoryId));
    }

    console.log(
      `Marketplace pipeline run complete (${scored.length} properties scored)`,
    );
  } catch (err) {
    console.error("Marketplace pipeline error:", err);
  }
}

// Run once a day by default. Also exposed via a manual-trigger endpoint
// below for testing without waiting 24h.
const PIPELINE_INTERVAL_MS = 24 * 60 * 60 * 1000;
const marketplacePipelineInterval = setInterval(
  runMarketplacePipeline,
  PIPELINE_INTERVAL_MS,
);

// ---------------------------------------------------------------------
// API: Categories
// ---------------------------------------------------------------------

// GET /api/categories
app.get("/api/categories", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM categories ORDER BY name");
    res.json({ success: true, categories: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// POST /api/categories - create/upsert a category. Admin-only
app.post(
  "/api/categories",
  verifyFloSignature(["adminFloId", "slug", "name", "time"], {
    floIdField: "adminFloId",
  }),
  requireAdmin("adminFloId"),
  async (req, res) => {
    const { slug, name } = req.body;

    if (!slug || !name) {
      return res
        .status(400)
        .json({ success: false, error: "Missing slug or name" });
    }

    try {
      const result = await pool.query(
        `INSERT INTO categories (slug, name) VALUES ($1, $2)
       ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
       RETURNING *`,
        [slug, name],
      );
      res.json({ success: true, category: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// GET /api/categories/:slug/components?component=music
// Component-discovery only, by raw engagement (plays+likes) - this is
// NOT the marketplace ranking. Marketplace ranking is Property-based
// (see GET /api/categories/:slug/properties?top100=true and the ranking
// pipeline above). This endpoint just helps an admin find which
// lyrics/music/marketing/vocals components are worth attaching to a
// bundle via POST /api/properties/:propertyId/components.
app.get("/api/categories/:slug/components", async (req, res) => {
  const { slug } = req.params;
  const componentType = req.query.component;
  const limit = Math.min(parseInt(req.query.limit, 10) || 100, 100);

  if (!componentType || !PROPERTY_COMPONENT_TYPES.includes(componentType)) {
    return res.status(400).json({
      success: false,
      error: `component must be one of: ${PROPERTY_COMPONENT_TYPES.join(", ")}`,
    });
  }

  try {
    const category = await pool.query(
      "SELECT id FROM categories WHERE slug = $1",
      [slug],
    );
    if (!category.rows.length) {
      return res
        .status(404)
        .json({ success: false, error: "Category not found" });
    }

    const result = await pool.query(
      `
      SELECT tc.id AS component_id, tc.track_id, tc.contributor_flo_id, tc.metadata,
             COALESCE(p.play_count, 0) + COALESCE(l.like_count, 0) AS score
      FROM tracks_components tc
      LEFT JOIN plays p ON p.track_id = tc.track_id
      LEFT JOIN (
        SELECT track_id, COUNT(*)::int AS like_count
        FROM likes GROUP BY track_id
      ) l ON l.track_id = tc.track_id
      WHERE tc.category_id = $1 AND tc.component_type = $2
      ORDER BY score DESC
      LIMIT $3
      `,
      [category.rows[0].id, componentType, limit],
    );

    res.json({ success: true, components: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// ---------------------------------------------------------------------
// API: Track components
// ---------------------------------------------------------------------

// POST /api/tracks/:trackId/components
// { contributorFloId, categorySlug, componentType, metadata, time, pubKey, sign }
// Signed but not admin-gated - a creator registering their own track's
// components is a normal action, just needs to actually be them.
app.post(
  "/api/tracks/:trackId/components",
  verifyFloSignature(
    ["trackId", "contributorFloId", "categorySlug", "componentType", "time"],
    { floIdField: "contributorFloId" },
  ),
  async (req, res) => {
    const { trackId } = req.params;
    const { categorySlug, componentType, contributorFloId, metadata } =
      req.body;

    if (!categorySlug || !componentType || !contributorFloId) {
      return res.status(400).json({
        success: false,
        error: "Missing categorySlug, componentType, or contributorFloId",
      });
    }

    if (!ALL_COMPONENT_TYPES.includes(componentType)) {
      return res.status(400).json({
        success: false,
        error: `componentType must be one of: ${ALL_COMPONENT_TYPES.join(", ")}`,
      });
    }

    try {
      const category = await pool.query(
        "SELECT id FROM categories WHERE slug = $1",
        [categorySlug],
      );
      if (!category.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Category not found" });
      }

      const result = await pool.query(
        `INSERT INTO tracks_components
        (track_id, category_id, component_type, contributor_flo_id, metadata)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
        [
          trackId,
          category.rows[0].id,
          componentType,
          contributorFloId,
          metadata || {},
        ],
      );

      res.json({ success: true, component: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// GET /api/marketplace/payment-address
// Public - the buyer needs this to know where to send FLO before calling
// the buy endpoint with the resulting txid.
app.get("/api/marketplace/payment-address", (req, res) => {
  if (!MARKETPLACE_FLO_ADDRESS) {
    return res
      .status(503)
      .json({ success: false, error: "Payments not configured" });
  }
  res.json({ success: true, address: MARKETPLACE_FLO_ADDRESS });
});

// ---------------------------------------------------------------------
// API: People
// A person is a FLO address with a work profile that
// can be attached to properties via POST /api/properties/:id/people.
// Self-service (a person registers/updates their own profile) rather
// than admin-gated - signed as themself, floIdField defaults to "floId".
// ---------------------------------------------------------------------

// POST /api/people  { floId, workProfile, experience, name, cvUrl, time, pubKey, sign }
app.post(
  "/api/people",
  verifyFloSignature(["floId", "workProfile", "time"]),
  async (req, res) => {
    const { floId, workProfile, experience, name, cvUrl } = req.body;

    if (!floId || !workProfile) {
      return res.status(400).json({
        success: false,
        error: "Missing floId or workProfile",
      });
    }

    try {
      const result = await pool.query(
        `INSERT INTO people (flo_id, work_profile, experience, name, cv_url)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (flo_id) DO UPDATE SET
           work_profile = EXCLUDED.work_profile,
           experience   = EXCLUDED.experience,
           name         = EXCLUDED.name,
           cv_url       = EXCLUDED.cv_url,
           updated_at   = now()
         RETURNING *`,
        [floId, workProfile, experience || null, name || null, cvUrl || null],
      );
      res.json({ success: true, person: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// GET /api/people/:floId
app.get("/api/people/:floId", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM people WHERE flo_id = $1", [
      req.params.floId,
    ]);
    if (!result.rows.length) {
      return res
        .status(404)
        .json({ success: false, error: "Person not found" });
    }
    res.json({ success: true, person: result.rows[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// ---------------------------------------------------------------------
// API: Properties (bundles)
// Property = zero-or-many lyrics + zero-or-many music + zero-or-many
// marketing + zero-or-many finance + zero-or-many people, all attached
// under a category. Creating and attaching to a bundle is admin-only;
// reading is open.
// ---------------------------------------------------------------------

// POST /api/properties  { adminFloId, categorySlug, name, time, pubKey, sign }
// Creates the bundle and its initial slots.
app.post(
  "/api/properties",
  verifyFloSignature(["adminFloId", "categorySlug", "name", "time"], {
    floIdField: "adminFloId",
  }),
  requireAdmin("adminFloId"),
  async (req, res) => {
    const { adminFloId, categorySlug, name } = req.body;

    if (!categorySlug || !name) {
      return res.status(400).json({
        success: false,
        error: "Missing categorySlug or name",
      });
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const category = await client.query(
        "SELECT id FROM categories WHERE slug = $1",
        [categorySlug],
      );
      if (!category.rows.length) {
        await client.query("ROLLBACK");
        return res
          .status(404)
          .json({ success: false, error: "Category not found" });
      }

      const property = await client.query(
        `INSERT INTO properties (category_id, name, created_by_flo_id)
         VALUES ($1, $2, $3)
         RETURNING *`,
        [category.rows[0].id, name, adminFloId],
      );

      for (let i = 1; i <= property.rows[0].total_slots; i++) {
        await client.query(
          `INSERT INTO property_slots (property_id, slot_index) VALUES ($1, $2)`,
          [property.rows[0].id, i],
        );
      }

      await client.query("COMMIT");
      res.json({ success: true, property: property.rows[0] });
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    } finally {
      client.release();
    }
  },
);

// GET /api/properties?top100=true
// Global listing across all non-independent categories (or everything if
// top100=false). This is what the "All Categories" frontend view uses.
app.get("/api/properties", async (req, res) => {
  const top100Only = req.query.top100 === "true";

  try {
    // Properties in categories that have NOT graduated to independent
    // ranking share the global pool. If no categories are independent,
    // this returns all active properties.
    const result = await pool.query(
      `SELECT p.* FROM properties p
       JOIN categories c ON c.id = p.category_id
       WHERE p.status = 'active'
         AND c.independent_ranking = false
         ${top100Only ? "AND p.in_top_100 = true" : ""}
       ORDER BY p.category_rank ASC NULLS LAST, p.created_at DESC
       LIMIT 100`,
    );

    res.json({ success: true, properties: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// GET /api/properties/:propertyId - the bundle plus everything attached
// to it (components, people, financing).
app.get("/api/properties/:propertyId", async (req, res) => {
  const { propertyId } = req.params;

  try {
    const property = await pool.query(
      "SELECT * FROM properties WHERE id = $1",
      [propertyId],
    );
    if (!property.rows.length) {
      return res
        .status(404)
        .json({ success: false, error: "Property not found" });
    }

    const [components, people, financing, tasks] = await Promise.all([
      pool.query(
        `SELECT tc.* FROM property_components pc
         JOIN tracks_components tc ON tc.id = pc.component_id
         WHERE pc.property_id = $1`,
        [propertyId],
      ),
      pool.query(
        `SELECT pe.*, pp.role FROM property_people pp
         JOIN people pe ON pe.flo_id = pp.person_flo_id
         WHERE pp.property_id = $1`,
        [propertyId],
      ),
      pool.query(`SELECT * FROM financing_positions WHERE property_id = $1`, [
        propertyId,
      ]),
      pool.query(
        `SELECT t.* FROM property_tasks pt
         JOIN tasks t ON t.id = pt.task_id
         WHERE pt.property_id = $1`,
        [propertyId],
      ),
    ]);

    res.json({
      success: true,
      property: property.rows[0],
      components: components.rows,
      people: people.rows,
      financing: financing.rows,
      tasks: tasks.rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// GET /api/categories/:slug/properties?top100=true
app.get("/api/categories/:slug/properties", async (req, res) => {
  const { slug } = req.params;
  const top100Only = req.query.top100 === "true";

  try {
    const category = await pool.query(
      "SELECT id FROM categories WHERE slug = $1",
      [slug],
    );
    if (!category.rows.length) {
      return res
        .status(404)
        .json({ success: false, error: "Category not found" });
    }

    const result = await pool.query(
      `SELECT * FROM properties
       WHERE category_id = $1 ${top100Only ? "AND in_top_100 = true" : ""}
       ORDER BY category_rank ASC NULLS LAST, created_at DESC`,
      [category.rows[0].id],
    );

    res.json({ success: true, properties: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// POST /api/properties/:propertyId/components  { adminFloId, componentId, time, pubKey, sign }
// Attaches an existing lyrics/music/marketing/vocals component (created
// via POST /api/tracks/:trackId/components) to the bundle.
app.post(
  "/api/properties/:propertyId/components",
  verifyFloSignature(["propertyId", "adminFloId", "componentId", "time"], {
    floIdField: "adminFloId",
  }),
  requireAdmin("adminFloId"),
  async (req, res) => {
    const { propertyId } = req.params;
    const { adminFloId, componentId } = req.body;

    if (!componentId) {
      return res
        .status(400)
        .json({ success: false, error: "Missing componentId" });
    }

    try {
      const check = await pool.query(
        `SELECT p.category_id AS property_category_id, tc.category_id AS component_category_id
         FROM properties p, tracks_components tc
         WHERE p.id = $1 AND tc.id = $2`,
        [propertyId, componentId],
      );
      if (!check.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Property or component not found" });
      }
      const { property_category_id, component_category_id } = check.rows[0];
      if (property_category_id !== component_category_id) {
        return res.status(400).json({
          success: false,
          error: "Component belongs to a different category than this property",
        });
      }

      await pool.query(
        `INSERT INTO property_components (property_id, component_id, added_by_flo_id)
         VALUES ($1, $2, $3)
         ON CONFLICT (property_id, component_id) DO NOTHING`,
        [propertyId, componentId, adminFloId],
      );
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// POST /api/properties/:propertyId/components/:componentId/remove  { adminFloId, time, pubKey, sign }
app.post(
  "/api/properties/:propertyId/components/:componentId/remove",
  verifyFloSignature(["propertyId", "componentId", "adminFloId", "time"], {
    floIdField: "adminFloId",
  }),
  requireAdmin("adminFloId"),
  async (req, res) => {
    const { propertyId, componentId } = req.params;
    try {
      await pool.query(
        `DELETE FROM property_components WHERE property_id = $1 AND component_id = $2`,
        [propertyId, componentId],
      );
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// POST /api/properties/:propertyId/tasks  { adminFloId, taskId, time, pubKey, sign }
// Links an existing task (POST /api/tasks) to a bundle, so a property's
// detail view can show what work is in flight for it.
app.post(
  "/api/properties/:propertyId/tasks",
  verifyFloSignature(["propertyId", "adminFloId", "taskId", "time"], {
    floIdField: "adminFloId",
  }),
  requireAdmin("adminFloId"),
  async (req, res) => {
    const { propertyId } = req.params;
    const { taskId } = req.body;

    if (!taskId) {
      return res.status(400).json({ success: false, error: "Missing taskId" });
    }

    try {
      const check = await pool.query("SELECT 1 FROM properties WHERE id = $1", [
        propertyId,
      ]);
      if (!check.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Property not found" });
      }
      const taskCheck = await pool.query("SELECT 1 FROM tasks WHERE id = $1", [
        taskId,
      ]);
      if (!taskCheck.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Task not found" });
      }

      await pool.query(
        `INSERT INTO property_tasks (property_id, task_id)
         VALUES ($1, $2)
         ON CONFLICT (property_id, task_id) DO NOTHING`,
        [propertyId, taskId],
      );
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// POST /api/properties/:propertyId/tasks/:taskId/remove  { adminFloId, time, pubKey, sign }
app.post(
  "/api/properties/:propertyId/tasks/:taskId/remove",
  verifyFloSignature(["propertyId", "taskId", "adminFloId", "time"], {
    floIdField: "adminFloId",
  }),
  requireAdmin("adminFloId"),
  async (req, res) => {
    const { propertyId, taskId } = req.params;
    try {
      await pool.query(
        `DELETE FROM property_tasks WHERE property_id = $1 AND task_id = $2`,
        [propertyId, taskId],
      );
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// POST /api/properties/:propertyId/people  { adminFloId, personFloId, role, time, pubKey, sign }
// personFloId must already have a people profile (POST /api/people).
app.post(
  "/api/properties/:propertyId/people",
  verifyFloSignature(["propertyId", "adminFloId", "personFloId", "time"], {
    floIdField: "adminFloId",
  }),
  requireAdmin("adminFloId"),
  async (req, res) => {
    const { propertyId } = req.params;
    const { adminFloId, personFloId, role } = req.body;

    if (!personFloId) {
      return res
        .status(400)
        .json({ success: false, error: "Missing personFloId" });
    }

    try {
      const person = await pool.query(
        "SELECT 1 FROM people WHERE flo_id = $1",
        [personFloId],
      );
      if (!person.rows.length) {
        return res.status(404).json({
          success: false,
          error:
            "No people profile for this FLO ID yet - they must POST /api/people first",
        });
      }

      await pool.query(
        `INSERT INTO property_people (property_id, person_flo_id, role, added_by_flo_id)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (property_id, person_flo_id, role) DO NOTHING`,
        [propertyId, personFloId, role || "", adminFloId],
      );
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// POST /api/properties/:propertyId/people/:personFloId/remove  { adminFloId, role, time, pubKey, sign }
app.post(
  "/api/properties/:propertyId/people/:personFloId/remove",
  verifyFloSignature(["propertyId", "personFloId", "adminFloId", "time"], {
    floIdField: "adminFloId",
  }),
  requireAdmin("adminFloId"),
  async (req, res) => {
    const { propertyId, personFloId } = req.params;
    const { role } = req.body;
    try {
      await pool.query(
        `DELETE FROM property_people WHERE property_id = $1 AND person_flo_id = $2 AND role = $3`,
        [propertyId, personFloId, role || ""],
      );
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// POST /api/properties/:propertyId/financing  { financierFloId, amount, revenueSharePct, time, pubKey, sign }
// The "finance" leg of a bundle - not admin-gated, since backing a
// property with your own money is a normal financier action (same as
// the existing task/track financing endpoints below).
app.post(
  "/api/properties/:propertyId/financing",
  verifyFloSignature(["propertyId", "financierFloId", "amount", "time"], {
    floIdField: "financierFloId",
  }),
  async (req, res) => {
    const { propertyId } = req.params;
    const { financierFloId, amount, revenueSharePct } = req.body;

    if (!financierFloId || !amount) {
      return res.status(400).json({
        success: false,
        error: "Missing financierFloId or amount",
      });
    }

    try {
      const property = await pool.query(
        "SELECT id FROM properties WHERE id = $1",
        [propertyId],
      );
      if (!property.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Property not found" });
      }

      const result = await pool.query(
        `INSERT INTO financing_positions
          (property_id, financier_flo_id, stage, amount, revenue_share_pct)
         VALUES ($1, $2, 'property_backing', $3, $4)
         RETURNING *`,
        [propertyId, financierFloId, amount, revenueSharePct || null],
      );

      res.json({ success: true, position: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// GET /api/properties/:propertyId/slots
app.get("/api/properties/:propertyId/slots", async (req, res) => {
  const { propertyId } = req.params;

  try {
    const result = await pool.query(
      "SELECT * FROM property_slots WHERE property_id = $1 ORDER BY slot_index",
      [propertyId],
    );
    res.json({ success: true, slots: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// GET /api/properties/:propertyId/history
app.get("/api/properties/:propertyId/history", async (req, res) => {
  const { propertyId } = req.params;

  try {
    const result = await pool.query(
      `SELECT type, price, flo_id, created_at
       FROM property_transactions
       WHERE property_id = $1
       ORDER BY created_at ASC`,
      [propertyId],
    );
    res.json({ success: true, history: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// POST /api/properties/:propertyId/interest  { floId, intent, time, pubKey, sign }
// Signed: hashcontent = [propertyId, floId, intent, time].join("|")
app.post(
  "/api/properties/:propertyId/interest",
  rateLimit({ max: 20, windowMs: 60000 }),
  verifyFloSignature(["propertyId", "floId", "intent", "time"]),
  async (req, res) => {
    const { propertyId } = req.params;
    const { floId, intent } = req.body;

    if (!floId || !["want_to_buy", "want_to_sell"].includes(intent)) {
      return res.status(400).json({
        success: false,
        error: "Missing floId or invalid intent",
      });
    }

    try {
      await pool.query(
        `INSERT INTO property_interest (property_id, flo_id, intent)
         VALUES ($1, $2, $3)`,
        [propertyId, floId, intent],
      );
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// Validates that a route param is a positive integer before it's used in
// a query, returning a clean 400 instead of leaking a Postgres cast error
// through as a generic 500.
function parsePositiveInt(value) {
  const n = parseInt(value, 10);
  return Number.isInteger(n) && n > 0 ? n : null;
}

// POST /api/properties/:propertyId/slots/:slotId/buy  { floId, floTxid, time, pubKey, sign }
// Signed: hashcontent = [propertyId, slotId, floId, time].join("|") - see flo-auth.js.
// Verify the payment before modifying the slot.
// Lock the slot during the purchase so concurrent requests cannot buy the same slot.
// The unique txid index prevents the same payment from being reused.
app.post(
  "/api/properties/:propertyId/slots/:slotId/buy",
  rateLimit({ max: 10, windowMs: 60000 }),
  verifyFloSignature(["propertyId", "slotId", "floId", "time"]),
  async (req, res) => {
    const propertyId = parsePositiveInt(req.params.propertyId);
    const slotId = parsePositiveInt(req.params.slotId);
    const { floId, floTxid } = req.body;

    if (!propertyId || !slotId) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid propertyId or slotId" });
    }
    if (!floId) {
      return res.status(400).json({ success: false, error: "Missing floId" });
    }
    if (!floTxid) {
      return res.status(400).json({
        success: false,
        error:
          "Missing floTxid - payment must be sent and confirmed before buying a slot",
      });
    }

    let verifiedAmount;
    try {
      const peek = await pool.query(
        "SELECT current_price FROM properties WHERE id = $1",
        [propertyId],
      );
      if (!peek.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Property not found" });
      }
      verifiedAmount = peek.rows[0].current_price || BASE_PROPERTY_PRICE;
      await verifyFloPayment(floTxid, verifiedAmount, floId);
    } catch (err) {
      console.error("Buy payment verification failed:", err);
      return res.status(402).json({
        success: false,
        error: `Payment verification failed: ${err.message || err}`,
      });
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const slot = await client.query(
        "SELECT * FROM property_slots WHERE id = $1 AND property_id = $2 FOR UPDATE",
        [slotId, propertyId],
      );

      if (!slot.rows.length) {
        await client.query("ROLLBACK");
        return res
          .status(404)
          .json({ success: false, error: "Slot not found" });
      }
      if (slot.rows[0].owner_flo_id) {
        await client.query("ROLLBACK");
        return res
          .status(409)
          .json({ success: false, error: "Slot already owned" });
      }

      const property = await client.query(
        "SELECT current_price FROM properties WHERE id = $1 FOR UPDATE",
        [propertyId],
      );
      const price = property.rows[0]?.current_price || BASE_PROPERTY_PRICE;

      if (price > verifiedAmount) {
        // Price moved up after payment was verified - the buyer didn't
        // pay enough for the current price. Reject rather than under-charge.
        await client.query("ROLLBACK");
        return res.status(409).json({
          success: false,
          error:
            "Property price increased since payment was sent - resend at the new price",
        });
      }

      // Hold the slot for 14 days before it can be resold.
      const holdingPeriodDays = 14;

      const updated = await client.query(
        `
        UPDATE property_slots
        SET owner_flo_id = $1,
            acquired_at = now(),
            acquired_price = $2,
            eligible_to_sell_at = now() + ($3 || ' days')::interval
        WHERE id = $4
        RETURNING *
        `,
        [floId, price, holdingPeriodDays, slotId],
      );

      try {
        await client.query(
          `INSERT INTO property_transactions (property_id, slot_id, type, flo_id, price, flo_txid)
           VALUES ($1, $2, 'buy', $3, $4, $5)`,
          [propertyId, slotId, floId, price, floTxid],
        );
      } catch (err) {
        if (err.code === "23505") {
          // unique violation on flo_txid - this payment was already redeemed for a different slot.
          await client.query("ROLLBACK");
          return res.status(409).json({
            success: false,
            error:
              "This payment has already been used for a different purchase",
          });
        }
        throw err;
      }

      await client.query("COMMIT");
      res.json({ success: true, slot: updated.rows[0] });
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    } finally {
      client.release();
    }
  },
);

// POST /api/properties/:propertyId/slots/:slotId/sell  { floId, time, pubKey, sign }
// Signed the same way as buy - see flo-auth.js.
//
// Sell the slot and pay the owner in FLO.
// Mark the slot as pending first to prevent duplicate sales.
// If the payment succeeds, clear ownership and record the transaction.
// If the server crashes after payment but before finalizing, manual
// reconciliation may be required.
app.post(
  "/api/properties/:propertyId/slots/:slotId/sell",
  rateLimit({ max: 10, windowMs: 60000 }),
  verifyFloSignature(["propertyId", "slotId", "floId", "time"]),
  async (req, res) => {
    const propertyId = parsePositiveInt(req.params.propertyId);
    const slotId = parsePositiveInt(req.params.slotId);
    const { floId } = req.body;

    if (!propertyId || !slotId) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid propertyId or slotId" });
    }
    if (!floId) {
      return res.status(400).json({ success: false, error: "Missing floId" });
    }

    // Phase 1: validate ownership/eligibility, mark pending_payout, commit.
    let price;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const slot = await client.query(
        "SELECT * FROM property_slots WHERE id = $1 AND property_id = $2 FOR UPDATE",
        [slotId, propertyId],
      );

      if (!slot.rows.length) {
        await client.query("ROLLBACK");
        return res
          .status(404)
          .json({ success: false, error: "Slot not found" });
      }
      const current = slot.rows[0];
      if (current.owner_flo_id !== floId) {
        await client.query("ROLLBACK");
        return res
          .status(403)
          .json({ success: false, error: "Not the slot owner" });
      }
      if (current.pending_payout) {
        await client.query("ROLLBACK");
        return res.status(409).json({
          success: false,
          error: "A sale for this slot is already in progress",
        });
      }
      if (
        !current.eligible_to_sell_at ||
        new Date(current.eligible_to_sell_at) > new Date()
      ) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          error: "Slot is not yet eligible for sale (still in holding period)",
        });
      }

      const property = await client.query(
        "SELECT current_price, scarcity_score FROM properties WHERE id = $1 FOR UPDATE",
        [propertyId],
      );
      if ((property.rows[0]?.scarcity_score ?? 0) < 1.0) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          error: "Insufficient buy demand to sell into right now",
        });
      }

      price = property.rows[0].current_price || BASE_PROPERTY_PRICE;

      await client.query(
        "UPDATE property_slots SET pending_payout = true WHERE id = $1",
        [slotId],
      );

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(err);
      return res.status(500).json({ success: false, error: "Database error" });
    } finally {
      client.release();
    }

    // Phase 2: send the actual payout, outside the row lock (broadcasting
    // a tx can take seconds).
    let payoutTxid;
    try {
      payoutTxid = await sendFloPayment(floId, price);
    } catch (err) {
      console.error("Payout send failed:", err);
      await pool
        .query(
          "UPDATE property_slots SET pending_payout = false WHERE id = $1",
          [slotId],
        )
        .catch((resetErr) =>
          console.error(
            `CRITICAL: failed to clear pending_payout on slot ${slotId} after a failed send - it will look permanently stuck:`,
            resetErr,
          ),
        );
      return res.status(502).json({
        success: false,
        error: `Payment to seller failed: ${err.message || err}`,
      });
    }

    // Phase 3: finalize - clear ownership, record the payout txid.
    const client2 = await pool.connect();
    try {
      await client2.query("BEGIN");

      const updated = await client2.query(
        `
        UPDATE property_slots
        SET owner_flo_id = NULL,
            acquired_at = NULL,
            acquired_price = NULL,
            eligible_to_sell_at = NULL,
            pending_payout = false
        WHERE id = $1 AND owner_flo_id = $2
        RETURNING *
        `,
        [slotId, floId],
      );

      if (!updated.rows.length) {
        await client2.query("ROLLBACK");
        console.error(
          `CRITICAL: paid out ${price} FLO (txid ${payoutTxid}) to ${floId} for slot ${slotId} ` +
            "but the slot no longer matched the expected owner during finalize - needs manual reconciliation",
        );
        return res.status(500).json({
          success: false,
          error:
            "Payment was sent but finalizing the sale failed - contact support",
          payoutTxid,
        });
      }

      await client2.query(
        `INSERT INTO property_transactions (property_id, slot_id, type, flo_id, price, flo_txid)
         VALUES ($1, $2, 'sell', $3, $4, $5)`,
        [propertyId, slotId, floId, price, payoutTxid],
      );

      await client2.query("COMMIT");
      res.json({ success: true, slot: updated.rows[0], payoutTxid });
    } catch (err) {
      await client2.query("ROLLBACK");
      console.error(
        `CRITICAL: paid out ${price} FLO (txid ${payoutTxid}) to ${floId} for slot ${slotId} ` +
          "but recording the sale threw - needs manual reconciliation:",
        err,
      );
      res.status(500).json({
        success: false,
        error:
          "Payment was sent but recording the sale failed - contact support",
        payoutTxid,
      });
    } finally {
      client2.release();
    }
  },
);

// POST /api/properties/:propertyId/usage-events - admin-only. Logging a
// usage event directly moves utility_score (and so price), so it needs
// to be gated to trusted admins, not just signed by any user.
// { adminFloId, componentId, usageType, actorFloId, ..., time, pubKey, sign }
app.post(
  "/api/properties/:propertyId/usage-events",
  rateLimit({ max: 20, windowMs: 60000 }),
  verifyFloSignature(["propertyId", "adminFloId", "usageType", "time"], {
    floIdField: "adminFloId",
  }),
  requireAdmin("adminFloId"),
  async (req, res) => {
    const { propertyId } = req.params;
    const {
      componentId,
      usageType,
      actorFloId,
      rightsDurationDays,
      valueType,
      valueAmount,
      valueDescription,
      metadata,
    } = req.body;

    if (!usageType) {
      return res
        .status(400)
        .json({ success: false, error: "Missing usageType" });
    }

    try {
      const result = await pool.query(
        `
      INSERT INTO property_usage_events
        (property_id, component_id, usage_type, actor_flo_id,
         rights_duration_days, value_type, value_amount, value_description, metadata)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      RETURNING *
      `,
        [
          propertyId,
          componentId || null,
          usageType,
          actorFloId || null,
          rightsDurationDays || null,
          valueType || null,
          valueAmount || null,
          valueDescription || null,
          metadata || {},
        ],
      );

      res.json({ success: true, event: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// ---------------------------------------------------------------------
// API: Financing
// ---------------------------------------------------------------------

// POST /api/financing/tasks/:taskId/back
// { financierFloId, amount, revenueSharePct, time, pubKey, sign }
// Signed: hashcontent = [taskId, financierFloId, amount, revenueSharePct, time].join("|")
app.post(
  "/api/financing/tasks/:taskId/back",
  verifyFloSignature(
    ["taskId", "financierFloId", "amount", "revenueSharePct", "time"],
    { floIdField: "financierFloId" },
  ),
  async (req, res) => {
    const { taskId } = req.params;
    const { financierFloId, amount, revenueSharePct } = req.body;

    if (!financierFloId || !amount) {
      return res.status(400).json({
        success: false,
        error: "Missing financierFloId or amount",
      });
    }

    try {
      const task = await pool.query("SELECT id FROM tasks WHERE id = $1", [
        taskId,
      ]);
      if (!task.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Task not found" });
      }

      const result = await pool.query(
        `INSERT INTO financing_positions
          (task_id, financier_flo_id, stage, amount, revenue_share_pct)
         VALUES ($1, $2, 'pre_creation', $3, $4)
         RETURNING *`,
        [taskId, financierFloId, amount, revenueSharePct || null],
      );

      res.json({ success: true, position: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// POST /api/financing/tracks/:trackId/invest
// { financierFloId, amount, revenueSharePct, time, pubKey, sign }
// Signed: hashcontent = [trackId, financierFloId, amount, revenueSharePct, time].join("|")
app.post(
  "/api/financing/tracks/:trackId/invest",
  verifyFloSignature(
    ["trackId", "financierFloId", "amount", "revenueSharePct", "time"],
    { floIdField: "financierFloId" },
  ),
  async (req, res) => {
    const { trackId } = req.params;
    const { financierFloId, amount, revenueSharePct } = req.body;

    if (!financierFloId || !amount) {
      return res.status(400).json({
        success: false,
        error: "Missing financierFloId or amount",
      });
    }

    try {
      const result = await pool.query(
        `INSERT INTO financing_positions
          (track_id, financier_flo_id, stage, amount, revenue_share_pct)
         VALUES ($1, $2, 'post_production', $3, $4)
         RETURNING *`,
        [trackId, financierFloId, amount, revenueSharePct || null],
      );

      res.json({ success: true, position: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// ---------------------------------------------------------------------
// API: Task board
// ---------------------------------------------------------------------

// GET /api/tasks
app.get("/api/tasks", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM tasks ORDER BY created_at DESC",
    );
    res.json({ success: true, tasks: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// POST /api/tasks  { adminFloId, requesterFloId, brief, budget, componentType, time, pubKey, sign }
// componentType is what kind of creative work this is asking for
// ("generate lyrics for X") - optional, but when given must be one of
// PROPERTY_COMPONENT_TYPES so it can be filtered/tagged consistently
// with the rest of the marketplace.
// Admin-only, same as categories/usage-events - keeps the task board
// from being spammed with junk listings.
app.post(
  "/api/tasks",
  verifyFloSignature(
    [
      "adminFloId",
      "requesterFloId",
      "brief",
      "budget",
      "componentType",
      "time",
    ],
    { floIdField: "adminFloId" },
  ),
  requireAdmin("adminFloId"),
  async (req, res) => {
    const { requesterFloId, brief, budget, componentType } = req.body;

    if (!requesterFloId || !brief) {
      return res.status(400).json({
        success: false,
        error: "Missing requesterFloId or brief",
      });
    }

    if (componentType && !PROPERTY_COMPONENT_TYPES.includes(componentType)) {
      return res.status(400).json({
        success: false,
        error: `componentType must be one of: ${PROPERTY_COMPONENT_TYPES.join(", ")}`,
      });
    }

    try {
      const result = await pool.query(
        `INSERT INTO tasks (requester_flo_id, brief, budget, component_type)
         VALUES ($1, $2, $3, $4)
         RETURNING *`,
        [requesterFloId, brief, budget || null, componentType || null],
      );
      res.json({ success: true, task: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// POST /api/tasks/:taskId/claim  { creatorFloId, time, pubKey, sign }
// Signed: hashcontent = [taskId, creatorFloId, time].join("|")
// A creator claims an open task, committing to do the work. Row-locked
// the same way slot buy/sell are, so two creators racing to claim the
// same task can't both succeed.
app.post(
  "/api/tasks/:taskId/claim",
  rateLimit({ max: 10, windowMs: 60000 }),
  verifyFloSignature(["taskId", "creatorFloId", "time"], {
    floIdField: "creatorFloId",
  }),
  async (req, res) => {
    const taskId = parsePositiveInt(req.params.taskId);
    const { creatorFloId } = req.body;

    if (!taskId) {
      return res.status(400).json({ success: false, error: "Invalid taskId" });
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const task = await client.query(
        "SELECT * FROM tasks WHERE id = $1 FOR UPDATE",
        [taskId],
      );
      if (!task.rows.length) {
        await client.query("ROLLBACK");
        return res
          .status(404)
          .json({ success: false, error: "Task not found" });
      }
      if (task.rows[0].status !== "open") {
        await client.query("ROLLBACK");
        return res
          .status(409)
          .json({ success: false, error: "Task is not open" });
      }

      const updated = await client.query(
        `UPDATE tasks
         SET status = 'claimed', fulfilled_by_flo_id = $1, claimed_at = now()
         WHERE id = $2
         RETURNING *`,
        [creatorFloId, taskId],
      );

      await client.query("COMMIT");
      res.json({ success: true, task: updated.rows[0] });
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    } finally {
      client.release();
    }
  },
);

// POST /api/tasks/:taskId/complete  { creatorFloId, trackId, time, pubKey, sign }
// Signed: hashcontent = [taskId, creatorFloId, trackId, time].join("|")
// Only the creator who claimed the task can complete it - enforced by
// checking fulfilled_by_flo_id against the (signature-verified) caller,
// not just trusting the body.
app.post(
  "/api/tasks/:taskId/complete",
  rateLimit({ max: 10, windowMs: 60000 }),
  verifyFloSignature(["taskId", "creatorFloId", "trackId", "time"], {
    floIdField: "creatorFloId",
  }),
  async (req, res) => {
    const taskId = parsePositiveInt(req.params.taskId);
    const { creatorFloId, trackId } = req.body;

    if (!taskId) {
      return res.status(400).json({ success: false, error: "Invalid taskId" });
    }
    if (!trackId) {
      return res.status(400).json({ success: false, error: "Missing trackId" });
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const task = await client.query(
        "SELECT * FROM tasks WHERE id = $1 FOR UPDATE",
        [taskId],
      );
      if (!task.rows.length) {
        await client.query("ROLLBACK");
        return res
          .status(404)
          .json({ success: false, error: "Task not found" });
      }
      const current = task.rows[0];
      if (current.status !== "claimed") {
        await client.query("ROLLBACK");
        return res
          .status(409)
          .json({ success: false, error: "Task is not claimed" });
      }
      if (current.fulfilled_by_flo_id !== creatorFloId) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          error: "Not the creator who claimed this task",
        });
      }

      const updated = await client.query(
        `UPDATE tasks
         SET status = 'completed', track_id = $1, completed_at = now()
         WHERE id = $2
         RETURNING *`,
        [trackId, taskId],
      );

      await client.query("COMMIT");
      res.json({ success: true, task: updated.rows[0] });
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    } finally {
      client.release();
    }
  },
);

// POST /api/admin/whoami  { floId, time, pubKey, sign }
// Signed: hashcontent = [floId, time].join("|")
// Lets the frontend know whether the verified caller is an admin, so it
// can hide admin-only controls (post task, add category, etc.) entirely
// instead of showing them to everyone and 403ing on submit.
app.post(
  "/api/admin/whoami",
  verifyFloSignature(["floId", "time"]),
  (req, res) => {
    const { floId } = req.body;
    res.json({ success: true, isAdmin: ADMIN_FLO_IDS.includes(floId) });
  },
);

// Manual trigger for the pipeline job - useful for testing without
// waiting for the daily interval. Admin-only.
// { adminFloId, time, pubKey, sign }
app.post(
  "/api/marketplace/run-pipeline",
  verifyFloSignature(["adminFloId", "time"], { floIdField: "adminFloId" }),
  requireAdmin("adminFloId"),
  async (req, res) => {
    try {
      await runMarketplacePipeline();
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Pipeline run failed" });
    }
  },
);

// Start the server
console.log("Starting MusicMarketplace Oracle...");

app.listen(port, () => {
  console.log(`MusicMarketplace Oracle listening on port ${port}`);
});

async function shutdown() {
  console.log("Closing PostgreSQL pool...");

  clearInterval(cacheCleanupInterval);
  clearInterval(marketplacePipelineInterval);

  await pool.end();

  console.log("PostgreSQL pool closed.");

  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Safety nets: log and exit cleanly instead of an unhandled rejection or
// exception silently killing the process (or worse, leaving it running in
// an unknown state).
process.on("unhandledRejection", (reason) => {
  console.error("Unhandled promise rejection:", reason);
});

process.on("uncaughtException", (err) => {
  console.error("Uncaught exception, shutting down:", err);
  shutdown();
});
