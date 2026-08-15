const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
require("dotenv").config();
const { verifyFloSignature } = require("./flo-auth");

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
      console.warn(
        "Could not find play_count in Google Flow HTML",
      );

      return null;
    }

    const playCount = parseInt(playCountMatch[1], 10);

    console.log(
      `Google Flow play count: ${playCount}`,
    );

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

  if (
    cached &&
    Date.now() - cached.timestamp < CACHE_DURATION
  ) {
    console.log(
      `Serving cached Google Flow plays for ${targetUrl}`,
    );

    return res.json({
      success: true,
      playCount: cached.playCount,
      cached: true,
    });
  }

  console.log(
    `Fetching Google Flow plays: ${targetUrl}`,
  );

  const playCount =
    await fetchGoogleFlowPlayCount(targetUrl);

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
// 5-component properties (lyrics, music, vocals, marketing, financing),
// scarcity/utility scoring, algorithmic pricing, and the ranking pipeline.
// =====================================================================

// financing isn't ranked - it's priced by deal terms, not scarcity/utility,
// and lives in financing_positions instead
const RANKABLE_COMPONENT_TYPES = ["lyrics", "music", "vocals", "marketing"];
const ALL_COMPONENT_TYPES = [...RANKABLE_COMPONENT_TYPES, "financing"];

// consecutive high-scarcity pipeline runs required before minting a new slot
const SLOT_RELEASE_SUSTAINED_RUNS = 3;
const SLOT_RELEASE_SCARCITY_THRESHOLD = 2.0;

// price weights, tune from real trading data
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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS properties (
      id                SERIAL PRIMARY KEY,
      category_id       INT REFERENCES categories(id),
      component_type    TEXT NOT NULL,
      status            TEXT DEFAULT 'active',
      total_slots       INT DEFAULT 5,
      scarcity_score    NUMERIC DEFAULT 0,
      utility_score     NUMERIC DEFAULT 0,
      current_price     NUMERIC DEFAULT 0,
      high_scarcity_streak INT DEFAULT 0,
      created_at        TIMESTAMPTZ DEFAULT now(),
      updated_at        TIMESTAMPTZ DEFAULT now(),
      UNIQUE (category_id, component_type)
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_members (
      property_id     INT REFERENCES properties(id),
      component_id    INT REFERENCES tracks_components(id),
      rank            INT NOT NULL,
      snapshot_at     TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (property_id, component_id, snapshot_at)
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

  // component_type: what kind of creative work this task is asking for
  // ("generate lyrics for X", "generate music for Y") - one of
  // RANKABLE_COMPONENT_TYPES, or NULL for tasks that don't map to a
  // single component. claimed_at/completed_at track the two lifecycle
  // transitions the claim/complete endpoints below drive (open -> claimed
  // -> completed), mirroring acquired_at's role on property_slots.
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

  console.log("Marketplace v2 schema ready");
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
      COUNT(*) FILTER (WHERE intent = 'want_to_buy')  AS buy_count,
      COUNT(*) FILTER (WHERE intent = 'want_to_sell') AS sell_count
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

// real usage events + an engagement-growth proxy blended together
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

  // Placeholder proxy: engagement growth of this property's member
  // components' parent tracks, trailing 7 days vs the 7 days before that.
  const engagementResult = await pool.query(
    `
    WITH members AS (
      SELECT DISTINCT tc.track_id
      FROM property_members pm
      JOIN tracks_components tc ON tc.id = pm.component_id
      WHERE pm.property_id = $1
        AND pm.snapshot_at = (
          SELECT MAX(snapshot_at) FROM property_members WHERE property_id = $1
        )
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
// Ranking pipeline - recomputes Top 100 per (category, component_type),
// creates/updates properties, snapshots membership, recomputes scores +
// price, and mints new slots once scarcity has stayed high for
// SLOT_RELEASE_SUSTAINED_RUNS consecutive runs.
// ---------------------------------------------------------------------
async function runMarketplacePipeline() {
  console.log("Running marketplace pipeline...");
  const snapshotAt = new Date();

  try {
    const categories = await pool.query("SELECT id FROM categories");

    for (const category of categories.rows) {
      for (const componentType of RANKABLE_COMPONENT_TYPES) {
        // Rank components by their parent track's engagement (plays +
        // likes). No per-component tracking yet, so all components of a
        // track currently inherit the track's own numbers.
        const ranked = await pool.query(
          `
          SELECT tc.id AS component_id,
                 COALESCE(p.play_count, 0) + COALESCE(l.like_count, 0) AS score
          FROM tracks_components tc
          LEFT JOIN plays p ON p.track_id = tc.track_id
          LEFT JOIN (
            SELECT track_id, COUNT(*)::int AS like_count
            FROM likes GROUP BY track_id
          ) l ON l.track_id = tc.track_id
          WHERE tc.category_id = $1 AND tc.component_type = $2
          ORDER BY score DESC
          LIMIT 100
          `,
          [category.id, componentType],
        );

        if (ranked.rows.length < 100) {
          // not enough components yet for a property in this pair
          continue;
        }

        // Create the property if it doesn't exist yet, otherwise fetch it.
        const propertyResult = await pool.query(
          `
          INSERT INTO properties (category_id, component_type)
          VALUES ($1, $2)
          ON CONFLICT (category_id, component_type) DO NOTHING
          RETURNING *
          `,
          [category.id, componentType],
        );

        const property =
          propertyResult.rows[0] ||
          (
            await pool.query(
              "SELECT * FROM properties WHERE category_id = $1 AND component_type = $2",
              [category.id, componentType],
            )
          ).rows[0];

        const wasFreshlyCreated = propertyResult.rows.length > 0;
        if (wasFreshlyCreated) {
          // Freshly created - mint its initial 5 slots.
          for (let i = 1; i <= property.total_slots; i++) {
            await pool.query(
              `INSERT INTO property_slots (property_id, slot_index)
               VALUES ($1, $2) ON CONFLICT DO NOTHING`,
              [property.id, i],
            );
          }
        }

        // Strict Top-100 snapshot each run (no stickiness yet - a member
        // can drop out the moment it's outranked). Batched into one
        // multi-row INSERT instead of one query per member, since this
        // runs per (category, component_type) pair.
        const memberValues = [];
        const memberParams = [];
        ranked.rows.forEach((row, i) => {
          const base = memberParams.length;
          memberValues.push(
            `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4})`,
          );
          memberParams.push(property.id, row.component_id, i + 1, snapshotAt);
        });

        if (memberValues.length) {
          await pool.query(
            `INSERT INTO property_members (property_id, component_id, rank, snapshot_at)
             VALUES ${memberValues.join(", ")}`,
            memberParams,
          );
        }

        // Recompute scores + price
        const scarcity = await computeScarcityScore(property.id);
        const utility = await computeUtilityScore(property.id);
        const price = computePrice(scarcity, utility);

        // Sustained excess demand streak - gates when a new slot mints
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
      }
    }

    console.log("Marketplace pipeline run complete");
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
    const result = await pool.query(
      "SELECT * FROM categories ORDER BY name",
    );
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

// GET /api/categories/:slug/leaderboard?component=music
app.get("/api/categories/:slug/leaderboard", async (req, res) => {
  const { slug } = req.params;
  const componentType = req.query.component;
  const limit = Math.min(parseInt(req.query.limit, 10) || 100, 100);

  if (!componentType || !RANKABLE_COMPONENT_TYPES.includes(componentType)) {
    return res.status(400).json({
      success: false,
      error: `component must be one of: ${RANKABLE_COMPONENT_TYPES.join(", ")}`,
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

    res.json({ success: true, leaderboard: result.rows });
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

// ---------------------------------------------------------------------
// API: Properties
// ---------------------------------------------------------------------

// GET /api/properties/:categoryId?component=lyrics
app.get("/api/properties/:categoryId", async (req, res) => {
  const { categoryId } = req.params;
  const componentType = req.query.component;

  if (!componentType || !RANKABLE_COMPONENT_TYPES.includes(componentType)) {
    return res.status(400).json({
      success: false,
      error: `component must be one of: ${RANKABLE_COMPONENT_TYPES.join(", ")}`,
    });
  }

  try {
    const result = await pool.query(
      "SELECT * FROM properties WHERE category_id = $1 AND component_type = $2",
      [categoryId, componentType],
    );

    if (!result.rows.length) {
      return res.status(404).json({
        success: false,
        error: "No property exists yet for this category/component",
      });
    }

    res.json({ success: true, property: result.rows[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

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

// POST /api/properties/:propertyId/slots/:slotId/buy  { floId, time, pubKey, sign }
// Signed: hashcontent = [propertyId, slotId, floId, time].join("|") - see
// flo-auth.js.
//
// Wrapped in a transaction with SELECT ... FOR UPDATE so two concurrent
// buy requests for the same slot can't both succeed (the row lock forces
// the second request to wait, then see the slot already owned).
app.post(
  "/api/properties/:propertyId/slots/:slotId/buy",
  rateLimit({ max: 10, windowMs: 60000 }),
  verifyFloSignature(["propertyId", "slotId", "floId", "time"]),
  async (req, res) => {
    const propertyId = parsePositiveInt(req.params.propertyId);
    const slotId = parsePositiveInt(req.params.slotId);
    const { floId, floTxid } = req.body;

    if (!propertyId || !slotId) {
      return res.status(400).json({ success: false, error: "Invalid propertyId or slotId" });
    }
    if (!floId) {
      return res.status(400).json({ success: false, error: "Missing floId" });
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
        return res.status(404).json({ success: false, error: "Slot not found" });
      }
      if (slot.rows[0].owner_flo_id) {
        await client.query("ROLLBACK");
        return res.status(409).json({ success: false, error: "Slot already owned" });
      }

      const property = await client.query(
        "SELECT current_price FROM properties WHERE id = $1 FOR UPDATE",
        [propertyId],
      );
      const price = property.rows[0]?.current_price || BASE_PROPERTY_PRICE;

      // TODO: confirm the real vesting rule - 14 days is a placeholder
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

      await client.query(
        `INSERT INTO property_transactions (property_id, slot_id, type, flo_id, price, flo_txid)
         VALUES ($1, $2, 'buy', $3, $4, $5)`,
        [propertyId, slotId, floId, price, floTxid || null],
      );

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
// Sells the slot back to the pool (clears ownership) rather than a
// direct transfer to a named buyer. Same transaction + row-lock
// treatment as buy, above.
app.post(
  "/api/properties/:propertyId/slots/:slotId/sell",
  rateLimit({ max: 10, windowMs: 60000 }),
  verifyFloSignature(["propertyId", "slotId", "floId", "time"]),
  async (req, res) => {
    const propertyId = parsePositiveInt(req.params.propertyId);
    const slotId = parsePositiveInt(req.params.slotId);
    const { floId, floTxid } = req.body;

    if (!propertyId || !slotId) {
      return res.status(400).json({ success: false, error: "Invalid propertyId or slotId" });
    }
    if (!floId) {
      return res.status(400).json({ success: false, error: "Missing floId" });
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
        return res.status(404).json({ success: false, error: "Slot not found" });
      }
      const current = slot.rows[0];
      if (current.owner_flo_id !== floId) {
        await client.query("ROLLBACK");
        return res.status(403).json({ success: false, error: "Not the slot owner" });
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

      const price = property.rows[0].current_price || BASE_PROPERTY_PRICE;

      const updated = await client.query(
        `
        UPDATE property_slots
        SET owner_flo_id = NULL,
            acquired_at = NULL,
            acquired_price = NULL,
            eligible_to_sell_at = NULL
        WHERE id = $1
        RETURNING *
        `,
        [slotId],
      );

      await client.query(
        `INSERT INTO property_transactions (property_id, slot_id, type, flo_id, price, flo_txid)
         VALUES ($1, $2, 'sell', $3, $4, $5)`,
        [propertyId, slotId, floId, price, floTxid || null],
      );

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
        return res.status(404).json({ success: false, error: "Task not found" });
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
// API: Task board (minimal - full flow is a separate feature)
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
// RANKABLE_COMPONENT_TYPES so it can be filtered/tagged consistently
// with the rest of the marketplace.
// Admin-only, same as categories/usage-events - keeps the task board
// from being spammed with junk listings.
app.post(
  "/api/tasks",
  verifyFloSignature(
    ["adminFloId", "requesterFloId", "brief", "budget", "componentType", "time"],
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

    if (componentType && !RANKABLE_COMPONENT_TYPES.includes(componentType)) {
      return res.status(400).json({
        success: false,
        error: `componentType must be one of: ${RANKABLE_COMPONENT_TYPES.join(", ")}`,
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
  verifyFloSignature(["taskId", "creatorFloId", "time"], { floIdField: "creatorFloId" }),
  async (req, res) => {
    const taskId = parsePositiveInt(req.params.taskId);
    const { creatorFloId } = req.body;

    if (!taskId) {
      return res.status(400).json({ success: false, error: "Invalid taskId" });
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const task = await client.query("SELECT * FROM tasks WHERE id = $1 FOR UPDATE", [taskId]);
      if (!task.rows.length) {
        await client.query("ROLLBACK");
        return res.status(404).json({ success: false, error: "Task not found" });
      }
      if (task.rows[0].status !== "open") {
        await client.query("ROLLBACK");
        return res.status(409).json({ success: false, error: "Task is not open" });
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
  verifyFloSignature(["taskId", "creatorFloId", "trackId", "time"], { floIdField: "creatorFloId" }),
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

      const task = await client.query("SELECT * FROM tasks WHERE id = $1 FOR UPDATE", [taskId]);
      if (!task.rows.length) {
        await client.query("ROLLBACK");
        return res.status(404).json({ success: false, error: "Task not found" });
      }
      const current = task.rows[0];
      if (current.status !== "claimed") {
        await client.query("ROLLBACK");
        return res.status(409).json({ success: false, error: "Task is not claimed" });
      }
      if (current.fulfilled_by_flo_id !== creatorFloId) {
        await client.query("ROLLBACK");
        return res.status(403).json({ success: false, error: "Not the creator who claimed this task" });
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