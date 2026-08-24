const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
require("dotenv").config();
const { verifyFloSignature } = require("./flo-auth");
const {
  verifyFloPayment,
  sendFloPayment,
  sendUsdaiPayment,
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

// Price model weights.
const PRICE_ALPHA = 0.15;
const PRICE_BETA = 0.15;

// --- Main Token economy -------------------------------------------------
// base_price is per-property, driven by consumption. Main Token,
// liquidity, sell-gating, and payouts are a layer on top of the existing
// scarcity/utility/price/Top 100 system.
//
// The numbers below are placeholders until there's real trading data to
// tune them against.
const PER_UNIT_VALUE = 1; // $ per consumption unit - a $1/view starting anchor, purely a placeholder
const PLATFORM_EXPENSE_PCT = 0.05; // cut taken before anything hits liquidity
const PLATFORM_LIQUIDITY_TARGET = 10000; // one shared pool, needed before selling OR contributor payouts open at all
const SELL_PRESSURE_FLOOR = 1.0; // buy/sell ratio at or below this = queue stays fully closed
const SELL_PRESSURE_CEILING = 3.0; // ratio at or above this = queue fully released
const PORTFOLIO_FLOOR_SHARE = 0.002; // no property in the Top 100 gets less than this share of new investment
const CONTRIBUTOR_RELEASE_PCT = 0.1; // Percentage of the shared pool released for contributor payouts each cycle
const PER_TRACK_MIN_PAYOUT = 5; // a track's pending payout has to clear this before it actually pays out
const CONSUMPTION_GROWTH_BURN_THRESHOLD = 0.02; // growth rate below which the future token-burn mechanism is meant to trigger - see isConsumptionGrowthFlat

// --- USDAI payment verification (Main Token buy) ---------------------------
// Main Token is priced in USDAI directly - USDAI is RanchiMall's own token on the FLO blockchain, assumed to equal $1. A USDAI transfer is verified through the token API rather than a plain FLO balance check, and both sender and receiver are checked against the live response.
const TOKEN_API_BASE_URL =
  process.env.TOKEN_API_BASE_URL ||
  "https://ranchimallflo.ranchimall.net/api/v2";
const USDAI_TOKEN_IDENTIFIER = process.env.USDAI_TOKEN_IDENTIFIER || "usdai";
const USDAI_PAYMENT_ADDRESS = process.env.USDAI_PAYMENT_ADDRESS;
const SENDER_FIELD_CANDIDATES = ["senderAddress"];
const DEST_FIELD_CANDIDATES = ["receiverAddress"];

async function verifyUsdaiPayment(txid, requiredAmount, expectedSender) {
  if (!txid || typeof txid !== "string") {
    throw new Error("Missing USDAI transaction ID");
  }

  const response = await fetch(
    `${TOKEN_API_BASE_URL}/transactionDetails/${txid}`,
    { signal: AbortSignal.timeout(8000) },
  );
  if (!response.ok) {
    throw new Error(`Token API returned ${response.status} for txid ${txid}`);
  }
  const data = await response.json();

  const tx = data;
  if (!tx) {
    throw new Error(`Unexpected token API response shape for txid ${txid}`);
  }

  if (
    String(tx.tokenIdentification || "").toLowerCase() !==
    USDAI_TOKEN_IDENTIFIER.toLowerCase()
  ) {
    throw new Error(
      `Transaction ${txid} is not a ${USDAI_TOKEN_IDENTIFIER} transfer (got ${tx.tokenIdentification})`,
    );
  }
  if (
    tx.type !== "transfer" &&
    tx.transferType !== "token" &&
    tx.operation !== "token-transfer"
  ) {
    throw new Error(`Transaction ${txid} is not a token transfer`);
  }

  const tokenAmount = Number(tx.tokenAmount);
  if (!Number.isFinite(tokenAmount) || tokenAmount < requiredAmount) {
    throw new Error(
      `Insufficient USDAI in transaction ${txid}: got ${tokenAmount}, required ${requiredAmount}`,
    );
  }

  if (!tx.confirmations || tx.confirmations < 1) {
    throw new Error(`Transaction ${txid} is not confirmed yet`);
  }

  let senderField = null;
  for (const candidate of SENDER_FIELD_CANDIDATES) {
    if (tx[candidate]) {
      senderField = tx[candidate];
      break;
    }
  }
  if (!senderField) {
    throw new Error(
      `Could not find a sender address field on txid ${txid} using any of ` +
        `[${SENDER_FIELD_CANDIDATES.join(", ")}] - refusing rather than skipping ` +
        "the sender check. Confirm the real field name against the live API response.",
    );
  }
  if (senderField !== expectedSender) {
    throw new Error(
      `Transaction ${txid} was sent by ${senderField}, not ${expectedSender}`,
    );
  }

  // Make sure the payment reached the right address.
  if (!USDAI_PAYMENT_ADDRESS) {
    throw new Error("Payments not configured (USDAI_PAYMENT_ADDRESS unset)");
  }
  let destField = null;
  for (const candidate of DEST_FIELD_CANDIDATES) {
    if (tx[candidate]) {
      destField = tx[candidate];
      break;
    }
  }
  if (!destField) {
    throw new Error(
      `Could not find a destination address field on txid ${txid} using any of ` +
        `[${DEST_FIELD_CANDIDATES.join(", ")}] - refusing rather than skipping ` +
        "the destination check. Confirm the real field name against the live API response.",
    );
  }
  if (destField !== USDAI_PAYMENT_ADDRESS) {
    throw new Error(
      `Transaction ${txid} was sent to ${destField}, not the USDAI payment address`,
    );
  }

  return {
    valid: true,
    txid,
    tokenAmount,
    confirmations: tx.confirmations,
    sender: senderField,
  };
}

const VALUATION_FORMULA_VERSION = "v1-per-unit-value";

// ---------------------------------------------------------------------
// Schema setup
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
  // consumption drives base_price now (see calculateBasePrice) - kept
  // alongside scarcity_score/utility_score above, doesn't replace them.
  await pool.query(`
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS consumption NUMERIC DEFAULT 0;
  `);
  await pool.query(`
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS base_price NUMERIC DEFAULT 0;
  `);
  await pool.query(`
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS valuation_updated_at TIMESTAMPTZ;
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_valuation_history (
      id                SERIAL PRIMARY KEY,
      property_id       INT REFERENCES properties(id),
      consumption       NUMERIC NOT NULL,
      base_price        NUMERIC NOT NULL,
      formula_version   TEXT NOT NULL,
      created_at        TIMESTAMPTZ DEFAULT now()
    );
  `);

  // --- Main Token ledger ---------------------------------------------------
  // Still just a DB ledger, not an on-chain token - that's a later phase.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS main_token_balances (
      flo_id          TEXT PRIMARY KEY,
      balance         NUMERIC DEFAULT 0,
      updated_at      TIMESTAMPTZ DEFAULT now()
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS main_token_transactions (
      id              SERIAL PRIMARY KEY,
      flo_id          TEXT NOT NULL,
      type            TEXT NOT NULL,
      token_amount    NUMERIC NOT NULL,
      price_at_time   NUMERIC NOT NULL,
      payment_txid    TEXT,
      created_at      TIMESTAMPTZ DEFAULT now()
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS main_token_price_history (
      id                SERIAL PRIMARY KEY,
      price             NUMERIC NOT NULL,
      total_supply      NUMERIC NOT NULL,
      system_valuation  NUMERIC NOT NULL,
      recorded_at       TIMESTAMPTZ DEFAULT now()
    );
  `);
  // Prevent the same payment transaction from being redeemed more than once.
  try {
    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS main_token_transactions_payment_txid_unique
      ON main_token_transactions (payment_txid) WHERE payment_txid IS NOT NULL;
    `);
  } catch (err) {
    console.error(
      "WARNING: could not create unique index on main_token_transactions.payment_txid " +
        "(likely pre-existing duplicates) - txid replay protection is NOT active:",
      err,
    );
  }

  // --- Platform liquidity ---------------------------------------------------
  // One platform-wide balance that has to fill up before selling opens at all - selling stays fully closed until this crosses liquidity_target.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS platform_liquidity (
      id                SERIAL PRIMARY KEY,
      balance           NUMERIC DEFAULT 0,
      expenses_taken    NUMERIC DEFAULT 0,
      liquidity_target  NUMERIC,
      updated_at        TIMESTAMPTZ DEFAULT now()
    );
  `);

  // --- Sell-gating -----------------------------------------------------------
  await pool.query(`
    CREATE TABLE IF NOT EXISTS sell_queue (
      id              SERIAL PRIMARY KEY,
      flo_id          TEXT NOT NULL,
      token_amount    NUMERIC NOT NULL,
      requested_at    TIMESTAMPTZ DEFAULT now(),
      released_amount NUMERIC DEFAULT 0,
      status          TEXT DEFAULT 'queued'
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_payouts (
      id                SERIAL PRIMARY KEY,
      property_id       INT REFERENCES properties(id),
      component_id      INT REFERENCES tracks_components(id),
      recipient_flo_id  TEXT NOT NULL,
      amount            NUMERIC NOT NULL,
      status            TEXT DEFAULT 'pending',
      created_at        TIMESTAMPTZ DEFAULT now()
    );
  `);
  // status also uses 'sending' (claimed by an in-progress payout run) and
  // 'paid' (actually sent) - see executePropertyPayouts.
  await pool.query(`
    ALTER TABLE property_payouts ADD COLUMN IF NOT EXISTS flo_txid TEXT;
  `);
  await pool.query(`
    ALTER TABLE property_payouts ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ;
  `);

  // tracks how much of a queued sell has actually been paid out in FLO so
  // far, separate from released_amount (which just says how much is
  // *allowed* to be sold) - lets executeReleasedSells() pick up only the
  // newly-unlocked slice each time it runs.
  await pool.query(`
    ALTER TABLE sell_queue ADD COLUMN IF NOT EXISTS paid_amount NUMERIC DEFAULT 0;
  `);
  // Written the moment a payout send succeeds, before any other
  // bookkeeping - so if the rest of that bookkeeping fails, the txid
  // still exists somewhere other than a console log.
  await pool.query(`
    ALTER TABLE sell_queue ADD COLUMN IF NOT EXISTS payout_txid TEXT;
  `);

  // --- User portfolio (holdings resulting from Main Token buys) -----------
  // One row per (holder, property) they're currently exposed to through
  // Main Token. Rebalanced on every pipeline run as Top 100 changes -
  // see rebalancePortfolioPositions().
  await pool.query(`
    CREATE TABLE IF NOT EXISTS portfolio_positions (
      flo_id          TEXT NOT NULL,
      property_id     INT NOT NULL REFERENCES properties(id),
      token_amount    NUMERIC NOT NULL DEFAULT 0,
      allocation_pct  NUMERIC NOT NULL DEFAULT 0,
      value           NUMERIC NOT NULL DEFAULT 0,
      updated_at      TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (flo_id, property_id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS portfolio_snapshots (
      id              SERIAL PRIMARY KEY,
      flo_id          TEXT NOT NULL,
      total_value     NUMERIC NOT NULL,
      token_price     NUMERIC NOT NULL,
      created_at      TIMESTAMPTZ DEFAULT now()
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
  // Financing positions are funded by real USDAI payments - store the funding txid (unique when present, so a payment can't be reused).
  await pool.query(`
    ALTER TABLE financing_positions ADD COLUMN IF NOT EXISTS usdai_txid TEXT;
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS financing_positions_usdai_txid_key
      ON financing_positions (usdai_txid) WHERE usdai_txid IS NOT NULL;
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

function parsePositiveInt(value) {
  const n = parseInt(value, 10);
  return Number.isInteger(n) && n > 0 ? n : null;
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

// basePrice used to always be BASE_PROPERTY_PRICE (a flat constant).
// Now the caller passes in the property's own consumption-driven
// base_price instead - the scarcity/utility math around it hasn't
// changed at all.
function computePrice(basePrice, scarcityScore, utilityScore) {
  return (
    basePrice *
    (1 + PRICE_ALPHA * scarcityScore) *
    (1 + PRICE_BETA * utilityScore)
  );
}

// consumption = plays + likes across the property's component tracks -
// same join computeUtilityScore already uses for its engagement proxy,
// just summing raw counts instead of log-scaling them.
async function computePropertyConsumption(propertyId) {
  const result = await pool.query(
    `
    WITH members AS (
      SELECT DISTINCT tc.track_id
      FROM property_components pc
      JOIN tracks_components tc ON tc.id = pc.component_id
      WHERE pc.property_id = $1
    ),
    play_totals AS (
      SELECT COALESCE(SUM(p.play_count), 0)::numeric AS total_plays
      FROM plays p
      JOIN members m ON m.track_id = p.track_id
    ),
    like_totals AS (
      SELECT COUNT(*)::numeric AS total_likes
      FROM likes l
      JOIN members m ON m.track_id = l.track_id
    )
    SELECT play_totals.total_plays + like_totals.total_likes AS total_consumption
    FROM play_totals, like_totals
    `,
    [propertyId],
  );
  return Number(result.rows[0]?.total_consumption || 0);
}

// Base property price is determined by consumption and the configured per-unit value.
function calculateBasePrice(consumption) {
  return consumption * PER_UNIT_VALUE;
}

async function recordValuation(propertyId, consumption, basePrice) {
  await pool.query(
    `
    INSERT INTO property_valuation_history
      (property_id, consumption, base_price, formula_version)
    VALUES ($1, $2, $3, $4)
    `,
    [propertyId, consumption, basePrice, VALUATION_FORMULA_VERSION],
  );
}

// --- Main Token price -------------------------------------------------------
// token_price = system_valuation / total_token_supply. Whenever
// consumption grows faster than new tokens get issued, price goes up -
// Main Token price is based on system valuation and total token supply.
async function getTotalMainTokenSupply() {
  const result = await pool.query(
    `SELECT COALESCE(SUM(balance), 0)::numeric AS total FROM main_token_balances`,
  );
  return Number(result.rows[0]?.total || 0);
}

function computeMainTokenPrice(systemValuation, totalSupply) {
  // nobody holds any tokens yet - price isn't meaningful, just anchor it
  // to system_valuation so the first buyer gets a sane starting price.
  if (totalSupply <= 0) return systemValuation > 0 ? systemValuation : 1;
  return systemValuation / totalSupply;
}

async function recordMainTokenPrice(price, totalSupply, systemValuation) {
  await pool.query(
    `
    INSERT INTO main_token_price_history (price, total_supply, system_valuation)
    VALUES ($1, $2, $3)
    `,
    [price, totalSupply, systemValuation],
  );
}

// Not called anywhere yet - the design calls for burning tokens once consumption growth flattens out, to keep token price growing after the consumption-driven half of the valuation plateaus. This is the detection check for that; the actual burn logic doesn't exist yet.
function isConsumptionGrowthFlat(previousValuation, currentValuation) {
  if (!previousValuation) return false;
  const growthRate = (currentValuation - previousValuation) / previousValuation;
  return growthRate < CONSUMPTION_GROWTH_BURN_THRESHOLD;
}

// --- Platform liquidity -----------------------------------------------------
// One shared pool backs seller and contributor payouts.
function splitInvestment(grossAmount) {
  const expense = grossAmount * PLATFORM_EXPENSE_PCT;
  const netAfterExpense = grossAmount - expense;
  return { expense, netAfterExpense };
}

async function depositToPlatformLiquidity(
  netAmount,
  expense = 0,
  dbClient = pool,
) {
  await dbClient.query(
    `
    INSERT INTO platform_liquidity (id, balance, expenses_taken, liquidity_target)
    VALUES (1, $1, $2, $3)
    ON CONFLICT (id) DO UPDATE
    SET balance = platform_liquidity.balance + $1,
        expenses_taken = platform_liquidity.expenses_taken + $2,
        updated_at = now()
    `,
    [netAmount, expense, PLATFORM_LIQUIDITY_TARGET],
  );
  return { netAmount, expense };
}

async function reservePlatformLiquidity(dbClient, amount) {
  const result = await dbClient.query(
    `SELECT balance FROM platform_liquidity WHERE id = 1 FOR UPDATE`,
  );
  const balance = Number(result.rows[0]?.balance || 0);
  if (balance < amount) {
    return false;
  }
  await dbClient.query(
    `UPDATE platform_liquidity SET balance = balance - $1, updated_at = now() WHERE id = 1`,
    [amount],
  );
  return true;
}

async function isPlatformLiquidityBuilt() {
  const result = await pool.query(
    `SELECT balance, liquidity_target FROM platform_liquidity WHERE id = 1`,
  );
  const row = result.rows[0];
  if (!row) return false;
  return (
    Number(row.balance) >=
    Number(row.liquidity_target || PLATFORM_LIQUIDITY_TARGET)
  );
}

// --- Sell-gating -------------------------------------------------------------
// Same buy/sell-intent idea as computeScarcityScore, just at the Main
// Token level instead of per property.
async function computeMainTokenPressureRatio() {
  const buyResult = await pool.query(
    `
    SELECT COALESCE(SUM(token_amount), 0)::numeric AS total
    FROM main_token_transactions
    WHERE type = 'buy' AND created_at > now() - INTERVAL '7 days'
    `,
  );
  const sellResult = await pool.query(
    `
    SELECT COALESCE(SUM(token_amount), 0)::numeric AS total
    FROM sell_queue
    WHERE requested_at > now() - INTERVAL '7 days'
    `,
  );
  const buyVolume = Number(buyResult.rows[0]?.total || 0);
  const sellVolume = Number(sellResult.rows[0]?.total || 0);
  return buyVolume / Math.max(sellVolume, 1);
}

// 0 below SELL_PRESSURE_FLOOR, scaling straight up to 1 at
// SELL_PRESSURE_CEILING - kept linear for now, exact curve shape TBD.
function computeSellReleaseFraction(pressureRatio) {
  const span = SELL_PRESSURE_CEILING - SELL_PRESSURE_FLOOR;
  const raw = (pressureRatio - SELL_PRESSURE_FLOOR) / span;
  return Math.min(Math.max(raw, 0), 1);
}

// Releases the oldest queued requests first, in proportion to
// releaseFraction. This only marks how much of each request is allowed
// to settle; executeReleasedSells() performs the actual payout.
async function releaseSellQueue() {
  const liquidityBuilt = await isPlatformLiquidityBuilt();
  if (!liquidityBuilt) return { releaseFraction: 0, updated: 0 };

  const pressureRatio = await computeMainTokenPressureRatio();
  const releaseFraction = computeSellReleaseFraction(pressureRatio);

  const queued = await pool.query(
    `
    SELECT id, token_amount, released_amount FROM sell_queue
    WHERE status IN ('queued', 'partially_released')
    ORDER BY requested_at ASC
    `,
  );

  let updated = 0;
  for (const row of queued.rows) {
    const targetReleased = Number(row.token_amount) * releaseFraction;
    if (targetReleased <= Number(row.released_amount)) continue;

    const fulfilled = targetReleased >= Number(row.token_amount);
    await pool.query(
      `
      UPDATE sell_queue
      SET released_amount = $1,
          status = $2
      WHERE id = $3
      `,
      [
        fulfilled ? row.token_amount : targetReleased,
        fulfilled ? "fulfilled" : "partially_released",
        row.id,
      ],
    );
    updated += 1;
  }

  return { releaseFraction, updated };
}

// --- Portfolio allocation across the Top 100 --------------------------------
// Buying Main Token buys a slice of the whole Top-100 portfolio, weighted
// by each property's consumption, with a floor so nobody in the Top 100
// ever gets zero. The contributor payout cycle below reuses this too -
// same rule, same floor, in both places.
function computePortfolioAllocation(top100Properties) {
  const totalConsumption = top100Properties.reduce(
    (sum, p) => sum + Number(p.consumption || 0),
    0,
  );
  if (totalConsumption <= 0 || top100Properties.length === 0) return [];

  const rawShares = top100Properties.map((p) => ({
    property_id: p.id,
    share: Math.max(
      Number(p.consumption || 0) / totalConsumption,
      PORTFOLIO_FLOOR_SHARE,
    ),
  }));

  // floors push the total above 1 - renormalize so shares actually sum to 1
  const total = rawShares.reduce((sum, s) => sum + s.share, 0);
  return rawShares.map((s) => ({
    property_id: s.property_id,
    share: s.share / total,
  }));
}

// --- Contributor payouts -----------------------------------------------------
// Draws from the same shared pool and liquidity_target that gates Main
// Token selling. Each cycle releases a slice, splits it across the
// current Top 100 by consumption, then splits each property's share
// across its components by their own consumption. Fraud/fairness
// adjustment is not yet implemented.
async function releaseContributorPayouts() {
  const liquidityBuilt = await isPlatformLiquidityBuilt();
  if (!liquidityBuilt)
    return { released: false, reason: "still building liquidity" };

  const poolResult = await pool.query(
    `SELECT balance FROM platform_liquidity WHERE id = 1`,
  );
  const poolBalance = Number(poolResult.rows[0]?.balance || 0);
  const releaseAmount = poolBalance * CONTRIBUTOR_RELEASE_PCT;
  if (releaseAmount <= 0)
    return { released: false, reason: "nothing to release" };

  const top100 = await pool.query(
    "SELECT id, consumption FROM properties WHERE in_top_100 = true",
  );
  const allocation = computePortfolioAllocation(top100.rows);

  let totalCreated = 0;
  for (const a of allocation) {
    const propertyReleaseAmount = releaseAmount * a.share;

    // Same consumption metric used everywhere else: plays + likes, not
    // plays alone.
    const componentsResult = await pool.query(
      `
      SELECT tc.id AS component_id, tc.contributor_flo_id,
             COALESCE(play_totals.total_plays, 0) + COALESCE(like_totals.total_likes, 0) AS consumption
      FROM property_components pc
      JOIN tracks_components tc ON tc.id = pc.component_id
      LEFT JOIN (
        SELECT track_id, SUM(play_count)::numeric AS total_plays FROM plays GROUP BY track_id
      ) play_totals ON play_totals.track_id = tc.track_id
      LEFT JOIN (
        SELECT track_id, COUNT(*)::numeric AS total_likes FROM likes GROUP BY track_id
      ) like_totals ON like_totals.track_id = tc.track_id
      WHERE pc.property_id = $1
      `,
      [a.property_id],
    );

    const totalComponentConsumption = componentsResult.rows.reduce(
      (sum, r) => sum + Number(r.consumption),
      0,
    );
    if (totalComponentConsumption <= 0) continue;

    for (const row of componentsResult.rows) {
      // TODO: real fraud/fairness signal not built yet
      const share = Number(row.consumption) / totalComponentConsumption;
      const amount = propertyReleaseAmount * share;
      if (amount < PER_TRACK_MIN_PAYOUT) continue; // held back, doesn't pay out this round

      await pool.query(
        `
        INSERT INTO property_payouts (property_id, component_id, recipient_flo_id, amount, status)
        VALUES ($1, $2, $3, $4, 'pending')
        `,
        [a.property_id, row.component_id, row.contributor_flo_id, amount],
      );
      totalCreated += amount;
    }
  }

  // These are still pending, so the pool isn't touched yet.
  return { released: true, created: totalCreated };
}

// Claims each payout, reserves the pool balance, then sends USDAI.
async function executePropertyPayouts() {
  const pendingResult = await pool.query(
    `SELECT id FROM property_payouts WHERE status = 'pending'`,
  );

  let paid = 0;
  let failed = 0;

  for (const { id } of pendingResult.rows) {
    const client = await pool.connect();
    let payout;
    try {
      await client.query("BEGIN");
      const claimed = await client.query(
        `SELECT * FROM property_payouts WHERE id = $1 AND status = 'pending' FOR UPDATE`,
        [id],
      );
      if (!claimed.rows.length) {
        // Another run claimed it first.
        await client.query("ROLLBACK");
        continue;
      }
      payout = claimed.rows[0];
      const amount = Number(payout.amount);
      const reserved = await reservePlatformLiquidity(client, amount);
      if (!reserved) {
        await client.query("ROLLBACK");
        continue;
      }
      await client.query(
        `UPDATE property_payouts SET status = 'sending' WHERE id = $1`,
        [id],
      );
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(`Failed to claim property payout ${id}:`, err);
      failed += 1;
      continue;
    } finally {
      client.release();
    }

    // Send USDAI directly.
    const usdaiAmount = Number(payout.amount);

    let payoutTxid;
    try {
      payoutTxid = await sendUsdaiPayment(payout.recipient_flo_id, usdaiAmount);
    } catch (err) {
      console.error(
        `Property payout ${id} failed to send, reverting to pending:`,
        err,
      );
      const refundClient = await pool.connect();
      try {
        await refundClient.query("BEGIN");
        await refundClient.query(
          `UPDATE property_payouts SET status = 'pending' WHERE id = $1`,
          [id],
        );
        await refundClient.query(
          `UPDATE platform_liquidity SET balance = balance + $1, updated_at = now() WHERE id = 1`,
          [Number(payout.amount)],
        );
        await refundClient.query("COMMIT");
      } catch (refundErr) {
        await refundClient.query("ROLLBACK");
        console.error(
          `CRITICAL: failed to refund liquidity for property payout ${id}:`,
          refundErr,
        );
      } finally {
        refundClient.release();
      }
      failed += 1;
      continue;
    }

    // Record the txid right away, best-effort, before anything else that
    // could fail - if the finalize step below fails, this is the
    // difference between "the txid is stuck in a log line" and "the
    // txid is on the row, ready for reconciliation."
    try {
      await pool.query(
        `UPDATE property_payouts SET flo_txid = $1 WHERE id = $2`,
        [payoutTxid, id],
      );
    } catch (txidErr) {
      console.error(
        `Sent ${usdaiAmount} USDAI (txid ${payoutTxid}) for property payout ${id} ` +
          "but failed to record the txid on the row:",
        txidErr,
      );
    }

    // The send already happened. This step just records it.
    const payClient = await pool.connect();
    try {
      await payClient.query("BEGIN");
      await payClient.query(
        `UPDATE property_payouts SET status = 'paid', flo_txid = $1, paid_at = now() WHERE id = $2`,
        [payoutTxid, id],
      );
      await payClient.query("COMMIT");
      paid += 1;
    } catch (err) {
      await payClient.query("ROLLBACK");
      console.error(
        `CRITICAL: sent ${usdaiAmount} USDAI (txid ${payoutTxid}) for property payout ${id} ` +
          "but marking it paid failed - needs manual reconciliation:",
        err,
      );
      failed += 1;
    } finally {
      payClient.release();
    }
  }

  return { paid, failed };
}

// Run contributor releases, then send the pending payouts.
async function runContributorPayoutCycle() {
  await releaseContributorPayouts();
  return executePropertyPayouts();
}

// Standalone version of the sum the pipeline already does inline -
// buy/sell endpoints need a fresh number too, not just once a day.
async function computeSystemValuation() {
  const result = await pool.query(
    `SELECT COALESCE(SUM(base_price), 0)::numeric AS total FROM properties WHERE status = 'active'`,
  );
  return Number(result.rows[0]?.total || 0);
}

// --- Portfolio rebalancing ---------------------------------------------------
// Each holder's position is a snapshot, not a fixed claim on specific
// properties - every run, we take what a holder's portfolio is currently
// worth, wipe their positions, and re-split that same value across
// whatever's in the Top 100 now. That's what makes it "automatically
// rebalance" - a property dropping out of the Top 100 doesn't leave a
// holder stuck holding a dead asset, it just stops getting a share next
// time this runs.
// Rebalances from each holder's actual Main Token balance, not from the
// old position rows - token_amount and value are different units
// (token quantity vs. dollar value) and re-deriving totalValue from a
// SUM(value) of prior positions would compound any drift between them.
async function rebalancePortfolioPositions(top100Properties, tokenPrice) {
  const allocation = computePortfolioAllocation(top100Properties);
  if (!allocation.length) return { rebalanced: 0 };

  const holders = await pool.query(
    `SELECT flo_id, balance FROM main_token_balances WHERE balance > 0`,
  );

  let rebalanced = 0;
  for (const { flo_id, balance } of holders.rows) {
    const holderTokenBalance = Number(balance);
    if (holderTokenBalance <= 0) continue;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`DELETE FROM portfolio_positions WHERE flo_id = $1`, [
        flo_id,
      ]);
      for (const a of allocation) {
        const tokenAmount = holderTokenBalance * a.share;
        const value = tokenAmount * tokenPrice;
        await client.query(
          `
          INSERT INTO portfolio_positions (flo_id, property_id, token_amount, allocation_pct, value)
          VALUES ($1, $2, $3, $4, $5)
          `,
          [flo_id, a.property_id, tokenAmount, a.share, value],
        );
      }
      await client.query("COMMIT");
      rebalanced += 1;
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(`Failed to rebalance portfolio for ${flo_id}:`, err);
    } finally {
      client.release();
    }
  }

  return { rebalanced };
}

// --- Sell execution (finishing the loop) --------------------------------------
// This sends the released slice of each sell request and records it once.
async function executeReleasedSells() {
  const queued = await pool.query(
    `SELECT id, flo_id, token_amount, released_amount, paid_amount FROM sell_queue
     WHERE status IN ('partially_released', 'fulfilled')`,
  );

  let executed = 0;
  for (const row of queued.rows) {
    const newlyReleased = Number(row.released_amount) - Number(row.paid_amount);
    if (newlyReleased <= 0) continue;

    const tokenPrice = computeMainTokenPrice(
      await computeSystemValuation(),
      await getTotalMainTokenSupply(),
    );
    const usdaiAmount = newlyReleased * tokenPrice;

    const claimClient = await pool.connect();
    let payoutTxid;
    try {
      await claimClient.query("BEGIN");
      const claimed = await claimClient.query(
        `SELECT id, paid_amount, released_amount, status FROM sell_queue
         WHERE id = $1 AND status IN ('partially_released', 'fulfilled') FOR UPDATE`,
        [row.id],
      );
      if (!claimed.rows.length) {
        await claimClient.query("ROLLBACK");
        continue;
      }

      const lockedRow = claimed.rows[0];
      const lockedNewlyReleased =
        Number(lockedRow.released_amount) - Number(lockedRow.paid_amount);
      if (lockedNewlyReleased <= 0) {
        await claimClient.query("ROLLBACK");
        continue;
      }

      const lockedUsdaiAmount = lockedNewlyReleased * tokenPrice;
      const reserved = await reservePlatformLiquidity(
        claimClient,
        lockedUsdaiAmount,
      );
      if (!reserved) {
        await claimClient.query("ROLLBACK");
        continue;
      }

      await claimClient.query(
        `UPDATE sell_queue SET status = 'sending' WHERE id = $1`,
        [row.id],
      );
      await claimClient.query("COMMIT");

      payoutTxid = await sendUsdaiPayment(row.flo_id, lockedUsdaiAmount);

      // Record the txid right away, best-effort, before anything else
      // that could fail - if the finalize step below fails, this is the
      // difference between "the txid is stuck in a log line" and "the
      // txid is on the row, ready for reconciliation."
      try {
        await pool.query(
          `UPDATE sell_queue SET payout_txid = $1 WHERE id = $2`,
          [payoutTxid, row.id],
        );
      } catch (txidErr) {
        console.error(
          `Sent ${lockedUsdaiAmount} USDAI (txid ${payoutTxid}) for sell_queue row ${row.id} ` +
            "but failed to record the txid on the row:",
          txidErr,
        );
      }
    } catch (err) {
      try {
        await claimClient.query("ROLLBACK");
      } catch (_) {}
      console.error(`Sell payout failed for queue row ${row.id}:`, err);
      continue;
    } finally {
      claimClient.release();
    }

    const finalClient = await pool.connect();
    try {
      await finalClient.query("BEGIN");
      await finalClient.query(
        `UPDATE sell_queue SET paid_amount = released_amount, status = 'paid' WHERE id = $1`,
        [row.id],
      );
      await finalClient.query(
        `UPDATE main_token_balances SET balance = balance - $1, updated_at = now() WHERE flo_id = $2`,
        [newlyReleased, row.flo_id],
      );
      await finalClient.query(
        `
        INSERT INTO main_token_transactions (flo_id, type, token_amount, price_at_time, payment_txid)
        VALUES ($1, 'sell', $2, $3, $4)
        `,
        [row.flo_id, newlyReleased, tokenPrice, payoutTxid],
      );
      await finalClient.query("COMMIT");
      executed += 1;
    } catch (err) {
      await finalClient.query("ROLLBACK");
      console.error(
        `CRITICAL: sent ${usdaiAmount} USDAI (txid ${payoutTxid}) to ${row.flo_id} for sell_queue row ${row.id} ` +
          "but recording it failed - needs manual reconciliation:",
        err,
      );
    } finally {
      finalClient.release();
    }
  }

  return { executed };
}

// ---------------------------------------------------------------------
// Ranking pipeline
//
// Rank populated properties, update their scores and prices,
// maintain the Top 100.
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

    // Recompute price from the property's consumption-driven base price, scarcity score, and utility score.
    const scored = [];
    for (const property of populated.rows) {
      const scarcity = await computeScarcityScore(property.id);
      const utility = await computeUtilityScore(property.id);

      const consumption = await computePropertyConsumption(property.id);
      const basePrice = calculateBasePrice(consumption);
      const price = computePrice(basePrice, scarcity, utility);
      await recordValuation(property.id, consumption, basePrice);

      const newStreak =
        scarcity > 2.0 ? (property.high_scarcity_streak || 0) + 1 : 0;

      // current_price is still the full scarcity/utility-adjusted price -
      // buy/sell code downstream doesn't need to change at all. base_price
      // is the new piece, stored separately since system_valuation (for
      // the Main Token) sums base_price, not current_price.
      await pool.query(
        `
        UPDATE properties
        SET scarcity_score = $1,
            utility_score = $2,
            consumption = $3,
            base_price = $4,
            current_price = $5,
            valuation_updated_at = now(),
            total_slots = $6,
            high_scarcity_streak = $7,
            updated_at = now()
        WHERE id = $8
        `,
        [
          scarcity,
          utility,
          consumption,
          basePrice,
          price,
          property.total_slots,
          newStreak,
          property.id,
        ],
      );

      scored.push({
        id: property.id,
        category_id: property.category_id,
        base_price: basePrice,
        consumption,
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
    const globalTop100 = [...scored]
      .filter((s) => !independentCategoryIds.has(s.category_id))
      .sort((a, b) => b.price - a.price)
      .slice(0, 100);
    await rankAndFlag(globalTop100);

    // Independent pools: each graduated category ranks only within itself.
    for (const categoryId of independentCategoryIds) {
      await rankAndFlag(scored.filter((s) => s.category_id === categoryId));
    }

    // 4: Main Token price uses the shared valuation helper.
    const systemValuation = await computeSystemValuation();
    const totalSupply = await getTotalMainTokenSupply();
    const tokenPrice = computeMainTokenPrice(systemValuation, totalSupply);
    await recordMainTokenPrice(tokenPrice, totalSupply, systemValuation);

    // 5: Open sells once liquidity is high enough.
    await releaseSellQueue();

    // 6: Pay sells, then rebalance the Top 100 portfolio.
    await executeReleasedSells();
    await rebalancePortfolioPositions(globalTop100, tokenPrice);

    // 7: Finish with contributor payouts.
    const payoutResult = await runContributorPayoutCycle();

    console.log(
      `Marketplace pipeline run complete (${scored.length} properties scored, ` +
        `system_valuation=${systemValuation}, token_price=${tokenPrice}, ` +
        `payouts: ${payoutResult.paid} paid / ${payoutResult.failed} failed)`,
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
// Component-discovery only, by raw engagement (plays+likes) - this is NOT the marketplace ranking. Marketplace ranking is Property-based (see GET /api/categories/:slug/properties?top100=true and the ranking pipeline above). This endpoint just helps an admin find which lyrics/music/marketing/vocals components are worth attaching to a bundle via POST /api/properties/:propertyId/components.
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
// the property-marketplace buy endpoint with the resulting txid.
app.get("/api/marketplace/payment-address", (req, res) => {
  if (!MARKETPLACE_FLO_ADDRESS) {
    return res
      .status(503)
      .json({ success: false, error: "Payments not configured" });
  }
  res.json({ success: true, address: MARKETPLACE_FLO_ADDRESS });
});

// GET /api/main-token/payment-address
// Public - the buyer needs this to know where to send USDAI before calling
// the Main Token buy endpoint with the resulting txid.
app.get("/api/main-token/payment-address", (req, res) => {
  if (!USDAI_PAYMENT_ADDRESS) {
    return res
      .status(503)
      .json({ success: false, error: "Payments not configured" });
  }
  res.json({ success: true, address: USDAI_PAYMENT_ADDRESS });
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
// Creates the bundle.
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
      const pid = parsePositiveInt(propertyId);
      const cid = parsePositiveInt(componentId);
      if (!pid || !cid) {
        return res
          .status(400)
          .json({ success: false, error: "Invalid propertyId or componentId" });
      }
      const check = await pool.query(
        `SELECT p.category_id AS property_category_id, tc.category_id AS component_category_id
         FROM properties p, tracks_components tc
         WHERE p.id = $1 AND tc.id = $2`,
        [pid, cid],
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

// POST /api/properties/:propertyId/financing
// { financierFloId, amount, revenueSharePct, usdaiTxid, time, pubKey, sign }
// Real-money financing since the Main Token migration: the USDAI payment
// sent to the USDAI payment address is verified on-chain, the position is
// credited at its verified value (body.amount is treated as intent only),
// and the funds go into the ONE shared liquidity pool after the platform
// cut - same flow as Main Token buys.
app.post(
  "/api/properties/:propertyId/financing",
  verifyFloSignature(
    ["propertyId", "financierFloId", "amount", "revenueSharePct", "usdaiTxid", "time"],
    { floIdField: "financierFloId" },
  ),
  async (req, res) => {
    const { propertyId } = req.params;
    const { financierFloId, revenueSharePct } = req.body;
    const usdaiTxid = req.body.usdaiTxid;

    if (!financierFloId) {
      return res
        .status(400)
        .json({ success: false, error: "Missing financierFloId" });
    }
    if (!usdaiTxid) {
      return res.status(400).json({
        success: false,
        error:
          "Missing usdaiTxid - send USDAI to the USDAI payment address first",
      });
    }

    let usdaiValue;
    try {
      const payment = await verifyUsdaiPayment(
        usdaiTxid,
        MIN_INVESTMENT_USDAI,
        financierFloId,
      );
      usdaiValue = payment.tokenAmount;
    } catch (err) {
      console.error("Financing payment verification failed:", err);
      return res.status(402).json({
        success: false,
        error: `Payment verification failed: ${err.message || err}`,
      });
    }
    const { expense, netAfterExpense } = splitInvestment(usdaiValue);

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

      const client = await pool.connect();
      let position;
      try {
        await client.query("BEGIN");
        try {
          const inserted = await client.query(
            `INSERT INTO financing_positions
              (property_id, financier_flo_id, stage, amount, revenue_share_pct, usdai_txid)
             VALUES ($1, $2, 'property_backing', $3, $4, $5)
             RETURNING *`,
            [propertyId, financierFloId, usdaiValue, revenueSharePct || null, usdaiTxid],
          );
          position = inserted.rows[0];
        } catch (err) {
          if (err.code === "23505") {
            await client.query("ROLLBACK");
            return res.status(409).json({
              success: false,
              error: "This payment has already been credited to a financing position",
            });
          }
          throw err;
        }
        await depositToPlatformLiquidity(netAfterExpense, expense, client);
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      } finally {
        client.release();
      }

      res.json({
        success: true,
        position,
        usdaiValue,
        platformExpense: expense,
        liquidityPoolDeposit: netAfterExpense,
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

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

const MIN_MAIN_TOKEN_BUY_USDAI = 0.01; // dust floor, just enough to make sure a real payment happened
const MIN_INVESTMENT_USDAI = 0.01; // same dust floor for financing positions (tasks, tracks, properties)

// GET /api/main-token/price - current token price plus the numbers behind it
app.get("/api/main-token/price", async (req, res) => {
  try {
    const systemValuation = await computeSystemValuation();
    const totalSupply = await getTotalMainTokenSupply();
    const tokenPrice = computeMainTokenPrice(systemValuation, totalSupply);
    res.json({ success: true, tokenPrice, systemValuation, totalSupply });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// GET /api/main-token/portfolio/:floId - a holder's current Top-100 exposure
app.get("/api/main-token/portfolio/:floId", async (req, res) => {
  const { floId } = req.params;
  try {
    const balanceResult = await pool.query(
      "SELECT balance FROM main_token_balances WHERE flo_id = $1",
      [floId],
    );
    const positions = await pool.query(
      `
      SELECT pp.property_id, pp.token_amount, pp.allocation_pct, pp.value,
             p.name AS property_name
      FROM portfolio_positions pp
      JOIN properties p ON p.id = pp.property_id
      WHERE pp.flo_id = $1
      ORDER BY pp.value DESC
      `,
      [floId],
    );
    res.json({
      success: true,
      balance: Number(balanceResult.rows[0]?.balance || 0),
      positions: positions.rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// POST /api/main-token/buy  { floId, usdaiTxid, time, pubKey, sign }
// USDAI buys mint Main Token and update the Top 100.
app.post(
  "/api/main-token/buy",
  rateLimit({ max: 10, windowMs: 60000 }),
  verifyFloSignature(["floId", "usdaiTxid", "time"]),
  async (req, res) => {
    const { floId, usdaiTxid } = req.body;

    if (!floId) {
      return res.status(400).json({ success: false, error: "Missing floId" });
    }
    if (!usdaiTxid) {
      return res.status(400).json({
        success: false,
        error:
          "Missing usdaiTxid - send USDAI to the USDAI payment address first",
      });
    }

    let usdaiValue;
    try {
      const payment = await verifyUsdaiPayment(
        usdaiTxid,
        MIN_MAIN_TOKEN_BUY_USDAI,
        floId,
      );
      usdaiValue = payment.tokenAmount; // 1 USDAI assumed == $1
    } catch (err) {
      console.error("Main Token buy payment verification failed:", err);
      return res.status(402).json({
        success: false,
        error: `Payment verification failed: ${err.message || err}`,
      });
    }

    try {
      const systemValuation = await computeSystemValuation();
      const totalSupply = await getTotalMainTokenSupply();
      const tokenPrice = computeMainTokenPrice(systemValuation, totalSupply);
      const tokenAmount = usdaiValue / tokenPrice;
      const { expense, netAfterExpense } = splitInvestment(usdaiValue);

      const top100 = await pool.query(
        "SELECT id, consumption FROM properties WHERE in_top_100 = true",
      );
      const allocation = computePortfolioAllocation(top100.rows);

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        await client.query(
          `
          INSERT INTO main_token_balances (flo_id, balance)
          VALUES ($1, $2)
          ON CONFLICT (flo_id) DO UPDATE
          SET balance = main_token_balances.balance + $2, updated_at = now()
          `,
          [floId, tokenAmount],
        );

        try {
          await client.query(
            `
            INSERT INTO main_token_transactions (flo_id, type, token_amount, price_at_time, payment_txid)
            VALUES ($1, 'buy', $2, $3, $4)
            `,
            [floId, tokenAmount, tokenPrice, usdaiTxid],
          );
        } catch (err) {
          if (err.code === "23505") {
            await client.query("ROLLBACK");
            return res.status(409).json({
              success: false,
              error: "This payment has already been redeemed for Main Token",
            });
          }
          throw err;
        }

        await depositToPlatformLiquidity(netAfterExpense, expense, client);

        for (const a of allocation) {
          const positionValue = usdaiValue * a.share;
          await client.query(
            `
            INSERT INTO portfolio_positions (flo_id, property_id, token_amount, allocation_pct, value)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (flo_id, property_id) DO UPDATE
            SET token_amount = portfolio_positions.token_amount + $3,
                allocation_pct = $4,
                value = portfolio_positions.value + $5,
                updated_at = now()
            `,
            [
              floId,
              a.property_id,
              tokenAmount * a.share,
              a.share,
              positionValue,
            ],
          );
        }

        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      } finally {
        client.release();
      }

      res.json({
        success: true,
        tokenAmount,
        tokenPrice,
        usdaiValue,
        platformExpense: expense,
        liquidityPoolDeposit: netAfterExpense,
        allocation,
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// POST /api/main-token/sell  { floId, tokenAmount, time, pubKey, sign }
//
// Just queues the sell request.
app.post(
  "/api/main-token/sell",
  rateLimit({ max: 10, windowMs: 60000 }),
  verifyFloSignature(["floId", "tokenAmount", "time"]),
  async (req, res) => {
    const { floId } = req.body;
    const tokenAmount = Number(req.body.tokenAmount);

    if (!floId) {
      return res.status(400).json({ success: false, error: "Missing floId" });
    }
    if (!tokenAmount || tokenAmount <= 0) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid tokenAmount" });
    }

    try {
      const balanceResult = await pool.query(
        "SELECT balance FROM main_token_balances WHERE flo_id = $1",
        [floId],
      );
      const balance = Number(balanceResult.rows[0]?.balance || 0);

      const outstandingResult = await pool.query(
        `
        SELECT COALESCE(SUM(token_amount - paid_amount), 0)::numeric AS outstanding
        FROM sell_queue
        WHERE flo_id = $1 AND status != 'fulfilled'
        `,
        [floId],
      );
      const alreadyQueued = Number(outstandingResult.rows[0]?.outstanding || 0);

      if (balance - alreadyQueued < tokenAmount) {
        return res.status(400).json({
          success: false,
          error: "Not enough unqueued Main Token balance to sell that amount",
        });
      }

      const inserted = await pool.query(
        `
        INSERT INTO sell_queue (flo_id, token_amount)
        VALUES ($1, $2)
        RETURNING *
        `,
        [floId, tokenAmount],
      );

      const pressureRatio = await computeMainTokenPressureRatio();
      res.json({
        success: true,
        queued: inserted.rows[0],
        currentPressureRatio: pressureRatio,
        currentReleaseFraction: computeSellReleaseFraction(pressureRatio),
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

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
// { financierFloId, amount, revenueSharePct, usdaiTxid, time, pubKey, sign }
// Real USDAI payment required - verified on-chain, credited at its verified
// value, deposited into the one shared liquidity pool after the platform cut.
app.post(
  "/api/financing/tasks/:taskId/back",
  verifyFloSignature(
    ["taskId", "financierFloId", "amount", "revenueSharePct", "usdaiTxid", "time"],
    { floIdField: "financierFloId" },
  ),
  async (req, res) => {
    const { taskId } = req.params;
    const { financierFloId, revenueSharePct } = req.body;
    const usdaiTxid = req.body.usdaiTxid;

    if (!financierFloId) {
      return res
        .status(400)
        .json({ success: false, error: "Missing financierFloId" });
    }
    if (!usdaiTxid) {
      return res.status(400).json({
        success: false,
        error:
          "Missing usdaiTxid - send USDAI to the USDAI payment address first",
      });
    }

    let usdaiValue;
    try {
      const payment = await verifyUsdaiPayment(
        usdaiTxid,
        MIN_INVESTMENT_USDAI,
        financierFloId,
      );
      usdaiValue = payment.tokenAmount;
    } catch (err) {
      console.error("Financing payment verification failed:", err);
      return res.status(402).json({
        success: false,
        error: `Payment verification failed: ${err.message || err}`,
      });
    }
    const { expense, netAfterExpense } = splitInvestment(usdaiValue);

    try {
      const task = await pool.query("SELECT id FROM tasks WHERE id = $1", [
        taskId,
      ]);
      if (!task.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Task not found" });
      }

      const client = await pool.connect();
      let position;
      try {
        await client.query("BEGIN");
        try {
          const inserted = await client.query(
            `INSERT INTO financing_positions
              (task_id, financier_flo_id, stage, amount, revenue_share_pct, usdai_txid)
             VALUES ($1, $2, 'pre_creation', $3, $4, $5)
             RETURNING *`,
            [taskId, financierFloId, usdaiValue, revenueSharePct || null, usdaiTxid],
          );
          position = inserted.rows[0];
        } catch (err) {
          if (err.code === "23505") {
            await client.query("ROLLBACK");
            return res.status(409).json({
              success: false,
              error: "This payment has already been credited to a financing position",
            });
          }
          throw err;
        }
        await depositToPlatformLiquidity(netAfterExpense, expense, client);
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      } finally {
        client.release();
      }

      res.json({
        success: true,
        position,
        usdaiValue,
        platformExpense: expense,
        liquidityPoolDeposit: netAfterExpense,
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// POST /api/financing/tracks/:trackId/invest
// { financierFloId, amount, revenueSharePct, usdaiTxid, time, pubKey, sign }
// Real USDAI payment required - verified on-chain, credited at its verified
// value, deposited into the one shared liquidity pool after the platform cut.
app.post(
  "/api/financing/tracks/:trackId/invest",
  verifyFloSignature(
    ["trackId", "financierFloId", "amount", "revenueSharePct", "usdaiTxid", "time"],
    { floIdField: "financierFloId" },
  ),
  async (req, res) => {
    const { trackId } = req.params;
    const { financierFloId, revenueSharePct } = req.body;
    const usdaiTxid = req.body.usdaiTxid;

    if (!financierFloId) {
      return res
        .status(400)
        .json({ success: false, error: "Missing financierFloId" });
    }
    if (!usdaiTxid) {
      return res.status(400).json({
        success: false,
        error:
          "Missing usdaiTxid - send USDAI to the USDAI payment address first",
      });
    }

    let usdaiValue;
    try {
      const payment = await verifyUsdaiPayment(
        usdaiTxid,
        MIN_INVESTMENT_USDAI,
        financierFloId,
      );
      usdaiValue = payment.tokenAmount;
    } catch (err) {
      console.error("Financing payment verification failed:", err);
      return res.status(402).json({
        success: false,
        error: `Payment verification failed: ${err.message || err}`,
      });
    }
    const { expense, netAfterExpense } = splitInvestment(usdaiValue);

    try {
      const client = await pool.connect();
      let position;
      try {
        await client.query("BEGIN");
        try {
          const inserted = await client.query(
            `INSERT INTO financing_positions
              (track_id, financier_flo_id, stage, amount, revenue_share_pct, usdai_txid)
             VALUES ($1, $2, 'post_production', $3, $4, $5)
             RETURNING *`,
            [trackId, financierFloId, usdaiValue, revenueSharePct || null, usdaiTxid],
          );
          position = inserted.rows[0];
        } catch (err) {
          if (err.code === "23505") {
            await client.query("ROLLBACK");
            return res.status(409).json({
              success: false,
              error: "This payment has already been credited to a financing position",
            });
          }
          throw err;
        }
        await depositToPlatformLiquidity(netAfterExpense, expense, client);
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      } finally {
        client.release();
      }

      res.json({
        success: true,
        position,
        usdaiValue,
        platformExpense: expense,
        liquidityPoolDeposit: netAfterExpense,
      });
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
// the same way other row-locked actions are, so two creators racing to claim the
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

// Manually trigger the marketplace pipeline. Admin-only.
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
