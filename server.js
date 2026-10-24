const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const { Pool } = require("pg");
require("dotenv").config();
const { verifyFloSignature, rateLimitAuth } = require("./flo-auth");
const {
  verifyFloPayment,
  sendFloPayment,
  sendUsdaiPayment,
  MARKETPLACE_FLO_ADDRESS,
} = require("./flo-chain");
const {
  DEFAULT_MARKETPLACE_CONFIG,
  CONFIG_KEYS,
  normalizeConfigValue,
  normalizeMarketplaceConfig,
  computeConsumption,
  computeBasePrice,
  computeConsumptionShares,
  buildFormulaDescription,
} = require("./marketplace-formulas");

// Fail fast if the DB isn't configured
if (!process.env.DATABASE_URL) {
  console.error("FATAL: DATABASE_URL is not set. Exiting.");
  process.exit(1);
}

const app = express();
const port = process.env.PORT || 3000;
const sunoCache = new Map();
const flowCache = new Map();
const top100Cache = new Map();
const playTracking = new Map();
const sunoResolveCache = new Map();
const CACHE_DURATION = 5 * 60 * 1000;
const MAX_CACHE_ENTRIES = 2000;

// ==================== SCRAPER HELPERS ====================

async function fetchSunoPlayCount(inputUrl) {
  try {
    const response = await fetch(inputUrl, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      },
      signal: AbortSignal.timeout(8000), // 8 second timeout
    });
    if (!response.ok) {
      throw new Error(`Failed to fetch from Suno. Status: ${response.status}`);
    }

    const html = await response.text();
    let playCount = null;

    const playCountMatch = html.match(/play_count\\?["']?\s*:\s*(\d+)/i);
    if (playCountMatch) {
      playCount = parseInt(playCountMatch[1], 10);
    } else {
      console.warn("Could not find play_count in Suno HTML");
    }

    return playCount;
  } catch (e) {
    console.error("Suno scrape error:", e);
    return null;
  }
}

async function fetchGoogleFlowPlayCount(inputUrl) {
  try {
    const response = await fetch(inputUrl, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      },
      signal: AbortSignal.timeout(8000),
    });

    if (!response.ok) {
      throw new Error(
        `Failed to fetch Google Flow page. Status: ${response.status}`,
      );
    }

    const html = await response.text();

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

// Pipeline lock
let pipelineRunning = false;
let pipelineLockTime = null;
let pipelineStartTime = null;

// Request idempotency - prevent replay attacks
const processedRequests = new Map();

function isRequestProcessed(signature, timestamp) {
  const key = `${signature}:${timestamp}`;
  if (processedRequests.has(key)) {
    return true;
  }
  const now = Date.now();
  for (const [k, ts] of processedRequests) {
    if (now - ts > 5 * 60 * 1000) {
      processedRequests.delete(k);
    }
  }
  processedRequests.set(key, now);
  return false;
}

function preventReplay(fields, floIdField = "floId") {
  return (req, res, next) => {
    const body = req.body || {};
    const floId = body[floIdField];
    const sign = body.sign;
    const time = body.time;

    if (!floId || !sign || !time) {
      return next();
    }

    if (isRequestProcessed(sign, Number(time))) {
      console.warn(
        `Replay attack detected: ${floId} at ${new Date(time).toISOString()}`,
      );
      return res.status(409).json({
        success: false,
        error: "This request has already been processed",
      });
    }

    next();
  };
}

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

  for (const [key, entry] of sunoResolveCache.entries()) {
    if (now - entry.timestamp > CACHE_DURATION) {
      sunoResolveCache.delete(key);
    }
  }

  for (const [key, entry] of top100Cache.entries()) {
    if (now - entry.timestamp > 60000) {
      top100Cache.delete(key);
    }
  }

  const cutoff = now - 3600000;
  for (const [key, data] of playTracking) {
    if (data.timestamp < cutoff) {
      playTracking.delete(key);
    }
  }

  for (const [key, ts] of processedRequests) {
    if (now - ts > 5 * 60 * 1000) {
      processedRequests.delete(key);
    }
  }
}, 60 * 1000);

function cacheSet(cache, key, value) {
  if (cache.size >= MAX_CACHE_ENTRIES) {
    const oldestKey = cache.keys().next().value;
    cache.delete(oldestKey);
  }
  cache.set(key, value);
}

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
  ssl: { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

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
    await ensureConfigSchema();
    await loadMarketplaceConfig();
    await backfillSellQueueExpiry();
    await ensureAuditLogSchema();
    await ensureLifecycleSchema();
    await ensureComponentSchema();
    dbReady = true;
  })
  .catch((err) => {
    console.error("Database connection failed:", err);
  });

// =====================================================================
// DATABASE MIGRATION HELPER - Fix existing duplicate payment_txid rows
// =====================================================================

async function fixDuplicatePaymentTxids() {
  console.log(
    "Checking for duplicate payment_txid rows in main_token_transactions...",
  );

  // Find duplicates
  const duplicates = await pool.query(`
    SELECT payment_txid, COUNT(*) as count, array_agg(id ORDER BY id ASC) as ids
    FROM main_token_transactions
    WHERE payment_txid IS NOT NULL
    GROUP BY payment_txid
    HAVING COUNT(*) > 1
  `);

  if (duplicates.rows.length === 0) {
    console.log("No duplicate payment_txid rows found.");
    return;
  }

  console.log(`Found ${duplicates.rows.length} duplicate payment_txid values.`);

  for (const row of duplicates.rows) {
    const ids = row.ids;
    // Keep the first one (oldest), mark others as duplicate and set payment_txid to NULL
    const keepId = ids[0];
    const duplicateIds = ids.slice(1);

    console.log(
      `  Payment txid ${row.payment_txid}: keeping id ${keepId}, removing ${duplicateIds.length} duplicates`,
    );

    // Set payment_txid to NULL for duplicates so they don't violate the unique constraint
    await pool.query(
      `UPDATE main_token_transactions 
       SET payment_txid = NULL 
       WHERE id = ANY($1)`,
      [duplicateIds],
    );

    // Log the duplicates for audit
    await pool.query(
      `INSERT INTO audit_log (event_type, details, created_at)
       VALUES ('duplicate_payment_txid_fixed', $1, NOW())`,
      [
        JSON.stringify({
          payment_txid: row.payment_txid,
          kept_id: keepId,
          duplicate_ids: duplicateIds,
        }),
      ],
    );
  }

  console.log("Duplicate payment_txid rows fixed.");
}

// =====================================================================
// ORPHAN PAYOUT RECONCILIATION
// =====================================================================

async function reconcileOrphanPayouts() {
  console.log("Checking for orphan payouts (sent but not recorded)...");

  // Check sell_queue for locked but unpaid rows (stuck after send)
  const stuckSells = await pool.query(`
    SELECT id, flo_id, token_amount, paid_amount, payout_txid, payout_locked_at
    FROM sell_queue
    WHERE payout_locked_at IS NOT NULL
      AND status != 'paid'
      AND paid_amount < token_amount
  `);

  if (stuckSells.rows.length > 0) {
    console.log(`Found ${stuckSells.rows.length} stuck sell payouts.`);
    for (const row of stuckSells.rows) {
      console.log(
        `  Sell row ${row.id}: locked at ${row.payout_locked_at}, txid ${row.payout_txid || "unknown"}`,
      );
      // Log for manual review
      await pool.query(
        `INSERT INTO audit_log (event_type, details, created_at)
         VALUES ('stuck_sell_payout_detected', $1, NOW())`,
        [
          JSON.stringify({
            queueRowId: row.id,
            floId: row.flo_id,
            amount: row.token_amount - row.paid_amount,
            payoutTxid: row.payout_txid,
            lockedAt: row.payout_locked_at,
          }),
        ],
      );
    }
  }

  // Check property_payouts for sending but not paid rows
  const stuckPropertyPayouts = await pool.query(`
    SELECT id, property_id, recipient_flo_id, amount, flo_txid
    FROM property_payouts
    WHERE status = 'sending'
      AND paid_at IS NULL
  `);

  if (stuckPropertyPayouts.rows.length > 0) {
    console.log(
      `Found ${stuckPropertyPayouts.rows.length} stuck property payouts.`,
    );
    for (const row of stuckPropertyPayouts.rows) {
      console.log(
        `  Property payout ${row.id}: sending, txid ${row.flo_txid || "unknown"}`,
      );
      await pool.query(
        `INSERT INTO audit_log (event_type, details, created_at)
         VALUES ('stuck_property_payout_detected', $1, NOW())`,
        [
          JSON.stringify({
            payoutId: row.id,
            propertyId: row.property_id,
            recipient: row.recipient_flo_id,
            amount: row.amount,
            txid: row.flo_txid,
          }),
        ],
      );
    }
  }

  return {
    stuckSells: stuckSells.rows.length,
    stuckPropertyPayouts: stuckPropertyPayouts.rows.length,
  };
}

// Admin endpoint to resolve orphan payouts
app.post(
  "/api/admin/resolve-orphan/:type/:id",
  secureEndpoint({
    fields: ["adminFloId", "resolution", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 5, windowMs: 60000 },
  }),
  async (req, res) => {
    const { type, id } = req.params;
    const { resolution } = req.body; // 'confirm_paid' or 'revert'

    if (!["confirm_paid", "revert"].includes(resolution)) {
      return res.status(400).json({
        success: false,
        error: "resolution must be 'confirm_paid' or 'revert'",
      });
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      if (type === "sell") {
        const row = await client.query(
          `SELECT * FROM sell_queue WHERE id = $1 AND payout_locked_at IS NOT NULL FOR UPDATE`,
          [id],
        );
        if (!row.rows.length) {
          await client.query("ROLLBACK");
          return res.status(404).json({
            success: false,
            error: "Sell queue row not found or not locked",
          });
        }

        if (resolution === "confirm_paid") {
          await client.query(
            `UPDATE sell_queue
            SET paid_amount = released_amount,
            status = CASE
            WHEN released_amount >= token_amount THEN 'paid'
            ELSE 'partially_released'
            END,
            payout_txid = $1
            WHERE id = $2`,
            [id],
          );
          await client.query(
            `INSERT INTO audit_log (event_type, details, created_at)
             VALUES ('orphan_sell_resolved_paid', $1, NOW())`,
            [JSON.stringify({ sellId: id, resolvedBy: req.verifiedFloId })],
          );
        } else {
          // Revert: unlock and refund liquidity
          const rowData = row.rows[0];
          const amountToRefund =
            Number(rowData.released_amount) - Number(rowData.paid_amount);
          const tokenPrice = await getCurrentTokenPrice();
          const usdaiAmount = amountToRefund * tokenPrice;

          await client.query(
            `UPDATE platform_liquidity SET balance = balance + $1, updated_at = now() WHERE id = 1`,
            [usdaiAmount],
          );
          await client.query(
            `UPDATE sell_queue SET payout_locked_at = NULL WHERE id = $1`,
            [id],
          );
          await client.query(
            `INSERT INTO audit_log (event_type, details, created_at)
             VALUES ('orphan_sell_resolved_reverted', $1, NOW())`,
            [
              JSON.stringify({
                sellId: id,
                resolvedBy: req.verifiedFloId,
                refundedAmount: usdaiAmount,
              }),
            ],
          );
        }
      } else if (type === "property_payout") {
        const row = await client.query(
          `SELECT * FROM property_payouts WHERE id = $1 AND status = 'sending' FOR UPDATE`,
          [id],
        );
        if (!row.rows.length) {
          await client.query("ROLLBACK");
          return res.status(404).json({
            success: false,
            error: "Property payout not found or not in sending state",
          });
        }

        if (resolution === "confirm_paid") {
          await client.query(
            `UPDATE property_payouts 
             SET status = 'paid', paid_at = now() 
             WHERE id = $1`,
            [id],
          );
          await client.query(
            `INSERT INTO audit_log (event_type, details, created_at)
             VALUES ('orphan_property_payout_resolved_paid', $1, NOW())`,
            [JSON.stringify({ payoutId: id, resolvedBy: req.verifiedFloId })],
          );
        } else {
          // Revert: refund liquidity
          const rowData = row.rows[0];
          const usdaiAmount = Number(rowData.amount);

          await client.query(
            `UPDATE platform_liquidity SET balance = balance + $1, updated_at = now() WHERE id = 1`,
            [usdaiAmount],
          );
          await client.query(
            `UPDATE property_payouts SET status = 'pending' WHERE id = $1`,
            [id],
          );
          await client.query(
            `INSERT INTO audit_log (event_type, details, created_at)
             VALUES ('orphan_property_payout_resolved_reverted', $1, NOW())`,
            [
              JSON.stringify({
                payoutId: id,
                resolvedBy: req.verifiedFloId,
                refundedAmount: usdaiAmount,
              }),
            ],
          );
        }
      } else {
        await client.query("ROLLBACK");
        return res.status(400).json({
          success: false,
          error: "type must be 'sell' or 'property_payout'",
        });
      }

      await client.query("COMMIT");
      res.json({ success: true, message: "Orphan resolved" });
    } catch (err) {
      await client.query("ROLLBACK");
      console.error("Failed to resolve orphan:", err);
      res.status(500).json({ success: false, error: err.message });
    } finally {
      client.release();
    }
  },
);

// Admin endpoint to list orphans
app.get(
  "/api/admin/orphans",
  secureEndpoint({
    fields: ["adminFloId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 5, windowMs: 60000 },
  }),
  async (req, res) => {
    try {
      const [stuckSells, stuckPropertyPayouts] = await Promise.all([
        pool.query(`
          SELECT id, flo_id, token_amount, paid_amount, released_amount, 
                 payout_txid, payout_locked_at, requested_at
          FROM sell_queue
          WHERE payout_locked_at IS NOT NULL
            AND status != 'paid'
            AND paid_amount < token_amount
        `),
        pool.query(`
          SELECT id, property_id, recipient_flo_id, amount, flo_txid
          FROM property_payouts
          WHERE status = 'sending'
            AND paid_at IS NULL
        `),
      ]);

      res.json({
        success: true,
        orphans: {
          sell_queue: stuckSells.rows,
          property_payouts: stuckPropertyPayouts.rows,
        },
      });
    } catch (err) {
      console.error("Failed to list orphans:", err);
      res.status(500).json({ success: false, error: err.message });
    }
  },
);

// =====================================================================
// SCHEMA SETUP
// =====================================================================

async function ensureMarketplaceSchema() {
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
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS name TEXT;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS created_by_flo_id TEXT;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS category_rank INT;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS in_top_100 BOOLEAN DEFAULT false;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS consumption NUMERIC DEFAULT 0;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS base_price NUMERIC DEFAULT 0;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS valuation_updated_at TIMESTAMPTZ;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS in_global_top_100 BOOLEAN DEFAULT false;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS admin_score NUMERIC DEFAULT 0;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS admin_score_reason TEXT;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS admin_score_updated_at TIMESTAMPTZ;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS is_bundle BOOLEAN DEFAULT false;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS bundle_key TEXT;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS bundled_into_property_id INT REFERENCES properties(id) ON DELETE SET NULL;`,
  );
  await pool.query(
    `ALTER TABLE properties ADD COLUMN IF NOT EXISTS bundle_member_count INT DEFAULT 0;`,
  );
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_properties_bundle_key
    ON properties (bundle_key) WHERE bundle_key IS NOT NULL;
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_bundle_members (
      bundle_property_id INT REFERENCES properties(id) ON DELETE CASCADE,
      member_property_id INT REFERENCES properties(id) ON DELETE CASCADE,
      added_at TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (bundle_property_id, member_property_id)
    );
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_property_bundle_members_member
    ON property_bundle_members (member_property_id);
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

  // Fix duplicate payment_txid rows before creating unique index
  await fixDuplicatePaymentTxids();

  // Create unique index with IF NOT EXISTS
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE indexname = 'main_token_transactions_payment_txid_unique'
      ) THEN
        CREATE UNIQUE INDEX main_token_transactions_payment_txid_unique
        ON main_token_transactions (payment_txid) WHERE payment_txid IS NOT NULL;
      END IF;
    END $$;
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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS platform_liquidity (
      id                SERIAL PRIMARY KEY,
      balance           NUMERIC DEFAULT 0,
      expenses_taken    NUMERIC DEFAULT 0,
      liquidity_target  NUMERIC,
      last_payout_created_at TIMESTAMPTZ,
      updated_at        TIMESTAMPTZ DEFAULT now()
    );
  `);

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
  await pool.query(
    `ALTER TABLE sell_queue ADD COLUMN IF NOT EXISTS paid_amount NUMERIC DEFAULT 0;`,
  );
  await pool.query(
    `ALTER TABLE sell_queue ADD COLUMN IF NOT EXISTS payout_txid TEXT;`,
  );
  await pool.query(
    `ALTER TABLE sell_queue ADD COLUMN IF NOT EXISTS payout_locked_at TIMESTAMPTZ;`,
  );
  await pool.query(
    `ALTER TABLE sell_queue ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;`,
  );
  await pool.query(
    `ALTER TABLE sell_queue ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ;`,
  );

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
  await pool.query(
    `ALTER TABLE property_payouts ADD COLUMN IF NOT EXISTS flo_txid TEXT;`,
  );
  await pool.query(
    `ALTER TABLE property_payouts ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ;`,
  );

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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_components (
      property_id     INT REFERENCES properties(id),
      component_id    INT REFERENCES tracks_components(id),
      added_by_flo_id TEXT,
      added_at        TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (property_id, component_id)
    );
  `);

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
  await pool.query(
    `ALTER TABLE property_people ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active';`,
  );
  await pool.query(
    `ALTER TABLE property_people ADD COLUMN IF NOT EXISTS part TEXT DEFAULT 'people';`,
  );
  await pool.query(
    `ALTER TABLE property_people ADD COLUMN IF NOT EXISTS joined_at TIMESTAMPTZ DEFAULT now();`,
  );
  await pool.query(
    `ALTER TABLE property_people ADD COLUMN IF NOT EXISTS removed_at TIMESTAMPTZ;`,
  );

  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_invitations (
      id                  SERIAL PRIMARY KEY,
      property_id         INT REFERENCES properties(id) ON DELETE CASCADE NOT NULL,
      inviter_flo_id      TEXT NOT NULL,
      invitee_flo_id      TEXT NOT NULL,
      part                TEXT NOT NULL DEFAULT 'people',
      role                TEXT NOT NULL DEFAULT 'contributor',
      token               TEXT UNIQUE NOT NULL,
      status              TEXT NOT NULL DEFAULT 'pending',
      read_at             TIMESTAMPTZ,
      expires_at          TIMESTAMPTZ NOT NULL,
      responded_at        TIMESTAMPTZ,
      created_at          TIMESTAMPTZ DEFAULT now(),
      updated_at          TIMESTAMPTZ DEFAULT now()
    );
  `);
  await pool.query(
    `ALTER TABLE property_invitations ADD COLUMN IF NOT EXISTS read_at TIMESTAMPTZ;`,
  );
  await pool.query(
    `ALTER TABLE property_people ADD COLUMN IF NOT EXISTS invitation_id INT REFERENCES property_invitations(id) ON DELETE SET NULL;`,
  );
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_property_invitations_property_status
    ON property_invitations (property_id, status);
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_property_invitations_invitee_status
    ON property_invitations (invitee_flo_id, status);
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_property_invitations_unread
    ON property_invitations (invitee_flo_id)
    WHERE status = 'pending' AND read_at IS NULL;
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_property_people_property_status
    ON property_people (property_id, status);
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
  await pool.query(
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS component_type TEXT;`,
  );
  await pool.query(
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;`,
  );
  await pool.query(
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;`,
  );
  await pool.query(
    `ALTER TABLE tasks ADD COLUMN IF NOT EXISTS delivery_link TEXT;`,
  );

  await pool.query(`
    CREATE TABLE IF NOT EXISTS task_submissions (
      id SERIAL PRIMARY KEY,
      task_id INT REFERENCES tasks(id) NOT NULL,
      submitter_flo_id TEXT NOT NULL,
      delivery_link TEXT NOT NULL,
      is_selected BOOLEAN DEFAULT false,
      created_at TIMESTAMPTZ DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS task_claims (
      id SERIAL PRIMARY KEY,
      task_id INT REFERENCES tasks(id) NOT NULL,
      claimer_flo_id TEXT NOT NULL,
      claimed_at TIMESTAMPTZ DEFAULT now(),
      UNIQUE(task_id, claimer_flo_id)
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

  // FINANCE COMPONENTS
  await pool.query(`
    CREATE TABLE IF NOT EXISTS finance_components (
      id                  SERIAL PRIMARY KEY,
      property_id         INT REFERENCES properties(id) NOT NULL,
      component_type      TEXT NOT NULL,
      title               TEXT NOT NULL,
      description         TEXT,
      amount              NUMERIC DEFAULT 0,
      currency            TEXT DEFAULT NULL,
      terms               TEXT,
      created_by_flo_id   TEXT NOT NULL,
      metadata            JSONB,
      created_at          TIMESTAMPTZ DEFAULT now(),
      updated_at          TIMESTAMPTZ DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS royalty_splits (
      id                  SERIAL PRIMARY KEY,
      finance_component_id INT REFERENCES finance_components(id) NOT NULL,
      recipient_flo_id    TEXT NOT NULL,
      share_percentage    NUMERIC NOT NULL CHECK (share_percentage >= 0 AND share_percentage <= 100),
      role                TEXT,
      created_at          TIMESTAMPTZ DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS property_finance_allocations (
      id                  SERIAL PRIMARY KEY,
      property_id         INT REFERENCES properties(id) NOT NULL,
      category            TEXT NOT NULL,
      amount              NUMERIC NOT NULL DEFAULT 0,
      allocation_pct      NUMERIC NOT NULL,
      description         TEXT,
      created_at          TIMESTAMPTZ DEFAULT now(),
      updated_at          TIMESTAMPTZ DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_property_finance_allocations_property_category 
    ON property_finance_allocations (property_id, category);
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_property_finance_allocations_property 
    ON property_finance_allocations (property_id);
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_property_finance_allocations_category 
    ON property_finance_allocations (category);
  `);

      await pool.query(
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS profile_image_url TEXT;`,
  );
  await pool.query(
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS x_url TEXT;`,
  );
  await pool.query(
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS linkedin_url TEXT;`,
  );
  await pool.query(
    `ALTER TABLE people ADD COLUMN IF NOT EXISTS website_url TEXT;`,
  );
  console.log("Marketplace v3 schema ready");
}

// =====================================================================
// MARKETPLACE CONFIG
// =====================================================================

async function ensureConfigSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS marketplace_config (
      key             TEXT PRIMARY KEY,
      value           JSONB NOT NULL,
      description     TEXT,
      updated_by_flo_id TEXT,
      updated_at      TIMESTAMPTZ DEFAULT now()
    );
  `);

  // Remove settings from the discarded unit-ratio mode.
  await pool.query(
    `DELETE FROM marketplace_config
     WHERE key IN ('views_per_unit', 'persons_per_unit', 'consumption_mode')`,
  );

  for (const key of CONFIG_KEYS) {
    const value = DEFAULT_MARKETPLACE_CONFIG[key];
    await pool.query(
      `INSERT INTO marketplace_config (key, value, description)
       VALUES ($1, $2::jsonb, $3)
       ON CONFLICT (key) DO NOTHING`,
      [key, JSON.stringify(value), CONFIG_DESCRIPTIONS[key] || null],
    );
  }

  console.log("Marketplace config schema ready");
}

async function loadMarketplaceConfig() {
  try {
    const result = await pool.query(
      `SELECT key, value FROM marketplace_config`,
    );
    const raw = {};
    for (const row of result.rows) {
      raw[row.key] = row.value;
    }
    marketplaceConfig = normalizeMarketplaceConfig(raw);
    console.log("Marketplace config loaded:", {
      people_weight: marketplaceConfig.people_weight,
      task_weight: marketplaceConfig.task_weight,
      top_n_properties: marketplaceConfig.top_n_properties,
      sell_queue_period_days: marketplaceConfig.sell_queue_period_days,
      weak_bundle: marketplaceConfig.weak_bundle_enabled
        ? `${marketplaceConfig.weak_bundle_size} properties`
        : "off",
    });
    return marketplaceConfig;
  } catch (err) {
    console.error("Failed to load marketplace config:", err);
    return marketplaceConfig;
  }
}

function invitationToken() {
  return crypto.randomBytes(INVITATION_TOKEN_BYTES).toString("hex");
}

function normalizeInvitationPart(part) {
  const value = String(part || "people").trim().toLowerCase();
  return INVITATION_PARTS.includes(value) ? value : null;
}

function defaultInvitationRole(part) {
  if (part === "finance") return "finance";
  if (part === "marketing") return "marketing";
  if (part === "creative") return "creative";
  return "contributor";
}

async function expireStaleInvitations(db = pool) {
  const result = await db.query(
    `UPDATE property_invitations
     SET status = 'expired',
         responded_at = now(),
         updated_at = now()
     WHERE status = 'pending' AND expires_at <= now()
     RETURNING id`,
  );
  return result.rowCount;
}

async function ensureAuditLogSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS play_audit_log (
      id SERIAL PRIMARY KEY,
      track_id TEXT NOT NULL,
      user_flo_id TEXT NOT NULL,
      ip_address TEXT,
      user_agent TEXT,
      created_at TIMESTAMPTZ DEFAULT now()
    );
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_play_audit_track_user 
    ON play_audit_log (track_id, user_flo_id, created_at);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id SERIAL PRIMARY KEY,
      event_type TEXT NOT NULL,
      flo_id TEXT,
      ip_address TEXT,
      user_agent TEXT,
      details JSONB,
      created_at TIMESTAMPTZ DEFAULT now()
    );
  `);
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_audit_log_flo_id ON audit_log (flo_id);`,
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_audit_log_event_type ON audit_log (event_type);`,
  );
  await pool.query(
    `CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON audit_log (created_at);`,
  );

  console.log("Audit log schema ready");
}

async function ensureLifecycleSchema() {
  await pool.query(`
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS lifecycle_state 
    TEXT DEFAULT 'incubating'
  `);
  await pool.query(`
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS incubation_started_at TIMESTAMPTZ
  `);
  await pool.query(`
    ALTER TABLE properties ADD COLUMN IF NOT EXISTS graduated_at TIMESTAMPTZ
  `);
  console.log("Lifecycle schema ready");
}

async function ensureComponentSchema() {
  await pool.query(`
    ALTER TABLE tracks_components 
    ADD COLUMN IF NOT EXISTS component_category TEXT
  `);
  console.log("Component schema ready");
}

// =====================================================================
// HELPER FUNCTIONS
// =====================================================================

async function logAuditEvent(eventType, details) {
  try {
    await pool.query(
      `INSERT INTO audit_log (
        event_type, flo_id, ip_address, user_agent, details, created_at
      ) VALUES ($1, $2, $3, $4, $5, NOW())`,
      [
        eventType,
        details.floId || null,
        details.ip || null,
        details.userAgent || null,
        JSON.stringify(details),
      ],
    );
  } catch (err) {
    console.error("Failed to log audit event:", err);
  }
}

// =====================================================================
// RATE LIMITING MIDDLEWARE
// =====================================================================

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

const ADMIN_FLO_IDS = (
  process.env.ADMIN_FLO_IDS || "FSLjdS5mtMzfZ3BRHMyqueshFSRxNkuJeN"
)
  .split(",")
  .map((id) => id.trim())
  .filter(Boolean);

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

// =====================================================================
// SECURE ENDPOINT WRAPPER
// =====================================================================

function secureEndpoint({
  fields,
  floIdField = "floId",
  requireAdmin: requireAdminAccess = false,
  adminFloIdField = "adminFloId",
  rateLimitOpts = { max: 20, windowMs: 60000 },
}) {
  const middlewares = [
    rateLimit(rateLimitOpts),
    rateLimitAuth,
    verifyFloSignature(fields, { floIdField }),
    preventReplay(fields, floIdField),
  ];

  if (requireAdminAccess) {
    middlewares.push(requireAdmin(adminFloIdField || floIdField));
  }

  return middlewares;
}

// =====================================================================
// HEALTH CHECK
// =====================================================================

app.get("/api/health", async (req, res) => {
  const checks = {
    database: false,
    pipeline: false,
    liquidity: false,
    top100: false,
  };

  try {
    await pool.query("SELECT 1");
    checks.database = true;
  } catch (err) {
    console.error("Health check DB failed:", err);
  }

  try {
    const top100 = await getGlobalTop100();
    checks.top100 = top100.length > 0;
  } catch (err) {
    console.error("Health check Top 100 failed:", err);
  }

  try {
    const liquidity = await isPlatformLiquidityBuilt();
    checks.liquidity = liquidity;
  } catch (err) {
    console.error("Health check liquidity failed:", err);
  }

  checks.pipeline = !pipelineRunning;

  const healthy = Object.values(checks).every((v) => v === true);
  const dbOk = checks.database;
  const overallHealthy = dbReady && dbOk && healthy;

  res.status(overallHealthy ? 200 : 503).json({
    success: overallHealthy,
    status: overallHealthy ? "ok" : "degraded",
    checks,
    db: dbOk ? "connected" : "unreachable",
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
  });
});

// =====================================================================
// API ENDPOINTS
// =====================================================================

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
    return res.json({
      success: true,
      playCount: cached.playCount,
      cached: true,
    });
  }

  const playCount = await fetchSunoPlayCount(targetUrl);
  if (playCount !== null) {
    cacheSet(sunoCache, targetUrl, { playCount, timestamp: Date.now() });
    return res.json({ success: true, playCount, cached: false });
  } else {
    res.status(500).json({
      success: false,
      error: "Failed to extract play count",
    });
  }
});

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

// POST /api/platform-plays
app.post(
  "/api/platform-plays",
  secureEndpoint({
    fields: ["trackId", "floId", "time"],
    rateLimitOpts: { max: 20, windowMs: 60000 },
  }),
  async (req, res) => {
    const { trackId } = req.body;

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
  },
);

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

app.post(
  "/api/likes",
  secureEndpoint({
    fields: ["trackId", "floId", "time"],
    rateLimitOpts: { max: 20, windowMs: 60000 },
  }),
  async (req, res) => {
    const { trackId, floId } = req.body;

    if (!trackId || !floId) {
      return res.status(400).json({
        success: false,
        error: "Missing trackId or floId",
      });
    }

    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      await client.query(
        `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
        [`${trackId}:${floId}`],
      );

      const existing = await client.query(
        `SELECT 1
         FROM likes
         WHERE track_id = $1 AND user_id = $2`,
        [trackId, floId],
      );

      let liked;

      if (existing.rowCount > 0) {
        await client.query(
          `DELETE FROM likes
           WHERE track_id = $1 AND user_id = $2`,
          [trackId, floId],
        );

        liked = false;
      } else {
        await client.query(
          `INSERT INTO likes (track_id, user_id)
           VALUES ($1, $2)`,
          [trackId, floId],
        );

        liked = true;
      }

      const countResult = await client.query(
        `SELECT COUNT(*)::int AS like_count
         FROM likes
         WHERE track_id = $1`,
        [trackId],
      );

      await client.query("COMMIT");

      return res.json({
        success: true,
        liked,
        likeCount: countResult.rows[0].like_count,
      });
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});

      console.error("Failed to toggle track like:", err);

      return res.status(500).json({
        success: false,
        error: "Database error",
      });
    } finally {
      client.release();
    }
  },
);

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
    return res.json({ success: true, totalPlays: 0, totalLikes: 0 });
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

app.get("/api/google-flow-plays", async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) {
    return res.status(400).json({
      success: false,
      error: "Missing url parameter",
    });
  }

  if (!targetUrl.includes("flowmusic.app")) {
    return res.status(400).json({
      success: false,
      error: "Invalid Google Flow URL",
    });
  }

  const cached = flowCache.get(targetUrl);
  if (cached && Date.now() - cached.timestamp < CACHE_DURATION) {
    return res.json({
      success: true,
      playCount: cached.playCount,
      cached: true,
    });
  }

  const playCount = await fetchGoogleFlowPlayCount(targetUrl);
  if (playCount !== null) {
    cacheSet(flowCache, targetUrl, { playCount, timestamp: Date.now() });
    return res.json({ success: true, playCount, cached: false });
  }

  return res.status(500).json({
    success: false,
    error: "Failed to extract Google Flow play count",
  });
});

// =====================================================================
// MARKETPLACE - Core Constants and Helper Functions
// =====================================================================

const PROPERTY_COMPONENT_TYPES = [
  "lyrics",
  "music",
  "vocals",
  "marketing",
  "finance",
];
const ALL_COMPONENT_TYPES = [...PROPERTY_COMPONENT_TYPES];

const INVITATION_PARTS = ["people", "creative", "finance", "marketing"];
const DEFAULT_INVITATION_EXPIRY_DAYS = 7;
const MAX_INVITATION_EXPIRY_DAYS = 90;
const INVITATION_TOKEN_BYTES = 24;

const COMPONENT_CATEGORIES = {
  CREATIVE: ["lyrics", "music", "vocals", "production"],
  BUSINESS: ["marketing", "distribution", "legal", "finance"],
  PEOPLE: ["artist", "producer", "engineer", "manager"],
};

const PRICE_ALPHA = 0.15;
const PRICE_BETA = 0.15;

let marketplaceConfig = normalizeMarketplaceConfig(DEFAULT_MARKETPLACE_CONFIG);

function getTopN() {
  const n = parseInt(marketplaceConfig.top_n_properties, 10);
  return Number.isInteger(n) && n > 0 ? n : 100;
}

function getSellQueuePeriodDays() {
  const days = Number(marketplaceConfig.sell_queue_period_days);
  return Number.isFinite(days) && days > 0 ? days : 7;
}

const CONFIG_DESCRIPTIONS = {
  sell_queue_period_days: "Days before an unpaid sell-queue request expires",
  top_n_properties: "How many properties are included in the global Top N",
  weak_bundle_enabled: "Bundle the weakest Top-N properties into one property",
  weak_bundle_size: "How many weakest properties are bundled together",
  weak_bundle_name: "Display name for the weakest-properties bundle",
  people_weight: "Points added per person in the consumption formula",
  task_weight: "Points added per task",
  base_unit_value: "USDAI value of one consumption unit",
  portfolio_floor_share: "Minimum portfolio allocation share (0 to disable)",
};

const PLATFORM_EXPENSE_PCT = 0.05;
const PLATFORM_LIQUIDITY_TARGET = 10000;
const SELL_PRESSURE_FLOOR = 1.0;
const SELL_PRESSURE_CEILING = 3.0;

const CONTRIBUTOR_RELEASE_PCT = 0.1;
const PER_TRACK_MIN_PAYOUT = 5;
const CONSUMPTION_GROWTH_BURN_THRESHOLD = 0.02;

const TOKEN_API_BASE_URL =
  process.env.TOKEN_API_BASE_URL ||
  "https://ranchimallflo.ranchimall.net/api/v2";
const USDAI_TOKEN_IDENTIFIER = process.env.USDAI_TOKEN_IDENTIFIER || "usdai";
const USDAI_PAYMENT_ADDRESS = process.env.USDAI_PAYMENT_ADDRESS;
const SENDER_FIELD_CANDIDATES = ["senderAddress"];
const DEST_FIELD_CANDIDATES = ["receiverAddress"];

const VALUATION_FORMULA_VERSION = "v3-weighted-consumption";
const MIN_MAIN_TOKEN_BUY_USDAI = 0.001;

// =====================================================================
// USDAI Payment Verification
// =====================================================================

async function verifyUsdaiPayment(txid, requiredAmount, expectedSender) {
  if (!txid || typeof txid !== "string") {
    throw new Error("Missing USDAI transaction ID");
  }

  let tx;
  let response;
  for (let attempt = 0; attempt < 3; attempt++) {
    response = await fetch(`${TOKEN_API_BASE_URL}/transactionDetails/${txid}`, {
      signal: AbortSignal.timeout(8000),
    });
    if (response.ok || response.status !== 404) break;
    await new Promise((r) => setTimeout(r, 2000));
  }

  if (response.ok) {
    const data = await response.json();
    tx = data;
  } else if (response.status === 404) {
    console.warn(
      `verifyUsdaiPayment: transactionDetails 404 for ${txid}, falling back to floAddressTransactions`,
    );
    const addrsToTry = [expectedSender, USDAI_PAYMENT_ADDRESS].filter(Boolean);
    let found = null;

    for (const addr of addrsToTry) {
      for (const tokenQs of [
        `?token=${encodeURIComponent(USDAI_TOKEN_IDENTIFIER)}`,
        `?token=${encodeURIComponent(USDAI_TOKEN_IDENTIFIER.toLowerCase())}`,
        "",
      ]) {
        try {
          const url = `${TOKEN_API_BASE_URL}/floAddressTransactions/${addr}${tokenQs}`;
          const r2 = await fetch(url, { signal: AbortSignal.timeout(8000) });
          if (!r2.ok) continue;
          const j2 = await r2.json();
          const list = Array.isArray(j2)
            ? j2
            : j2.transactions || j2.txs || j2.data || j2.result || [];

          const normSearch = String(txid).toLowerCase().trim();
          const getTxid = (x) =>
            String(
              x.txid ||
                x.hash ||
                x.txID ||
                x.transactionTrigger ||
                x.transactionId ||
                "",
            )
              .toLowerCase()
              .trim();

          const match = list.find((x) => getTxid(x) === normSearch);
          if (match) {
            found = match;
            break;
          }
        } catch (e) {
          console.warn(`fallback error ${addr}${tokenQs}: ${e.message}`);
        }
      }
      if (found) break;
    }

    // No Blockbook fallback - floData memo is not proof of a real token transfer
    if (!found) {
      throw new Error(`Token API returned 404 for txid ${txid}`);
    }
    tx = found;
  } else {
    throw new Error(`Token API returned ${response.status} for txid ${txid}`);
  }

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
        `[${SENDER_FIELD_CANDIDATES.join(", ")}]`,
    );
  }
  if (senderField !== expectedSender) {
    throw new Error(
      `Transaction ${txid} was sent by ${senderField}, not ${expectedSender}`,
    );
  }

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
        `[${DEST_FIELD_CANDIDATES.join(", ")}]`,
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

// =====================================================================
// SCARCITY / UTILITY / PRICE
// =====================================================================

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
  return Math.min(Math.max(raw, 0.1), 10);
}

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

function computePrice(basePrice, scarcityScore, utilityScore) {
  return (
    basePrice *
    (1 + PRICE_ALPHA * scarcityScore) *
    (1 + PRICE_BETA * utilityScore)
  );
}

async function computePropertyConsumption(propertyId) {
  const propertyMeta = await pool.query(
    `SELECT is_bundle FROM properties WHERE id = $1`,
    [propertyId],
  );
  if (propertyMeta.rows[0]?.is_bundle) {
    return computeBundleConsumption(propertyId);
  }

  const trackResult = await pool.query(
    `WITH members AS (
    SELECT DISTINCT tc.track_id
    FROM property_components pc
    JOIN tracks_components tc ON tc.id = pc.component_id
    WHERE pc.property_id = $1
  )
  SELECT 
    COALESCE((SELECT SUM(play_count) FROM plays WHERE track_id IN (SELECT track_id FROM members)), 0)::numeric AS total_plays,
    COALESCE((SELECT COUNT(*) FROM likes WHERE track_id IN (SELECT track_id FROM members)), 0)::numeric AS total_likes
  `,
    [propertyId],
  );

  const totalPlays = Number(trackResult.rows[0]?.total_plays || 0);
  const totalLikes = Number(trackResult.rows[0]?.total_likes || 0);

  // Count distinct people: a person with two roles still counts once.
  const peopleResult = await pool.query(
    `SELECT COUNT(DISTINCT person_flo_id)::int AS people_count
     FROM property_people
     WHERE property_id = $1 AND COALESCE(status, 'active') = 'active'`,
    [propertyId],
  );

  const tasksResult = await pool.query(
    `SELECT COUNT(*)::int AS task_count FROM property_tasks WHERE property_id = $1`,
    [propertyId],
  );

  // Admin score: additive bonus (positive) or penalty (negative)
  const adminScoreResult = await pool.query(
    `SELECT COALESCE(admin_score, 0)::numeric AS admin_score FROM properties WHERE id = $1`,
    [propertyId],
  );

  return computeConsumption(
    {
      plays: totalPlays,
      likes: totalLikes,
      people: Number(peopleResult.rows[0]?.people_count || 0),
      tasks: Number(tasksResult.rows[0]?.task_count || 0),
      adminScore: Number(adminScoreResult.rows[0]?.admin_score || 0),
    },
    marketplaceConfig,
  );
}

async function computeBundleConsumption(bundlePropertyId) {
  const result = await pool.query(
    `SELECT COALESCE(SUM(p.consumption), 0)::numeric AS total
     FROM property_bundle_members bm
     JOIN properties p ON p.id = bm.member_property_id
     WHERE bm.bundle_property_id = $1`,
    [bundlePropertyId],
  );
  return Number(result.rows[0]?.total || 0);
}

function calculateBasePrice(consumption) {
  return computeBasePrice(consumption, marketplaceConfig);
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

// =====================================================================
// MAIN TOKEN PRICE
// =====================================================================

async function computeSystemValuation() {
  const result = await pool.query(
    `SELECT COALESCE(SUM(base_price), 0)::numeric AS total 
     FROM properties 
     WHERE status = 'active' AND in_global_top_100 = true`,
  );
  return Number(result.rows[0]?.total || 0);
}

async function getTotalMainTokenSupply() {
  const result = await pool.query(
    `SELECT COALESCE(SUM(balance), 0)::numeric AS total FROM main_token_balances`,
  );
  return Number(result.rows[0]?.total || 0);
}

function computeMainTokenPrice(systemValuation, totalSupply) {
  if (totalSupply <= 0) return systemValuation > 0 ? systemValuation : 1;
  if (systemValuation <= 0) return 1;
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

async function getCurrentTokenPrice() {
  const systemValuation = await computeSystemValuation();
  const totalSupply = await getTotalMainTokenSupply();
  return computeMainTokenPrice(systemValuation, totalSupply);
}

function isConsumptionGrowthFlat(previousValuation, currentValuation) {
  if (!previousValuation) return false;
  const growthRate = (currentValuation - previousValuation) / previousValuation;
  return growthRate < CONSUMPTION_GROWTH_BURN_THRESHOLD;
}

// =====================================================================
// Authoritative Global Top 100
// =====================================================================

async function getGlobalTop100() {
  const result = await pool.query(
    `
    SELECT id, name, consumption, base_price, current_price,
           category_id, category_rank, in_top_100, in_global_top_100,
           is_bundle, bundle_member_count
    FROM properties
    WHERE status = 'active' AND in_global_top_100 = true
    ORDER BY 
      CASE 
        WHEN category_rank IS NOT NULL THEN category_rank 
        ELSE 999 
      END ASC,
      current_price DESC
    LIMIT $1
  `,
    [getTopN()],
  );
  return result.rows;
}

async function getTop100Properties() {
  const cacheKey = "top100_cache";
  const cached = top100Cache.get(cacheKey);
  if (cached && Date.now() - cached.timestamp < 60000) {
    return cached.data;
  }

  const data = await getGlobalTop100();
  top100Cache.set(cacheKey, { data, timestamp: Date.now() });
  return data;
}

// =====================================================================
// PLATFORM LIQUIDITY
// =====================================================================

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

// =====================================================================
// SELL-GATING
// =====================================================================

async function computeMainTokenPressureRatio() {
  const windowDays = getSellQueuePeriodDays();
  const buyResult = await pool.query(
    `
    SELECT COALESCE(SUM(token_amount), 0)::numeric AS total
    FROM main_token_transactions
    WHERE type = 'buy' AND created_at > now() - ($1 * INTERVAL '1 day')
    `,
    [windowDays],
  );
  const sellResult = await pool.query(
    `
    SELECT COALESCE(SUM(token_amount - paid_amount), 0)::numeric AS total
    FROM sell_queue
    WHERE requested_at > now() - ($1 * INTERVAL '1 day')
      AND status <> 'expired'
    `,
    [windowDays],
  );
  const buyVolume = Number(buyResult.rows[0]?.total || 0);
  const sellVolume = Number(sellResult.rows[0]?.total || 0);
  return buyVolume / Math.max(sellVolume, 1);
}

function computeSellReleaseFraction(pressureRatio) {
  const span = SELL_PRESSURE_CEILING - SELL_PRESSURE_FLOOR;
  const raw = (pressureRatio - SELL_PRESSURE_FLOOR) / span;
  return Math.min(Math.max(raw, 0), 1);
}

async function backfillSellQueueExpiry() {
  const days = getSellQueuePeriodDays();
  await pool.query(
    `UPDATE sell_queue
     SET expires_at = requested_at + ($1 * INTERVAL '1 day')
     WHERE expires_at IS NULL
       AND status IN ('queued', 'partially_released')`,
    [days],
  );
}

/**
 * Expire sell requests that have been waiting longer than
 * sell_queue_period_days (default 7).  Only unpaid, unlocked rows are touched; any amount already released but not paid is clawed back so the seller keeps their unqueued tokens.
 */
async function clearExpiredSellQueue() {
  const result = await pool.query(
    `UPDATE sell_queue
     SET status = 'expired',
         expired_at = now(),
         released_amount = LEAST(released_amount, paid_amount)
     WHERE status IN ('queued', 'partially_released')
       AND expires_at IS NOT NULL
       AND expires_at <= now()
       AND payout_locked_at IS NULL
       AND paid_amount < token_amount
     RETURNING id, flo_id, token_amount, paid_amount, released_amount`,
  );

  if (result.rowCount > 0) {
    await logAuditEvent("sell_queue_expired", {
      count: result.rowCount,
      rows: result.rows.slice(0, 100).map((r) => ({
        id: r.id,
        floId: r.flo_id,
        tokenAmount: Number(r.token_amount),
        paidAmount: Number(r.paid_amount),
      })),
    });
  }

  return result.rowCount;
}

async function releaseSellQueue() {
  const expired = await clearExpiredSellQueue();

  const liquidityBuilt = await isPlatformLiquidityBuilt();
  if (!liquidityBuilt) {
    return { releaseFraction: 0, updated: 0, expired };
  }

  const poolResult = await pool.query(
    `SELECT balance FROM platform_liquidity WHERE id = 1`,
  );
  const poolBalance = Number(poolResult.rows[0]?.balance || 0);
  const safeBudget = poolBalance * 0.8;

  const tokenPrice = await getCurrentTokenPrice();

  const queuedResult = await pool.query(
    `SELECT COALESCE(SUM((token_amount - paid_amount) * $1), 0)::numeric AS total_value
     FROM sell_queue
     WHERE status IN ('queued', 'partially_released')`,
    [tokenPrice],
  );
  const totalQueuedValue = Number(queuedResult.rows[0]?.total_value || 0);

  if (totalQueuedValue <= 0) return { releaseFraction: 0, updated: 0 };

  const pressureRatio = await computeMainTokenPressureRatio();
  const pressureRelease = computeSellReleaseFraction(pressureRatio);
  const budgetRelease = safeBudget / totalQueuedValue;
  const releaseFraction = Math.min(pressureRelease, budgetRelease);

  const queued = await pool.query(
    `SELECT id, token_amount, paid_amount, released_amount FROM sell_queue
     WHERE status IN ('queued', 'partially_released')
     ORDER BY requested_at ASC`,
  );

  let updated = 0;
  for (const row of queued.rows) {
    const currentReleased = Number(row.released_amount || 0);
    const remainingUnreleased = Number(row.token_amount) - currentReleased;
    const additionalRelease = remainingUnreleased * releaseFraction;
    const targetReleased = currentReleased + additionalRelease;
    if (targetReleased <= currentReleased) continue;

    const fulfilled = targetReleased >= Number(row.token_amount);
    await pool.query(
      `UPDATE sell_queue
       SET released_amount = $1,
           status = $2
       WHERE id = $3`,
      [targetReleased, fulfilled ? "fulfilled" : "partially_released", row.id],
    );
    updated += 1;
  }

  return {
    releaseFraction,
    updated,
    totalQueuedValue,
    safeBudget,
    tokenPrice,
    expired,
  };
}

// =====================================================================
// Portfolio allocation with configurable floor
// =====================================================================

function computePortfolioAllocation(top100Properties) {
  const totalConsumption = top100Properties.reduce(
    (sum, p) => sum + Number(p.consumption || 0),
    0,
  );
  if (totalConsumption <= 0 || top100Properties.length === 0) return [];

  const floorShare = Number(marketplaceConfig.portfolio_floor_share) || 0;
  const rawShares = top100Properties.map((p) => {
    const consumptionShare = Number(p.consumption || 0) / totalConsumption;
    const share =
      floorShare > 0 ? Math.max(consumptionShare, floorShare) : consumptionShare;
    return {
      property_id: p.id,
      share: share,
      consumptionShare: consumptionShare,
    };
  });

  const total = rawShares.reduce((sum, s) => sum + s.share, 0);
  return rawShares.map((s) => ({
    property_id: s.property_id,
    share: s.share / total,
    consumptionShare: s.consumptionShare,
  }));
}

// =====================================================================
// CONTRIBUTOR PAYOUTS
// =====================================================================

async function resolvePayoutTargets(dbClient, propertyId) {
  const property = await dbClient.query(
    `SELECT is_bundle FROM properties WHERE id = $1`,
    [propertyId],
  );
  if (!property.rows.length) return [];
  if (!property.rows[0].is_bundle) {
    return [{ propertyId, weight: 1 }];
  }

  const members = await dbClient.query(
    `SELECT p.id, p.consumption
     FROM property_bundle_members bm
     JOIN properties p ON p.id = bm.member_property_id
     WHERE bm.bundle_property_id = $1`,
    [propertyId],
  );
  if (!members.rows.length) return [];

  const total = members.rows.reduce(
    (sum, m) => sum + Number(m.consumption || 0),
    0,
  );
  if (total <= 0) {
    return members.rows.map((m) => ({
      propertyId: m.id,
      weight: 1 / members.rows.length,
    }));
  }
  return members.rows.map((m) => ({
    propertyId: m.id,
    weight: Number(m.consumption || 0) / total,
  }));
}

async function releaseContributorPayouts() {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Lock the platform_liquidity row so only one instance can proceed
    const row = await client.query(
      `SELECT balance, last_payout_created_at
       FROM platform_liquidity
       WHERE id = 1
       FOR UPDATE`,
    );

    if (!row.rows.length) {
      await client.query("ROLLBACK");
      return { released: false, reason: "platform_liquidity row not found" };
    }

    const poolBalance = Number(row.rows[0].balance || 0);
    const lastPayout = row.rows[0].last_payout_created_at;

    // Check liquidity is built
    const liquidityTarget = Number(PLATFORM_LIQUIDITY_TARGET);
    if (poolBalance < liquidityTarget) {
      await client.query("ROLLBACK");
      return {
        released: false,
        reason: `still building liquidity (${poolBalance} < ${liquidityTarget})`,
      };
    }

    // Check cooldown (24 hours)
    if (lastPayout) {
      const hoursSinceLastPayout =
        (Date.now() - new Date(lastPayout).getTime()) / (1000 * 60 * 60);
      if (hoursSinceLastPayout < 24) {
        await client.query("ROLLBACK");
        return {
          released: false,
          reason: `payouts already created ${Math.floor(hoursSinceLastPayout)} hours ago (cooldown: 24h)`,
        };
      }
    }

    // Calculate release amount
    const releaseAmount = poolBalance * CONTRIBUTOR_RELEASE_PCT;
    if (releaseAmount <= 0) {
      await client.query("ROLLBACK");
      return { released: false, reason: "nothing to release" };
    }

    //Get Top 100 and allocate
    const top100 = await getGlobalTop100();
    const allocation = computePortfolioAllocation(top100);

    let totalCreated = 0;
    for (const a of allocation) {
      const propertyReleaseAmount = releaseAmount * a.share;
      const targets = await resolvePayoutTargets(client, a.property_id);

      for (const target of targets) {
        const targetReleaseAmount = propertyReleaseAmount * target.weight;

        const componentsResult = await client.query(
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
          [target.propertyId],
        );

        const totalComponentConsumption = componentsResult.rows.reduce(
          (sum, r) => sum + Number(r.consumption),
          0,
        );
        if (totalComponentConsumption <= 0) continue;

        for (const row of componentsResult.rows) {
          const share = Number(row.consumption) / totalComponentConsumption;
          const amount = targetReleaseAmount * share;
          if (amount < PER_TRACK_MIN_PAYOUT) continue;

          await client.query(
            `
            INSERT INTO property_payouts (property_id, component_id, recipient_flo_id, amount, status)
            VALUES ($1, $2, $3, $4, 'pending')
            `,
            [target.propertyId, row.component_id, row.contributor_flo_id, amount],
          );
          totalCreated += amount;
        }
      }
    }

    // Record the payout creation timestamp inside the same transaction
    await client.query(
      `UPDATE platform_liquidity SET last_payout_created_at = now() WHERE id = 1`,
    );

    await client.query("COMMIT");

    console.log(
      `Contributor payouts created: ${totalCreated} USDAI across ${allocation.length} properties`,
    );

    return { released: true, created: totalCreated };
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("releaseContributorPayouts error:", err);
    return { released: false, reason: err.message };
  } finally {
    client.release();
  }
}
// =====================================================================
// Property payouts - reserve FIRST, then send
// =====================================================================

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
        await client.query("ROLLBACK");
        continue;
      }
      payout = claimed.rows[0];
      const usdaiAmount = Number(payout.amount);

      const reserved = await reservePlatformLiquidity(client, usdaiAmount);
      if (!reserved) {
        await client.query("ROLLBACK");
        console.error(
          `Property payout ${id}: insufficient liquidity, leaving pending`,
        );
        failed += 1;
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

    const usdaiAmount = Number(payout.amount);

    let payoutTxid;
    try {
      payoutTxid = await sendUsdaiPayment(payout.recipient_flo_id, usdaiAmount);
    } catch (sendErr) {
      console.error(
        `Property payout ${id} send failed, refunding liquidity and reverting to pending:`,
        sendErr,
      );
      await pool.query(
        `UPDATE platform_liquidity SET balance = balance + $1, updated_at = now() WHERE id = 1`,
        [usdaiAmount],
      );
      await pool.query(
        `UPDATE property_payouts SET status = 'pending' WHERE id = $1`,
        [id],
      );
      failed += 1;
      continue;
    }

    try {
      await pool.query(
        `UPDATE property_payouts SET status = 'paid', flo_txid = $1, paid_at = now() WHERE id = $2`,
        [payoutTxid, id],
      );
      paid += 1;
    } catch (err) {
      console.error(
        `CRITICAL: Property payout ${payoutTxid} sent for ${usdaiAmount} USDAI ` +
          `but DB recording failed - needs manual reconciliation:`,
        err,
      );
      await pool.query(
        `INSERT INTO audit_log (event_type, details, created_at)
         VALUES ('property_payout_orphan', $1, NOW())`,
        [
          JSON.stringify({
            payoutId: id,
            recipient: payout.recipient_flo_id,
            amount: usdaiAmount,
            txid: payoutTxid,
            issue: "db_recording_failed",
            error: err.message,
          }),
        ],
      );
      failed += 1;
    }
  }

  return { paid, failed };
}

async function runContributorPayoutCycle() {
  await releaseContributorPayouts();
  return executePropertyPayouts();
}

// =====================================================================
// Sell execution - reserve FIRST, then send
// =====================================================================

async function executeReleasedSells() {
  const cycleTokenPrice = await getCurrentTokenPrice();

  const queued = await pool.query(
    `SELECT id, flo_id, token_amount, paid_amount, released_amount 
     FROM sell_queue
     WHERE status IN ('partially_released', 'fulfilled')
       AND paid_amount < released_amount
       AND payout_locked_at IS NULL`,
  );

  let executed = 0;
  for (const row of queued.rows) {
    const newlyReleased = Number(row.released_amount) - Number(row.paid_amount);
    if (newlyReleased <= 0) continue;

    const usdaiAmount = newlyReleased * cycleTokenPrice;

    const claimClient = await pool.connect();
    let claimed = false;
    try {
      await claimClient.query("BEGIN");
      const lockResult = await claimClient.query(
        `SELECT payout_locked_at FROM sell_queue WHERE id = $1 FOR UPDATE`,
        [row.id],
      );
      if (!lockResult.rows.length || lockResult.rows[0].payout_locked_at) {
        await claimClient.query("ROLLBACK");
        continue;
      }
      const reserved = await reservePlatformLiquidity(claimClient, usdaiAmount);
      if (!reserved) {
        await claimClient.query("ROLLBACK");
        console.error(
          `Sell payout for queue row ${row.id}: insufficient liquidity, leaving unlocked`,
        );
        continue;
      }
      await claimClient.query(
        `UPDATE sell_queue SET payout_locked_at = now() WHERE id = $1`,
        [row.id],
      );
      await claimClient.query("COMMIT");
      claimed = true;
    } catch (err) {
      await claimClient.query("ROLLBACK");
      console.error(
        `Failed to claim sell payout for queue row ${row.id}:`,
        err,
      );
    } finally {
      claimClient.release();
    }
    if (!claimed) continue;

    let payoutTxid;
    try {
      payoutTxid = await sendUsdaiPayment(row.flo_id, usdaiAmount);
    } catch (sendErr) {
      console.error(
        `Sell payout send failed for queue row ${row.id}, refunding liquidity and unlocking:`,
        sendErr,
      );
      await pool.query(
        `UPDATE platform_liquidity SET balance = balance + $1, updated_at = now() WHERE id = 1`,
        [usdaiAmount],
      );
      await pool.query(
        `UPDATE sell_queue SET payout_locked_at = NULL WHERE id = $1`,
        [row.id],
      );
      continue;
    }

    const finalClient = await pool.connect();
    try {
      await finalClient.query("BEGIN");

      await finalClient.query(
        `UPDATE sell_queue 
        SET paid_amount = released_amount,
        status = CASE
         WHEN released_amount >= token_amount THEN 'paid'
         ELSE 'partially_released'
        END,
       payout_txid = $1
      WHERE id = $2`,
        [payoutTxid, row.id],
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
        [row.flo_id, newlyReleased, cycleTokenPrice, payoutTxid],
      );

      await finalClient.query("COMMIT");
      executed += 1;
    } catch (err) {
      await finalClient.query("ROLLBACK");
      console.error(
        `CRITICAL: Sell payout ${payoutTxid} sent to ${row.flo_id} for ${usdaiAmount} USDAI ` +
          `but DB recording failed - needs manual reconciliation:`,
        err,
      );
      await pool.query(
        `INSERT INTO audit_log (event_type, details, created_at)
         VALUES ('sell_payout_orphan', $1, NOW())`,
        [
          JSON.stringify({
            queueRowId: row.id,
            floId: row.flo_id,
            amount: usdaiAmount,
            txid: payoutTxid,
            issue: "db_recording_failed",
            error: err.message,
          }),
        ],
      );
    } finally {
      finalClient.release();
    }
  }

  return { executed, cyclePrice: cycleTokenPrice };
}

// =====================================================================
// PORTFOLIO REBALANCING
// =====================================================================

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

// =====================================================================
// PIPELINE
// =====================================================================

async function updatePropertyLifecycles() {
  await pool.query(`
    UPDATE properties 
    SET lifecycle_state = 'graduated',
        graduated_at = now()
    WHERE lifecycle_state = 'eligible'
      AND status = 'active'
      AND NOT in_global_top_100
      AND bundled_into_property_id IS NULL
      AND consumption >= 1000
      AND created_at < now() - INTERVAL '14 days'
  `);
}

// =====================================================================
// WEAKEST-PROPERTIES BUNDLE
// =====================================================================

const WEAK_BUNDLE_KEY = "weakest";

async function getWeakestBundleProperty(db = pool) {
  const result = await db.query(
    `SELECT * FROM properties WHERE bundle_key = $1 LIMIT 1`,
    [WEAK_BUNDLE_KEY],
  );
  return result.rows[0] || null;
}

async function deactivateWeakestBundle(db = pool) {
  const bundle = await getWeakestBundleProperty(db);
  if (!bundle) return null;

  await db.query(
    `UPDATE properties SET bundled_into_property_id = NULL
     WHERE bundled_into_property_id = $1`,
    [bundle.id],
  );
  await db.query(
    `DELETE FROM property_bundle_members WHERE bundle_property_id = $1`,
    [bundle.id],
  );
  await db.query(
    `UPDATE properties
     SET status = 'archived',
         in_top_100 = false,
         in_global_top_100 = false,
         category_rank = NULL,
         bundle_member_count = 0
     WHERE id = $1`,
    [bundle.id],
  );
  return bundle.id;
}

/**
 * Replace the bottom `weak_bundle_size` entries of the global ranking with one synthetic property.  Its consumption/price are the sum of its members, so portfolio shares and system valuation stay unchanged, but it occupies a single Top-N slot and pays members' contributors proportionally.
 */
async function syncWeakestBundle(globalRanked) {
  const size = parseInt(marketplaceConfig.weak_bundle_size, 10);
  const enabled = marketplaceConfig.weak_bundle_enabled === true;
  const topN = getTopN();
  // Only collapse the tail when the Top N is actually full.  With 100 qualifying properties this produces 95 individual properties + one bundle of the weakest 5, exactly as discussed.
  const canBundle =
    enabled &&
    Number.isInteger(size) &&
    size >= 2 &&
    globalRanked.length >= topN &&
    globalRanked.length >= size + 1;

  if (!canBundle) {
    await deactivateWeakestBundle(pool);
    return { individuals: globalRanked, bundle: null };
  }

  const individualCount = globalRanked.length - size;
  const individuals = globalRanked.slice(0, individualCount);
  const members = globalRanked.slice(individualCount);

  const totalConsumption = members.reduce(
    (sum, m) => sum + Number(m.consumption || 0),
    0,
  );
  const weightSum = members.reduce(
    (sum, m) => sum + Math.max(Number(m.consumption || 0), 0),
    0,
  );
  const weightedAverage = (key) => {
    if (weightSum > 0) {
      return (
        members.reduce(
          (sum, m) =>
            sum + Number(m[key] || 0) * Math.max(Number(m.consumption || 0), 0),
          0,
        ) / weightSum
      );
    }
    return (
      members.reduce((sum, m) => sum + Number(m[key] || 0), 0) /
      Math.max(members.length, 1)
    );
  };

  const bundleConsumption = totalConsumption;
  const bundleBasePrice = members.reduce(
    (sum, m) => sum + Number(m.base_price || 0),
    0,
  );
  const bundleCurrentPrice = members.reduce(
    (sum, m) => sum + Number(m.price || 0),
    0,
  );
  const bundleScarcity = weightedAverage("scarcity");
  const bundleUtility = weightedAverage("utility");
  const bundleRank = individualCount + 1;
  const memberIds = members.map((m) => m.id);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    let bundle = await getWeakestBundleProperty(client);

    if (bundle) {
      const updated = await client.query(
        `UPDATE properties
         SET name = $1,
             status = 'active',
             category_id = NULL,
             is_bundle = true,
             bundle_key = $2,
             bundle_member_count = $3,
             consumption = $4,
             base_price = $5,
             current_price = $6,
             scarcity_score = $7,
             utility_score = $8,
             category_rank = $9,
             in_top_100 = true,
             in_global_top_100 = true,
             total_slots = $3,
             lifecycle_state = 'active',
             valuation_updated_at = now(),
             updated_at = now()
         WHERE id = $10
         RETURNING *`,
        [
          marketplaceConfig.weak_bundle_name,
          WEAK_BUNDLE_KEY,
          members.length,
          bundleConsumption,
          bundleBasePrice,
          bundleCurrentPrice,
          bundleScarcity,
          bundleUtility,
          bundleRank,
          bundle.id,
        ],
      );
      bundle = updated.rows[0];
    } else {
      const inserted = await client.query(
        `INSERT INTO properties
           (name, status, category_id, is_bundle, bundle_key,
            bundle_member_count, consumption, base_price, current_price,
            scarcity_score, utility_score, category_rank, in_top_100,
            in_global_top_100, total_slots, lifecycle_state,
            valuation_updated_at)
         VALUES ($1, 'active', NULL, true, $2,
                 $3, $4, $5, $6,
                 $7, $8, $9, true,
                 true, $3, 'active',
                 now())
         RETURNING *`,
        [
          marketplaceConfig.weak_bundle_name,
          WEAK_BUNDLE_KEY,
          members.length,
          bundleConsumption,
          bundleBasePrice,
          bundleCurrentPrice,
          bundleScarcity,
          bundleUtility,
          bundleRank,
        ],
      );
      bundle = inserted.rows[0];
    }

    // Release properties that were bundled previously but are not members now.
    await client.query(
      `UPDATE properties
       SET bundled_into_property_id = NULL
       WHERE bundled_into_property_id = $1
         AND NOT (id = ANY($2::int[]))`,
      [bundle.id, memberIds],
    );

    // Members are represented by the bundle, not individually.
    await client.query(
      `UPDATE properties
       SET bundled_into_property_id = $1,
           in_top_100 = false,
           in_global_top_100 = false,
           category_rank = NULL
       WHERE id = ANY($2::int[])`,
      [bundle.id, memberIds],
    );

    await client.query(
      `DELETE FROM property_bundle_members WHERE bundle_property_id = $1`,
      [bundle.id],
    );
    await client.query(
      `INSERT INTO property_bundle_members (bundle_property_id, member_property_id)
       SELECT $1, unnest($2::int[])`,
      [bundle.id, memberIds],
    );

    await client.query("COMMIT");
    return { individuals, bundle };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

// =====================================================================
// FINANCE ALLOCATION
// =====================================================================

async function computeFinanceAllocation(propertyId) {
  const components = await pool.query(
    `SELECT * FROM finance_components WHERE property_id = $1`,
    [propertyId],
  );

  const propertyData = await pool.query(
    `SELECT consumption, current_price, scarcity_score, utility_score, 
            lifecycle_state, created_at, in_top_100, in_global_top_100
     FROM properties WHERE id = $1`,
    [propertyId],
  );
  const property = propertyData.rows[0];

  if (!property) {
    return {
      allocations: [],
      algorithmInputs: {},
      componentCount: 0,
      hasFinancePlanning: false,
      hasRoyaltySplits: false,
      hasBudget: false,
      hasFinancialTask: false,
    };
  }

  const algorithmInputs = {
    consumption: Number(property.consumption || 0),
    price: Number(property.current_price || 0),
    scarcity: Number(property.scarcity_score || 0),
    utility: Number(property.utility_score || 0),
    lifecycle: property.lifecycle_state || "incubating",
    inTop100: property.in_global_top_100 || false,
    age:
      (Date.now() - new Date(property.created_at).getTime()) /
      (1000 * 60 * 60 * 24),
  };

  if (!components.rows.length) {
    return {
      algorithmInputs,
      componentCount: 0,
      hasFinancePlanning: false,
      hasRoyaltySplits: false,
      hasBudget: false,
      hasFinancialTask: false,
      allocations: [],
    };
  }

  const allocations = [];
  let hasRoyaltySplits = false;
  let hasBudget = false;
  let hasFinancialPlan = false;
  let hasFinancialTask = false;
  let royaltySplitCount = 0;

  for (const comp of components.rows) {
    const rsResult = await pool.query(
      `SELECT COUNT(*)::int FROM royalty_splits WHERE finance_component_id = $1`,
      [comp.id],
    );
    const rsCount = rsResult.rows[0]?.count || 0;
    royaltySplitCount += rsCount;

    if (comp.component_type === "royalty_split" && rsCount > 0) {
      hasRoyaltySplits = true;
    } else if (comp.component_type === "budget") {
      hasBudget = true;
    } else if (comp.component_type === "financial_plan") {
      hasFinancialPlan = true;
    } else if (comp.component_type === "financial_task") {
      hasFinancialTask = true;
    }

    allocations.push({
      componentId: comp.id,
      type: comp.component_type,
      title: comp.title,
      description: comp.description,
      currency: comp.currency || null,
      royaltySplitCount: rsCount,
    });
  }

  const componentCount = components.rows.length;

  return {
    hasFinancePlanning: hasFinancialPlan,
    hasRoyaltySplits,
    hasBudget,
    hasFinancialPlan,
    hasFinancialTask,
    componentCount,
    royaltySplitCount,
    allocations,
    algorithmInputs,
  };
}

async function runFinancialAllocation() {
  const top100 = await getGlobalTop100();
  const rawAllocations = [];

  // Compute Total_Consumption over the Top N first, then derive each property's Consumption_Share from the same denominator.
  const enriched = [];
  for (const property of top100) {
    const consumption = property.is_bundle
      ? Number(property.consumption || 0)
      : await computePropertyConsumption(property.id);
    enriched.push({ property, consumption });
  }
  const totalConsumption = enriched.reduce(
    (sum, entry) => sum + Number(entry.consumption || 0),
    0,
  );

  for (const { property, consumption } of enriched) {
    const financeAllocation = await computeFinanceAllocation(property.id);
    const consumptionShare =
      totalConsumption > 0 ? consumption / totalConsumption : 0;

    let rawScore = 0;

    rawScore += consumptionShare * 100;

    const priceFactor = Number(property.current_price) / 1000;
    rawScore += priceFactor * 5;

    const scarcity = Number(property.scarcity_score || 0);
    if (scarcity > 2) {
      rawScore += (scarcity - 2) * 2;
    }

    const utility = Number(property.utility_score || 0);
    if (utility > 5) {
      rawScore += (utility - 5) * 1.5;
    }

    let lifecycleFactor = 1;
    if (property.lifecycle_state === "graduated") lifecycleFactor = 1.3;
    else if (property.lifecycle_state === "eligible") lifecycleFactor = 1.1;
    else if (property.lifecycle_state === "incubating") lifecycleFactor = 0.8;
    rawScore = rawScore * lifecycleFactor;

    if (financeAllocation.hasFinancePlanning) {
      rawScore = rawScore * 1.1;
    }

    if (rawScore < 0.1) rawScore = 0.1;

    rawAllocations.push({
      property_id: property.id,
      rawScore: rawScore,
      lifecycle: property.lifecycle_state,
      consumption: consumption,
      price: property.current_price,
      componentCount: financeAllocation.componentCount || 0,
      royaltySplitCount: financeAllocation.royaltySplitCount || 0,
    });
  }

  const totalRawScore = rawAllocations.reduce((sum, a) => sum + a.rawScore, 0);

  if (totalRawScore > 0) {
    for (const alloc of rawAllocations) {
      const normalizedPercentage = (alloc.rawScore / totalRawScore) * 100;

      await pool.query(
        `INSERT INTO property_finance_allocations 
          (property_id, category, amount, allocation_pct, description)
         VALUES ($1, 'algorithmic', 0, $2, $3)
         ON CONFLICT (property_id, category) 
         DO UPDATE SET 
          amount = 0,
          allocation_pct = EXCLUDED.allocation_pct,
          description = EXCLUDED.description,
          updated_at = now()`,
        [
          alloc.property_id,
          normalizedPercentage,
          `Algorithmic finance priority: score=${alloc.rawScore.toFixed(2)}, lifecycle=${alloc.lifecycle || "unknown"}, components=${alloc.componentCount}, royalty_splits=${alloc.royaltySplitCount}`,
        ],
      );
    }

    console.log(
      `Financial allocation normalized for ${rawAllocations.length} properties, ` +
        `all allocations sum to ${rawAllocations.reduce((sum, a) => sum + (a.rawScore / totalRawScore) * 100, 0).toFixed(2)}%`,
    );
  } else {
    console.warn("No raw scores calculated for finance allocation");
  }

  return { updated: rawAllocations.length };
}

// =====================================================================
// MARKETPLACE PIPELINE
// =====================================================================

async function runMarketplacePipeline() {
  if (pipelineRunning) {
    console.log("Pipeline already running, skipping...");
    return;
  }

  pipelineRunning = true;
  pipelineLockTime = Date.now();
  pipelineStartTime = Date.now();

  console.log(`Starting marketplace pipeline at ${new Date().toISOString()}`);

  try {
    await expireStaleInvitations();

    const populated = await pool.query(`
      SELECT DISTINCT p.id, p.category_id, p.high_scarcity_streak, p.total_slots
      FROM properties p
      WHERE p.status = 'active'
        AND COALESCE(p.is_bundle, false) = false
        AND (
          EXISTS (SELECT 1 FROM property_components pc WHERE pc.property_id = p.id)
          OR EXISTS (SELECT 1 FROM property_people pp WHERE pp.property_id = p.id AND COALESCE(pp.status, 'active') = 'active')
          OR EXISTS (SELECT 1 FROM finance_components fc WHERE fc.property_id = p.id)
          OR EXISTS (SELECT 1 FROM property_tasks pt WHERE pt.property_id = p.id)
        )
    `);

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
        scarcity,
        utility,
      });
    }

    const categories = await pool.query(
      "SELECT id, independent_ranking FROM categories",
    );
    const independentCategoryIds = new Set(
      categories.rows.filter((c) => c.independent_ranking).map((c) => c.id),
    );

    const existingBundle = await getWeakestBundleProperty(pool);
    const resetIds = scored.map((s) => s.id);
    if (existingBundle) resetIds.push(existingBundle.id);

    await pool.query(
      `UPDATE properties 
       SET category_rank = NULL, 
           in_top_100 = false, 
           in_global_top_100 = false 
       WHERE id = ANY($1)`,
      [resetIds],
    );

    const topN = getTopN();
    const globalEligible = scored.filter(
      (s) => !independentCategoryIds.has(s.category_id),
    );
    const globalRanked = [...globalEligible]
      .sort((a, b) => b.price - a.price)
      .slice(0, topN);

    const { individuals, bundle } = await syncWeakestBundle(globalRanked);

    for (let i = 0; i < individuals.length; i++) {
      await pool.query(
        `UPDATE properties 
         SET category_rank = $1, 
             in_top_100 = true, 
             in_global_top_100 = true 
         WHERE id = $2`,
        [i + 1, individuals[i].id],
      );
    }

    for (const categoryId of independentCategoryIds) {
      const categoryScored = scored.filter((s) => s.category_id === categoryId);
      const categoryRanked = [...categoryScored]
        .sort((a, b) => b.price - a.price)
        .slice(0, topN);

      for (let i = 0; i < categoryRanked.length; i++) {
        await pool.query(
          `UPDATE properties 
           SET category_rank = $1, 
               in_top_100 = true, 
               in_global_top_100 = false 
           WHERE id = $2`,
          [i + 1, categoryRanked[i].id],
        );
      }
    }

    // Run after ranking so newly bundled members cannot graduate.
    await updatePropertyLifecycles();

    const systemValuation = await computeSystemValuation();
    const totalSupply = await getTotalMainTokenSupply();
    const tokenPrice = computeMainTokenPrice(systemValuation, totalSupply);
    await recordMainTokenPrice(tokenPrice, totalSupply, systemValuation);

    const financeResult = await runFinancialAllocation();
    console.log(
      `Financial allocation updated for ${financeResult.updated} properties`,
    );

    await releaseSellQueue();
    await executeReleasedSells();

    const top100 = await getGlobalTop100();
    await rebalancePortfolioPositions(top100, tokenPrice);

    const payoutResult = await runContributorPayoutCycle();

    const duration = Math.round((Date.now() - pipelineStartTime) / 1000);
    console.log(
      `Marketplace pipeline complete (${duration}s): ${scored.length} properties scored, ` +
        `global_top_n=${top100.length}${bundle ? ` (incl. bundle of ${bundle.bundle_member_count})` : ""}, ` +
        `system_valuation=${systemValuation}, token_price=${tokenPrice}, ` +
        `payouts: ${payoutResult.paid} paid / ${payoutResult.failed} failed`,
    );
  } catch (err) {
    console.error("Marketplace pipeline error:", err);
    try {
      await pool.query(
        `INSERT INTO audit_log (event_type, details, created_at)
         VALUES ('pipeline_error', $1, NOW())`,
        [JSON.stringify({ error: err.message, stack: err.stack })],
      );
    } catch (logErr) {
      console.error("Failed to log pipeline error:", logErr);
    }
  } finally {
    pipelineRunning = false;
    pipelineLockTime = null;
    pipelineStartTime = null;
    console.log("Pipeline lock released");
  }
}

const PIPELINE_INTERVAL_MS = (() => {
  const parsed = parseInt(process.env.PIPELINE_INTERVAL_MS || "", 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : 5 * 60 * 1000;
})();
const marketplacePipelineInterval = setInterval(
  runMarketplacePipeline,
  PIPELINE_INTERVAL_MS,
);

// =====================================================================
// Run orphan reconciliation on startup
// =====================================================================

setTimeout(async () => {
  try {
    await reconcileOrphanPayouts();
  } catch (err) {
    console.error("Orphan reconciliation failed on startup:", err);
  }
}, 5000);

// =====================================================================
// API: CATEGORIES
// =====================================================================

app.get("/api/categories", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM categories ORDER BY name");
    res.json({ success: true, categories: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// =====================================================================
// API: MARKETPLACE OVERVIEW (Unified Views)
// =====================================================================

app.get("/api/marketplace/overview", async (req, res) => {
  try {
    const [
      memberships,
      uniquePeople,
      propertyCount,
      finance,
      financeAllocations,
      payouts,
      royaltySplits,
      creative,
      marketing,
      tasks,
      taskClaims,
      invitationCounts,
    ] = await Promise.all([
      pool.query(`
        SELECT p.flo_id, p.name, p.work_profile, p.profile_image_url,
               pp.property_id, pr.name AS property_name, pp.role, pp.part,
               pp.joined_at
        FROM people p
        LEFT JOIN property_people pp
          ON pp.person_flo_id = p.flo_id
         AND COALESCE(pp.status, 'active') = 'active'
        LEFT JOIN properties pr ON pp.property_id = pr.id
        ORDER BY p.name NULLS LAST, pr.name NULLS LAST, pp.role
      `),
      pool.query(`
        SELECT p.*,
               COUNT(DISTINCT pp.property_id) FILTER (
                 WHERE COALESCE(pp.status, 'active') = 'active'
               )::int AS property_count,
               COUNT(pp.*) FILTER (
                 WHERE COALESCE(pp.status, 'active') = 'active'
               )::int AS membership_count
        FROM people p
        LEFT JOIN property_people pp ON pp.person_flo_id = p.flo_id
        GROUP BY p.flo_id
        ORDER BY p.name NULLS LAST, p.flo_id
      `),
      pool.query(
        `SELECT COUNT(*)::int AS count FROM properties WHERE status = 'active'`,
      ),
      pool.query(`
        SELECT fc.*, pr.name AS property_name
        FROM finance_components fc
        LEFT JOIN properties pr ON fc.property_id = pr.id
        ORDER BY fc.created_at DESC
      `),
      pool.query(`
        SELECT pfa.*, pr.name AS property_name
        FROM property_finance_allocations pfa
        LEFT JOIN properties pr ON pr.id = pfa.property_id
        ORDER BY pfa.allocation_pct DESC
      `),
      pool.query(`
        SELECT pp.*, pr.name AS property_name
        FROM property_payouts pp
        LEFT JOIN properties pr ON pr.id = pp.property_id
        ORDER BY pp.created_at DESC
      `),
      pool.query(`
        SELECT rs.*, fc.title AS finance_title, fc.property_id,
               pr.name AS property_name
        FROM royalty_splits rs
        JOIN finance_components fc ON fc.id = rs.finance_component_id
        LEFT JOIN properties pr ON pr.id = fc.property_id
        ORDER BY rs.created_at DESC
      `),
      pool.query(
        `SELECT tc.*, pc.property_id, pr.name AS property_name
         FROM tracks_components tc
         LEFT JOIN property_components pc ON pc.component_id = tc.id
         LEFT JOIN properties pr ON pr.id = pc.property_id
         WHERE tc.component_type = ANY($1::text[])
         ORDER BY tc.created_at DESC`,
        [["lyrics", "music", "vocals"]],
      ),
      pool.query(`
        SELECT tc.*, pc.property_id, pr.name AS property_name
        FROM tracks_components tc
        LEFT JOIN property_components pc ON pc.component_id = tc.id
        LEFT JOIN properties pr ON pr.id = pc.property_id
        WHERE tc.component_type = 'marketing'
        ORDER BY tc.created_at DESC
      `),
      pool.query(`
        SELECT t.*, pt.property_id, pr.name AS property_name
        FROM tasks t
        LEFT JOIN property_tasks pt ON pt.task_id = t.id
        LEFT JOIN properties pr ON pr.id = pt.property_id
        ORDER BY t.created_at DESC
      `),
      pool.query(`SELECT DISTINCT task_id FROM task_claims`),
      pool.query(
        `SELECT CASE
                  WHEN status = 'pending' AND expires_at <= now() THEN 'expired'
                  ELSE status
                END AS status,
                COUNT(*)::int AS count
         FROM property_invitations
         GROUP BY 1`,
      ),
    ]);

    const uniqueCreativeComponents = new Map(
      creative.rows.map((component) => [component.id, component]),
    );
    const uniqueMarketingComponents = new Set(
      marketing.rows.map((component) => component.id),
    );
    const uniqueTasks = new Map(tasks.rows.map((task) => [task.id, task]));

    const creativeByType = { lyrics: 0, music: 0, vocals: 0 };
    for (const component of uniqueCreativeComponents.values()) {
      if (component.component_type in creativeByType) {
        creativeByType[component.component_type] += 1;
      }
    }

    const invitationCountsByStatus = {};
    for (const row of invitationCounts.rows) {
      invitationCountsByStatus[row.status] = Number(row.count);
    }

    const financeComponentAmount = finance.rows.reduce(
      (sum, item) => sum + Number(item.amount || 0),
      0,
    );
    const pendingPayoutAmount = payouts.rows
      .filter((p) => ["pending", "sending"].includes(p.status))
      .reduce((sum, p) => sum + Number(p.amount || 0), 0);
    const paidPayoutAmount = payouts.rows
      .filter((p) => p.status === "paid")
      .reduce((sum, p) => sum + Number(p.amount || 0), 0);
    const royaltyShareTotal = royaltySplits.rows.reduce(
      (sum, split) => sum + Number(split.share_percentage || 0),
      0,
    );

    res.json({
      success: true,
      counts: {
        uniquePeople: uniquePeople.rows.length,
        memberships: memberships.rows.filter((row) => row.property_id).length,
        properties: Number(propertyCount.rows[0]?.count || 0),
        creativeComponents: uniqueCreativeComponents.size,
        creativeByType,
        financeComponents: finance.rows.length,
        financeAllocations: financeAllocations.rows.length,
        payouts: payouts.rows.length,
        royaltySplits: royaltySplits.rows.length,
        tasks: uniqueTasks.size,
        taskLinks: tasks.rows.length,
        claimedTasks: taskClaims.rows.length,
        marketingComponents: uniqueMarketingComponents.size,
        pendingInvitations: invitationCountsByStatus.pending || 0,
      },
      invitations: invitationCountsByStatus,
      summaries: {
        finance: {
          componentAmount: financeComponentAmount,
          pendingPayoutAmount,
          paidPayoutAmount,
          allocationPctTotal: financeAllocations.rows.reduce(
            (sum, item) => sum + Number(item.allocation_pct || 0),
            0,
          ),
        },
        royalty: {
          splitCount: royaltySplits.rows.length,
          totalSharePercentage: royaltyShareTotal,
        },
        tasks: {
          open: [...uniqueTasks.values()].filter(
            (task) => task.status === "open",
          ).length,
          completed: [...uniqueTasks.values()].filter(
            (task) => task.status === "completed",
          ).length,
        },
      },
      // Legacy shape: one row per membership, kept for existing UI clients.
      people: memberships.rows,
      uniquePeople: uniquePeople.rows,
      finance: finance.rows,
      financeAllocations: financeAllocations.rows,
      payouts: payouts.rows,
      royaltySplits: royaltySplits.rows,
      creative: creative.rows,
      marketing: marketing.rows,
      tasks: tasks.rows,
    });
  } catch (err) {
    console.error("Failed to fetch overview:", err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// =====================================================================
// API: MARKETPLACE CONFIG + FORMULAS + CONSUMPTION SHARES
// =====================================================================

app.get("/api/marketplace/config", (req, res) => {
  res.json({
    success: true,
    config: marketplaceConfig,
    topN: getTopN(),
  });
});

app.get("/api/marketplace/formulas", (req, res) => {
  res.json({
    success: true,
    config: marketplaceConfig,
    topN: getTopN(),
    formulas: buildFormulaDescription(marketplaceConfig),
  });
});

app.post(
  "/api/marketplace/config",
  secureEndpoint({
    fields: ["adminFloId", "key", "value", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 20, windowMs: 60000 },
  }),
  async (req, res) => {
    const { key, value } = req.body;

    if (!key || !CONFIG_KEYS.includes(key)) {
      return res.status(400).json({
        success: false,
        error: `key must be one of: ${CONFIG_KEYS.join(", ")}`,
      });
    }

    let normalized;
    try {
      normalized = normalizeConfigValue(key, value);
    } catch (err) {
      return res.status(400).json({ success: false, error: err.message });
    }

    try {
      await pool.query(
        `INSERT INTO marketplace_config (key, value, description, updated_by_flo_id, updated_at)
         VALUES ($1, $2::jsonb, $3, $4, now())
         ON CONFLICT (key) DO UPDATE SET
           value = EXCLUDED.value,
           description = EXCLUDED.description,
           updated_by_flo_id = EXCLUDED.updated_by_flo_id,
           updated_at = now()`,
        [
          key,
          JSON.stringify(normalized),
          CONFIG_DESCRIPTIONS[key] || null,
          req.verifiedFloId || req.body.adminFloId || null,
        ],
      );

      marketplaceConfig = normalizeMarketplaceConfig({
        ...marketplaceConfig,
        [key]: normalized,
      });

      await logAuditEvent("marketplace_config_updated", {
        floId: req.verifiedFloId || req.body.adminFloId,
        key,
        value: normalized,
        ip: req.ip,
      });

      res.json({
        success: true,
        key,
        value: marketplaceConfig[key],
        config: marketplaceConfig,
        note: "Applies on the next pipeline run (within 5 minutes).",
      });
    } catch (err) {
      console.error("marketplace config update error:", err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.get("/api/marketplace/consumption", async (req, res) => {
  try {
    const top = await getGlobalTop100();
    const { totalConsumption, properties } = computeConsumptionShares(top);

    res.json({
      success: true,
      topN: getTopN(),
      totalConsumption,
      properties: properties.map((property) => ({
        id: property.id,
        name: property.name,
        consumption: property.consumption,
        consumptionShare: property.consumptionShare,
        consumptionSharePct: property.consumptionShare * 100,
        isBundle: property.is_bundle || false,
        bundleMemberCount: Number(property.bundle_member_count || 0),
      })),
    });
  } catch (err) {
    console.error("Failed to calculate consumption shares:", err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

app.post(
  "/api/categories",
  secureEndpoint({
    fields: ["adminFloId", "slug", "name", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 5, windowMs: 60000 },
  }),
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
      await logAuditEvent("category_created", {
        floId: req.verifiedFloId,
        slug,
        name,
        ip: req.ip,
      });
      res.json({ success: true, category: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

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

// =====================================================================
// API: TRACK COMPONENTS
// =====================================================================

app.post(
  "/api/tracks/:trackId/components",
  secureEndpoint({
    fields: [
      "trackId",
      "contributorFloId",
      "categorySlug",
      "componentType",
      "time",
    ],
    floIdField: "contributorFloId",
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
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

      await logAuditEvent("component_created", {
        floId: contributorFloId,
        trackId,
        componentType,
        ip: req.ip,
      });

      res.json({ success: true, component: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.get("/api/marketplace/payment-address", (req, res) => {
  if (!MARKETPLACE_FLO_ADDRESS) {
    return res
      .status(503)
      .json({ success: false, error: "Payments not configured" });
  }
  res.json({ success: true, address: MARKETPLACE_FLO_ADDRESS });
});

app.get("/api/main-token/payment-address", (req, res) => {
  if (!USDAI_PAYMENT_ADDRESS) {
    return res
      .status(503)
      .json({ success: false, error: "Payments not configured" });
  }
  res.json({ success: true, address: USDAI_PAYMENT_ADDRESS });
});

// =====================================================================
// API: PEOPLE
// =====================================================================

app.post(
  "/api/people",
  secureEndpoint({
    fields: [
      "floId",
      "workProfile",
      "experience",
      "name",
      "cvUrl",
      "profileImageUrl",
      "xUrl",
      "linkedinUrl",
      "websiteUrl",
      "time",
    ],
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
  async (req, res) => {
    const {
      floId,
      workProfile,
      experience,
      name,
      cvUrl,
      profileImageUrl,
      xUrl,
      linkedinUrl,
      websiteUrl,
    } = req.body;

    if (!floId || !workProfile) {
      return res.status(400).json({
        success: false,
        error: "Missing floId or workProfile",
      });
    }

    try {
      const result = await pool.query(
        `INSERT INTO people (
           flo_id,
           work_profile,
           experience,
           name,
           cv_url,
           profile_image_url,
           x_url,
           linkedin_url,
           website_url
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (flo_id) DO UPDATE SET
           work_profile      = EXCLUDED.work_profile,
           experience        = EXCLUDED.experience,
           name              = EXCLUDED.name,
           cv_url            = EXCLUDED.cv_url,
           profile_image_url = EXCLUDED.profile_image_url,
           x_url             = EXCLUDED.x_url,
           linkedin_url      = EXCLUDED.linkedin_url,
           website_url       = EXCLUDED.website_url,
           updated_at        = now()
         RETURNING *`,
        [
          floId,
          workProfile,
          experience || null,
          name || null,
          cvUrl || null,
          profileImageUrl || null,
          xUrl || null,
          linkedinUrl || null,
          websiteUrl || null,
        ],
      );
      await logAuditEvent("person_created", {
        floId,
        workProfile,
        ip: req.ip,
      });
      res.json({ success: true, person: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.get("/api/people", async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit, 10) || 100, 500);
  try {
    const result = await pool.query(
      `SELECT p.*,
              COUNT(DISTINCT pp.property_id) FILTER (
                WHERE COALESCE(pp.status, 'active') = 'active'
              )::int AS property_count,
              COUNT(pp.*) FILTER (
                WHERE COALESCE(pp.status, 'active') = 'active'
              )::int AS membership_count
       FROM people p
       LEFT JOIN property_people pp ON pp.person_flo_id = p.flo_id
       GROUP BY p.flo_id
       ORDER BY p.name NULLS LAST, p.flo_id
       LIMIT $1`,
      [limit],
    );

    res.json({
      success: true,
      count: result.rows.length,
      people: result.rows,
    });
  } catch (err) {
    console.error("Failed to list people:", err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

app.get("/api/people/:floId", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT p.*,
        (SELECT COUNT(DISTINCT pp.property_id) FROM property_people pp
          WHERE pp.person_flo_id = p.flo_id
            AND COALESCE(pp.status, 'active') = 'active') AS property_count,
        (SELECT COUNT(*) FROM property_people pp
          WHERE pp.person_flo_id = p.flo_id
            AND COALESCE(pp.status, 'active') = 'active') AS membership_count,
        (SELECT COALESCE(json_agg(DISTINCT pp.role), '[]')
          FROM property_people pp
          WHERE pp.person_flo_id = p.flo_id
            AND COALESCE(pp.status, 'active') = 'active') AS roles,
        (SELECT COALESCE(json_agg(json_build_object(
            'property_id', pp.property_id,
            'property_name', pr.name,
            'role', pp.role,
            'part', pp.part,
            'joined_at', pp.joined_at
          )), '[]')
          FROM property_people pp
          LEFT JOIN properties pr ON pr.id = pp.property_id
          WHERE pp.person_flo_id = p.flo_id
            AND COALESCE(pp.status, 'active') = 'active') AS memberships,
        (SELECT COUNT(*) FROM tracks_components tc WHERE tc.contributor_flo_id = p.flo_id) AS component_count,
        (SELECT COUNT(DISTINCT ts.task_id) FROM task_submissions ts WHERE ts.submitter_flo_id = p.flo_id AND ts.is_selected = true) AS task_count
      FROM people p 
      WHERE p.flo_id = $1
    `, [req.params.floId]);
    
    if (!result.rows.length) {
      return res.status(404).json({ success: false, error: "Person not found" });
    }
    res.json({ success: true, person: result.rows[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// =====================================================================
// API: PROPERTIES
// =====================================================================

app.post(
  "/api/properties",
  secureEndpoint({
    fields: ["adminFloId", "categorySlug", "name", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 5, windowMs: 60000 },
  }),
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
        `INSERT INTO properties (category_id, name, created_by_flo_id, lifecycle_state, incubation_started_at)
         VALUES ($1, $2, $3, 'incubating', now())
         RETURNING *`,
        [category.rows[0].id, name, adminFloId],
      );

      await client.query("COMMIT");
      await logAuditEvent("property_created", {
        floId: adminFloId,
        propertyId: property.rows[0].id,
        name,
        categorySlug,
        ip: req.ip,
      });
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

app.get("/api/properties", async (req, res) => {
  const top100Only = req.query.top100 === "true";
  const topN = getTopN();

  try {
    const result = await pool.query(
      `SELECT p.* FROM properties p
       LEFT JOIN categories c ON c.id = p.category_id
       WHERE p.status = 'active'
         AND (p.is_bundle = true OR p.bundled_into_property_id IS NULL)
         AND (p.is_bundle = true OR (c.id IS NOT NULL AND c.independent_ranking = false))
         ${top100Only ? "AND p.in_global_top_100 = true" : ""}
       ORDER BY p.category_rank ASC NULLS LAST, p.created_at DESC
       LIMIT $1`,
      [topN],
    );

    let properties = result.rows;
    let totalConsumption = null;
    if (top100Only) {
      const shares = computeConsumptionShares(properties);
      properties = shares.properties;
      totalConsumption = shares.totalConsumption;
    }

    res.json({
      success: true,
      properties,
      totalConsumption,
      topN,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

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

    const [components, people, finance, tasks, usageEvents, invitations] =
      await Promise.all([
        pool.query(
          `SELECT tc.* FROM property_components pc
         JOIN tracks_components tc ON tc.id = pc.component_id
         WHERE pc.property_id = $1`,
          [propertyId],
        ),
        pool.query(
          `SELECT pe.*, pp.role, pp.part, pp.status, pp.joined_at, pp.invitation_id
         FROM property_people pp
         JOIN people pe ON pe.flo_id = pp.person_flo_id
         WHERE pp.property_id = $1
           AND COALESCE(pp.status, 'active') = 'active'
         ORDER BY pe.name NULLS LAST, pp.role`,
          [propertyId],
        ),
        pool.query(
          `SELECT fc.*,
          COALESCE(
            json_agg(
              json_build_object(
                'recipient_flo_id', rs.recipient_flo_id,
                'share_percentage', rs.share_percentage,
                'role', rs.role
              )
            ) FILTER (WHERE rs.id IS NOT NULL),
            '[]'
          ) AS royalty_splits
         FROM finance_components fc
         LEFT JOIN royalty_splits rs
           ON rs.finance_component_id = fc.id
         WHERE fc.property_id = $1
         GROUP BY fc.id
         ORDER BY fc.created_at DESC`,
          [propertyId],
        ),
        pool.query(
          `SELECT t.*
         FROM property_tasks pt
         JOIN tasks t ON t.id = pt.task_id
         WHERE pt.property_id = $1`,
          [propertyId],
        ),
        pool.query(
          `SELECT * FROM property_usage_events WHERE property_id = $1 ORDER BY created_at DESC`,
          [propertyId],
        ),
        pool.query(
          `SELECT pi.id, pi.invitee_flo_id, pe.name AS invitee_name,
                  pi.part, pi.role, pi.status, pi.expires_at, pi.created_at
           FROM property_invitations pi
           LEFT JOIN people pe ON pe.flo_id = pi.invitee_flo_id
           WHERE pi.property_id = $1 AND pi.status = 'pending'
             AND pi.expires_at > now()
           ORDER BY pi.created_at DESC`,
          [propertyId],
        ),
      ]);

    let bundleMembers = [];
    if (property.rows[0].is_bundle) {
      const members = await pool.query(
        `SELECT p.id, p.name, p.consumption, p.base_price, p.current_price,
                p.category_id, p.category_rank
         FROM property_bundle_members bm
         JOIN properties p ON p.id = bm.member_property_id
         WHERE bm.bundle_property_id = $1
         ORDER BY p.consumption DESC`,
        [propertyId],
      );
      bundleMembers = members.rows;
    }

    const uniquePeople = new Set(people.rows.map((p) => p.flo_id)).size;
    const creativeComponents = components.rows.filter((c) =>
      ["lyrics", "music", "vocals"].includes(c.component_type),
    ).length;
    const counts = {
      people: uniquePeople,
      memberships: people.rows.length,
      components: components.rows.length,
      creativeComponents,
      financeComponents: finance.rows.length,
      tasks: tasks.rows.length,
      usageEvents: usageEvents.rows.length,
      pendingInvitations: invitations.rows.length,
    };

    res.json({
      success: true,
      property: property.rows[0],
      components: components.rows,
      people: people.rows,
      peopleCount: uniquePeople,
      membershipCount: people.rows.length,
      counts,
      finance: finance.rows,
      tasks: tasks.rows,
      usageEvents: usageEvents.rows,
      invitations: invitations.rows,
      bundleMembers,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

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
       WHERE category_id = $1
         AND COALESCE(is_bundle, false) = false
         AND bundled_into_property_id IS NULL
         ${top100Only ? "AND in_top_100 = true" : ""}
       ORDER BY category_rank ASC NULLS LAST, created_at DESC`,
      [category.rows[0].id],
    );

    res.json({ success: true, properties: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

app.post(
  "/api/properties/:propertyId/components",
  secureEndpoint({
    fields: ["propertyId", "adminFloId", "componentId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
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
      await logAuditEvent("component_added_to_property", {
        floId: adminFloId,
        propertyId,
        componentId,
        ip: req.ip,
      });
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.post(
  "/api/properties/:propertyId/components/:componentId/remove",
  secureEndpoint({
    fields: ["propertyId", "componentId", "adminFloId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
  async (req, res) => {
    const { propertyId, componentId } = req.params;
    try {
      await pool.query(
        `DELETE FROM property_components WHERE property_id = $1 AND component_id = $2`,
        [propertyId, componentId],
      );
      await logAuditEvent("component_removed_from_property", {
        floId: req.verifiedFloId,
        propertyId,
        componentId,
        ip: req.ip,
      });
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// =====================================================================
// API: FINANCE COMPONENTS
// =====================================================================

app.post(
  "/api/properties/:propertyId/finance-components",
  secureEndpoint({
    fields: ["propertyId", "adminFloId", "componentType", "title", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
  async (req, res) => {
    const { propertyId } = req.params;
    const {
      adminFloId,
      componentType,
      title,
      description,
      amount,
      currency = null,
      terms,
      metadata,
      royaltySplits,
    } = req.body;

    if (!componentType || !title) {
      return res
        .status(400)
        .json({ success: false, error: "Missing componentType or title" });
    }

    const validTypes = [
      "budget",
      "royalty_split",
      "financial_plan",
      "financial_task",
    ];
    if (!validTypes.includes(componentType)) {
      return res.status(400).json({
        success: false,
        error: `componentType must be one of: ${validTypes.join(", ")}`,
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

      const client = await pool.connect();
      let result;
      try {
        await client.query("BEGIN");

        const insertResult = await client.query(
          `INSERT INTO finance_components 
            (property_id, component_type, title, description, amount, currency, terms, created_by_flo_id, metadata)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           RETURNING *`,
          [
            propertyId,
            componentType,
            title,
            description || null,
            amount || 0,
            currency,
            terms || null,
            adminFloId,
            metadata || {},
          ],
        );
        result = insertResult.rows[0];

        if (
          componentType === "royalty_split" &&
          royaltySplits &&
          royaltySplits.length > 0
        ) {
          const totalPercentage = royaltySplits.reduce(
            (sum, s) => sum + Number(s.share_percentage),
            0,
          );
          if (totalPercentage !== 100) {
            await client.query("ROLLBACK");
            return res.status(400).json({
              success: false,
              error: `Royalty splits must total 100%. Current total: ${totalPercentage}%`,
            });
          }
          for (const split of royaltySplits) {
            await client.query(
              `INSERT INTO royalty_splits (finance_component_id, recipient_flo_id, share_percentage, role)
               VALUES ($1, $2, $3, $4)`,
              [
                result.id,
                split.recipient_flo_id,
                split.share_percentage,
                split.role || null,
              ],
            );
          }
        }

        await client.query("COMMIT");
        await logAuditEvent("finance_component_created", {
          floId: adminFloId,
          propertyId,
          componentType,
          title,
          ip: req.ip,
        });

        res.json({ success: true, component: result });
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      } finally {
        client.release();
      }
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.get("/api/properties/:propertyId/finance-components", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT fc.*, 
        COALESCE(
          json_agg(
            json_build_object(
              'recipient_flo_id', rs.recipient_flo_id,
              'share_percentage', rs.share_percentage,
              'role', rs.role
            )
          ) FILTER (WHERE rs.id IS NOT NULL),
          '[]'
        ) AS royalty_splits
       FROM finance_components fc
       LEFT JOIN royalty_splits rs ON rs.finance_component_id = fc.id
       WHERE fc.property_id = $1
       GROUP BY fc.id
       ORDER BY fc.created_at DESC`,
      [req.params.propertyId],
    );
    res.json({ success: true, components: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

app.delete(
  "/api/properties/:propertyId/finance-components/:componentId",
  secureEndpoint({
    fields: ["propertyId", "componentId", "adminFloId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
  async (req, res) => {
    const { propertyId, componentId } = req.params;

    try {
      await pool.query(
        "DELETE FROM royalty_splits WHERE finance_component_id = $1",
        [componentId],
      );

      const result = await pool.query(
        "DELETE FROM finance_components WHERE id = $1 AND property_id = $2 RETURNING id",
        [componentId, propertyId],
      );

      if (!result.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Component not found" });
      }

      await logAuditEvent("finance_component_deleted", {
        floId: req.verifiedFloId,
        propertyId,
        componentId,
        ip: req.ip,
      });

      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.get("/api/properties/:propertyId/finance-allocations", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM property_finance_allocations 
       WHERE property_id = $1 
       ORDER BY allocation_pct DESC`,
      [req.params.propertyId],
    );

    res.json({
      success: true,
      allocations: result.rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// =====================================================================
// API: TASKS
// =====================================================================

app.get("/api/tasks", async (req, res) => {
  try {
    const [tasks, claims, winners] = await Promise.all([
      pool.query("SELECT * FROM tasks ORDER BY created_at DESC"),
      pool.query("SELECT * FROM task_claims ORDER BY claimed_at DESC"),
      pool.query(
        "SELECT task_id, submitter_flo_id FROM task_submissions WHERE is_selected = true",
      ),
    ]);

    const winnerByTaskId = new Map(
      winners.rows.map((row) => [String(row.task_id), row.submitter_flo_id]),
    );

    const tasksWithWinners = tasks.rows.map((task) => ({
      ...task,
      winner_flo_id: winnerByTaskId.get(String(task.id)) || null,
    }));

    res.json({ success: true, tasks: tasksWithWinners, claims: claims.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

app.post(
  "/api/tasks",
  secureEndpoint({
    fields: [
      "adminFloId",
      "requesterFloId",
      "brief",
      "budget",
      "componentType",
      "time",
    ],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 5, windowMs: 60000 },
  }),
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
      await logAuditEvent("task_created", {
        floId: req.verifiedFloId,
        taskId: result.rows[0].id,
        requesterFloId,
        brief: brief.substring(0, 100),
        ip: req.ip,
      });
      res.json({ success: true, task: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.post(
  "/api/tasks/:taskId/claim",
  secureEndpoint({
    fields: ["taskId", "creatorFloId", "time"],
    floIdField: "creatorFloId",
    rateLimitOpts: { max: 10, windowMs: 60000 },
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

      const existingClaim = await client.query(
        "SELECT 1 FROM task_claims WHERE task_id = $1 AND claimer_flo_id = $2",
        [taskId, creatorFloId],
      );

      if (existingClaim.rows.length) {
        await client.query("ROLLBACK");
        return res.status(409).json({
          success: false,
          error: "You have already claimed this task.",
        });
      }

      await client.query(
        "INSERT INTO task_claims (task_id, claimer_flo_id) VALUES ($1, $2)",
        [taskId, creatorFloId],
      );

      await client.query("COMMIT");
      await logAuditEvent("task_claimed", {
        floId: creatorFloId,
        taskId,
        ip: req.ip,
      });
      res.json({ success: true, message: "Task claimed." });
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    } finally {
      client.release();
    }
  },
);

app.post(
  "/api/tasks/:taskId/submit",
  secureEndpoint({
    fields: ["taskId", "creatorFloId", "deliveryLink", "time"],
    floIdField: "creatorFloId",
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
  async (req, res) => {
    const taskId = parsePositiveInt(req.params.taskId);
    const { creatorFloId, deliveryLink } = req.body;

    if (!taskId || !deliveryLink) {
      return res
        .status(400)
        .json({ success: false, error: "Missing taskId or deliveryLink" });
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

      const claimCheck = await client.query(
        "SELECT 1 FROM task_claims WHERE task_id = $1 AND claimer_flo_id = $2",
        [taskId, creatorFloId],
      );

      if (!claimCheck.rows.length) {
        await client.query("ROLLBACK");
        return res
          .status(403)
          .json({ success: false, error: "You have not claimed this task." });
      }

      const insert = await client.query(
        `INSERT INTO task_submissions (task_id, submitter_flo_id, delivery_link)
         VALUES ($1, $2, $3) RETURNING *`,
        [taskId, creatorFloId, deliveryLink],
      );

      await client.query("COMMIT");
      await logAuditEvent("task_submission_created", {
        floId: creatorFloId,
        taskId,
        deliveryLink,
        ip: req.ip,
      });
      res.json({ success: true, submission: insert.rows[0] });
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    } finally {
      client.release();
    }
  },
);

app.post(
  "/api/tasks/:taskId/select-winner",
  secureEndpoint({
    fields: ["taskId", "submissionId", "adminFloId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
  async (req, res) => {
    const taskId = parsePositiveInt(req.params.taskId);
    const { submissionId } = req.body;

    if (!taskId || !submissionId) {
      return res
        .status(400)
        .json({ success: false, error: "Missing taskId or submissionId" });
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      await client.query(
        `UPDATE task_submissions SET is_selected = false WHERE task_id = $1`,
        [taskId],
      );

      const submission = await client.query(
        `UPDATE task_submissions
         SET is_selected = true
         WHERE id = $1 AND task_id = $2
         RETURNING *`,
        [submissionId, taskId],
      );

      if (!submission.rows.length) {
        await client.query("ROLLBACK");
        return res
          .status(404)
          .json({ success: false, error: "Submission not found" });
      }

      const updatedTask = await client.query(
        `UPDATE tasks
         SET status = 'completed', completed_at = now()
         WHERE id = $1
         RETURNING *`,
        [taskId],
      );

      await client.query("COMMIT");
      await logAuditEvent("task_submission_selected", {
        floId: req.verifiedFloId,
        taskId,
        submissionId,
        ip: req.ip,
      });
      res.json({
        success: true,
        task: updatedTask.rows[0],
        selected_submission: submission.rows[0],
      });
    } catch (err) {
      await client.query("ROLLBACK");
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    } finally {
      client.release();
    }
  },
);

// GET: Fetch all submissions for a specific task (for the Task Giver/Admin)
app.get("/api/tasks/:taskId/submissions", async (req, res) => {
  const taskId = parsePositiveInt(req.params.taskId);
  if (!taskId)
    return res.status(400).json({ success: false, error: "Invalid taskId" });

  try {
    const result = await pool.query(
      "SELECT * FROM task_submissions WHERE task_id = $1 ORDER BY created_at DESC",
      [taskId],
    );
    res.json({ success: true, submissions: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

app.post(
  "/api/properties/:propertyId/tasks",
  secureEndpoint({
    fields: ["propertyId", "adminFloId", "taskId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
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
      await logAuditEvent("task_added_to_property", {
        floId: req.verifiedFloId,
        propertyId,
        taskId,
        ip: req.ip,
      });
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.post(
  "/api/properties/:propertyId/tasks/:taskId/remove",
  secureEndpoint({
    fields: ["propertyId", "taskId", "adminFloId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
  async (req, res) => {
    const { propertyId, taskId } = req.params;
    try {
      await pool.query(
        `DELETE FROM property_tasks WHERE property_id = $1 AND task_id = $2`,
        [propertyId, taskId],
      );
      await logAuditEvent("task_removed_from_property", {
        floId: req.verifiedFloId,
        propertyId,
        taskId,
        ip: req.ip,
      });
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// =====================================================================
// API: PROPERTY PEOPLE
// =====================================================================

app.post(
  "/api/properties/:propertyId/people",
  secureEndpoint({
    fields: ["propertyId", "adminFloId", "personFloId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
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
        `INSERT INTO property_people
           (property_id, person_flo_id, role, part, status, added_by_flo_id, joined_at)
         VALUES ($1, $2, $3, 'people', 'active', $4, now())
         ON CONFLICT (property_id, person_flo_id, role) DO UPDATE SET
           status = 'active',
           part = 'people',
           removed_at = NULL,
           joined_at = CASE
             WHEN property_people.status = 'removed' THEN now()
             ELSE COALESCE(property_people.joined_at, now())
           END,
           added_by_flo_id = EXCLUDED.added_by_flo_id`,
        [propertyId, personFloId, role || "", adminFloId],
      );
      await logAuditEvent("person_added_to_property", {
        floId: adminFloId,
        propertyId,
        personFloId,
        role: role || "",
        ip: req.ip,
      });
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.post(
  "/api/properties/:propertyId/people/:personFloId/remove",
  secureEndpoint({
    fields: ["propertyId", "personFloId", "adminFloId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
  async (req, res) => {
    const { propertyId, personFloId } = req.params;
    const { role } = req.body;
    try {
      await pool.query(
        `UPDATE property_people
         SET status = 'removed', removed_at = now()
         WHERE property_id = $1 AND person_flo_id = $2 AND role = $3
           AND COALESCE(status, 'active') = 'active'`,
        [propertyId, personFloId, role || ""],
      );
      await logAuditEvent("person_removed_from_property", {
        floId: req.verifiedFloId,
        propertyId,
        personFloId,
        role: role || "",
        ip: req.ip,
      });
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// =====================================================================
// API: PROPERTY INVITATIONS
// =====================================================================

app.post(
  "/api/properties/:propertyId/invitations",
  secureEndpoint({
    fields: [
      "propertyId",
      "adminFloId",
      "inviteeFloId",
      "part",
      "role",
      "expiresInDays",
      "time",
    ],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 20, windowMs: 60000 },
  }),
  async (req, res) => {
    const { propertyId } = req.params;
    const inviteeFloId = String(req.body.inviteeFloId || "").trim();
    const part = normalizeInvitationPart(req.body.part);

    if (!inviteeFloId) {
      return res
        .status(400)
        .json({ success: false, error: "Missing inviteeFloId" });
    }
    if (!part) {
      return res.status(400).json({
        success: false,
        error: `part must be one of: ${INVITATION_PARTS.join(", ")}`,
      });
    }

    const role = (
      String(req.body.role || "").trim() || defaultInvitationRole(part)
    ).slice(0, 80);
    const requestedDays = Number(req.body.expiresInDays);
    const expiresInDays =
      Number.isFinite(requestedDays) && requestedDays > 0
        ? Math.min(
            Math.max(1, Math.floor(requestedDays)),
            MAX_INVITATION_EXPIRY_DAYS,
          )
        : DEFAULT_INVITATION_EXPIRY_DAYS;

    try {
      await expireStaleInvitations();
      const property = await pool.query(
        `SELECT id, name FROM properties WHERE id = $1 AND status = 'active'`,
        [propertyId],
      );
      if (!property.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Property not found" });
      }

      const activeMembership = await pool.query(
        `SELECT 1 FROM property_people
         WHERE property_id = $1 AND person_flo_id = $2 AND role = $3
           AND COALESCE(status, 'active') = 'active'`,
        [propertyId, inviteeFloId, role],
      );
      if (activeMembership.rows.length) {
        return res.status(409).json({
          success: false,
          error: "That person already has this role on the property",
        });
      }

      const existing = await pool.query(
        `SELECT id, token, expires_at FROM property_invitations
         WHERE property_id = $1 AND invitee_flo_id = $2
           AND part = $3 AND role = $4
           AND status = 'pending' AND expires_at > now()
         ORDER BY created_at DESC LIMIT 1`,
        [propertyId, inviteeFloId, part, role],
      );
      if (existing.rows.length) {
        return res.status(409).json({
          success: false,
          error: "A pending invitation already exists",
          invitation: {
            id: existing.rows[0].id,
            token: existing.rows[0].token,
            expiresAt: existing.rows[0].expires_at,
          },
        });
      }

      const token = invitationToken();
      const result = await pool.query(
        `INSERT INTO property_invitations
           (property_id, inviter_flo_id, invitee_flo_id, part, role, token,
            status, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, 'pending',
                 now() + ($7 * INTERVAL '1 day'))
         RETURNING *`,
        [
          propertyId,
          req.verifiedFloId || req.body.adminFloId,
          inviteeFloId,
          part,
          role,
          token,
          expiresInDays,
        ],
      );

      await logAuditEvent("property_invitation_created", {
        floId: req.verifiedFloId || req.body.adminFloId,
        propertyId,
        inviteeFloId,
        part,
        role,
        expiresInDays,
        ip: req.ip,
      });

      res.json({
        success: true,
        invitation: result.rows[0],
        acceptEndpoint: `/api/invitations/${token}/accept`,
      });
    } catch (err) {
      console.error("Create invitation error:", err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.get("/api/properties/:propertyId/invitations", async (req, res) => {
  try {
    await expireStaleInvitations();
    const result = await pool.query(
      `SELECT pi.id, pi.invitee_flo_id, pe.name AS invitee_name,
              pi.part, pi.role, pi.status, pi.expires_at,
              pi.created_at, pi.responded_at
       FROM property_invitations pi
       LEFT JOIN people pe ON pe.flo_id = pi.invitee_flo_id
       WHERE pi.property_id = $1
       ORDER BY pi.created_at DESC`,
      [req.params.propertyId],
    );

    const counts = {};
    for (const invitation of result.rows) {
      counts[invitation.status] = (counts[invitation.status] || 0) + 1;
    }

    res.json({
      success: true,
      invitations: result.rows,
      counts,
    });
  } catch (err) {
    console.error("List invitations error:", err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

app.post(
  "/api/invitations/inbox",
  secureEndpoint({
    fields: ["floId", "time"],
    rateLimitOpts: { max: 30, windowMs: 60000 },
  }),
  async (req, res) => {
    try {
      await expireStaleInvitations();
      const result = await pool.query(
        `SELECT pi.id, pi.property_id, pr.name AS property_name,
                pi.inviter_flo_id, inviter.name AS inviter_name,
                pi.part, pi.role,
                CASE WHEN pi.status = 'pending' THEN pi.token ELSE NULL END AS token,
                CASE WHEN pi.status = 'pending' AND pi.read_at IS NULL
                     THEN true ELSE false END AS is_unread,
                pi.read_at, pi.status, pi.expires_at, pi.created_at, pi.responded_at
         FROM property_invitations pi
         LEFT JOIN properties pr ON pr.id = pi.property_id
         LEFT JOIN people inviter ON inviter.flo_id = pi.inviter_flo_id
         WHERE pi.invitee_flo_id = $1
         ORDER BY pi.created_at DESC
         LIMIT 100`,
        [req.verifiedFloId],
      );

      const counts = {};
      for (const invitation of result.rows) {
        counts[invitation.status] = (counts[invitation.status] || 0) + 1;
      }

      res.json({
        success: true,
        invitations: result.rows,
        counts,
        unreadCount: result.rows.filter((row) => row.is_unread).length,
      });
    } catch (err) {
      console.error("Invitation inbox error:", err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.post(
  "/api/invitations/unread-count",
  secureEndpoint({
    fields: ["floId", "time"],
    rateLimitOpts: { max: 120, windowMs: 60000 },
  }),
  async (req, res) => {
    try {
      const result = await pool.query(
        `SELECT COUNT(*)::int AS count
         FROM property_invitations
         WHERE invitee_flo_id = $1
           AND status = 'pending'
           AND read_at IS NULL
           AND expires_at > now()`,
        [req.verifiedFloId],
      );
      res.json({
        success: true,
        unreadCount: Number(result.rows[0]?.count || 0),
      });
    } catch (err) {
      console.error("Unread invitation count error:", err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.post(
  "/api/invitations/mark-read",
  secureEndpoint({
    fields: ["floId", "time"],
    rateLimitOpts: { max: 60, windowMs: 60000 },
  }),
  async (req, res) => {
    try {
      const result = await pool.query(
        `UPDATE property_invitations
         SET read_at = now()
         WHERE invitee_flo_id = $1
           AND status = 'pending'
           AND read_at IS NULL
         RETURNING id`,
        [req.verifiedFloId],
      );
      res.json({
        success: true,
        markedRead: result.rowCount,
        unreadCount: 0,
      });
    } catch (err) {
      console.error("Mark invitations read error:", err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.get("/api/invitations/:token", async (req, res) => {
  try {
    await expireStaleInvitations();
    const result = await pool.query(
      `SELECT pi.id, pi.property_id, pr.name AS property_name,
              pi.inviter_flo_id, pi.invitee_flo_id, pi.part, pi.role,
              pi.status, pi.expires_at, pi.created_at, pi.responded_at
       FROM property_invitations pi
       LEFT JOIN properties pr ON pr.id = pi.property_id
       WHERE pi.token = $1`,
      [req.params.token],
    );
    if (!result.rows.length) {
      return res
        .status(404)
        .json({ success: false, error: "Invitation not found" });
    }

    const invitation = result.rows[0];
    res.json({
      success: true,
      invitation,
      canAccept: invitation.status === "pending",
    });
  } catch (err) {
    console.error("Get invitation error:", err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

app.post(
  "/api/invitations/:token/accept",
  secureEndpoint({
    fields: ["token", "floId", "time"],
    rateLimitOpts: { max: 20, windowMs: 60000 },
  }),
  async (req, res) => {
    const { token } = req.params;
    const { floId } = req.body;
    const client = await pool.connect();

    try {
      await client.query("BEGIN");
      await expireStaleInvitations(client);

      const invitationResult = await client.query(
        `SELECT * FROM property_invitations WHERE token = $1 FOR UPDATE`,
        [token],
      );
      if (!invitationResult.rows.length) {
        await client.query("ROLLBACK");
        return res
          .status(404)
          .json({ success: false, error: "Invitation not found" });
      }

      const invitation = invitationResult.rows[0];
      if (invitation.status === "accepted") {
        await client.query("ROLLBACK");
        if (invitation.invitee_flo_id !== floId) {
          return res.status(403).json({
            success: false,
            error: "This invitation belongs to a different FLO ID",
          });
        }
        return res.json({ success: true, alreadyAccepted: true });
      }
      if (invitation.status !== "pending") {
        await client.query("ROLLBACK");
        return res.status(409).json({
          success: false,
          error: `Invitation is ${invitation.status}`,
        });
      }
      if (invitation.invitee_flo_id !== floId) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          error: "This invitation belongs to a different FLO ID",
        });
      }

      // Auto-create a minimal profile so the person can accept even before
      // filling out their public profile.
      await client.query(
        `INSERT INTO people (flo_id, work_profile)
         VALUES ($1, 'contributor')
         ON CONFLICT (flo_id) DO NOTHING`,
        [floId],
      );

      const membership = await client.query(
        `INSERT INTO property_people
           (property_id, person_flo_id, role, part, status,
            added_by_flo_id, joined_at, invitation_id)
         VALUES ($1, $2, $3, $4, 'active', $5, now(), $6)
         ON CONFLICT (property_id, person_flo_id, role) DO UPDATE SET
           status = 'active',
           part = EXCLUDED.part,
           removed_at = NULL,
           added_by_flo_id = EXCLUDED.added_by_flo_id,
           joined_at = CASE
             WHEN property_people.status = 'removed' THEN now()
             ELSE COALESCE(property_people.joined_at, now())
           END,
           invitation_id = EXCLUDED.invitation_id
         RETURNING *`,
        [
          invitation.property_id,
          floId,
          invitation.role,
          invitation.part,
          invitation.inviter_flo_id,
          invitation.id,
        ],
      );

      await client.query(
        `UPDATE property_invitations
         SET status = 'accepted', responded_at = now(),
             read_at = COALESCE(read_at, now()), updated_at = now()
         WHERE id = $1`,
        [invitation.id],
      );

      await client.query("COMMIT");

      await logAuditEvent("property_invitation_accepted", {
        floId,
        propertyId: invitation.property_id,
        invitationId: invitation.id,
        part: invitation.part,
        role: invitation.role,
        ip: req.ip,
      });

      res.json({
        success: true,
        propertyId: invitation.property_id,
        part: invitation.part,
        role: invitation.role,
        membership: membership.rows[0],
      });
    } catch (err) {
      await client.query("ROLLBACK");
      console.error("Accept invitation error:", err);
      res.status(500).json({ success: false, error: "Database error" });
    } finally {
      client.release();
    }
  },
);

app.post(
  "/api/invitations/:token/decline",
  secureEndpoint({
    fields: ["token", "floId", "time"],
    rateLimitOpts: { max: 20, windowMs: 60000 },
  }),
  async (req, res) => {
    const { token } = req.params;
    const { floId } = req.body;
    const client = await pool.connect();

    try {
      await client.query("BEGIN");
      await expireStaleInvitations(client);

      const invitationResult = await client.query(
        `SELECT * FROM property_invitations WHERE token = $1 FOR UPDATE`,
        [token],
      );
      if (!invitationResult.rows.length) {
        await client.query("ROLLBACK");
        return res
          .status(404)
          .json({ success: false, error: "Invitation not found" });
      }

      const invitation = invitationResult.rows[0];
      if (invitation.invitee_flo_id !== floId) {
        await client.query("ROLLBACK");
        return res.status(403).json({
          success: false,
          error: "This invitation belongs to a different FLO ID",
        });
      }
      if (invitation.status === "declined") {
        await client.query("ROLLBACK");
        return res.json({ success: true, alreadyDeclined: true });
      }
      if (invitation.status !== "pending") {
        await client.query("ROLLBACK");
        return res.status(409).json({
          success: false,
          error: `Invitation is ${invitation.status}`,
        });
      }

      await client.query(
        `UPDATE property_invitations
         SET status = 'declined', responded_at = now(),
             read_at = COALESCE(read_at, now()), updated_at = now()
         WHERE id = $1`,
        [invitation.id],
      );
      await client.query("COMMIT");

      await logAuditEvent("property_invitation_declined", {
        floId,
        propertyId: invitation.property_id,
        invitationId: invitation.id,
        ip: req.ip,
      });

      res.json({ success: true, status: "declined" });
    } catch (err) {
      await client.query("ROLLBACK");
      console.error("Decline invitation error:", err);
      res.status(500).json({ success: false, error: "Database error" });
    } finally {
      client.release();
    }
  },
);

app.post(
  "/api/properties/:propertyId/invitations/:invitationId/revoke",
  secureEndpoint({
    fields: ["propertyId", "invitationId", "adminFloId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 20, windowMs: 60000 },
  }),
  async (req, res) => {
    const { propertyId, invitationId } = req.params;
    try {
      const result = await pool.query(
        `UPDATE property_invitations
         SET status = 'revoked', responded_at = now(), updated_at = now()
         WHERE id = $1 AND property_id = $2 AND status = 'pending'
         RETURNING *`,
        [invitationId, propertyId],
      );
      if (!result.rows.length) {
        return res.status(404).json({
          success: false,
          error: "Pending invitation not found",
        });
      }

      await logAuditEvent("property_invitation_revoked", {
        floId: req.verifiedFloId || req.body.adminFloId,
        propertyId,
        invitationId,
        ip: req.ip,
      });

      res.json({ success: true, invitation: result.rows[0] });
    } catch (err) {
      console.error("Revoke invitation error:", err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.post(
  "/api/properties/:propertyId/invitations/:invitationId/resend",
  secureEndpoint({
    fields: [
      "propertyId",
      "invitationId",
      "adminFloId",
      "expiresInDays",
      "time",
    ],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 20, windowMs: 60000 },
  }),
  async (req, res) => {
    const { propertyId, invitationId } = req.params;
    const requestedDays = Number(req.body.expiresInDays);
    const expiresInDays =
      Number.isFinite(requestedDays) && requestedDays > 0
        ? Math.min(
            Math.max(1, Math.floor(requestedDays)),
            MAX_INVITATION_EXPIRY_DAYS,
          )
        : DEFAULT_INVITATION_EXPIRY_DAYS;

    try {
      const existing = await pool.query(
        `SELECT * FROM property_invitations
         WHERE id = $1 AND property_id = $2`,
        [invitationId, propertyId],
      );
      if (!existing.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Invitation not found" });
      }
      if (!["pending", "expired"].includes(existing.rows[0].status)) {
        return res.status(409).json({
          success: false,
          error: `Cannot resend a ${existing.rows[0].status} invitation; create a new one`,
        });
      }

      const token = invitationToken();
      const result = await pool.query(
        `UPDATE property_invitations
         SET token = $1,
             status = 'pending',
             expires_at = now() + ($2 * INTERVAL '1 day'),
             read_at = NULL,
             responded_at = NULL,
             updated_at = now()
         WHERE id = $3 AND property_id = $4
         RETURNING *`,
        [token, expiresInDays, invitationId, propertyId],
      );

      await logAuditEvent("property_invitation_resent", {
        floId: req.verifiedFloId || req.body.adminFloId,
        propertyId,
        invitationId,
        expiresInDays,
        ip: req.ip,
      });

      res.json({
        success: true,
        invitation: result.rows[0],
        acceptEndpoint: `/api/invitations/${token}/accept`,
      });
    } catch (err) {
      console.error("Resend invitation error:", err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// =====================================================================
// API: PROPERTY INTEREST
// =====================================================================

app.post(
  "/api/properties/:propertyId/interest",
  secureEndpoint({
    fields: ["propertyId", "floId", "intent", "time"],
    rateLimitOpts: { max: 20, windowMs: 60000 },
  }),
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

// =====================================================================
// API: MAIN TOKEN
// =====================================================================

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

app.post(
  "/api/main-token/buy",
  secureEndpoint({
    fields: ["floId", "usdaiTxid", "time"],
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
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
      usdaiValue = payment.tokenAmount;
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

      const top100 = await getGlobalTop100();
      const allocation = computePortfolioAllocation(top100);

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

      await logAuditEvent("main_token_buy", {
        floId,
        usdaiValue,
        tokenAmount,
        tokenPrice,
        usdaiTxid,
        ip: req.ip,
      });

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

app.post(
  "/api/main-token/sell",
  secureEndpoint({
    fields: ["floId", "tokenAmount", "time"],
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
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
        `SELECT COALESCE(SUM(token_amount - paid_amount), 0)::numeric AS outstanding
         FROM sell_queue
         WHERE flo_id = $1
           AND paid_amount < token_amount
           AND status <> 'expired'`,
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
        INSERT INTO sell_queue (flo_id, token_amount, expires_at)
        VALUES ($1, $2, now() + ($3 * INTERVAL '1 day'))
        RETURNING *
        `,
        [floId, tokenAmount, getSellQueuePeriodDays()],
      );

      const pressureRatio = await computeMainTokenPressureRatio();
      await logAuditEvent("main_token_sell_queued", {
        floId,
        tokenAmount,
        pressureRatio,
        ip: req.ip,
      });

      res.json({
        success: true,
        queued: inserted.rows[0],
        expiresAt: inserted.rows[0].expires_at,
        sellQueuePeriodDays: getSellQueuePeriodDays(),
        currentPressureRatio: pressureRatio,
        currentReleaseFraction: computeSellReleaseFraction(pressureRatio),
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

app.get("/api/main-token/sell-queue", async (req, res) => {
  try {
    const pendingResult = await pool.query(
      `
      SELECT COUNT(*)::int AS count,
             COALESCE(SUM(token_amount - paid_amount), 0)::numeric AS outstanding_tokens,
             MIN(requested_at) AS oldest_requested_at,
             MIN(expires_at) AS next_expires_at
      FROM sell_queue
      WHERE paid_amount < token_amount
        AND status <> 'expired'
      `,
    );

    const expiredResult = await pool.query(
      `SELECT COUNT(*)::int AS count, MAX(expired_at) AS last_expired_at
       FROM sell_queue WHERE status = 'expired'`,
    );

    const [systemValuation, totalSupply, liquidityBuilt] = await Promise.all([
      computeSystemValuation(),
      getTotalMainTokenSupply(),
      isPlatformLiquidityBuilt(),
    ]);
    const tokenPrice = computeMainTokenPrice(systemValuation, totalSupply);
    const pressureRatio = await computeMainTokenPressureRatio();

    const row = pendingResult.rows[0];
    const outstandingTokens = Number(row?.outstanding_tokens || 0);
    res.json({
      success: true,
      liquidityBuilt,
      pendingCount: Number(row?.count || 0),
      outstandingTokens,
      outstandingUsdai: outstandingTokens * tokenPrice,
      oldestRequestedAt: row?.oldest_requested_at || null,
      nextExpiresAt: row?.next_expires_at || null,
      expiredCount: Number(expiredResult.rows[0]?.count || 0),
      lastExpiredAt: expiredResult.rows[0]?.last_expired_at || null,
      sellQueuePeriodDays: getSellQueuePeriodDays(),
      tokenPrice,
      pressureRatio7d: pressureRatio,
      pressureWindowDays: getSellQueuePeriodDays(),
      releaseFraction: computeSellReleaseFraction(pressureRatio),
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// =====================================================================
// API: USAGE EVENTS
// =====================================================================

app.post(
  "/api/properties/:propertyId/usage-events",
  secureEndpoint({
    fields: ["propertyId", "adminFloId", "usageType", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 20, windowMs: 60000 },
  }),
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

      await logAuditEvent("usage_event_created", {
        floId: req.verifiedFloId,
        propertyId,
        usageType,
        ip: req.ip,
      });

      res.json({ success: true, event: result.rows[0] });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// =====================================================================
// API: GET USAGE EVENTS
// =====================================================================

app.get("/api/properties/:propertyId/usage-events", async (req, res) => {
  const { propertyId } = req.params;

  try {
    const result = await pool.query(
      `SELECT * FROM property_usage_events WHERE property_id = $1 ORDER BY created_at DESC`,
      [propertyId],
    );

    res.json({ success: true, events: result.rows });
  } catch (err) {
    console.error("Failed to fetch usage events:", err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// =====================================================================
// API: DELETE USAGE EVENT
// =====================================================================

app.post(
  "/api/properties/:propertyId/usage-events/:eventId",
  secureEndpoint({
    fields: ["propertyId", "eventId", "adminFloId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 10, windowMs: 60000 },
  }),
  async (req, res) => {
    const { propertyId, eventId } = req.params;
    try {
      const result = await pool.query(
        "DELETE FROM property_usage_events WHERE id = $1 AND property_id = $2 RETURNING id",
        [eventId, propertyId],
      );
      if (!result.rows.length) {
        return res
          .status(404)
          .json({ success: false, error: "Usage event not found" });
      }
      await logAuditEvent("usage_event_deleted", {
        floId: req.verifiedFloId,
        propertyId,
        eventId,
        ip: req.ip,
      });
      res.json({ success: true });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);
// =====================================================================
// API: ADMIN
// =====================================================================

app.post(
  "/api/admin/whoami",
  secureEndpoint({
    fields: ["floId", "time"],
    rateLimitOpts: { max: 20, windowMs: 60000 },
  }),
  (req, res) => {
    const { floId } = req.body;
    res.json({ success: true, isAdmin: ADMIN_FLO_IDS.includes(floId) });
  },
);

app.post(
  "/api/admin/pipeline-status",
  secureEndpoint({
    fields: ["adminFloId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 5, windowMs: 60000 },
  }),
  async (req, res) => {
    res.json({
      success: true,
      running: pipelineRunning,
      startedAt: pipelineStartTime
        ? new Date(pipelineStartTime).toISOString()
        : null,
      lockTime: pipelineLockTime
        ? new Date(pipelineLockTime).toISOString()
        : null,
    });
  },
);

app.post(
  "/api/marketplace/run-pipeline",
  secureEndpoint({
    fields: ["adminFloId", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 2, windowMs: 60000 },
  }),
  async (req, res) => {
    try {
      setImmediate(() => runMarketplacePipeline());
      await logAuditEvent("pipeline_manual_trigger", {
        floId: req.verifiedFloId,
        ip: req.ip,
      });
      res.json({ success: true, message: "Pipeline triggered" });
    } catch (err) {
      console.error(err);
      res
        .status(500)
        .json({ success: false, error: "Pipeline trigger failed" });
    }
  },
);

// =====================================================================
// API: ADMIN SCORE
// =====================================================================

// Set (or clear) admin score for a property
app.post(
  "/api/properties/:propertyId/admin-score",
  secureEndpoint({
    fields: ["propertyId", "adminFloId", "score", "reason", "time"],
    floIdField: "adminFloId",
    requireAdmin: true,
    rateLimitOpts: { max: 30, windowMs: 60000 },
  }),
  async (req, res) => {
    const { propertyId } = req.params;
    const { score, reason } = req.body;

    const parsed = parseFloat(score);
    if (isNaN(parsed)) {
      return res
        .status(400)
        .json({ success: false, error: "score must be a number (positive to reward, negative to penalize, 0 to clear)" });
    }

    try {
      const result = await pool.query(
        `UPDATE properties
         SET admin_score = $1,
             admin_score_reason = $2,
             admin_score_updated_at = now()
         WHERE id = $3
         RETURNING id, name, admin_score, admin_score_reason, admin_score_updated_at`,
        [parsed, reason || null, propertyId],
      );

      if (result.rowCount === 0) {
        return res.status(404).json({ success: false, error: "Property not found" });
      }

      await logAuditEvent("admin_score_set", {
        floId: req.body.adminFloId,
        propertyId,
        score: parsed,
        reason: reason || null,
        ip: req.ip,
      });

      res.json({ success: true, property: result.rows[0] });
    } catch (err) {
      console.error("admin-score POST error:", err);
      res.status(500).json({ success: false, error: "Database error" });
    }
  },
);

// Get current admin score for a property
app.get("/api/properties/:propertyId/admin-score", async (req, res) => {
  const { propertyId } = req.params;
  try {
    const result = await pool.query(
      `SELECT id, name, admin_score, admin_score_reason, admin_score_updated_at
       FROM properties WHERE id = $1`,
      [propertyId],
    );
    if (result.rowCount === 0) {
      return res.status(404).json({ success: false, error: "Property not found" });
    }
    res.json({ success: true, property: result.rows[0] });
  } catch (err) {
    console.error("admin-score GET error:", err);
    res.status(500).json({ success: false, error: "Database error" });
  }
});

// Proxy Suno page to extract metadata (title, artist, image, lyrics, etc.)
// `video_url` and `media_urls` fields are the playable assets.
app.get(
  "/api/suno-resolve",
  rateLimit({ max: 60, windowMs: 60000 }),
  async (req, res) => {
    const input = String(req.query.url || "").trim();
    if (!input) {
      return res.status(400).json({ success: false, error: "Missing url" });
    }

    let parsed;
    try {
      parsed = new URL(input);
    } catch {
      return res.status(400).json({ success: false, error: "Invalid URL" });
    }

    if (
      !["suno.com", "www.suno.com"].includes(parsed.hostname.toLowerCase()) ||
      !(parsed.pathname.startsWith("/s/") || parsed.pathname.startsWith("/song/"))
    ) {
      return res.status(400).json({
        success: false,
        error: "Only Suno share/song links are supported",
      });
    }

    const cacheKey = parsed.href;
    const cached = sunoResolveCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < CACHE_DURATION) {
      return res.json({ success: true, ...cached.data, cached: true });
    }

    try {
      const response = await fetch(parsed.href, {
        redirect: "follow",
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        signal: AbortSignal.timeout(15000),
      });

      if (!response.ok) {
        throw new Error(`Suno page returned ${response.status}`);
      }

      const finalUrl = response.url;
      if (!["suno.com", "www.suno.com"].includes(new URL(finalUrl).hostname.toLowerCase())) {
        throw new Error("Suno redirected outside suno.com");
      }

      const html = await response.text();
      const decoded = html.replace(/\\"/g, '"').replace(/\\\//g, "/");
      const canonicalMatch = decoded.match(
        /<link rel="canonical" href="(https:\/\/suno\.com\/song\/[^"]+)"/i,
      );
      const videoMatch = decoded.match(
        /"video_url":"(https:\/\/[^"]+?\.mp4)"/i,
      );
      const mediaMatches = [
        ...decoded.matchAll(/"url":"(https:\/\/[^"]+?\.(?:m4a|mp3|mp4))"/gi),
      ].map((match) => match[1]);

      const videoUrl = videoMatch?.[1] || null;
      const audioUrl = mediaMatches.find((url) => /\.m4a(\?|$)/i.test(url)) || null;
      const mediaUrls = [...new Set([audioUrl, videoUrl, ...mediaMatches].filter(Boolean))];

      if (!audioUrl && !videoUrl) {
        throw new Error("Could not find an audio/video URL on the Suno page");
      }

      const songId =
        canonicalMatch?.[1]?.match(/song\/([a-f0-9-]{36})/i)?.[1] ||
        videoUrl?.match(/([a-f0-9-]{36})/i)?.[1] ||
        null;
      const titleMatch = decoded.match(/<title>([^<]*)<\/title>/i);

      const data = {
        songId,
        canonicalUrl: canonicalMatch?.[1] || finalUrl,
        title: titleMatch?.[1]?.replace(/&amp;/g, "&").trim() || null,
        audioUrl: audioUrl || videoUrl,
        videoUrl,
        mediaUrls,
      };

      cacheSet(sunoResolveCache, cacheKey, { data, timestamp: Date.now() });
      res.set("Cache-Control", "public, max-age=300");
      return res.json({ success: true, ...data, cached: false });
    } catch (err) {
      console.error("Suno resolve error:", err);
      return res.status(502).json({
        success: false,
        error: "Could not resolve the Suno song page",
      });
    }
  },
);

app.get("/api/suno-page-proxy", async (req, res) => {
  const url = req.query.url;
  if (!url) return res.status(400).send("Missing url");

  try {
    // Fetch the Suno page
    const response = await fetch(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      },
      signal: AbortSignal.timeout(10000),
    });

    if (!response.ok) {
      return res
        .status(response.status)
        .send(`Failed to fetch: ${response.status}`);
    }

    const html = await response.text();
    res.setHeader("Content-Type", "text/html");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.send(html);
  } catch (err) {
    console.error("Suno page proxy error:", err);
    res.status(500).send(`Proxy error: ${err.message}`);
  }
});

// =====================================================================
// SHUTDOWN
// =====================================================================

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

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled promise rejection:", reason);
});

process.on("uncaughtException", (err) => {
  console.error("Uncaught exception, shutting down:", err);
  shutdown();
});

console.log("Starting MusicMarketplace Oracle...");
app.listen(port, () => {
  console.log(`MusicMarketplace Oracle listening on port ${port}`);
});
