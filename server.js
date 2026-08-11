const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
require("dotenv").config();

const app = express();
const port = process.env.PORT || 3000;
const sunoCache = new Map();
const flowCache = new Map();
const CACHE_DURATION = 5 * 60 * 1000; // 5 minutes

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

// Enable CORS for frontend
app.use(cors());
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

pool
  .connect()
  .then((client) => {
    console.log("Connected to Neon PostgreSQL");
    client.release();
  })
  .catch((err) => {
    console.error("Database connection failed:", err);
  });

// Helper: Scrape Suno Plays
async function fetchSunoPlayCount(inputUrl) {
  try {
    const response = await fetch(inputUrl, {
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
    const response = await fetch(inputUrl, {
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
app.get("/health", (req, res) => {
  res.json({
    success: true,
    status: "ok",
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
    sunoCache.set(targetUrl, {
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
    flowCache.set(targetUrl, {
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


// Start the server
console.log("Starting MusicMarketplace Oracle...");

app.listen(port, () => {
  console.log(`MusicMarketplace Oracle listening on port ${port}`);
});

async function shutdown() {
  console.log("Closing PostgreSQL pool...");

  clearInterval(cacheCleanupInterval);

  await pool.end();

  console.log("PostgreSQL pool closed.");

  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);