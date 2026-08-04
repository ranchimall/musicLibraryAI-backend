const express = require("express");
const cors = require("cors");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const fs = require("fs");

const app = express();
const port = process.env.PORT || 3000;
const sunoCache = new Map();
const CACHE_DURATION = 5 * 60 * 1000; // 5 minutes

const cacheCleanupInterval = setInterval(() => {
  const now = Date.now();

  for (const [url, entry] of sunoCache.entries()) {
    if (now - entry.timestamp > CACHE_DURATION) {
      sunoCache.delete(url);
    }
  }
}, 60 * 1000);

// Enable CORS for frontend
app.use(cors());
app.use(express.json());

// Initialize SQLite database for Native Platform Plays
const dataDir = path.join(__dirname, "data");
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir);
}
const dbPath = path.join(dataDir, "plays.db");
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error("Error opening database", err);
  } else {
    console.log("Connected to SQLite database at", dbPath);
    db.run(`CREATE TABLE IF NOT EXISTS plays (
            track_id TEXT PRIMARY KEY,
            play_count INTEGER DEFAULT 0
        )`);
    db.run(`CREATE TABLE IF NOT EXISTS likes (
    track_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    liked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(track_id, user_id)
)`);
  }
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
app.get("/api/platform-plays", (req, res) => {
  const trackId = req.query.id;

  if (!trackId || !trackId.trim()) {
    return res.status(400).json({
      success: false,
      error: "Missing track id",
    });
  }

  db.get(
    "SELECT play_count FROM plays WHERE track_id = ?",
    [trackId],
    (err, row) => {
      if (err) {
        console.error(err);
        return res.status(500).json({
          success: false,
          error: "Database error",
        });
      }
      res.json({
        success: true,
        playCount: row ? row.play_count : 0,
      });
    },
  );
});

// 3. GET /api/likes
app.get("/api/likes", (req, res) => {
  const trackId = req.query.id;
  const userId = req.query.user;

  if (!trackId || !trackId.trim() || !userId || !userId.trim()) {
    return res.status(400).json({
      success: false,
      error: "Missing track id or user id",
    });
  }

  db.get(
    "SELECT COUNT(*) AS likeCount FROM likes WHERE track_id = ?",
    [trackId],
    (err, countRow) => {
      if (err) {
        return res.status(500).json({
          success: false,
          error: "Database error",
        });
      }

      db.get(
        "SELECT 1 FROM likes WHERE track_id = ? AND user_id = ?",
        [trackId, userId],
        (err, likeRow) => {
          if (err) {
            return res.status(500).json({
              success: false,
              error: "Database error",
            });
          }

          res.json({
            success: true,
            likeCount: countRow.likeCount,
            liked: !!likeRow,
          });
        },
      );
    },
  );
});

// 4. POST /api/platform-plays
app.post("/api/platform-plays", (req, res) => {
  const trackId = req.query.id;

  if (!trackId || !trackId.trim()) {
    return res.status(400).json({
      success: false,
      error: "Missing track id",
    });
  }

  // Upsert logic: insert if not exists, else increment
  const stmt = db.prepare(`
        INSERT INTO plays (track_id, play_count) 
        VALUES (?, 1)
        ON CONFLICT(track_id) DO UPDATE SET play_count = play_count + 1
    `);

  stmt.run([trackId], function (err) {
    if (err) {
      stmt.finalize();

      console.error(err);
      return res.status(500).json({
        success: false,
        error: "Database error",
      });
    }

    db.get(
      "SELECT play_count FROM plays WHERE track_id = ?",
      [trackId],
      (err, row) => {
        stmt.finalize();

        if (err) {
          console.error(err);
          return res.status(500).json({
            success: false,
            error: "Database error",
          });
        }

        res.json({
          success: true,
          playCount: row.play_count,
        });
      },
    );
  });
});

// 5. POST /api/likes

app.post("/api/likes", (req, res) => {
  const trackId = req.query.id;
  const userId = req.query.user;

  if (!trackId || !trackId.trim() || !userId || !userId.trim()) {
    return res.status(400).json({
      success: false,
      error: "Missing track id or user id",
    });
  }

  db.get(
    "SELECT 1 FROM likes WHERE track_id = ? AND user_id = ?",
    [trackId, userId],
    (err, row) => {
      if (err) {
        return res.status(500).json({
          success: false,
          error: "Database error",
        });
      }

      const finish = (likedState) => {
        db.get(
          "SELECT COUNT(*) AS likeCount FROM likes WHERE track_id = ?",
          [trackId],
          (err, countRow) => {
            if (err) {
              return res.status(500).json({
                success: false,
                error: "Database error",
              });
            }

            res.json({
              success: true,
              liked: likedState,
              likeCount: countRow.likeCount,
            });
          },
        );
      };

      if (row) {
        db.run(
          "DELETE FROM likes WHERE track_id = ? AND user_id = ?",
          [trackId, userId],
          (err) => {
            if (err) {
              return res.status(500).json({
                success: false,
                error: "Database error",
              });
            }

            finish(false);
          },
        );
      } else {
        db.run(
          "INSERT INTO likes (track_id, user_id) VALUES (?, ?)",
          [trackId, userId],
          (err) => {
            if (err) {
              return res.status(500).json({
                success: false,
                error: "Database error",
              });
            }

            finish(true);
          },
        );
      }
    },
  );
});

// Start the server
console.log("Starting MusicMarketplace Oracle...");

app.listen(port, () => {
  console.log(`MusicMarketplace Oracle listening on port ${port}`);
});

function shutdown() {
  console.log("Closing database...");

  clearInterval(cacheCleanupInterval);

  db.close(() => {
    console.log("Database closed.");
    process.exit(0);
  });
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
