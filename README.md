# Music Library Backend

Backend service for the Music Library platform.

This service provides REST APIs for:

- Fetching live Suno play counts
- Tracking native library play counts
- Caching Suno play counts for improved performance
- Storing library play counts using SQLite

---

## Features

- Live Suno play count scraping
- 5-minute in-memory caching for Suno play counts
- Native library play count tracking
- SQLite database with automatic initialization
- Atomic play count updates using SQLite UPSERT
- Input validation
- Consistent JSON API responses
- Graceful server shutdown

---

## Tech Stack

- Node.js
- Express.js
- SQLite3
- CORS

---

## Installation

Clone the repository:

```bash
git clone https://github.com/ranchimall/musicLibraryAI-backend.git
cd musicLibraryAI-backend
```

Install dependencies:

```bash
npm install
```

Start the server:

```bash
npm start
```

The server will start on:

```
http://localhost:3000
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3000` | Port used by the server |

Example:

```bash
PORT=5000 npm start
```

---

## API Endpoints

### Get Live Suno Play Count

Fetches the latest play count from a Suno song.

**Endpoint**

```
GET /api/suno-plays
```

**Query Parameters**

| Parameter | Description |
|-----------|-------------|
| `url` | Suno song URL |

Example:

```
GET /api/suno-plays?url=https://suno.com/s/xxxxxxxx
```

Response:

```json
{
  "success": true,
  "playCount": 25431,
  "cached": false
}
```

If the play count is served from cache:

```json
{
  "success": true,
  "playCount": 25431,
  "cached": true
}
```

---

### Get Library Play Count

Returns the native play count stored by the library.

**Endpoint**

```
GET /api/platform-plays
```

**Query Parameters**

| Parameter | Description |
|-----------|-------------|
| `id` | Track ID |

Example:

```
GET /api/platform-plays?id=track-id
```

Response:

```json
{
  "success": true,
  "playCount": 42
}
```

---

### Increment Library Play Count

Increments the native library play count by one.

**Endpoint**

```
POST /api/platform-plays
```

**Query Parameters**

| Parameter | Description |
|-----------|-------------|
| `id` | Track ID |

Example:

```
POST /api/platform-plays?id=track-id
```

Response:

```json
{
  "success": true,
  "playCount": 43
}
```

---

## Project Structure

```
musicLibraryAI-backend/
│
├── data/
│   └── plays.db
├── server.js
├── package.json
├── package-lock.json
├── README.md
└── .gitignore
```

---

## Database

The backend automatically creates a SQLite database on startup.

Table:

```
plays
-----------------------------
track_id     TEXT PRIMARY KEY
play_count   INTEGER DEFAULT 0
```

Library play counts are updated using SQLite's UPSERT feature, ensuring atomic updates.

---

## Caching

To reduce requests to Suno, play counts are cached in memory.

- Cache duration: **5 minutes**
- Expired cache entries are cleaned automatically every minute.

---

## Deployment

This backend can be deployed to any Node.js hosting provider.

Supported platforms include:

- Render
- Railway
- Fly.io
- VPS
- Docker

### Render Configuration

**Runtime**

```
Node
```

**Build Command**

```bash
npm install
```

**Start Command**

```bash
npm start
```

---

## API Base URL

### Local Development

```
http://localhost:3000
```

### Production

Replace with your deployed backend URL:

```
https://your-render-app.onrender.com
```

---

## Notes

- Library play counts are stored in SQLite.
- Suno play counts are fetched live and cached temporarily.
- Cached Suno play counts expire automatically after five minutes.
- On platforms with ephemeral storage (such as Render's free tier), the SQLite database may be reset after a service restart. For production deployments, use a persistent database such as PostgreSQL if permanent storage is required.

---

## License

This project is part of the Music Library platform.