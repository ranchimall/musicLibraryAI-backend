# Music Library / Music Marketplace Backend

Backend API for the Music Library marketplace. It combines Suno/Google Flow
music metadata, library play and like tracking, a property marketplace with
signed FLO actions, USDAI financing and payments, an algorithmic pricing
pipeline, invitations, bundles, and Main Token liquidity.

The previous SQLite-only implementation has been replaced by PostgreSQL.

---

## Features

### Music and engagement
- Live Suno play-count scraping with a 5-minute in-memory cache
- Google Flow Music play-count scraping
- Native library play tracking
- Like/unlike and liked-track APIs
- User play/like summaries
- Shared page proxy, Suno media resolver, and upload parsing for both Suno and Google Flow Music
- Suno media resolution to the current `m4a` (audio-only) and `mp4` assets

### Marketplace
- Categories and properties (bundles of creative, people, finance, and task data)
- Scarcity, utility, consumption, and price calculation
- Top-N ranking, configurable at runtime
- Weakest-property bundling (default: 5 weakest properties)
- Creation, ranking, lifecycle, and finance-allocation pipeline
- Property detail with people, components, finance, tasks, usage events, and invitations
- Public marketplace overview with unique people counts and finance/royalty/task summaries
- Total consumption and consumption shares for the Top N

### People and membership
- People profiles
- Active/inactive property memberships
- Invitation-based joining with parts (`people`, `creative`, `finance`, `marketing`)
- Signed create, accept, decline, revoke, resend, and inbox flows
- Unread invitation counts and read receipts

### Pricing and token economy
- Weighted consumption formula (see below)
- Configurable consumption weights, Top N, bundle settings, and sell-queue period
- Admin score (bonus or penalty) on properties
- Main Token price, buy, sell, portfolio, and sell-queue APIs
- USDAI on-chain payment verification
- Platform liquidity and contributor payouts
- Exact `Consumption_Share`-based portfolio allocation by default

### Security
- FLO signature verification for writes
- Admin-gated management endpoints
- Timestamp skew and replay protection
- Per-IP authentication rate limiting
- Input validation and consistent JSON responses
- Audit log for important actions

---

## Tech Stack

- Node.js (>= 20)
- Express.js
- PostgreSQL (`pg`) — Neon-compatible
- CORS
- dotenv
- FLO cryptography and payment helpers

---

## Project Structure

```text
.
├── server.js                 # API, schema migrations, marketplace pipeline
├── marketplace-formulas.js   # Pure consumption/config/share helpers
├── flo-auth.js               # FLO signature verification and auth rate limiting
├── flo-chain.js              # FLO/USDAI payment helpers
├── floBlockchainAPI.js       # FLO chain API client
├── floCrypto.js              # FLO cryptography
├── lib.js / node-shims.js    # Runtime shims used by FLO tooling
├── package.json
└── README.md
```

---

## Quick Start

### 1. Install

```bash
npm install
```

### 2. Configure

Create a `.env` file in the project root. At minimum:

```env
PORT=3000
DATABASE_URL=postgresql://user:password@host/database?sslmode=require
```

### 3. Start

```bash
npm start
```

The server listens on `http://localhost:3000` by default.

Health check:

```text
GET /api/health
```

> A `503` is expected while `liquidityBuilt` is false; database, pipeline, and
> Top-N checks should still report healthy.

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---:|---|---|
| `PORT` | No | `3000` | HTTP port |
| `DATABASE_URL` | **Yes** | — | PostgreSQL connection string. Server exits without it |
| `ALLOWED_ORIGINS` | No | all | Comma-separated CORS origins |
| `ADMIN_FLO_IDS` | No | one built-in ID | Comma-separated admin FLO IDs |
| `MAX_AUTH_SKEW_MS` | No | `300000` | Allowed signed-request clock skew |
| `MAX_AUTH_REQUESTS_PER_MINUTE` | No | `60` | Signed requests per IP per minute |
| `DEBUG_AUTH` | No | `false` | Log successful signature verification |
| `PIPELINE_INTERVAL_MS` | No | `300000` | Marketplace pipeline frequency |
| `TOKEN_API_BASE_URL` | No | RanchiMall FLO API | FLO token API base URL |
| `USDAI_TOKEN_IDENTIFIER` | No | `usdai` | USDAI token identifier |
| `USDAI_PAYMENT_ADDRESS` | For payments | — | USDAI receiving address |
| `MARKETPLACE_FLO_ADDRESS` | For payments | — | Marketplace FLO address |
| `MARKETPLACE_FLO_PRIVATE_KEY` | For payouts | — | Marketplace payout key |
| `FLO_BLOCKCHAIN` | No | helper default | FLO blockchain endpoint |

Marketplace defaults are stored in `marketplace_config` and seeded from code:

| Key | Default |
|---|---:|
| `people_weight` | `10` |
| `task_weight` | `5` |
| `base_unit_value` | `1` |
| `top_n_properties` | `100` |
| `weak_bundle_enabled` | `true` |
| `weak_bundle_size` | `5` |
| `weak_bundle_name` | `Weakest Properties Bundle` |
| `sell_queue_period_days` | `7` |
| `portfolio_floor_share` | `0` |

Config can be read at `GET /api/marketplace/config` and changed by an admin
through `POST /api/marketplace/config`.

---

## Pricing Formulas

Consumption uses the weighted formula:

```text
Consumption =
  (Total Plays + Total Likes)
  + (People × people_weight)
  + (Tasks × task_weight)
  + Admin Score
```

Defaults: `people_weight = 10`, `task_weight = 5`.

```text
Base Price     = Consumption × base_unit_value
Current Price  = Base Price × (1 + Scarcity × 0.15) × (1 + Utility × 0.15)
```

Top-N totals and shares:

```text
Total_Consumption = Sum of Consumption of each property in Top N
Consumption_Share = Property_Consumption / Total_Consumption
```

Portfolio allocation uses the exact consumption shares by default. The old
`portfolio_floor_share` setting remains available but defaults to `0`.

The literal “price per unit = total USDAI in the pool / total units” formula is
**not implemented yet**; it is deferred.

---

## Authentication

Write endpoints use FLO signatures.

A signed request includes:

```json
{
  "floId": "F...",
  "pubKey": "...",
  "sign": "...",
  "time": 1700000000000
}
```

The backend builds a canonical `field|field|...` string from a per-endpoint
list of fields, verifies the public key matches the FLO ID, checks timestamp
skew and replay protection, and optionally requires an admin FLO ID.

Relevant settings:

- `ADMIN_FLO_IDS`
- `MAX_AUTH_SKEW_MS`
- `MAX_AUTH_REQUESTS_PER_MINUTE`

Admin-only actions include category/property management, finance components,
admin scores, config changes, pipeline runs, invitation create/resend/revoke,
and task winner selection.

---

## API Overview

### Health / music
```text
GET  /api/health
GET  /api/suno-plays?url=...
GET  /api/google-flow-plays?url=...
GET  /api/suno-page-proxy?url=...
GET  /api/suno-resolve?url=...
GET  /api/platform-plays?id=...
POST /api/platform-plays
GET  /api/likes
POST /api/likes
GET  /api/liked-tracks
GET  /api/user-stats
```

### Supported music links

The frontend accepts both platforms:

```text
Suno share link:  https://suno.com/s/<share-id>
Suno song link:   https://suno.com/song/<song-id>
Flow Music link:  https://www.flowmusic.app/song/<song-id>
```

The upload flow uses:

```text
GET /api/suno-page-proxy?url=<encoded music page URL>
```

Despite its legacy name, this is a shared page proxy used for both Suno and
Google Flow Music. It returns the page HTML; the frontend then applies the
platform-specific parser:

- **Suno:** canonical link, `video_url`, `media_urls`, metadata prompt/lyrics,
  title, artist, cover, play count.
- **Google Flow Music:** `og:audio`, `og:video`, `og:image`, `og:title`,
  `music:musician`, author metadata, and the hydrated clip’s
  `lyrics.value.text` (no external CORS proxy required for lyrics).

For Suno, the current media URLs can also be resolved server-side through
`/api/suno-resolve`. Flow Music media URLs come from the page’s Open Graph tags.

### Suno media resolution
```text
GET /api/suno-resolve?url=https://suno.com/s/<share-id>
```

Returns the current `m4a` audio-only URL, the `mp4` URL, canonical song URL,
song ID, title, and media list. Results are cached for five minutes.

The page’s `audio_url` is usually `/api/forbidden`; the resolver uses
`video_url` and `media_urls` instead.

### People
```text
GET  /api/people
GET  /api/people/:floId
POST /api/people
```

### Marketplace
```text
GET  /api/categories
POST /api/categories                    # admin
GET  /api/categories/:slug/components
GET  /api/categories/:slug/properties
GET  /api/properties?top100=true
POST /api/properties                    # admin
GET  /api/properties/:propertyId
POST /api/properties/:propertyId/components
POST /api/properties/:propertyId/components/:componentId/remove
POST /api/properties/:propertyId/people
POST /api/properties/:propertyId/people/:personFloId/remove
POST /api/properties/:propertyId/tasks
POST /api/properties/:propertyId/tasks/:taskId/remove
POST /api/properties/:propertyId/interest
POST /api/properties/:propertyId/admin-score
GET  /api/properties/:propertyId/admin-score
```

### Marketplace analytics
```text
GET /api/marketplace/overview
GET /api/marketplace/consumption
GET /api/marketplace/formulas
GET /api/marketplace/config
POST /api/marketplace/config            # admin
POST /api/marketplace/run-pipeline      # admin
```

### Invitations
```text
POST /api/properties/:propertyId/invitations              # admin
GET  /api/properties/:propertyId/invitations
POST /api/properties/:propertyId/invitations/:invitationId/revoke
POST /api/properties/:propertyId/invitations/:invitationId/resend
GET  /api/invitations/:token
POST /api/invitations/:token/accept
POST /api/invitations/:token/decline
POST /api/invitations/inbox
POST /api/invitations/unread-count
POST /api/invitations/mark-read
```

### Finance and tasks
```text
POST /api/properties/:propertyId/finance-components
GET  /api/properties/:propertyId/finance-components
DELETE /api/properties/:propertyId/finance-components/:componentId
GET  /api/properties/:propertyId/finance-allocations
GET  /api/tasks
POST /api/tasks
POST /api/tasks/:taskId/claim
POST /api/tasks/:taskId/submit
POST /api/tasks/:taskId/select-winner
GET  /api/tasks/:taskId/submissions
```

### Usage events
```text
POST   /api/properties/:propertyId/usage-events
GET    /api/properties/:propertyId/usage-events
POST   /api/properties/:propertyId/usage-events/:eventId   # delete event
```

### Main Token and liquidity
```text
GET  /api/main-token/price
GET  /api/main-token/payment-address
POST /api/main-token/buy
POST /api/main-token/sell
GET  /api/main-token/sell-queue
GET  /api/main-token/portfolio/:floId
GET  /api/marketplace/payment-address
```

### Admin
```text
GET  /api/admin/whoami
GET  /api/admin/pipeline-status
GET  /api/admin/orphans
POST /api/admin/resolve-orphan/:type/:id
```

---

## Database

The backend uses PostgreSQL and runs schema setup automatically on startup.
Important tables include:

- Privacy/identity: `people`
- Engagement: `plays`, `likes`, `play_audit_log`
- Catalog: `categories`, `tracks_components`, `properties`, `property_components`
- People/membership: `property_people`, `property_invitations`
- Tasks: `tasks`, `task_claims`, `task_submissions`, `property_tasks`
- Finance: `finance_components`, `royalty_splits`, `financing_positions`,
  `property_finance_allocations`, `property_payouts`
- Pricing: `property_valuation_history`, `property_bundle_members`
- Main Token: `main_token_balances`, `main_token_transactions`,
  `main_token_price_history`, `sell_queue`, `platform_liquidity`,
  `portfolio_positions`, `portfolio_snapshots`
- Misc: `property_interest`, `property_usage_events`, `audit_log`,
  `marketplace_config`

Schema changes are applied with `CREATE TABLE IF NOT EXISTS` and
`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, making startup migrations
idempotent.

---

## Marketplace Pipeline

The pipeline runs every five minutes by default (`PIPELINE_INTERVAL_MS`). It:

1. Scores populated properties for scarcity, utility, consumption, and price.
2. Updates property rows and valuation history.
3. Ranks the Top N (global and independent categories).
4. Creates or updates the weakest-properties bundle when the Top N is full.
5. Records Main Token price.
6. Runs finance allocation, sell-queue expiry/release, portfolio rebalancing,
   and contributor payouts.

A manual run is available to admins:

```text
POST /api/marketplace/run-pipeline
```

---

## External Playback Notes (Suno + Google Flow)

- The backend does not currently use an object-storage bucket (R2/S3) for audio.
- `POST /api/platform-plays` tracks native plays.
- Suno tracks can be resolved with `/api/suno-resolve`, which returns fresh
  Suno CDN media URLs.
- Google Flow Music tracks use the audio/video URLs exposed in the song page’s
  Open Graph metadata through the shared page proxy.
- The UI plays the audio-only M4A first and falls back to the MP4 if the M4A
  cannot be decoded.
- Suno CDN URLs can change; the song page URL is the durable reference.
- Suno and Flow Music terms and royalty obligations still apply.

The UI also uses client-side `mailto:`/`sms:` handoffs for invitation delivery;
there is no server-side email/SMS provider configured.

---

## Deployment

### Vercel / Node host

- Runtime: Node.js 20+
- Build: `npm install`
- Start: `npm start`
- Required env: `DATABASE_URL`
- Add FLO/USDAI/admin env values for write and payment features.
- Run behind HTTPS; FLO signing and browser clipboard APIs expect a secure
  context in production.

Production backend:

```text
https://musiclibraryai-backend.vercel.app
```

Frontend API base URL is defined in the frontend’s `scripts/config.js`.

---

## Known Limitations

- Pool-price-per-unit formula is deferred.
- No R2/S3/object-storage integration; Suno/Flow CDN URLs are the media source.
- No server-side email/SMS delivery provider.
- Suno/Flow page structures and media URLs are external and may change.
- Weakness bundles currently have no category, so they appear in the global
  property list rather than category pages.
- Some legacy data/tables from earlier SQLite/FLO versions may remain in an
  existing database; current code targets PostgreSQL.

---

## License

Part of the Music Library platform.
