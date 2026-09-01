require("./node-shims");
require("./lib");
const floCrypto = require("./floCrypto");

const MAX_SKEW_MS = parseInt(process.env.MAX_AUTH_SKEW_MS) || 5 * 60 * 1000; // 5 minute replay window

// Shared rate limiting state (exported for server.js to access)
const authAttempts = new Map();
const bannedIPs = new Map();

// Cleanup old entries periodically
setInterval(() => {
  const now = Date.now();
  for (const [key, attempts] of authAttempts) {
    const recent = attempts.filter((t) => now - t < 60000);
    if (recent.length === 0) {
      authAttempts.delete(key);
    } else {
      authAttempts.set(key, recent);
    }
  }
  for (const [ip, expiry] of bannedIPs) {
    if (now > expiry) {
      bannedIPs.delete(ip);
    }
  }
}, 60000);

// Rate limiting middleware for auth failures
function rateLimitAuth(req, res, next) {
  const key = `${req.ip}:auth`;
  const now = Date.now();

  // Check if IP is banned
  const banExpiry = bannedIPs.get(req.ip);
  if (banExpiry && now < banExpiry) {
    console.warn(
      `Blocked banned IP ${req.ip} until ${new Date(banExpiry).toISOString()}`,
    );
    return res.status(403).json({
      success: false,
      error: "IP temporarily blocked due to excessive authentication failures",
      blockExpires: new Date(banExpiry).toISOString(),
    });
  }

  // Rate limit attempts
  const attempts = authAttempts.get(key) || [];
  const recent = attempts.filter((t) => now - t < 60000); // 1 minute window

  if (recent.length >= 5) {
    // Ban for 1 hour after 5 failures in 1 minute
    bannedIPs.set(req.ip, now + 3600000);
    authAttempts.delete(key);
    console.warn(
      `IP ${req.ip} banned for 1 hour due to ${recent.length} auth failures in 1 minute`,
    );
    return res.status(429).json({
      success: false,
      error: "Too many authentication attempts. IP blocked for 1 hour.",
    });
  }

  recent.push(now);
  authAttempts.set(key, recent);
  next();
}

// Missing/undefined fields canonicalize to "" on both sides - the client
// must do the same when building its hashcontent, or signatures won't
// match for requests that omit an optional field.
function buildHashcontent(fields, source) {
  return fields
    .map((f) => {
      const v = source[f];
      return v === undefined || v === null ? "" : String(v);
    })
    .join("|");
}

// fields: ordered list of field names (drawn from req.params and/or
//   req.body) that make up the signed canonical string.
// floIdField: which body field holds the FLO ID the signature is claimed
//   to be from (defaults to "floId"; endpoints may override this when
//   the authenticated FLO ID is provided under a different field).
function verifyFloSignature(fields, { floIdField = "floId" } = {}) {
  return async (req, res, next) => {
    const body = req.body || {};
    const floId = body[floIdField];
    const pubKey = body.pubKey;
    const sign = body.sign;
    const time = body.time;

    // Validate floIdField configuration
    if (!floIdField || typeof floIdField !== "string") {
      console.error(`Invalid floIdField configuration: ${floIdField}`);
      return res.status(500).json({
        success: false,
        error: "Server configuration error",
      });
    }

    // Validate timestamp format
    const timeNum = Number(time);
    if (isNaN(timeNum) || !isFinite(timeNum)) {
      console.warn(`Invalid timestamp format from ${req.ip}:`, {
        time,
        path: req.path,
        floId: floId || "unknown",
      });
      return res.status(401).json({
        success: false,
        error: "Invalid timestamp format",
      });
    }

    // Check timestamp skew with logging
    const skew = Math.abs(Date.now() - timeNum);

    if (skew > MAX_SKEW_MS) {
      console.warn(`Stale timestamp from ${req.ip}:`, {
        skew: Math.round(skew / 1000) + "s",
        maxAllowed: Math.round(MAX_SKEW_MS / 1000) + "s",
        path: req.path,
        floId,
      });
      return res.status(401).json({
        success: false,
        error: "Stale or invalid timestamp",
      });
    }

    // Verify required fields
    if (!floId || !pubKey || !sign) {
      console.warn(`Missing auth fields from ${req.ip}:`, {
        hasFloId: !!floId,
        hasPubKey: !!pubKey,
        hasSign: !!sign,
        path: req.path,
      });
      return res.status(401).json({
        success: false,
        error: `Missing ${floIdField}, pubKey, or sign`,
      });
    }

    // Verify pubKey matches floId with detailed error logging
    let pubKeyOk = false;
    try {
      pubKeyOk = !!floCrypto.verifyPubKey(pubKey, floId);
    } catch (e) {
      console.error(`PubKey verification error for ${floId} from ${req.ip}:`, {
        error: e.message,
        path: req.path,
      });
    }

    if (!pubKeyOk) {
      console.warn(`PubKey mismatch for ${floId} from ${req.ip}:`, {
        pubKeyPrefix: pubKey.substring(0, 20) + "...",
        path: req.path,
      });
      return res.status(401).json({
        success: false,
        error: "pubKey does not match the given FLO ID",
      });
    }

    // Verify signature with detailed logging
    const source = { ...req.params, ...body };
    const hashcontent = buildHashcontent(fields, source);

    let sigOk = false;
    try {
      sigOk = !!floCrypto.verifySign(hashcontent, sign, pubKey);
    } catch (e) {
      console.error(
        `Signature verification error for ${floId} from ${req.ip}:`,
        {
          error: e.message,
          path: req.path,
        },
      );
    }

    if (!sigOk) {
      console.warn(`Invalid signature for ${floId} from ${req.ip}:`, {
        hashcontent: hashcontent.substring(0, 50) + "...",
        path: req.path,
      });
      return res.status(401).json({
        success: false,
        error: "Invalid signature",
      });
    }

    // Log successful auth (debug level in production)
    if (process.env.DEBUG_AUTH === "true") {
      console.log(`Auth success: ${floId} from ${req.ip}`, {
        path: req.path,
        timestamp: new Date().toISOString(),
      });
    }

    // Store verified info for downstream
    req.verifiedFloId = floId;
    req.authTimestamp = timeNum;
    req.authPubKey = pubKey;
    req.authFields = fields;
    req.hashcontent = hashcontent;

    next();
  };
}

module.exports = {
  verifyFloSignature,
  rateLimitAuth,
  authAttempts,
  bannedIPs,
};
