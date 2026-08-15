
require("./node-shims");
require("./lib");
const floCrypto = require("./floCrypto");

const MAX_SKEW_MS = 5 * 60 * 1000; // 5 minute replay window

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
//   to be from (defaults to "floId"; financing endpoints use
//   "financierFloId" instead).
function verifyFloSignature(fields, { floIdField = "floId" } = {}) {
  return (req, res, next) => {
    const body = req.body || {};
    const floId = body[floIdField];
    const pubKey = body.pubKey;
    const sign = body.sign;
    const time = body.time;

    if (!floId || !pubKey || !sign || !time) {
      return res.status(401).json({
        success: false,
        error: `Missing ${floIdField}, pubKey, sign, or time`,
      });
    }

    const skew = Math.abs(Date.now() - Number(time));
    if (!Number.isFinite(skew) || skew > MAX_SKEW_MS) {
      return res.status(401).json({
        success: false,
        error: "Stale or invalid timestamp",
      });
    }

    let pubKeyOk = false;
    try {
      pubKeyOk = !!floCrypto.verifyPubKey(pubKey, floId);
    } catch (e) {
      console.error("verifyPubKey error:", e);
    }
    if (!pubKeyOk) {
      return res.status(401).json({
        success: false,
        error: "pubKey does not match the given FLO ID",
      });
    }

    const source = { ...req.params, ...body };
    const hashcontent = buildHashcontent(fields, source);

    let sigOk = false;
    try {
      sigOk = !!floCrypto.verifySign(hashcontent, sign, pubKey);
    } catch (e) {
      console.error("verifySign error:", e);
    }
    if (!sigOk) {
      return res.status(401).json({
        success: false,
        error: "Invalid signature",
      });
    }

    // Downstream handlers can trust this now that it's cryptographically
    // verified as coming from floId.
    req.verifiedFloId = floId;
    next();
  };
}

module.exports = { verifyFloSignature };
