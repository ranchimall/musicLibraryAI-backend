// Loads floBlockchainAPI.js under Node and exposes helpers for
// verifying and sending FLO and USDAI marketplace payments.
//
// floBlockchainAPI.js is a browser bundle - it expects a global
// `floGlobals` config object and a global `floCrypto` to already exist
// before it loads, or it throws immediately. node-shims + lib.js below
// take care of everything else it needs (Crypto, BigInteger, bitjs...).
require("./node-shims");

if (typeof global.floGlobals === "undefined") {
  global.floGlobals = {
    blockchain: process.env.FLO_BLOCKCHAIN || "FLO",
    adminID: process.env.MARKETPLACE_FLO_ADDRESS || undefined,
    application: "music-library-backend",
  };
}

require("./lib"); // sets global Crypto/BigInteger/EllipticCurve/coinjs/bitjs
// floBlockchainAPI has no require() of its own - it just expects
// floCrypto to already be a global.
global.floCrypto = global.floCrypto || require("./floCrypto");

const floBlockchainAPI = require("./floBlockchainAPI");

const MARKETPLACE_FLO_ADDRESS = process.env.MARKETPLACE_FLO_ADDRESS;
const MARKETPLACE_FLO_PRIVATE_KEY = process.env.MARKETPLACE_FLO_PRIVATE_KEY;

const REQUIRED_CONFIRMATIONS = 1;

function getOutputAmount(tx, address) {
  let total = 0;

  for (const vout of tx.vout || []) {
    const addresses = vout.scriptPubKey?.addresses || [];

    if (addresses.includes(address)) {
      total += Number(vout.value || 0);
    }
  }

  return total;
}

// Resolves one input's spending address. Some Blockbook deployments put
// `addresses` right on the vin, but not all do, so fall back to looking
// up the previous tx and reading the address off the output it spent.
async function resolveInputAddress(vin) {
  if (vin.addresses && vin.addresses.length) {
    return vin.addresses[0];
  }
  if (!vin.txid || vin.vout === undefined) {
    return null;
  }
  try {
    const prevTx = await floBlockchainAPI.getTx(vin.txid);
    const prevOut = prevTx && prevTx.vout && prevTx.vout[vin.vout];
    const addrs =
      prevOut && prevOut.scriptPubKey && prevOut.scriptPubKey.addresses;
    return (addrs && addrs[0]) || null;
  } catch (err) {
    console.error(
      `Could not resolve input address for prevout ${vin.txid}:${vin.vout}:`,
      err,
    );
    return null;
  }
}

async function getSenders(tx) {
  const resolved = await Promise.all((tx.vin || []).map(resolveInputAddress));
  return [...new Set(resolved.filter(Boolean))];
}

async function verifyFloPayment(txid, requiredAmount, expectedSender) {
  if (!MARKETPLACE_FLO_ADDRESS) {
    throw new Error("Payments not configured (MARKETPLACE_FLO_ADDRESS unset)");
  }
  if (!txid || typeof txid !== "string") {
    throw new Error("Missing FLO transaction ID");
  }

  const tx = await floBlockchainAPI.getTx(txid);

  if (!tx) {
    throw new Error("FLO transaction not found");
  }

  // Must be confirmed.
  if (!tx.confirmations || tx.confirmations < REQUIRED_CONFIRMATIONS) {
    throw new Error(
      `FLO transaction is not confirmed yet (${tx.confirmations || 0} confirmations)`,
    );
  }

  // Payment must actually reach our marketplace address.
  const receivedAmount = getOutputAmount(tx, MARKETPLACE_FLO_ADDRESS);

  if (receivedAmount < Number(requiredAmount)) {
    throw new Error(
      `Insufficient FLO payment. Received ${receivedAmount}, required ${requiredAmount}`,
    );
  }

  const senders = await getSenders(tx);

  if (expectedSender && !senders.includes(expectedSender)) {
    throw new Error("FLO payment sender does not match the buyer");
  }

  return {
    valid: true,
    txid: tx.txid,
    confirmations: tx.confirmations,
    amount: receivedAmount,
    senders,
    receiver: MARKETPLACE_FLO_ADDRESS,
  };
}

async function sendFloPayment(destinationFloId, amount) {
  if (!destinationFloId) {
    throw new Error("Missing destination FLO address");
  }

  if (!MARKETPLACE_FLO_PRIVATE_KEY) {
    throw new Error("Marketplace FLO private key is not configured");
  }

  if (!amount || Number(amount) <= 0) {
    throw new Error("Invalid FLO payout amount");
  }

  const txid = await floBlockchainAPI.sendTx(
    MARKETPLACE_FLO_ADDRESS,
    destinationFloId,
    Number(amount),
    MARKETPLACE_FLO_PRIVATE_KEY,
  );

  return txid;
}

async function sendUsdaiPayment(destinationFloId, amount) {
  if (!destinationFloId) {
    throw new Error("Missing destination FLO address");
  }

  if (!MARKETPLACE_FLO_PRIVATE_KEY || !MARKETPLACE_FLO_ADDRESS) {
    throw new Error("Marketplace USDAI sender is not configured");
  }

  if (!amount || Number(amount) <= 0) {
    throw new Error("Invalid USDAI payout amount");
  }

  const tokenAmount = Number(amount).toFixed(10);
  const floData = `send ${tokenAmount} usdai#`;
  const sendAmt = floBlockchainAPI.sendAmt || 0.0003;

  const txid = await floBlockchainAPI.sendTx(
    MARKETPLACE_FLO_ADDRESS,
    destinationFloId,
    sendAmt,
    MARKETPLACE_FLO_PRIVATE_KEY,
    floData,
  );

  return txid;
}

module.exports = {
  verifyFloPayment,
  sendFloPayment,
  sendUsdaiPayment,
  MARKETPLACE_FLO_ADDRESS,
};
