// Minimal browser-global shims required so RanchiMall's lib.js (a
// browser-oriented bundle) can be loaded under Node without crashing.
//
// lib.js does some *supplemental* entropy collection at load time by
// reading screen/navigator/history/location details (screen size, user
// agent, plugin list, etc.) and mixing them into its PRNG pool. None of
// that is the actual source of cryptographic randomness - the real
// randomness (getRandomBytes / securedMathRandom, used for key
// generation and ECDSA nonces) already comes from Node's
// crypto.randomBytes when `require` is available (see lib.js's own
// getRandomBytes/securedMathRandom definitions). These shims exist only
// so the property reads on these fake browser globals don't throw -
// they do not weaken the randomness actually used for anything security
// sensitive.
//
// Must be required before lib.js.

if (typeof global.screen === "undefined") {
  global.screen = {
    height: 0,
    width: 0,
    colorDepth: 0,
    availHeight: 0,
    availWidth: 0,
    pixelDepth: 0,
  };
}

// Node 21+ ships a partial built-in `navigator` global (e.g. just
// `userAgent`) - lib.js expects the fuller browser shape
// (plugins/mimeTypes/etc.), so patch in whatever's missing rather than
// assuming navigator is either fully absent or fully browser-shaped.
{
  const needed = {
    userAgent: "node",
    plugins: [],
    mimeTypes: [],
    cookieEnabled: false,
    language: "en",
  };
  if (typeof global.navigator === "undefined") {
    global.navigator = needed;
  } else {
    for (const [key, value] of Object.entries(needed)) {
      if (typeof global.navigator[key] === "undefined") {
        try {
          global.navigator[key] = value;
        } catch (e) {
          // Some Node versions expose navigator properties as read-only
          // getters - safe to ignore, lib.js only reads these values.
        }
      }
    }
  }
}

if (typeof global.history === "undefined") {
  global.history = { length: 0 };
}

if (typeof global.location === "undefined") {
  global.location = "";
}

if (typeof global.sessionStorage === "undefined") {
  global.sessionStorage = undefined;
}

if (typeof global.localStorage === "undefined") {
  global.localStorage = undefined;
}

if (typeof global.alert === "undefined") {
  global.alert = () => {};
}
