// Pure marketplace formula/config helpers.


function envNumber(name, fallback) {
  const parsed = Number.parseFloat(process.env[name]);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function envInt(name, fallback) {
  const parsed = Number.parseInt(process.env[name], 10);
  return Number.isInteger(parsed) ? parsed : fallback;
}

function envBool(name, fallback) {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  return ["1", "true", "yes", "on"].includes(String(raw).toLowerCase());
}

const DEFAULT_MARKETPLACE_CONFIG = Object.freeze({
  // Sell queue lifetime.  Entries older than this are expired/cleared.
  sell_queue_period_days: envNumber("SELL_QUEUE_PERIOD_DAYS", 7),

  // Ranking / bundle settings.
  top_n_properties: envInt("TOP_N_PROPERTIES", 100),
  weak_bundle_enabled: envBool("WEAK_BUNDLE_ENABLED", true),
  weak_bundle_size: envInt("WEAK_BUNDLE_SIZE", 5),
  weak_bundle_name:
    process.env.WEAK_BUNDLE_NAME || "Weakest Properties Bundle",

  // (plays + likes) + (people x people_weight) + (tasks x task_weight)
  // + admin score.
  people_weight: envNumber("PEOPLE_WEIGHT", 10),
  task_weight: envNumber("TASK_WEIGHT", 5),

  base_unit_value: envNumber("BASE_UNIT_VALUE", 1),
  portfolio_floor_share: envNumber("PORTFOLIO_FLOOR_SHARE", 0),
});

const CONFIG_KEYS = Object.keys(DEFAULT_MARKETPLACE_CONFIG);

const NUMERIC_CONFIG_KEYS = new Set([
  "sell_queue_period_days",
  "top_n_properties",
  "weak_bundle_size",
  "people_weight",
  "task_weight",
  "base_unit_value",
  "portfolio_floor_share",
]);

const BOOLEAN_CONFIG_KEYS = new Set(["weak_bundle_enabled"]);
const STRING_CONFIG_KEYS = new Set(["weak_bundle_name"]);

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

/**
 * Validate/normalise one config value.  Throws on an unknown key or a value
 * that cannot be coerced to the key's type.
 */
function normalizeConfigValue(key, value) {
  if (!CONFIG_KEYS.includes(key)) {
    throw new Error(`Unknown marketplace config key: ${key}`);
  }

  if (BOOLEAN_CONFIG_KEYS.has(key)) {
    if (typeof value === "boolean") return value;
    const text = String(value).toLowerCase();
    if (["1", "true", "yes", "on"].includes(text)) return true;
    if (["0", "false", "no", "off"].includes(text)) return false;
    throw new Error(`${key} must be a boolean`);
  }

  if (NUMERIC_CONFIG_KEYS.has(key)) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) {
      throw new Error(`${key} must be a finite number`);
    }

    if (
      key === "sell_queue_period_days" ||
      key === "base_unit_value"
    ) {
      if (parsed <= 0) throw new Error(`${key} must be greater than 0`);
      return parsed;
    }

    if (key === "top_n_properties" || key === "weak_bundle_size") {
      if (!Number.isInteger(parsed) || parsed < 1) {
        throw new Error(`${key} must be a positive integer`);
      }
      return key === "top_n_properties" ? clamp(parsed, 1, 10000) : clamp(parsed, 2, 10000);
    }

    if (key === "portfolio_floor_share") {
      return clamp(parsed, 0, 1);
    }

    // people_weight / task_weight
    if (parsed < 0) throw new Error(`${key} must be >= 0`);
    return parsed;
  }

  if (STRING_CONFIG_KEYS.has(key)) {
    const text = String(value ?? "").trim();
    if (!text) throw new Error(`${key} cannot be empty`);
    if (key === "weak_bundle_name" && text.length > 120) {
      throw new Error("weak_bundle_name must be 120 characters or less");
    }
    return text;
  }

  return value;
}

/** Merge raw DB/env values over the defaults and normalise everything. */
function normalizeMarketplaceConfig(raw = {}) {
  const merged = { ...DEFAULT_MARKETPLACE_CONFIG };
  for (const key of CONFIG_KEYS) {
    if (raw[key] !== undefined && raw[key] !== null) {
      try {
        merged[key] = normalizeConfigValue(key, raw[key]);
      } catch {
        // Keep the default when a stored row is invalid.
      }
    }
  }
  return merged;
}

function safeNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

/**
 * consumption formula:
 *   plays + likes
 *   + people * people_weight
 *   + tasks * task_weight
 *   + admin_score
 */
function computeConsumption(metrics, config = DEFAULT_MARKETPLACE_CONFIG) {
  const cfg = config || DEFAULT_MARKETPLACE_CONFIG;

  const plays = Math.max(0, safeNumber(metrics.plays));
  const likes = Math.max(0, safeNumber(metrics.likes));
  const people = Math.max(0, safeNumber(metrics.people));
  const tasks = Math.max(0, safeNumber(metrics.tasks));
  const adminScore = safeNumber(metrics.adminScore);

  const raw =
    plays +
    likes +
    people * safeNumber(cfg.people_weight, 10) +
    tasks * safeNumber(cfg.task_weight, 5) +
    adminScore;

  // Admin penalties can push the score below zero; clamp at zero so the
  // per-unit price never becomes negative.
  return Math.max(raw, 0);
}

function computeBasePrice(consumption, config = DEFAULT_MARKETPLACE_CONFIG) {
  const cfg = config || DEFAULT_MARKETPLACE_CONFIG;
  return safeNumber(consumption) * safeNumber(cfg.base_unit_value, 1);
}

/**
 * Total_Consumption = sum(consumption of every property in the Top N)
 * Consumption_Share = property consumption / Total_Consumption
 */
function computeConsumptionShares(properties, totalConsumption) {
  const rows = Array.isArray(properties) ? properties : [];
  const total =
    totalConsumption === undefined
      ? rows.reduce((sum, p) => sum + safeNumber(p.consumption), 0)
      : safeNumber(totalConsumption);

  return {
    totalConsumption: total,
    properties: rows.map((property) => ({
      ...property,
      consumption: safeNumber(property.consumption),
      consumptionShare: total > 0 ? safeNumber(property.consumption) / total : 0,
    })),
  };
}

function buildFormulaDescription(config = DEFAULT_MARKETPLACE_CONFIG) {
  const cfg = normalizeMarketplaceConfig(config);
  const topN = cfg.top_n_properties;

  return {
    mode: "weighted",
    consumption: `Consumption = (Total Plays + Total Likes) + (People * ${cfg.people_weight}) + (Tasks * ${cfg.task_weight}) + Admin Score`,
    playsDefinition:
      "Total Plays = native platform plays only. Suno/Flow source play counts are tracked separately.",
    likesDefinition:
      "Total Likes = distinct user likes on the attached tracks.",
    peopleDefinition:
      "People = distinct active members on the property (multiple roles count once).",
    tasksDefinition:
      "Tasks = all tasks linked to the property, regardless of task status.",
    adminScoreDefinition:
      "Admin Score = additive admin bonus (positive), penalty (negative), or 0.",
    basePrice: `Base Price = Consumption * ${cfg.base_unit_value} USDAI`,
    currentPrice:
      "Current Price = Base Price * (1 + Scarcity * 0.15) * (1 + Utility * 0.15)",
    scarcity:
      "Scarcity = clamp(unique want-to-buy / max(unique want-to-sell, 1), 0.1, 10) over the last 7 days.",
    utility:
      "Utility = sum(usage event count × event weight) + log10(1 + platform plays) × 0.5.",
    usageWeights: {
      licensed_media: 3,
      public_infrastructure: 4,
      product_integration: 4,
      rights_lease_as_payment: 3,
      embed: 1,
      external_play: 0.5,
    },
    totalConsumption: `Total_Consumption = Sum of Consumption of each property in Top ${topN}`,
    topNConfig: `Top N is configurable; current value = ${topN}.`,
    consumptionShare:
      "Consumption_Share = Property_Consumption / Total_Consumption",
    bundle:
      "Weakest bundle = sum of its member properties' consumption; its share is the sum of their shares.",
    poolPricePerUnit:
      "Price per unit = Total USDAI in the pool / Total number of units",
    poolPriceStatus: "deferred",
    weakBundle: {
      enabled: cfg.weak_bundle_enabled,
      size: cfg.weak_bundle_size,
      name: cfg.weak_bundle_name,
    },
    sellQueuePeriodDays: cfg.sell_queue_period_days,
  };
}

module.exports = {
  DEFAULT_MARKETPLACE_CONFIG,
  CONFIG_KEYS,
  normalizeConfigValue,
  normalizeMarketplaceConfig,
  computeConsumption,
  computeBasePrice,
  computeConsumptionShares,
  buildFormulaDescription,
};
