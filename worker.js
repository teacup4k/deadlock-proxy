// ============================================================
// GLOBAL CONFIGURATION & TUNING
// Optimized for Cache-First Architecture (~10 players)
//
// FIXES APPLIED (see inline comments marked FIX):
// 1. KV writes are now awaited instead of fire-and-forget. Un-awaited
//    put() calls could get silently cancelled when the Worker's
//    execution context ended (e.g. right after Promise.allSettled in
//    the cron job, or right after a Response was returned), leading to
//    partial/inconsistent KV state across edge isolates - which showed
//    up as stats "randomly changing" depending on which node served
//    the request.
// 2. Failed fetches (403/400/etc with no stale fallback) are now
//    negative-cached for a short TTL instead of never being cached at
//    all. Previously every single incoming request for a permanently
//    failing account (private profile, bad id, etc.) triggered a live
//    upstream fetch, eating into the shared CONCURRENCY/rate-limit
//    budget and occasionally causing *other*, working accounts to get
//    rate-limited too.
//
// NEW (this revision):
// 3. Deadlock shipped "Beta Season 1" ranked mode on July 30, 2026 -
//    a full matchmaking overhaul with seasonal Rank Points, calibration,
//    and loss protection, replacing the old simple badge system. The
//    Deadlock API added a dedicated GET /v1/players/{account_id}/rank
//    endpoint for this. That endpoint is now wired into the same
//    cache-first pipeline as card/stats/mates/history:
//      - New CONFIG.TTL.RANK + `rank:${id}` cache key
//      - Fetched in the cron job (forced) and on-demand in player mode
//      - normalizeRankData() merges it with legacy /card rank fields
//        so nothing breaks if the new endpoint is ever empty/down
//      - Squad rank leaderboard now sorts off the new endpoint first
//    NOTE: I could not get the exact JSON field names for /rank from
//    the live docs (the Scalar docs page is client-side rendered, so
//    my fetch tool only sees an empty shell). normalizeRankData() tries
//    several plausible field spellings (rank/ranked_rank, subrank/
//    sub_rank/division, rank_points/points/rp/score/mmr) and always
//    keeps the raw payload in the response under `raw` so you can
//    confirm the real shape from a live request and I can tighten the
//    field list in a follow-up edit if any guesses are wrong.
// ============================================================
const CONFIG = {
  // Was hardcoded `true`, which logs every single request/fetch/retry to the
  // Workers console permanently (extra noise + counts against your Workers
  // Logs volume). Flip this back to `true` only while you're actively
  // debugging, then set it back to `false`.
  DEBUG: false,

  // NEW: bump this any time the *compiled leaderboard* payload shape changes
  // (fields added/removed/renamed). The leaderboard cache keys below embed
  // this version, so a deploy that changes the shape gets a brand new KV/
  // memory key instead of serving an old-shaped payload that's technically
  // still "fresh" by TTL. Without this, KV persists across deploys - a
  // leaderboard_compiled:rank entry written by yesterday's code can sit
  // there for up to CONFIG.TTL.LEADERBOARD (10 min) after a deploy, serving
  // the old shape (e.g. ranked_badge_level instead of rank_points/season)
  // even though the new code is already running.
  CACHE_VERSION: "v21",

  CONCURRENCY: 8,              
  REQUEST_DELAY_MS: 100,       
  
  MAX_RETRIES: 3,              
  
  TTL_JITTER_MAX_MS: 30000,    
  REFRESH_WINDOW_PCT: 0.15,    
  
  TTL: {
    CARD: 15 * 60 * 1000,      
    STATS: 15 * 60 * 1000,     // RESTORED: 15 minutes for account-stats
    MATES: 15 * 60 * 1000,     
    HISTORY: 2 * 60 * 1000,    
    LEADERBOARD: 10 * 60 * 1000,
    RANK: 10 * 60 * 1000,      // NEW: seasonal rank moves faster than the old badge did (±250 RP/game), so refresh a bit more often than CARD
    ITEM_ASSETS: 24 * 60 * 60 * 1000, // NEW: item id->name/image static data only changes on patches, so this refreshes once a day instead of per-request
    ERROR: 90 * 1000           // FIX: how long to remember a failed fetch before retrying it again
  },

  // NEW / EXPERIMENTAL: /match-history returned slightly fewer matches than
  // account_stats.matches_played for a real account (276 vs 279) - the docs
  // site is JS-rendered so the real pagination params (if any) couldn't be
  // confirmed. This tries a generous `limit` query param on the request; if
  // the API doesn't recognize it, it's simply ignored and behavior is
  // unchanged. Worth checking ranked_eligibility.matches_seen vs
  // account_stats.matches_played after deploying to see if the gap closes.
  MATCH_HISTORY_LIMIT: 1000,
};

// ============================================================
// GLOBAL STATE (Memory Cache & Deduplication)
// ============================================================
const memoryCache = new Map();
const pendingRequests = new Map();
const localQueue = [];
let localActiveRequests = 0;

// ============================================================
// STATIC DICTIONARIES (BotGhost Mandatory)
// ============================================================
const SQUAD_MAP = {
  "1226015871": "teacup", "1002625397": "zes", "1530178825": "SamaBv",
  "771748301": "ali", "1117534927": "elyas", "999088547": "Saston"
};
const squadIds = Object.keys(SQUAD_MAP);

// NEW: each squad member's "identity" hero - a fixed personal icon used to
// represent them instead of their name in compact displays. Hero IDs match
// HERO_DICTIONARY above (65=Venator, 3=Vindicta, 63=Mina, 80=Silver,
// 50=Pocket, 69=The Doorman).
const SQUAD_IDENTITY_HEROES = {
  "771748301": 65,  // ali - Venator
  "999088547": 3,   // Saston - Vindicta
  "1117534927": 63, // elyas - Mina
  "1530178825": 80, // SamaBv - Silver
  "1226015871": 50, // teacup - Pocket
  "1002625397": 69  // zes - The Doorman
};

function squadIdentityEmoji(accountId) {
  const heroId = SQUAD_IDENTITY_HEROES[accountId];
  return heroId ? heroEmojiTag(heroId) : (SQUAD_MAP[accountId] || "?");
}

// NEW: Discord user IDs per squad member, for @mentioning them directly in
// messages (e.g. the weekly recap) instead of showing plain nicknames.
const SQUAD_DISCORD_IDS = {
  "1530178825": "479437528865439744", // sama
  "1226015871": "598794596226433024", // teacup
  "1002625397": "507986862166638592", // zes
  "1117534927": "685240365472940103", // elyas
  "771748301": "358366157981483049",  // ali
  "999088547": "1389410766637240381"  // Saston
};

// Reverse lookup (nickname -> account_id), needed for Power Duo since that
// category only tracks nicknames internally, not account_ids, until the
// final mention lookup.
const NICKNAME_TO_ACCOUNT_ID = Object.fromEntries(
  Object.entries(SQUAD_MAP).map(([id, name]) => [name, id])
);

function discordMention(accountId) {
  const discordId = SQUAD_DISCORD_IDS[accountId];
  return discordId ? `<@${discordId}>` : null;
}

const SQUAD_PHOTOS = {
  "1530178825": "https://raw.githubusercontent.com/teacup4k/deadlock-proxy/refs/heads/main/Silver_Gloat.png",
  "1226015871": "https://raw.githubusercontent.com/teacup4k/deadlock-proxy/refs/heads/main/Pocket_Gloat.png",
  "1002625397": "https://raw.githubusercontent.com/teacup4k/deadlock-proxy/refs/heads/main/Doorman_card.png",
  "771748301": "https://raw.githubusercontent.com/teacup4k/deadlock-proxy/refs/heads/main/Venator_Critical_Health.png",
  "1117534927": "https://raw.githubusercontent.com/teacup4k/deadlock-proxy/refs/heads/main/Mina_card.png",
  "999088547": "https://raw.githubusercontent.com/teacup4k/deadlock-proxy/refs/heads/main/Vindicta_Critical.png"
};

// TODO: set this to a real fallback avatar you host (e.g. one of your GitHub raw
// images). Left as null for now so a missing photo is an explicit absence rather
// than a guaranteed-dead link.
const DEFAULT_PHOTO_URL = null;

const HERO_DICTIONARY = {
  1: "Infernus", 2: "Seven", 3: "Vindicta", 4: "Lady Geist", 6: "Abrams", 7: "Wraith",
  8: "McGinnis", 10: "Paradox", 11: "Dynamo", 12: "Kelvin", 13: "Haze", 14: "Holliday",
  15: "Bebop", 16: "Calico", 17: "Grey Talon", 18: "Mo & Krill", 19: "Shiv", 20: "Ivy",
  25: "Warden", 27: "Yamato", 31: "Lash", 35: "Viscous", 50: "Pocket", 52: "Mirage",
  58: "Vyper", 60: "Sinclair", 63: "Mina", 64: "Drifter", 65: "Venator", 66: "Victor",
  67: "Paige", 69: "The Doorman", 72: "Billy", 76: "Graves", 77: "Apollo", 79: "Rem",
  80: "Silver", 81: "Celeste"
};

// NEW: normalizes a hero name for matching - strips spaces and hyphens,
// lowercases, keeps "&" as-is. This means "Grey Talon", "grey-talon", and
// "greytalon" all normalize to the exact same key ("greytalon"), and
// "Mo & Krill" normalizes to "mo&krill" - so multi-word hero names work
// with or without spaces/hyphens, matching however it's typed in a URL.
function normalizeHeroName(str) {
  return str.toLowerCase().replace(/[\s\-]/g, "");
}

// Reverse lookup (normalized hero name -> hero_id), so URLs can pass a
// readable name like "pocket" or "greytalon" instead of memorizing numeric
// IDs. Case-insensitive and space/hyphen-insensitive.
const HERO_NAME_TO_ID = Object.fromEntries(
  Object.entries(HERO_DICTIONARY).map(([id, name]) => [normalizeHeroName(name), Number(id)])
);

// NEW: Discord custom emoji lookup table, keyed by hero_id. This is a
// permanent server-side table - built into the worker so every API call
// resolves hero emojis instantly with zero extra lookups, no per-request
// Discord API calls needed.
//
// All 38 IDs below are CONFIRMED - pulled directly from your server via the
// "\:name:" escape trick, filled in on Aug 6 2026. hero 31 (Lash)'s actual
// emoji name is "dl_Lash" (capital L), used as-is below.
const HERO_EMOJIS = {
  1:  { name: "dl_infernus",  id: "1496217684944228586" },
  2:  { name: "dl_seven",     id: "1496220218433736949" },
  3:  { name: "dl_vindicta",  id: "1496221049338069052" },
  4:  { name: "dl_ladygeist", id: "1496218157696946196" },
  6:  { name: "dl_abrams",    id: "1496215586865418282" },
  7:  { name: "dl_wraith",    id: "1496221595226738849" },
  8:  { name: "dl_mcginnis",  id: "1496218687420498020" },
  10: { name: "dl_paradox",   id: "1496219423642747092" },
  11: { name: "dl_dynamo",    id: "1496216979479203901" },
  12: { name: "dl_kelvin",    id: "1496218005577662565" },
  13: { name: "dl_haze",      id: "1496217440324030495" },
  14: { name: "dl_holliday",  id: "1496217549787107328" },
  15: { name: "dl_bebop",     id: "1496215988004192386" },
  16: { name: "dl_calico",    id: "1496216233232826430" },
  17: { name: "dl_greytalon", id: "1496217274200359022" },
  18: { name: "dl_mokrill",   id: "1496219129177182288" },
  19: { name: "dl_shiv",      id: "1496220392820310086" },
  20: { name: "dl_ivy",       id: "1496217883334672485" },
  25: { name: "dl_warden",    id: "1496221455376187636" },
  27: { name: "dl_yamato",    id: "1496221761849790545" },
  31: { name: "dl_Lash",      id: "1496218319282376794" },
  35: { name: "dl_viscous",   id: "1496221183249612982" },
  50: { name: "dl_pocket",    id: "1496219591813107774" },
  52: { name: "dl_mirage",    id: "1496218993420402718" },
  58: { name: "dl_vyper",     id: "1496221332160123012" },
  60: { name: "dl_sinclair",  id: "1496220636400582768" },
  63: { name: "dl_mina",      id: "1496218881826492556" },
  64: { name: "dl_drifter",   id: "1496216814143799336" },
  65: { name: "dl_venator",   id: "1496220781825360025" },
  66: { name: "dl_victor",    id: "1496220910682636500" },
  67: { name: "dl_paige",     id: "1496219290293112964" },
  69: { name: "dl_doorman",   id: "1496216567451750460" },
  72: { name: "dl_billy",     id: "1496216114961580274" },
  76: { name: "dl_graves",    id: "1496217139017814251" },
  77: { name: "dl_apollo",    id: "1496215762312892426" },
  79: { name: "dl_rem",       id: "1496219761435086902" },
  80: { name: "dl_silver",    id: "1496220517697323231" },
  81: { name: "dl_celeste",   id: "1496216428645187716" }
};

// Returns a full "<:name:id>" Discord emoji tag, ready to render directly
// in any message BotGhost sends - no client-side autocomplete needed since
// this already includes the real snowflake ID.
function heroEmojiTag(heroId) {
  const entry = HERO_EMOJIS[heroId];
  if (!entry) return "";
  return `<:${entry.name}:${entry.id}>`;
}

// ============================================================
// NEW: ITEM EMOJI LOOKUP (item_id -> Discord custom emoji)
// ============================================================
// Same pattern as HERO_EMOJIS above - keyed by item_id (from
// getItemAssetsMap's numeric ids), pointing at Discord custom emoji
// name+snowflake. EMPTY BY DEFAULT: fill this in the same way HERO_EMOJIS
// was built - upload each item icon as a Discord server emoji, then use
// the "\:emoji_name:" escape trick in a Discord message to reveal each
// emoji's real ID, and paste entries in here as { <item_id>: { name:
// "...", id: "..." } }.
//
// You don't need all ~120 items on day one - itemEmojiTag() below returns
// "" for any item_id not yet in this table, so the emoji row just quietly
// skips items you haven't uploaded yet rather than breaking. A sensible
// approach: start with whatever items show up most in your squad's actual
// matches (check match_history.items after a few real pulls) and expand
// the table over time.
const ITEM_EMOJIS = {
  // e.g. 123456789: { name: "dl_extrahealth", id: "1496xxxxxxxxxxxxxxx" },
};

function itemEmojiTag(itemId) {
  const entry = ITEM_EMOJIS[itemId];
  if (!entry) return "";
  return `<:${entry.name}:${entry.id}>`;
}

const BASE_URL = "https://raw.githubusercontent.com/teacup4k/deadlock-proxy/refs/heads/main/";

const HERO_WON = {
  1:  `${BASE_URL}Infernus_Gloat.png`, 2:  `${BASE_URL}Seven_Gloat.png`, 3:  `${BASE_URL}Vindicta_Gloat.png`, 4:  `${BASE_URL}Lady_Geist_Gloat.png`,
  6:  `${BASE_URL}Abrams_Gloat.png`, 7:  `${BASE_URL}Wraith_Gloat.png`, 8:  `${BASE_URL}McGinnis_Gloat.png`, 10: `${BASE_URL}Paradox_Gloat.png`,
  11: `${BASE_URL}Dynamo_Gloat.png`, 12: `${BASE_URL}Kelvin_Gloat.png`, 13: `${BASE_URL}Haze_Gloat.png`, 14: `${BASE_URL}Holliday_Gloat.png`,
  15: `${BASE_URL}Bebop_Gloat.png`, 16: `${BASE_URL}Calico_Gloat.png`, 17: `${BASE_URL}Grey_Talon_Gloat.png`, 18: `${BASE_URL}Mo_&_Krill_Gloat.png`,
  19: `${BASE_URL}Shiv_Gloat.png`, 20: `${BASE_URL}Ivy_Gloat_.png`, 25: `${BASE_URL}Warden_Gloat.png`, 27: `${BASE_URL}Yamato_Gloat.png`,
  31: `${BASE_URL}Lash_Gloat.png`, 35: `${BASE_URL}Viscous_Gloat.png`, 50: `${BASE_URL}Pocket_Gloat.png`, 52: `${BASE_URL}Mirage_Gloat_.png`,
  58: `${BASE_URL}Vyper_Gloat.png`, 60: `${BASE_URL}Sinclair_Gloat.png`, 63: `${BASE_URL}Mina_Gloat.png`, 64: `${BASE_URL}Drifter_Gloat.png`,
  65: `${BASE_URL}Venator_Gloat.png`, 66: `${BASE_URL}Victor_Gloat.png`, 67: `${BASE_URL}Paige_Gloat.png`, 69: `${BASE_URL}Doorman_Gloat.png`,
  72: `${BASE_URL}Billy_Gloat.png`, 76: `${BASE_URL}Graves_Gloat.png`, 77: `${BASE_URL}Apollo_Gloat.png`, 79: `${BASE_URL}Rem_Gloat.png`,
  80: `${BASE_URL}Silver_Gloat.png`, 81: `${BASE_URL}Celeste_Gloat.png`
};

const HERO_LOST = {
  1:  `${BASE_URL}Infernus_Critical_.png`, 2:  `${BASE_URL}Seven_Critical.png`, 3:  `${BASE_URL}Vindicta_Critical.png`, 4:  `${BASE_URL}Lady_Geist_Critical_.png`,
  6:  `${BASE_URL}Abrams_Critical.png`, 7:  `${BASE_URL}Wraith_Critical.png`, 8:  `${BASE_URL}McGinnis_Critical.png`, 10: `${BASE_URL}Paradox_Critical.png`,
  11: `${BASE_URL}Dynamo_Critical.png`, 12: `${BASE_URL}Kelvin_Critical.png`, 13: `${BASE_URL}Haze_Critical.png`, 14: `${BASE_URL}Holliday_Critical.png`,
  15: `${BASE_URL}Bebop_Critical.png`, 16: `${BASE_URL}Calico_Critical_.png`, 17: `${BASE_URL}Grey_Talon_Critical.png`, 18: `${BASE_URL}Mo_&_Krill_Critical.png`,
  19: `${BASE_URL}Shiv_Critical.png`, 20: `${BASE_URL}Ivy_Critical_.png`, 25: `${BASE_URL}Warden_Critical.png`, 27: `${BASE_URL}Yamato_Critical.png`,
  31: `${BASE_URL}Lash_Critical.png`, 35: `${BASE_URL}Viscous_Critical_.png`, 50: `${BASE_URL}Pocket_Critical_.png`, 52: `${BASE_URL}Mirage_Critical.png`,
  58: `${BASE_URL}Vyper_Critical.png`, 60: `${BASE_URL}Sinclair_Critical.png`, 63: `${BASE_URL}Mina_Critical.png`, 64: `${BASE_URL}Drifter_Critical.png`,
  65: `${BASE_URL}Venator_Critical.png`, 66: `${BASE_URL}Victor_Critical.png`, 67: `${BASE_URL}Paige_Critical.png`, 69: `${BASE_URL}Doorman_Critical.png`,
  72: `${BASE_URL}Bily_Critical.png`, 76: `${BASE_URL}Graves_Critical.png`, 77: `${BASE_URL}Apollo_Critical.png`, 79: `${BASE_URL}Rem_Critical.png`,
  80: `${BASE_URL}Silver_Critical.png`, 81: `${BASE_URL}Celeste_Critical.png`
};

// UPDATED for Beta Season 1 (July 30, 2026 ranked overhaul). Valve renamed
// several mid-tiers and shifted the order - Ritualist moved 5->6, Emissary
// moved 6->7, and Alchemist/Arcanist were replaced by Acolyte/Sentinel/Mystic.
// Source: https://deadlock.wiki/Ranked_mode (checked live, Aug 2026).
const RANK_NAMES = {
  0: "Obscurus", 1: "Initiate", 2: "Seeker", 3: "Acolyte", 4: "Sentinel", 5: "Mystic",
  6: "Ritualist", 7: "Emissary", 8: "Oracle", 9: "Phantom", 10: "Ascendant", 11: "Eternus"
};

// UPDATED for Beta Season 1. The previous RANK_IMAGES dict pointed at
// per-subrank badges filed under the OLD tier names (e.g. .../Rank_Arcanist_
// Subrank_4.../), which are wrong now that tier 4 is "Sentinel". The current
// wiki only publishes one badge image per tier (not per-subrank), so this
// switches to that - keyed by rank number only, looked up as RANK_BADGE_IMAGES[rank].
// Source: https://deadlock.wiki/Ranked_mode (checked live, Aug 2026).
const RANK_BADGE_IMAGES = {
  0: "https://deadlock.wiki/images/thumb/a/aa/Rank_Obscurus_Badge.webp/120px-Rank_Obscurus_Badge.webp",
  1: "https://deadlock.wiki/images/thumb/5/59/Rank_Initiate_Badge.webp/120px-Rank_Initiate_Badge.webp",
  2: "https://deadlock.wiki/images/thumb/6/64/Rank_Seeker_Badge.webp/120px-Rank_Seeker_Badge.webp",
  3: "https://deadlock.wiki/images/thumb/f/f9/Rank_Acolyte_Badge.webp/120px-Rank_Acolyte_Badge.webp",
  4: "https://deadlock.wiki/images/thumb/a/a7/Rank_Sentinel_Badge.webp/120px-Rank_Sentinel_Badge.webp",
  5: "https://deadlock.wiki/images/thumb/0/08/Rank_Mystic_Badge.webp/120px-Rank_Mystic_Badge.webp",
  6: "https://deadlock.wiki/images/thumb/6/6a/Rank_Ritualist_Badge.webp/120px-Rank_Ritualist_Badge.webp",
  7: "https://deadlock.wiki/images/thumb/7/7d/Rank_Emissary_Badge.webp/120px-Rank_Emissary_Badge.webp",
  8: "https://deadlock.wiki/images/thumb/0/08/Rank_Oracle_Badge.webp/120px-Rank_Oracle_Badge.webp",
  9: "https://deadlock.wiki/images/thumb/3/3b/Rank_Phantom_Badge.webp/120px-Rank_Phantom_Badge.webp",
  10: "https://deadlock.wiki/images/thumb/c/cb/Rank_Ascendant_Badge.webp/120px-Rank_Ascendant_Badge.webp",
  11: "https://deadlock.wiki/images/thumb/4/47/Rank_Eternus_Badge.webp/120px-Rank_Eternus_Badge.webp"
};

const API_HEADERS = { "Accept": "application/json" };
const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, HEAD, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

// ============================================================
// HELPER FUNCTIONS
// ============================================================
const log = (type, msg) => { 
  if (CONFIG.DEBUG) console.log(`[${type}] ${msg}`); 
};

function getJitteredTTL(baseTTL) {
  return baseTTL + Math.floor(Math.random() * CONFIG.TTL_JITTER_MAX_MS);
}

// RESTORED: Parses the official `/account-stats` payload to ensure accurate lifetime damage/healing values.
function parseAccountStatsData(rawJson) {
  let parsed = {
    kills: 0, assists: 0, hero_damage: 0, last_hits: 0, denies: 0,
    games_won: 0, matches_played: 0, obj_damage: 0, healing: 0,
    souls_collected: 0, commends: 0, win_rate: "0.00",
    records: { most_kills: 0, most_obj_damage: 0, most_healing: 0, most_net_worth: 0, most_assists: 0, most_damage: 0 }
  };

  let statsArray = null;
  if (rawJson && Array.isArray(rawJson.stats)) statsArray = rawJson.stats;
  else if (rawJson && rawJson.response && Array.isArray(rawJson.response.stats)) statsArray = rawJson.response.stats;
  else if (Array.isArray(rawJson)) statsArray = rawJson;

  if (statsArray) {
    const overallStats = statsArray.find(s => s.hero_id == 0);
    if (overallStats && Array.isArray(overallStats.stat_id) && Array.isArray(overallStats.total_value)) {
      overallStats.stat_id.forEach((id, index) => {
        const value = Number(overallStats.total_value[index]) || 0;
        if (id == 1) parsed.kills = value;
        else if (id == 2) parsed.assists = value;
        else if (id == 3) parsed.hero_damage = value;
        else if (id == 4) parsed.last_hits = value;
        else if (id == 5) parsed.denies = value;
        else if (id == 6) parsed.games_won = value;
        else if (id == 7) parsed.matches_played = value;
        else if (id == 8) parsed.obj_damage = value;
        else if (id == 9) parsed.records.most_kills = value;
        else if (id == 10) parsed.records.most_obj_damage = value;
        else if (id == 11) parsed.records.most_healing = value;
        else if (id == 12) parsed.records.most_net_worth = value;
        else if (id == 13) parsed.records.most_assists = value;
        else if (id == 14) parsed.records.most_damage = value;
        else if (id == 15) parsed.healing = value;
        else if (id == 16) parsed.souls_collected = value;
        else if (id == 17) parsed.commends = value;
      });
    }
  }

  if (parsed.matches_played > 0) {
    parsed.win_rate = ((parsed.games_won / parsed.matches_played) * 100).toFixed(2);
  }
  return parsed;
}

// ============================================================
// NEW: /rank NORMALIZER
// ============================================================
// Same envelope-unwrapping trick used everywhere else in this file
// (deadlock-api sometimes wraps payloads in `.response`, sometimes
// returns a bare array with one entry, sometimes returns the object
// directly - card/mates/history all handle this inline, so this pulls
// that logic out into one place both /rank and /card can share).
function unwrapPayload(raw) {
  if (!raw) return null;
  if (raw.response !== undefined) return raw.response;
  if (Array.isArray(raw)) return raw.length > 0 ? raw[0] : null;
  return raw;
}

function pickField(obj, keys) {
  if (!obj) return undefined;
  for (const k of keys) {
    if (obj[k] !== undefined && obj[k] !== null) return obj[k];
  }
  return undefined;
}

// ============================================================
// NEW: MATCH SCOREBOARD (fair 1-12 performance ranking for one match)
// ============================================================
// PHILOSOPHY: with no public per-role/per-rank benchmark data to compare
// against (unlike a site with millions of matches to build a baseline
// from), the fairest way to rank 12 players in ONE game is peer-relative:
// z-score each player against the other 11 in that same match, across
// several dimensions so a support racking up assists/objective damage
// isn't buried under a carry's kill count. Deliberately does NOT bake a
// win/loss bonus into the score itself (matching how community rating
// systems like LockBlaze explicitly decouple outcome from individual skill
// assessment) - win/loss is exposed as a separate field instead, so a
// scoreboard can show it without it skewing "who actually played well."
//
// Given an array of raw numbers, returns a same-length array of z-scores
// (how many standard deviations each value is from the group's mean),
// clipped to [-3, 3] so one absurd outlier game doesn't blow the scale out
// - the same "logarithmic/resistant to outliers" principle trackers use,
// just via clipping instead of a log transform (simpler, same effect at
// the tail). If every value is identical (stdDev 0 - e.g. a field that
// turned out to be all zeros because a guessed field name was wrong),
// returns all zeros instead of dividing by zero.
function computeZScores(values) {
  const n = values.length;
  if (n === 0) return [];
  const mean = values.reduce((a, b) => a + b, 0) / n;
  const variance = values.reduce((a, b) => a + (b - mean) ** 2, 0) / n;
  const stdDev = Math.sqrt(variance);
  if (stdDev === 0) return values.map(() => 0);
  return values.map(v => {
    const z = (v - mean) / stdDev;
    return Math.max(-3, Math.min(3, z));
  });
}

// ============================================================
// NEW: ITEM ASSET LOOKUP (item_id -> name/image)
// ============================================================
// Static game data, not per-match - fetched from the same
// /v1/assets/items endpoint the item-win-rate pages on deadlock-api.com
// itself reference (see "See more: <.../v1/assets/items>" links throughout
// their API docs), cached for a full day since item names only change on
// patches. EXPERIMENTAL: the exact field names in that response (id/name/
// image vs class_name/display_name/icon_url, etc.) haven't been confirmed
// against a live payload, so this tries several plausible spellings and
// keeps the raw entry alongside in case the guesses are wrong.
async function getItemAssetsMap(env, ctx) {
  const res = await getEndpointData(
    "assets:items",
    "https://api.deadlock-api.com/v1/assets/items",
    CONFIG.TTL.ITEM_ASSETS, env, ctx, false
  );
  const list = Array.isArray(res.data) ? res.data : (res.data?.items || []);
  const map = {};
  for (const it of list) {
    const id = pickField(it, ["id", "item_id"]);
    if (id == null) continue;
    map[id] = {
      name: pickField(it, ["name", "display_name", "class_name"]) || `Item ${id}`,
      image: pickField(it, ["image", "icon_url", "image_webp"]) || null,
      cost: pickField(it, ["cost", "item_price", "price"]) ?? null,
      tier: pickField(it, ["tier", "item_tier"]) ?? null,
      raw: it
    };
  }
  return { map, error: res.error, count: list.length };
}

// ============================================================
// NEW: BANNED HERO TRADITION
// ============================================================
// A squad "ban list" - hero IDs your group has jokingly outlawed. This
// CANNOT stop anyone from picking the hero in-game (no live hook into the
// draft, only after-the-fact detection from match-history), so it's a
// shame mechanism, not real enforcement: is_banned_hero on every match
// tells you whether that specific game was played on a banned hero, so a
// scheduled BotGhost check can catch it after the fact and post the roast.
// Empty by default - add hero IDs from HERO_DICTIONARY above (e.g. 50 for
// Pocket) to activate. Hardcoded here (not KV-configurable) for simplicity;
// ask me to make it a KV-editable list via a command if you want to change
// it without redeploying each time.
const BANNED_HERO_IDS = []; // e.g. [50, 66] to ban Pocket and Victor

// NEW: Builds the full per-match summary object (hero, K/D/A, outcome, net
// worth, duration, ranked flag, etc.) from one raw /match-history entry.
// Shared by both the single "most recent match" field AND each of the
// last-5-games entries, so all 5 have identical full detail instead of
// last_5_results being a lightweight W/L-only summary - powers a "click a
// button to switch which of your last 5 games is shown" UI without any
// extra API calls, since match-history already returns everything needed.
function summarizeMatch(m) {
  const kills = m.player_kills ?? 0;
  const deaths = m.player_deaths ?? 0;
  const assists = m.player_assists ?? 0;
  const durationS = m.match_duration_s ?? 0;
  const durationMin = Math.floor(durationS / 60);
  const durationSec = durationS % 60;
  const won = m.match_result === m.player_team;

  return {
    match_id: String(m.match_id),
    hero: HERO_DICTIONARY[m.hero_id] || `Unknown (${m.hero_id})`,
    hero_id: m.hero_id,
    hero_won: HERO_WON[m.hero_id] || null,
    hero_lost: HERO_LOST[m.hero_id] || null,
    hero_icon: (won ? HERO_WON[m.hero_id] : HERO_LOST[m.hero_id]) || null, // NEW: picks the won/lost art automatically based on THIS match's outcome, no IF block needed in BotGhost
    hero_emoji: heroEmojiTag(m.hero_id),
    is_banned_hero: BANNED_HERO_IDS.includes(m.hero_id), // NEW: true if this specific match was played on a squad-banned hero
    // NEW: muted win/loss accent color, not pure red/green - pulled from
    // Discord's own classic role-color palette so it sits naturally on dark
    // theme instead of looking neon/clashing. Given as both hex (for a text
    // field) and decimal (in case BotGhost's embed color picker wants an int).
    result_color: won ? "#3BA55D" : "#992D22",
    result_color_int: won ? 3908957 : 10038562,
    // NEW: confirmed directly from BotGhost's own docs - the "Custom Button
    // Style" field on a button action accepts exactly these keywords:
    // PRIMARY (blue), SECONDARY (grey), SUCCESS (green), DANGER (red).
    // Drop this straight into that field so each "Game N" button under a
    // player's profile turns green on a win / red on a loss.
    button_style: won ? "SUCCESS" : "DANGER",
    kills, deaths, assists,
    kd_ratio: Number((deaths > 0 ? (kills / deaths) : kills).toFixed(2)),
    kda_ratio: Number((deaths > 0 ? ((kills + assists) / deaths) : (kills + assists)).toFixed(2)),
    kda_line: `${kills} / ${deaths} / ${assists}`,
    outcome: won ? "Victory" : "Defeat",
    won,
    net_worth: Number(m.net_worth ?? m.total_souls ?? 0).toLocaleString(),
    duration_formatted: `${durationMin}:${String(durationSec).padStart(2, "0")}`,
    was_ranked: pickField(m, ["ranked_delta", "ranked_display_badge", "ranked_calibration_match"]) !== undefined,
    ranked_delta: m.ranked_delta ?? null,
    last_hits: m.last_hits ?? 0,
    denies: m.denies ?? 0,
    hero_level: m.hero_level ?? null,
    raw: m
  };
}

// ============================================================
// NEW: SUBRANK PROGRESS (points into current subrank / points to next)
// ============================================================
// `rank_points` (last_match.player_rank_final_flat_progress) is a season-
// long CUMULATIVE total, not a per-subrank 0-1000 meter - so it's not
// directly useful for "how close am I to ranking up" without doing the
// threshold math. Per deadlock.wiki (checked live Aug 2026): 1000 points
// per subrank promotion, 2000 points for the final subrank of a rank
// crossing into the next rank's Subrank I. That gives a fixed points cost
// to reach the START of any (rank, subrank): 7000 points per full rank
// (5 normal subrank-ups @ 1000 + 1 rank-up @ 2000).
//
// Validated against a real account: rank 6 (Ritualist) subrank 4 with
// rank_points 38125 -> formula below gives subrank-start = 38000, so
// 125 points into the subrank, 875 to go. That's an exact match to the
// live rank/subrank the API independently returned for that same point
// total, which is why this is trustworthy rather than a guess.
//
// EXCEPTION: Eternus (rank 11) subranks are percentile-based per Valve's
// own docs (recalculated daily against the live player pool), NOT
// points-threshold based like every other rank - so this can't compute
// "points to next" there and says so explicitly instead of guessing.
const RANK_POINTS_PER_SUBRANK = 1000;
const RANK_POINTS_TO_NEXT_RANK = 2000;
const SUBRANKS_PER_RANK = 6;
const MAX_POINTS_RANK = 11; // Eternus - percentile-based, formula doesn't apply

function rankStartPoints(rank) {
  // Cumulative points at Rank `rank`, Subrank 1.
  return (rank - 1) * (RANK_POINTS_PER_SUBRANK * (SUBRANKS_PER_RANK - 1) + RANK_POINTS_TO_NEXT_RANK);
}

function computeSubrankProgress(rank, subrank, rankPoints) {
  if (!rank || rank <= 0 || rankPoints === null || rankPoints === undefined) return null; // Obscurus / no points data yet

  if (rank >= MAX_POINTS_RANK) {
    return {
      points_into_subrank: null,
      points_to_next_subrank: null,
      next_subrank_cost: null,
      is_percentile_tier: true,
      note: "Eternus subranks are percentile-based (recalculated daily against the live player pool), not points-threshold-based, so progress-to-next isn't computable from points alone."
    };
  }

  const subrankStart = rankStartPoints(rank) + (subrank - 1) * RANK_POINTS_PER_SUBRANK;
  const isLastSubrankOfRank = subrank >= SUBRANKS_PER_RANK;
  const cost = isLastSubrankOfRank ? RANK_POINTS_TO_NEXT_RANK : RANK_POINTS_PER_SUBRANK;

  const pointsInto = Math.max(0, rankPoints - subrankStart);
  const pointsToNext = Math.max(0, cost - pointsInto);
  const pct = Math.min(100, Math.round((pointsInto / cost) * 100));

  // NEW: a rendered 10-segment bar for actual visual weight in a Discord
  // message, instead of just raw numbers. ▰ (filled) / ▱ (empty) render
  // reliably across Discord's default font on both desktop and mobile.
  const filledSegments = Math.round((pointsInto / cost) * 10);
  const progressBar = "▰".repeat(filledSegments) + "▱".repeat(10 - filledSegments);

  return {
    points_into_subrank: pointsInto,
    points_to_next_subrank: pointsToNext,
    next_subrank_cost: cost,
    progress_pct: pct, // NEW: e.g. 37
    progress_bar: progressBar, // NEW: e.g. "▰▰▰▱▱▱▱▱▱▱"
    is_percentile_tier: false,
    note: "Derived from Valve's published point thresholds (1000/subrank, 2000 crossing into a new rank) - reverse-engineered from patch notes, not returned directly by the API."
  };
}

// Merges the new /rank endpoint with the legacy /card rank fields.
//
// CONFIRMED live shape (as of Aug 2026, from an actual response):
//   { badge: 64, rank: 6, subrank: 4, last_match: {
//       match_id, start_time,
//       player_rank_initial_display_rank, player_rank_initial_flat_progress,
//       player_rank_final_flat_progress,      <- current season points total
//       player_rank_desired_progress_change,  <- points swing from that match
//       player_rank_initial_calibration_games,
//       player_rank_initial_demotion_protection_games,
//       player_rank_consumed_demotion_protection,
//       player_rank_initial_win_streak
//   }}
// `badge` is NOT points - it's just rank*10+subrank re-encoded (64 = 6*10+4),
// redundant with rank/subrank we already have. The real season score is
// nested under last_match.player_rank_final_flat_progress, which is a
// continuously-growing total (not a per-subrank 0-1000 meter) - rank/subrank
// are effectively derived FROM this number crossing tier thresholds. That
// makes it a perfect single sort key on its own, so it's used directly when
// present instead of the earlier synthetic rank*100000+subrank*10000 formula.
// There's no top-level "season" or "calibrated" field in the live payload,
// so those are still guessed for forward-compatibility but calibration is
// now determined the reliable way instead: per Valve's own wiki, rank 0
// (Obscurus) literally means "hasn't finished placement matches yet" - so
// calibration status is derived from rank itself, not a guessed flag.
function normalizeRankData(rankRaw, cardRaw) {
  const rankData = unwrapPayload(rankRaw);
  const cardData = unwrapPayload(cardRaw);
  const lastMatch = rankData?.last_match || null;

  let rank = pickField(rankData, ["rank", "ranked_rank", "tier"]);
  let subrank = pickField(rankData, ["subrank", "sub_rank", "ranked_subrank", "division"]);
  // Try top-level guesses first (other accounts / future API changes might
  // differ), then fall back to the confirmed nested location.
  let rankPoints = pickField(rankData, ["rank_points", "rankPoints", "points", "rp", "score", "current_points", "rank_progress", "progress", "elo", "mmr"]);
  if (rankPoints === undefined && lastMatch) {
    rankPoints = pickField(lastMatch, ["player_rank_final_flat_progress"]);
  }
  const season = pickField(rankData, ["season", "season_id", "season_number"]);
  const leaderboardRank = pickField(rankData, ["leaderboard_rank", "global_rank", "position"]);

  // Bonus context from the most recent match, when available - useful for a
  // Discord embed line like "+150 last match, 1-game win streak".
  const lastMatchDelta = lastMatch ? pickField(lastMatch, ["player_rank_desired_progress_change"]) : null;
  // NOTE: the raw field here (player_rank_initial_win_streak) is the streak
  // BEFORE the last recorded match started, not after it ended - Deadlock's
  // API only provides a pre-match value (unlike flat_progress, which has
  // both an "initial" and "final" variant). Taken at face value, this shows
  // a stale streak count that doesn't reflect whether that last match was
  // actually won or lost - e.g. a real 3-win streak immediately followed by
  // a loss would still display "3" instead of correctly resetting to 0.
  const winStreakBeforeLastMatch = lastMatch ? (Number(pickField(lastMatch, ["player_rank_initial_win_streak"])) || 0) : null;
  const usedDemotionProtection = lastMatch ? pickField(lastMatch, ["player_rank_consumed_demotion_protection"]) : null;

  // NEW: the corrected, ACTUAL current streak - derived from whether the
  // last match won or lost points (lastMatchDelta), not just passed through
  // raw. A loss always breaks a win streak to 0; a win extends the pre-match
  // streak by one.
  let winStreak = winStreakBeforeLastMatch;
  if (lastMatchDelta !== undefined && lastMatchDelta !== null && winStreakBeforeLastMatch !== null) {
    if (lastMatchDelta < 0) {
      winStreak = 0; // last match was a loss - streak is broken
    } else if (lastMatchDelta > 0) {
      winStreak = winStreakBeforeLastMatch + 1; // last match was a win - streak extends
    }
  }

  let source = "rank_endpoint";
  rank = Number(rank);
  subrank = Number(subrank);

  if (!Number.isFinite(rank)) {
    // /rank had nothing usable - fall back to whatever /card still carries.
    rank = Number(cardData?.ranked_rank) || 0;
    subrank = Number(cardData?.ranked_subrank) || 0;
    rankPoints = rankPoints ?? (Number(cardData?.ranked_badge_level) || 0);
    source = "card_endpoint_fallback";
  }
  if (!Number.isFinite(subrank)) subrank = 0;

  const rankPointsNum = Number(rankPoints);
  const hasRankPoints = Number.isFinite(rankPointsNum);

  // Sort key: when we have the real flat-progress total, it alone is a
  // perfect monotonic sort key (rank/subrank are derived from it crossing
  // thresholds, so it already encodes tier+subrank+progress together).
  // Falls back to the old synthetic composite when flat-progress isn't
  // available (e.g. card-fallback, or a player with no last_match yet).
  const rankScore = hasRankPoints
    ? rankPointsNum
    : rank * 100000 + subrank * 10000;

  // NEW: calibration status derived from rank itself (Obscurus = rank 0 =
  // "hasn't finished placements", per deadlock.wiki) rather than a guessed
  // top-level field - the live payload confirmed there's no such field.
  const calibrated = rank > 0;

  // NEW: points into current subrank / points to next, computed only when
  // we have both a valid rank and a real points total (not the card-fallback
  // badge_level, which isn't on the same points scale).
  const subrankProgress = (source === "rank_endpoint" && hasRankPoints)
    ? computeSubrankProgress(rank, subrank, rankPointsNum)
    : null;

  return {
    ranked_rank: rank,
    ranked_subrank: subrank,
    rank_points: hasRankPoints ? rankPointsNum : null,
    rank_name: rank === 0 ? "Obscurus" : `${RANK_NAMES[rank] || "Unknown"} ${subrank}`,
    rank_image: RANK_BADGE_IMAGES[rank] || null,
    rank_score: rankScore,
    season: season ?? null,
    calibrated,
    calibration_status: calibrated ? "ranked" : "calibrating",
    leaderboard_rank: leaderboardRank ?? null,
    last_match_delta: lastMatchDelta ?? null,
    win_streak: winStreak ?? null,
    win_streak_before_last_match: winStreakBeforeLastMatch ?? null, // NEW: raw pre-match value, kept for verification against in-game display
    used_demotion_protection: usedDemotionProtection ?? null,
    points_into_subrank: subrankProgress?.points_into_subrank ?? null,
    points_to_next_subrank: subrankProgress?.points_to_next_subrank ?? null,
    next_subrank_cost: subrankProgress?.next_subrank_cost ?? null,
    progress_pct: subrankProgress?.progress_pct ?? null, // NEW
    progress_bar: subrankProgress?.progress_bar ?? null, // NEW: e.g. "▰▰▰▱▱▱▱▱▱▱"
    is_percentile_tier: subrankProgress?.is_percentile_tier ?? false,
    source,
    raw: rankData
  };
}

// ============================================================
// NEW: RANK HISTORY (per-player snapshot log in KV)
// ============================================================
// Appends a rank snapshot to `rank_history:${id}` whenever the cron job
// force-refreshes /rank, but ONLY if it actually changed since the last
// snapshot - polling every 10 minutes would otherwise fill the history
// with hundreds of identical entries between actual rank changes. This is
// what powers rank-up detection / "climbed 2 subranks this week" stats.
const RANK_HISTORY_MAX_ENTRIES = 200;

async function appendRankHistorySnapshot(id, normalized, env) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  if (!kvBinding) return;
  const key = `rank_history:${id}`;

  let history = [];
  try {
    const existing = await kvBinding.get(key);
    if (existing) history = JSON.parse(existing);
    if (!Array.isArray(history)) history = [];
  } catch (e) { log("RANK HISTORY ERR", `read ${key}: ${e.message}`); }

  const last = history[history.length - 1];
  const changed = !last || last.rank !== normalized.ranked_rank || last.subrank !== normalized.ranked_subrank || last.rank_points !== normalized.rank_points;

  if (changed) {
    history.push({
      rank: normalized.ranked_rank,
      subrank: normalized.ranked_subrank,
      rank_points: normalized.rank_points,
      rank_name: normalized.rank_name,
      ts: Date.now()
    });
    if (history.length > RANK_HISTORY_MAX_ENTRIES) {
      history = history.slice(history.length - RANK_HISTORY_MAX_ENTRIES);
    }
    try {
      // Awaited - see the top-of-file FIX note on why fire-and-forget KV
      // writes inside a cron job silently get dropped.
      await kvBinding.put(key, JSON.stringify(history));
    } catch (e) { log("RANK HISTORY ERR", `write ${key}: ${e.message}`); }
  }

  return history;
}

// Builds a simple up/down/same trend from the two most recent snapshots,
// for a Discord embed to show an arrow next to a player's rank. Also builds
// a ready-to-post emoji + sentence so a BotGhost rank-up/derank alert can
// just check `direction` and drop `text` straight into a message - no
// string-building needed on the BotGhost side.
function computeRankTrend(history) {
  if (!Array.isArray(history) || history.length < 2) return null;
  const curr = history[history.length - 1];
  const prev = history[history.length - 2];
  const currScore = curr.rank * 100000 + curr.subrank * 10000 + (curr.rank_points || 0);
  const prevScore = prev.rank * 100000 + prev.subrank * 10000 + (prev.rank_points || 0);
  const direction = currScore > prevScore ? "up" : currScore < prevScore ? "down" : "same";

  const emoji = direction === "up" ? "📈" : direction === "down" ? "📉" : "➖";
  const text = direction === "up"
    ? `climbed from ${prev.rank_name} to ${curr.rank_name}`
    : direction === "down"
    ? `dropped from ${prev.rank_name} to ${curr.rank_name}`
    : `holding steady at ${curr.rank_name}`;

  return {
    direction,
    from: prev.rank_name,
    to: curr.rank_name,
    since: prev.ts,
    emoji, // NEW: "📈" / "📉" / "➖"
    text,  // NEW: e.g. "climbed from Ritualist 3 to Ritualist 4"
    changed: direction !== "same" // NEW: convenience bool for a simple IF check
  };
}

// ============================================================
// NEW: SEASON PEAK (highest rank reached this season, per player)
// ============================================================
// Beta Season 1 only preserves each player's HIGHEST rank of the season
// (per Valve's own patch notes), and everything resets on Oct 7, 2026.
// This tracks that locally so it survives derank/decay between now and
// then. Like rank history, this only ever gets checked/updated by the
// cron job's forced /rank refresh - on-demand player lookups just read it.
async function updateSeasonPeak(id, normalized, env) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  if (!kvBinding) return null;
  const key = `season_peak:${id}`;

  let peak = null;
  try {
    const existing = await kvBinding.get(key);
    if (existing) peak = JSON.parse(existing);
  } catch (e) { log("SEASON PEAK ERR", `read ${key}: ${e.message}`); }

  const newScore = normalized.ranked_rank * 100000 + normalized.ranked_subrank * 10000 + (normalized.rank_points || 0);
  const peakScore = peak ? (peak.rank * 100000 + peak.subrank * 10000 + (peak.rank_points || 0)) : -1;

  if (newScore > peakScore) {
    peak = {
      rank: normalized.ranked_rank,
      subrank: normalized.ranked_subrank,
      rank_points: normalized.rank_points,
      rank_name: normalized.rank_name,
      ts: Date.now()
    };
    try {
      await kvBinding.put(key, JSON.stringify(peak));
    } catch (e) { log("SEASON PEAK ERR", `write ${key}: ${e.message}`); }
  }

  return peak;
}

// ============================================================
// NEW: STATS HISTORY (daily lifetime-stats snapshots, for weekly deltas)
// ============================================================
// rank_history only logs actual RANK changes (sparse). account_stats fields
// (kills, commends, damage, etc.) change almost every game though, so
// dedup-on-change would spam storage the same way. Instead this snapshots
// once per CALENDAR DAY (dedup by date string, not by value) - one entry/day
// is plenty of resolution to compute "how much changed in the last 7 days"
// regardless of which day of the week the recap actually gets posted on.
const STATS_HISTORY_MAX_ENTRIES = 60; // ~2 months at 1/day

async function appendStatsHistorySnapshot(id, stats, env) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  if (!kvBinding) return;
  const key = `stats_history:${id}`;

  let history = [];
  try {
    const existing = await kvBinding.get(key);
    if (existing) history = JSON.parse(existing);
    if (!Array.isArray(history)) history = [];
  } catch (e) { log("STATS HISTORY ERR", `read ${key}: ${e.message}`); }

  const today = new Date().toISOString().slice(0, 10); // "YYYY-MM-DD"
  const last = history[history.length - 1];
  const alreadySnapshottedToday = last && last.date === today;

  if (!alreadySnapshottedToday) {
    history.push({
      date: today,
      ts: Date.now(),
      kills: stats.kills,
      assists: stats.assists,
      hero_damage: stats.hero_damage,
      obj_damage: stats.obj_damage,
      healing: stats.healing,
      souls_collected: stats.souls_collected,
      commends: stats.commends,
      games_won: stats.games_won,
      matches_played: stats.matches_played
    });
    if (history.length > STATS_HISTORY_MAX_ENTRIES) {
      history = history.slice(history.length - STATS_HISTORY_MAX_ENTRIES);
    }
    try {
      await kvBinding.put(key, JSON.stringify(history));
    } catch (e) { log("STATS HISTORY ERR", `write ${key}: ${e.message}`); }
  }

  return history;
}

// ============================================================
// NEW: PERSONAL RECORD ALERTS (detect a new all-time single-game best)
// ============================================================
// Deadlock's own API already tracks each player's all-time single-game
// bests via account_stats.records (most_kills, most_obj_damage,
// most_healing, most_net_worth, most_assists, most_damage) - that part
// isn't new. What's missing is knowing the MOMENT one of those changes, so
// a Discord alert can fire. Same dedup-on-change pattern as rank_history:
// only snapshot when something actually changed, not every cron tick.
const PERSONAL_RECORDS_MAX_ENTRIES = 60;

async function appendPersonalRecordSnapshot(id, records, env) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  if (!kvBinding) return;
  const key = `personal_records_history:${id}`;

  let history = [];
  try {
    const existing = await kvBinding.get(key);
    if (existing) history = JSON.parse(existing);
    if (!Array.isArray(history)) history = [];
  } catch (e) { log("PERSONAL RECORDS ERR", `read ${key}: ${e.message}`); }

  const last = history[history.length - 1];
  const fields = ["most_kills", "most_obj_damage", "most_healing", "most_net_worth", "most_assists", "most_damage"];
  const changed = !last || fields.some(f => (records[f] ?? 0) !== (last[f] ?? 0));

  if (changed) {
    history.push({ ts: Date.now(), ...Object.fromEntries(fields.map(f => [f, records[f] ?? 0])) });
    if (history.length > PERSONAL_RECORDS_MAX_ENTRIES) {
      history = history.slice(history.length - PERSONAL_RECORDS_MAX_ENTRIES);
    }
    try {
      await kvBinding.put(key, JSON.stringify(history));
    } catch (e) { log("PERSONAL RECORDS ERR", `write ${key}: ${e.message}`); }
  }

  return history;
}

// Compares the two most recent snapshots and returns exactly which
// record(s) got broken, with a ready-to-post emoji + sentence per broken
// stat - same "no string-building needed in BotGhost" approach as
// computeRankTrend. Only reports INCREASES (records can't decrease).
const RECORD_LABELS = {
  most_kills: "Most Kills",
  most_obj_damage: "Most Objective Damage",
  most_healing: "Most Healing",
  most_net_worth: "Most Net Worth",
  most_assists: "Most Assists",
  most_damage: "Most Hero Damage"
};

function computePersonalRecordTrend(history) {
  if (!Array.isArray(history) || history.length < 2) return { changed: false, broken_records: [] };
  const curr = history[history.length - 1];
  const prev = history[history.length - 2];

  const broken = [];
  for (const field of Object.keys(RECORD_LABELS)) {
    const oldVal = prev[field] ?? 0;
    const newVal = curr[field] ?? 0;
    if (newVal > oldVal) {
      broken.push({
        stat: field,
        label: RECORD_LABELS[field],
        old_value: oldVal,
        new_value: newVal,
        text: `New personal best: ${RECORD_LABELS[field]} - ${newVal} (previous: ${oldVal})`
      });
    }
  }

  return {
    changed: broken.length > 0,
    broken_records: broken,
    // Convenience single-line summary for the most common case (exactly one
    // record broken) - if multiple broke at once, this is just the first.
    emoji: broken.length > 0 ? "🏆" : "➖",
    text: broken.length > 0 ? broken[0].text : null
  };
}

// ============================================================
// NEW: WEEKLY CHECKPOINT (exact Friday 1pm diff, not a rolling window)
// ============================================================
// Unlike movement_leaderboard/weekly_stats_leaderboard above (which diff
// against "whatever the oldest snapshot happens to be within the last 7
// days" - a rolling approximation), this takes an EXACT checkpoint at a
// fixed weekly moment and diffs against LAST week's exact checkpoint. That
// makes "since last Friday 1pm" a precise, reproducible number instead of
// an approximation that shifts depending on cron timing.
//
// TIMEZONE: currently checks for Friday 13:00 UTC. If your server's actual
// timezone is different, this fires at the wrong real-world time - tell me
// your UTC offset and this gets corrected in one line.
const WEEKLY_CHECKPOINT_DAY_UTC = 5;  // 0=Sunday, 5=Friday
const WEEKLY_CHECKPOINT_HOUR_UTC = 8; // 8am UTC = 11am Saudi Arabia (AST, UTC+3, no DST) - gives a 1hr buffer before a 12pm Saudi post

function isWeeklyCheckpointWindow() {
  const now = new Date();
  return now.getUTCDay() === WEEKLY_CHECKPOINT_DAY_UTC && now.getUTCHours() === WEEKLY_CHECKPOINT_HOUR_UTC;
}

// Takes a fresh checkpoint (current rank + stats) for one player, diffs it
// against whatever checkpoint was stored last week, stores the diff for
// later retrieval, and overwrites the checkpoint for next week's diff.
// Guarded by a same-day dedup check so a 10-min cron interval doesn't fire
// this multiple times within the same 1-hour window.
async function processWeeklyCheckpoint(id, normalizedRank, stats, env) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  if (!kvBinding) return;

  const dedupKey = `weekly_checkpoint_last_run:${id}`;
  const today = new Date().toISOString().slice(0, 10);
  try {
    const lastRun = await kvBinding.get(dedupKey);
    if (lastRun === today) return; // already processed this week's checkpoint
  } catch (e) { /* proceed anyway if the dedup check itself fails */ }

  const checkpointKey = `weekly_checkpoint:${id}`;
  let previous = null;
  try {
    const existing = await kvBinding.get(checkpointKey);
    if (existing) previous = JSON.parse(existing);
  } catch (e) { log("WEEKLY CHECKPOINT ERR", `read ${checkpointKey}: ${e.message}`); }

  const current = {
    ts: Date.now(),
    rank: normalizedRank.ranked_rank,
    subrank: normalizedRank.ranked_subrank,
    rank_points: normalizedRank.rank_points,
    rank_name: normalizedRank.rank_name,
    kills: stats.kills,
    assists: stats.assists,
    hero_damage: stats.hero_damage,
    obj_damage: stats.obj_damage,
    healing: stats.healing,
    souls_collected: stats.souls_collected,
    commends: stats.commends,
    games_won: stats.games_won,
    matches_played: stats.matches_played
  };

  // Compute and store the diff (if we have a previous checkpoint to diff
  // against - the very first Friday this runs, there's nothing to compare
  // to yet, so no diff is stored that week).
  if (previous) {
    const diff = {
      has_data: true,
      previous_checkpoint_ts: previous.ts,
      current_checkpoint_ts: current.ts,
      rank_points_delta: (current.rank_points ?? 0) - (previous.rank_points ?? 0),
      current_rank_name: current.rank_name,
      previous_rank_name: previous.rank_name,
      kills_delta: current.kills - previous.kills,
      assists_delta: current.assists - previous.assists,
      hero_damage_delta: current.hero_damage - previous.hero_damage,
      obj_damage_delta: current.obj_damage - previous.obj_damage,
      healing_delta: current.healing - previous.healing,
      souls_delta: current.souls_collected - previous.souls_collected,
      commends_delta: current.commends - previous.commends,
      wins_delta: current.games_won - previous.games_won,
      matches_delta: current.matches_played - previous.matches_played
    };
    try {
      await kvBinding.put(`weekly_diff:${id}`, JSON.stringify(diff));
    } catch (e) { log("WEEKLY CHECKPOINT ERR", `write weekly_diff:${id}: ${e.message}`); }
  }

  // Reset the checkpoint for next week's diff.
  try {
    await kvBinding.put(checkpointKey, JSON.stringify(current));
    await kvBinding.put(dedupKey, today);
  } catch (e) { log("WEEKLY CHECKPOINT ERR", `write ${checkpointKey}: ${e.message}`); }
}

// Builds the compiled squad leaderboard from whatever weekly_diff:${id}
// entries exist. Pure KV reads - never computes anything itself, that all
// happens in processWeeklyCheckpoint above during the Friday cron window.
async function buildWeeklyRecapLeaderboard(env, ctx) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  const diffs = await Promise.all(squadIds.map(async id => {
    if (!kvBinding) return null;
    try {
      const raw = await kvBinding.get(`weekly_diff:${id}`);
      return raw ? JSON.parse(raw) : null;
    } catch (e) { return null; }
  }));

  const board = squadIds.map((id, index) => {
    const diff = diffs[index];
    if (!diff) {
      return {
        account_id: id, nickname: SQUAD_MAP[id], photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
        has_data: false
      };
    }
    return {
      account_id: id,
      nickname: SQUAD_MAP[id],
      photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
      ...diff
    };
  });

  board.sort((a, b) => (b.kills_delta ?? -Infinity) - (a.kills_delta ?? -Infinity));
  board.forEach((member, idx) => member.position = idx + 1);
  return board;
}

// ============================================================
// NEW: SQUAD CARRY LEADERBOARD ("who carried the squad this week")
// ============================================================
// Different from every other leaderboard here - this cross-references ALL
// squad members' match histories against each other to find matches where
// 2+ of you actually queued together, then figures out who had the best
// individual performance in each of those shared games. Whoever "won" the
// most shared games this week (highest carry score, AND the match was
// actually a victory - you can't carry a loss) gets crowned Squad Carry.
//
// Reuses each player's already-cached `history:${id}` KV entry (same data
// match_history/eligibility already pull from) - no extra upstream API
// calls beyond what the cron already fetches every run.
// FIX: was a ROLLING 7-day window (Date.now() - 7 days), which drifts
// depending on what day/time it's queried - Tuesday's version would show
// Tuesday-to-Tuesday, not Friday-to-Friday, inconsistent with the
// checkpoint-based sections (weekly_recap/weekly_awards) which ARE exactly
// anchored to Friday 1pm. This computes the most recent PAST Friday 1pm UTC
// as a fixed cutoff instead, so squad_carry/weekly_moments always align to
// the same week boundary as everything else, no matter what day it's read.
function getLastFridayCheckpointSeconds() {
  const now = new Date();
  const nowDay = now.getUTCDay();   // 0=Sun ... 5=Fri ... 6=Sat
  const nowHour = now.getUTCHours();

  let daysSinceFriday = (nowDay - WEEKLY_CHECKPOINT_DAY_UTC + 7) % 7;
  // If it's Friday but before the 1pm checkpoint hour, the "current" week
  // hasn't started yet - fall back to LAST week's Friday instead.
  if (daysSinceFriday === 0 && nowHour < WEEKLY_CHECKPOINT_HOUR_UTC) {
    daysSinceFriday = 7;
  }

  const checkpoint = new Date(Date.UTC(
    now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - daysSinceFriday,
    WEEKLY_CHECKPOINT_HOUR_UTC, 0, 0, 0
  ));
  return Math.floor(checkpoint.getTime() / 1000); // seconds, to match match start_time's unit
}

// ============================================================
// NEW: ALL-TIME SINGLE-MATCH RECORD LEADERBOARD (generic)
// ============================================================
// account_stats.records.most_kills was assumed to mean "highest kills in
// one match" but that was never confirmed against real docs and turned out
// to be wrong (per direct correction: it appears to count something else
// entirely, like how many games a player led in kills, not the peak value
// itself). This computes the real thing directly: scans a player's full
// available match history (same cached data as everything else, zero extra
// API calls) with NO time window - unlike Bloodbath in weekly_moments,
// which is deliberately scoped to just the last 7 days, this is the
// lifetime best. Generic over which raw match field to track (kills,
// assists, etc.) so Kill Record and Assist Record share one implementation.
//
// CAVEAT: same limitation as ranked_eligibility - this can only see
// whatever /match-history actually returns, which may not be a player's
// full literal lifetime history (see the elyas Haze investigation). So
// this is "highest [stat] across whatever matches we have visibility
// into," not a 100% guaranteed lifetime peak - but it's the best available
// without real per-match Valve data, and uses the exact same data source as
// every other match-history-based feature already in use.
async function buildSingleMatchRecordLeaderboard(env, ctx, rawField, resultKey) {
  const historyResults = await Promise.all(
    squadIds.map(id => getEndpointData(
      `history:${id}`,
      `https://api.deadlock-api.com/v1/players/${id}/match-history?limit=${CONFIG.MATCH_HISTORY_LIMIT}`,
      CONFIG.TTL.HISTORY, env, ctx, false
    ))
  );

  const board = squadIds.map((id, idx) => {
    const raw = historyResults[idx].data;
    const matches = Array.isArray(raw) ? raw : (raw?.matches || raw?.history || []);

    let best = null;
    for (const m of matches) {
      const value = m[rawField] ?? 0;
      const won = m.match_result === m.player_team;
      if (!best || value > best.value) {
        best = {
          value,
          hero: HERO_DICTIONARY[m.hero_id] || `Unknown (${m.hero_id})`,
          hero_id: m.hero_id,
          hero_icon: (won ? HERO_WON[m.hero_id] : HERO_LOST[m.hero_id]) || null, // NEW: picks won/lost art automatically based on this record match's outcome
          match_id: String(m.match_id),
          outcome: won ? "Victory" : "Defeat"
        };
      }
    }

    return {
      account_id: id,
      nickname: SQUAD_MAP[id],
      mention: discordMention(id),
      photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
      has_data: best !== null,
      [resultKey]: best?.value ?? 0, // e.g. "best_kills" or "best_assists" - dynamic key name
      hero: best?.hero ?? null,
      hero_id: best?.hero_id ?? null,
      hero_icon: best?.hero_icon ?? null, // NEW
      match_id: best?.match_id ?? null,
      outcome: best?.outcome ?? null,
      matches_scanned: matches.length
    };
  });

  board.sort((a, b) => b[resultKey] - a[resultKey]);
  board.forEach((member, idx) => member.position = idx + 1);
  return board;
}

function buildKillRecordLeaderboard(env, ctx) {
  return buildSingleMatchRecordLeaderboard(env, ctx, "player_kills", "best_kills");
}

function buildAssistRecordLeaderboard(env, ctx) {
  return buildSingleMatchRecordLeaderboard(env, ctx, "player_assists", "best_assists");
}

// ============================================================
// NEW: ACHIEVEMENT / BADGE SYSTEM (permanent unlocks)
// ============================================================
// Only built on CONFIRMED raw match fields (player_kills, player_deaths,
// player_assists, denies, net_worth, match_duration_s, hero_id, win/loss).
// No badge here depends on per-match damage or healing - those fields were
// checked and don't exist in the raw match object we've ever seen (only as
// LIFETIME totals in account_stats), so any "40k damage in a game" or
// "10k healing" badge isn't buildable with what we have.
function isStreetBrawlMatch(m) {
  return (m.brawl_avg_round_time_s > 0) || (m.brawl_score_team0 > 0) || (m.brawl_score_team1 > 0);
}

// NEW: for streak-based badges (Free Fall, Giant Slayer) - scans a match
// list in CHRONOLOGICAL order (sorts internally, so caller doesn't need to
// worry about the original order) and returns the match that completed a
// run of `count` consecutive wins/losses, or null if no such run exists.
function findConsecutiveStreak(matches, count, wantWin) {
  const sorted = [...matches].sort((a, b) => (a.start_time ?? 0) - (b.start_time ?? 0));
  let streak = 0;
  for (const m of sorted) {
    const won = m.match_result === m.player_team;
    if (won === wantWin) {
      streak++;
      if (streak >= count) return m; // the match that completed the streak
    } else {
      streak = 0;
    }
  }
  return null;
}

// Same "is this match ranked" signal already used in computeRankedEligibility
// and summarizeMatch (was_ranked) - a ranked match carries at least one of
// these fields; a non-ranked match never has any of them.
function isRankedMatch(m) {
  return pickField(m, ["ranked_delta", "ranked_display_badge", "ranked_calibration_match"]) !== undefined;
}

const BADGE_DEFINITIONS = [
  // --- Combat ---
  { id: "kills_25", name: "25+ Kills", emoji: "💥", desc: "25+ kills in a single match", check: m => (m.player_kills ?? 0) >= 25 },
  { id: "bloodbath_40", name: "40+ Kills", emoji: "🩸", desc: "40+ kills in a single match", check: m => (m.player_kills ?? 0) >= 40 },
  { id: "kda_perfect", name: "20 Kills + 20 Assists", emoji: "⭐", desc: "20+ kills and 20+ assists in the same match", check: m => (m.player_kills ?? 0) >= 20 && (m.player_assists ?? 0) >= 20 },
  { id: "zero_kill_win", name: "0-Kill Win (30+ Min)", emoji: "👻", desc: "Won a 30+ minute match with 0 kills - contributed some other way", check: m => m.match_result === m.player_team && (m.player_kills ?? 0) === 0 && (m.match_duration_s ?? 0) >= 30 * 60 },
  // --- Support / Utility --- (20 Assists (0 Kills) < 30+ Assists < 40+ Assists, clean ladder)
  { id: "assists_30", name: "30+ Assists", emoji: "🤝", desc: "30+ assists in a single match", check: m => (m.player_assists ?? 0) >= 30 },
  { id: "support_machine", name: "20+ Assists, 0 Kills", emoji: "🫡", desc: "0 kills but 20+ assists - all-in on the team", check: m => (m.player_kills ?? 0) === 0 && (m.player_assists ?? 0) >= 20 },
  { id: "deny_king_15", name: "15+ Denies", emoji: "🚫", desc: "15+ denies in a single match", check: m => (m.denies ?? 0) >= 15 },
  // --- Survival --- (12 Kills No Deaths < 15 Kills No Deaths, clean ladder)
  { id: "flawless_0d", name: "12+ Kills, 0 Deaths (Win)", emoji: "🛡️", desc: "Won a match with 12+ kills and 0 deaths", check: m => m.match_result === m.player_team && (m.player_kills ?? 0) >= 12 && (m.player_deaths ?? 0) === 0 },
  { id: "the_wall", name: "0 Deaths, 40+ Min", emoji: "🧱", desc: "0 deaths across a 40+ minute match", check: m => (m.player_deaths ?? 0) === 0 && (m.match_duration_s ?? 0) >= 40 * 60 },
  { id: "feeding_20", name: "20+ Deaths", emoji: "🍔", desc: "20+ deaths in a single match", check: m => (m.player_deaths ?? 0) >= 20 },
  { id: "pacifist_win", name: "0 Kills/Assists Win (30+ Min)", emoji: "🕊️", desc: "\"I have no enemies.\" - Won a 30+ minute match with 0 kills and 0 assists (Street Brawl doesn't count)", check: m => m.match_result === m.player_team && (m.player_kills ?? 0) === 0 && (m.player_assists ?? 0) === 0 && (m.match_duration_s ?? 0) >= 30 * 60 && !isStreetBrawlMatch(m) },
  // --- Wealth / Farming --- (40K < 80K < 100K, three real tiers)
  { id: "net_worth_40k", name: "40K+ Net Worth", emoji: "💰", desc: "40,000+ net worth in a single match", check: m => (m.net_worth ?? 0) >= 40000 },
  { id: "net_worth_80k", name: "80K+ Net Worth", emoji: "💎", desc: "80,000+ net worth in a single match", check: m => (m.net_worth ?? 0) >= 80000 },
  { id: "net_worth_100k", name: "100K+ Net Worth", emoji: "🐋", desc: "100,000+ net worth in a single match", check: m => (m.net_worth ?? 0) >= 100000 },
  { id: "last_hits_150", name: "150+ Last Hits", emoji: "💯", desc: "150+ last hits in a single match", check: m => (m.last_hits ?? 0) >= 150 },
  { id: "last_hits_300", name: "300+ Last Hits", emoji: "🏹", desc: "300+ last hits in a single match", check: m => (m.last_hits ?? 0) >= 300 },
  // --- Time-based ---
  { id: "marathon_70", name: "Win, 70+ Min Match", emoji: "⏱️", desc: "Won a match lasting 70+ minutes", check: m => (m.match_duration_s ?? 0) >= 70 * 60 && m.match_result === m.player_team },
  { id: "speedrun_15", name: "Win Under 15 Min", emoji: "⚡", desc: "Won a match in under 15 minutes (Street Brawl doesn't count)", check: m => (m.match_duration_s ?? 0) < 15 * 60 && m.match_result === m.player_team && !isStreetBrawlMatch(m) },
  { id: "night_owl", name: "Played 12AM-5AM", emoji: "🦉", desc: "Played a match between midnight-5am (Saudi time)", check: m => {
      if (!m.start_time) return false;
      const saudiHour = (new Date(m.start_time * 1000).getUTCHours() + 3) % 24; // UTC+3, matches the weekly checkpoint timezone
      return saudiHour >= 0 && saudiHour < 5;
    } },
  // --- Level ---
  { id: "max_level_35", name: "Hero Level 35 (Max)", emoji: "📈", desc: "Reached hero level 35 (the true cap) in a single match", check: m => (m.hero_level ?? 0) >= 35 },
  // --- Harder tiers of existing badges ---
  { id: "god_mode", name: "15+ Kills, 0 Deaths (Win)", emoji: "🔥", desc: "Won a match with 15+ kills and 0 deaths", check: m => m.match_result === m.player_team && (m.player_kills ?? 0) >= 15 && (m.player_deaths ?? 0) === 0 },
  { id: "denial_master_30", name: "30+ Denies", emoji: "⛔", desc: "30+ denies in a single match", check: m => (m.denies ?? 0) >= 30 },
  { id: "the_machine_40", name: "40+ Assists", emoji: "🦾", desc: "40+ assists in a single match", check: m => (m.player_assists ?? 0) >= 40 },
  // --- New single-match badges ---
  { id: "iron_will_15", name: "Win Despite 15+ Deaths", emoji: "💪", desc: "Won a match despite 15+ deaths", check: m => m.match_result === m.player_team && (m.player_deaths ?? 0) >= 15 },
  { id: "glass_cannon_20", name: "20+ Kills & 20+ Deaths", emoji: "🔫", desc: "20+ kills and 20+ deaths in the same match", check: m => (m.player_kills ?? 0) >= 20 && (m.player_deaths ?? 0) >= 20 },
  { id: "denial_of_service", name: "0 Last Hits & 0 Denies", emoji: "😴", desc: "0 last hits and 0 denies in a match - total lane non-factor", check: m => (m.last_hits ?? 0) === 0 && (m.denies ?? 0) === 0 },
  { id: "iron_curtain", name: "0 Deaths (Any Match)", emoji: "🚧", desc: "0 deaths in any match, any length", check: m => (m.player_deaths ?? 0) === 0 },
  { id: "underleveled", name: "Level ≤10 at 30+ Min", emoji: "🐌", desc: "Hero level 10 or under in a match lasting 30+ minutes", check: m => (m.hero_level ?? 99) <= 10 && (m.match_duration_s ?? 0) >= 30 * 60 },
  // --- Untapped fields ---
  { id: "held_the_line", name: "Win Despite Teammate Abandon", emoji: "🎖️", desc: "Won despite a teammate abandoning the match", check: m => !!m.team_abandoned && m.match_result === m.player_team },
  { id: "street_fighter", name: "Won a Street Brawl", emoji: "🥊", desc: "Won a Street Brawl match", check: m => isStreetBrawlMatch(m) && m.match_result === m.player_team },
  // --- Streak badges (NEW mechanism - scans match order, not one match at a time) ---
  { id: "giant_slayer_streak5", name: "5 Ranked Wins in a Row", emoji: "🗡️", desc: "Won 5 ranked matches in a row", streakCheck: matches => findConsecutiveStreak(matches.filter(isRankedMatch), 5, true) },
  { id: "free_fall_streak5", name: "5 Ranked Losses in a Row", emoji: "📉", desc: "Lost 5 ranked matches in a row", streakCheck: matches => findConsecutiveStreak(matches.filter(isRankedMatch), 5, false) }
];
// NOTE: "win as an all-support comp" from the original idea list needs a
// hero->role classification we don't currently maintain, and detecting
// whether ALL 6 team slots were support requires full match roster data we
// don't have (we only see each squad member's own individual match entry,
// not the other 5 players on their team unless they're also squad members).
// Left out of v1 - can revisit if you want to hand-build a hero role map.

const BADGE_MAX_STORED_PER_PLAYER = 200; // safety cap, one entry per badge earned (first time only)

async function scanAndStoreBadges(id, matches, env) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  if (!kvBinding) return;
  const key = `badges:${id}`;

  let earned = {};
  try {
    const existing = await kvBinding.get(key);
    if (existing) earned = JSON.parse(existing);
  } catch (e) { log("BADGES ERR", `read ${key}: ${e.message}`); }

  let newlyEarned = [];
  for (const badge of BADGE_DEFINITIONS) {
    if (earned[badge.id]) continue; // already have it - badges are one-time, permanent unlocks

    if (badge.streakCheck) {
      // NEW: streak-type badge - checks the whole match list at once (in
      // chronological order internally), not one match at a time.
      const triggerMatch = badge.streakCheck(matches);
      if (triggerMatch) {
        earned[badge.id] = {
          earned_at: Date.now(),
          match_id: String(triggerMatch.match_id),
          hero: HERO_DICTIONARY[triggerMatch.hero_id] || `Unknown (${triggerMatch.hero_id})`
        };
        newlyEarned.push({ ...badge, match_id: String(triggerMatch.match_id), hero: earned[badge.id].hero });
      }
      continue;
    }

    for (const m of matches) {
      if (badge.check(m)) {
        earned[badge.id] = {
          earned_at: Date.now(),
          match_id: String(m.match_id),
          hero: HERO_DICTIONARY[m.hero_id] || `Unknown (${m.hero_id})`
        };
        newlyEarned.push({ ...badge, match_id: String(m.match_id), hero: earned[badge.id].hero });
        break; // stop scanning once earned, move to next badge
      }
    }
  }

  if (newlyEarned.length > 0) {
    try {
      await kvBinding.put(key, JSON.stringify(earned));
    } catch (e) { log("BADGES ERR", `write ${key}: ${e.message}`); }
  }

  return { earned, newlyEarned };
}

async function buildBadgesLeaderboard(env, ctx) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  const board = await Promise.all(squadIds.map(async id => {
    let earned = {};
    if (kvBinding) {
      try {
        const raw = await kvBinding.get(`badges:${id}`);
        if (raw) earned = JSON.parse(raw);
      } catch (e) { /* leave empty */ }
    }
    const badgeList = Object.entries(earned).map(([badgeId, info]) => {
      const def = BADGE_DEFINITIONS.find(b => b.id === badgeId);
      return { id: badgeId, name: def?.name ?? badgeId, emoji: def?.emoji ?? "🏅", ...info };
    });
    return {
      account_id: id,
      nickname: SQUAD_MAP[id],
      mention: discordMention(id),
      photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
      badge_count: badgeList.length,
      badges: badgeList,
      total_possible: BADGE_DEFINITIONS.length
    };
  }));

  board.sort((a, b) => b.badge_count - a.badge_count);
  board.forEach((member, idx) => member.position = idx + 1);
  return board;
}

// NEW: Badge DIRECTORY - the same underlying badge data, but organized by
// ACHIEVEMENT instead of by player. For each of the 18 badges: description,
// which squad members have earned it (could be none, one, or several), and
// a ready-to-post comma-joined mention string so a "here's every
// achievement and who's got it" message doesn't need per-badge-per-player
// loops in BotGhost - just reference each badge's holders_text directly.
async function buildBadgeDirectory(env, ctx) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  const allPlayerBadges = await Promise.all(squadIds.map(async id => {
    let earned = {};
    if (kvBinding) {
      try {
        const raw = await kvBinding.get(`badges:${id}`);
        if (raw) earned = JSON.parse(raw);
      } catch (e) { /* leave empty */ }
    }
    return { id, nickname: SQUAD_MAP[id], mention: discordMention(id), earned };
  }));

  const EMOJI_DISPLAY_CAP = 2; // REDUCED from 3 - still too heavy at 3 when combining many badges into one message. Each custom emoji tag is 30+ characters, so a universal badge (6 holders) was ~190+ chars on one line.

  const directory = BADGE_DEFINITIONS.map(def => {
    const holders = allPlayerBadges
      .filter(p => p.earned[def.id])
      .map(p => ({ nickname: p.nickname, mention: p.mention, account_id: p.id, identity_emoji: squadIdentityEmoji(p.id), earned_at: p.earned[def.id].earned_at, hero: p.earned[def.id].hero }));

    const shownEmojis = holders.slice(0, EMOJI_DISPLAY_CAP).map(h => h.identity_emoji);
    const overflowCount = holders.length - shownEmojis.length;
    const singleEmoji = holders.length > 0 ? holders[0].identity_emoji : null;
    const singleOverflow = holders.length - 1;

    return {
      id: def.id,
      name: def.name,
      emoji: def.emoji,
      desc: def.desc,
      holders,
      holder_count: holders.length,
      holders_text: holders.length > 0 ? holders.map(h => h.nickname).join(", ") : "Nobody yet",
      holders_emoji_text: holders.length > 0 ? holders.map(h => h.identity_emoji).join(" ") : "—", // full uncapped version - only safe alone or in very short lists
      holders_emoji_short: holders.length > 0 ? (shownEmojis.join(" ") + (overflowCount > 0 ? ` +${overflowCount}` : "")) : "—", // capped at 2 emojis + "+N"
      holders_emoji_mini: holders.length > 0 ? (singleEmoji + (singleOverflow > 0 ? ` +${singleOverflow}` : "")) : "—", // NEW: absolute minimum - 1 emoji + count, for the tightest combined-message case (worst case ~40 chars per line instead of ~200)
      total_squad: squadIds.length,
      is_universal: holders.length === squadIds.length // NEW: true if literally everyone has earned it
    };
  });

  // NEW: FIX - a full 32-badge listing turned out to be mostly noise, since
  // almost every badge ends up earned by the whole squad given enough
  // matches played - showing "ali, Saston, zes, elyas, teacup, SamaBv" for
  // 20+ badges isn't interesting content, it's just repetitive. Split into
  // `rare` (not everyone has it - the actually interesting ones, sorted by
  // fewest holders first so empty/near-empty badges lead) and a plain count
  // of how many are universal, for a one-line summary instead of listing
  // them all individually. `all` is still included for anyone who wants
  // the complete unfiltered directory.
  const rare = directory.filter(d => !d.is_universal).sort((a, b) => a.holder_count - b.holder_count);
  const universalCount = directory.length - rare.length;

  return {
    all: directory,
    rare,
    top_rare: rare.slice(0, 10), // NEW: fixed-size "highlight reel" - the 10 rarest badges, always a consistent short length for a Discord template regardless of how many total badges end up non-universal over time
    universal_count: universalCount,
    total_badges: directory.length
  };
}

// ============================================================
// NEW: SQUAD CHEMISTRY (Optimal Lineup Finder + Hero-Combo Synergy)
// ============================================================
// Both need ALL-TIME cross-referencing of shared squad matches (unlike
// squad_carry/weekly_moments, which deliberately scope to the last week).
// Reuses the same cached match-history data everything else already pulls
// - zero extra API calls.
async function fetchAllTimeSharedMatches(env, ctx) {
  const historyResults = await Promise.all(
    squadIds.map(id => getEndpointData(
      `history:${id}`,
      `https://api.deadlock-api.com/v1/players/${id}/match-history?limit=${CONFIG.MATCH_HISTORY_LIMIT}`,
      CONFIG.TTL.HISTORY, env, ctx, false
    ))
  );

  const matchRecords = {}; // match_id -> { participants: [{nickname, account_id, hero_id, hero, won}] }
  squadIds.forEach((id, idx) => {
    const raw = historyResults[idx].data;
    const matches = Array.isArray(raw) ? raw : (raw?.matches || raw?.history || []);
    const nickname = SQUAD_MAP[id];
    for (const m of matches) {
      const matchId = m.match_id;
      if (!matchRecords[matchId]) matchRecords[matchId] = { participants: [] };
      matchRecords[matchId].participants.push({
        nickname, account_id: id, hero_id: m.hero_id,
        hero: HERO_DICTIONARY[m.hero_id] || `Unknown (${m.hero_id})`,
        won: m.match_result === m.player_team
      });
    }
  });

  return matchRecords;
}

// Generates all combinations of `size` items from an array.
function getCombinations(arr, size) {
  if (size > arr.length) return [];
  if (size === arr.length) return [arr];
  if (size === 1) return arr.map(x => [x]);
  const combos = [];
  for (let i = 0; i <= arr.length - size; i++) {
    const tailCombos = getCombinations(arr.slice(i + 1), size - 1);
    for (const tc of tailCombos) combos.push([arr[i], ...tc]);
  }
  return combos;
}

const LINEUP_MIN_GAMES = 3; // filters out noise from a single lucky/unlucky game together

// Optimal Lineup Finder: for every distinct group of 3, 4, 5, or 6 squad
// members who've played together, tracks games/wins as a group. Win/loss
// for the group is taken from the first participant (squad-mates in a
// shared match are virtually always on the same team, same assumption used
// throughout squad_carry/weekly_moments).
async function buildLineupLeaderboard(env, ctx) {
  const matchRecords = await fetchAllTimeSharedMatches(env, ctx);
  const lineupStats = {}; // "name1|name2|name3" -> {members, size, games, wins}

  for (const matchId in matchRecords) {
    const participants = matchRecords[matchId].participants;
    if (participants.length < 3) continue; // Power Duo already covers pairs
    const won = participants[0].won;

    for (let size = 3; size <= Math.min(6, participants.length); size++) {
      const combos = getCombinations(participants.map(p => p.nickname), size);
      for (const combo of combos) {
        const sortedCombo = [...combo].sort();
        const key = sortedCombo.join("|");
        if (!lineupStats[key]) {
          lineupStats[key] = {
            members: sortedCombo,
            mentions: sortedCombo.map(n => discordMention(NICKNAME_TO_ACCOUNT_ID[n])),
            size,
            games: 0,
            wins: 0
          };
        }
        lineupStats[key].games++;
        if (won) lineupStats[key].wins++;
      }
    }
  }

  const board = Object.values(lineupStats)
    .filter(l => l.games >= LINEUP_MIN_GAMES)
    .map(l => ({ ...l, win_rate: Math.round((l.wins / l.games) * 100) }));

  board.sort((a, b) => b.win_rate - a.win_rate);
  board.forEach((lineup, idx) => lineup.position = idx + 1);
  return board;
}

const SYNERGY_MIN_GAMES = 3;

// Hero-Combo Synergy: for every pair of squad members who've played
// together, tracks win rate for the SPECIFIC hero pairing they used most
// (e.g. "sama's Haze + teacup's Dynamo"), not generic meta pairings.
// NEW: extracted so both the full leaderboard AND the filtered
// player+hero lookup can share the same combo-tallying logic.
function buildComboStats(matchRecords) {
  const comboStats = {}; // "nameA:heroA|nameB:heroB" -> {...}

  for (const matchId in matchRecords) {
    const participants = matchRecords[matchId].participants;
    if (participants.length < 2) continue;
    const won = participants[0].won;

    for (let i = 0; i < participants.length; i++) {
      for (let j = i + 1; j < participants.length; j++) {
        const pair = [participants[i], participants[j]].sort((a, b) => a.nickname.localeCompare(b.nickname));
        const key = pair.map(p => `${p.nickname}:${p.hero}`).join("|");
        if (!comboStats[key]) {
          comboStats[key] = {
            players: pair.map(p => ({ nickname: p.nickname, mention: discordMention(p.account_id), account_id: p.account_id, hero: p.hero, hero_id: p.hero_id })), // NEW: account_id added for filtering
            games: 0,
            wins: 0
          };
        }
        comboStats[key].games++;
        if (won) comboStats[key].wins++;
      }
    }
  }

  return comboStats;
}

async function buildHeroSynergyLeaderboard(env, ctx) {
  const matchRecords = await fetchAllTimeSharedMatches(env, ctx);
  const comboStats = buildComboStats(matchRecords);

  const board = Object.values(comboStats)
    .filter(c => c.games >= SYNERGY_MIN_GAMES)
    .map(c => ({ ...c, win_rate: Math.round((c.wins / c.games) * 100) }));

  board.sort((a, b) => b.win_rate - a.win_rate);
  board.forEach((combo, idx) => combo.position = idx + 1);
  return board;
}

// ============================================================
// NEW: PLAYER+HERO SYNERGY LOOKUP (interactive, not a cron-compiled leaderboard)
// ============================================================
// Given a specific player playing a specific hero, finds their best
// synergy partners (which teammate + which hero) sorted by win rate.
// Computed LIVE on request rather than cached/compiled every cron cycle -
// there could be up to squadSize x heroCount unique combos, too many to
// precompile affordably, but this reuses the same cached match-history
// data everything else does, so it's still cheap per-request.
const SYNERGY_LOOKUP_MIN_GAMES = 5; // only show a pairing once you've actually got a sample size worth trusting

async function findTopSynergiesForPlayerHero(env, ctx, playerId, heroId) {
  const matchRecords = await fetchAllTimeSharedMatches(env, ctx);
  const comboStats = buildComboStats(matchRecords);

  const matches = Object.values(comboStats).filter(c =>
    c.players.some(p => p.account_id === playerId && p.hero_id === heroId)
  );

  const results = matches
    .filter(c => c.games >= SYNERGY_LOOKUP_MIN_GAMES)
    .map(c => {
      const you = c.players.find(p => p.account_id === playerId && p.hero_id === heroId);
      const partner = c.players.find(p => !(p.account_id === playerId && p.hero_id === heroId));
      const winRate = Math.round((c.wins / c.games) * 100);
      const yourHeroEmoji = heroEmojiTag(you.hero_id);
      const partnerHeroEmoji = heroEmojiTag(partner.hero_id);
      return {
        partner_nickname: partner.nickname,
        partner_mention: partner.mention, // kept for anyone who still wants an @mention option
        partner_hero: partner.hero,
        partner_hero_id: partner.hero_id,
        partner_hero_emoji: partnerHeroEmoji, // NEW
        your_hero: you.hero,
        your_hero_id: you.hero_id, // NEW: so a template can render your own hero's emoji too
        your_hero_emoji: yourHeroEmoji, // NEW
        games: c.games,
        wins: c.wins,
        win_rate: winRate,
        // NEW: one ready-to-drop-in line, no BotGhost-side string building or
        // hero->emoji lookup table needed - e.g.
        // "<:dl_pocket:...> x <:dl_venator:...> (SamaBv) — 75% win rate (12g)"
        display_line: `${yourHeroEmoji} x ${partnerHeroEmoji} (${partner.nickname}) — ${winRate}% win rate (${c.games} games)`
      };
    });

  results.sort((a, b) => b.win_rate - a.win_rate);
  results.forEach((r, idx) => r.position = idx + 1);
  return results;
}

async function buildSquadCarryLeaderboard(env, ctx) {
  const historyResults = await Promise.all(
    squadIds.map(id => getEndpointData(
      `history:${id}`,
      `https://api.deadlock-api.com/v1/players/${id}/match-history?limit=${CONFIG.MATCH_HISTORY_LIMIT}`,
      CONFIG.TTL.HISTORY, env, ctx, false
    ))
  );

  // start_time in the raw match payload is unix SECONDS (confirmed from a
  // real match: 1785963148 - too small to be milliseconds for 2026 dates).
  // FIX: was Date.now() - 7 days (a rolling window) - now anchored to the
  // exact last Friday 1pm UTC checkpoint, matching weekly_recap/weekly_awards.
  const cutoffSeconds = getLastFridayCheckpointSeconds();

  // Group every squad member's recent matches by match_id, so matches with
  // 2+ squad participants can be found.
  const matchMap = {};
  squadIds.forEach((id, idx) => {
    const raw = historyResults[idx].data;
    const matches = Array.isArray(raw) ? raw : (raw?.matches || raw?.history || []);
    for (const m of matches) {
      if ((m.start_time ?? 0) < cutoffSeconds) continue;
      const matchId = m.match_id;
      if (!matchMap[matchId]) matchMap[matchId] = [];
      matchMap[matchId].push({
        account_id: id,
        nickname: SQUAD_MAP[id],
        kills: m.player_kills ?? 0,
        assists: m.player_assists ?? 0,
        hero_id: m.hero_id,
        won: m.match_result === m.player_team
      });
    }
  });

  const carryCounts = {};
  squadIds.forEach(id => { carryCounts[SQUAD_MAP[id]] = 0; });
  let sharedMatchesFound = 0;
  let sharedMatchesWon = 0;

  for (const matchId in matchMap) {
    const participants = matchMap[matchId];
    if (participants.length < 2) continue; // solo queue, not a shared squad game
    sharedMatchesFound++;
    if (!participants[0].won) continue; // can't carry a loss - only wins count
    sharedMatchesWon++;

    // Carry score: kills weighted higher than assists (rewards closing out
    // fights, not just tagging along), same weighting as carry_of_the_week
    // above but 2x on kills specifically for this squad-context award.
    const scored = participants.map(p => ({ ...p, carryScore: p.kills * 2 + p.assists }));
    scored.sort((a, b) => b.carryScore - a.carryScore);
    const winner = scored[0];
    carryCounts[winner.nickname] = (carryCounts[winner.nickname] || 0) + 1;
  }

  const board = squadIds.map(id => ({
    account_id: id,
    nickname: SQUAD_MAP[id],
    mention: discordMention(id),
    photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
    carries: carryCounts[SQUAD_MAP[id]] || 0,
    shared_matches_found: sharedMatchesFound, // same for every entry - total shared squad games this week, for context
    shared_matches_won: sharedMatchesWon      // same for every entry - how many of those were victories (carry-eligible)
  }));

  board.sort((a, b) => b.carries - a.carries);
  board.forEach((member, idx) => member.position = idx + 1);
  return board;
}

// ============================================================
// NEW: WEEKLY MOMENTS ("best single game this week" categories)
// ============================================================
// Different flavor from every other award so far - these are PEAK
// single-match highlights, not cumulative weekly totals. "Remember that
// one game where..." energy instead of "who ground the most this week."
// Reuses the exact same cached match-history data squad_carry already
// fetches (same `history:${id}` KV keys, same cron cycle) - zero additional
// upstream API calls, so no rate-limit risk from adding this.
//
// SAFETY: every category defaults to `null` if there's no valid data for
// it (nobody played, nobody won a game, etc.) rather than returning a
// partial/broken object - a Discord template referencing a null field just
// shows blank/skips, which is much safer than "undefined" leaking into a
// live message. Wrapped in try/catch as a whole so one unexpected data
// shape can't take down the entire leaderboard compile.
async function buildWeeklyMomentsLeaderboard(env, ctx) {
  try {
    const historyResults = await Promise.all(
      squadIds.map(id => getEndpointData(
        `history:${id}`,
        `https://api.deadlock-api.com/v1/players/${id}/match-history?limit=${CONFIG.MATCH_HISTORY_LIMIT}`,
        CONFIG.TTL.HISTORY, env, ctx, false
      ))
    );

    const cutoffSeconds = getLastFridayCheckpointSeconds();

    let bloodbath = null; // stays individual - kills is a personal stat, not a match-level fact
    const oneTrickCandidates = [];
    const explorerCandidates = [];
    const pairWinCounts = {};
    const perPlayerGames = {}; // NEW: account_id -> [{kills,deaths,assists,net_worth,won,ts,hero,match_id}] this week, powers int_of_week/coin_flip/stat_padder/time_of_day

    // FIX: matchRecords is now keyed by match_id and tracks the FULL list of
    // squad participants per match (not just Power Duo pairs like before).
    // Marathon/Speedrun are MATCH-level facts (duration is identical for
    // everyone in that game) - previously this only credited whichever
    // squad member's individual history entry got processed first in the
    // loop, silently leaving out any teammates who were in the exact same
    // game. Now every squad member present in the record-holding match gets
    // mentioned together.
    const matchRecords = {}; // match_id -> { duration_seconds, hero, hero_id, participants: [{nickname, account_id, won}] }

    squadIds.forEach((id, idx) => {
      const raw = historyResults[idx].data;
      const matches = Array.isArray(raw) ? raw : (raw?.matches || raw?.history || []);
      const nickname = SQUAD_MAP[id];
      const photoUrl = SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL;
      const heroCounts = {};

      for (const m of matches) {
        if ((m.start_time ?? 0) < cutoffSeconds) continue;

        const kills = m.player_kills ?? 0;
        const won = m.match_result === m.player_team;
        const durationS = m.match_duration_s ?? 0;
        const heroId = m.hero_id;
        const heroName = HERO_DICTIONARY[heroId] || `Unknown (${heroId})`;
        const matchId = m.match_id;

        heroCounts[heroId] = (heroCounts[heroId] || 0) + 1;

        // Bloodbath: most kills in any single match this week - individual
        // stat, stays single-winner (two teammates having the exact same
        // kill count in the same match is a real tie, not a data gap).
        if (!bloodbath || kills > bloodbath.kills) {
          bloodbath = { nickname, mention: discordMention(id), photo_url: photoUrl, kills, hero: heroName, hero_id: heroId, match_id: String(matchId) };
        }

        // NEW: build the match-level record (duration/hero identical
        // regardless of which squad member's entry we're reading), and
        // append this player to that match's participant list.
        if (!matchRecords[matchId]) {
          matchRecords[matchId] = { duration_seconds: durationS, hero: heroName, hero_id: heroId, participants: [] };
        }
        matchRecords[matchId].participants.push({ nickname, account_id: id, won });

        // NEW: track this game for int_of_week/coin_flip/stat_padder/time_of_day
        if (!perPlayerGames[id]) perPlayerGames[id] = [];
        perPlayerGames[id].push({
          kills, deaths: m.player_deaths ?? 0, assists: m.player_assists ?? 0,
          net_worth: m.net_worth ?? 0, won, ts: m.start_time ?? 0,
          hero: heroName, match_id: String(matchId)
        });
      }

      const heroEntries = Object.entries(heroCounts);
      if (heroEntries.length > 0) {
        const [topHeroId, topCount] = heroEntries.sort((a, b) => b[1] - a[1])[0];
        oneTrickCandidates.push({ nickname, mention: discordMention(id), photo_url: photoUrl, hero: HERO_DICTIONARY[Number(topHeroId)] || `Unknown (${topHeroId})`, games: topCount });
        explorerCandidates.push({ nickname, mention: discordMention(id), photo_url: photoUrl, distinct_heroes: heroEntries.length });
      }
    });

    // NEW: helper - turns a match's participant list into mention data,
    // used by both Marathon and Speedrun below.
    const buildMentionGroup = (participants) => ({
      nickname: participants[0].nickname, // kept for backward-compat with existing templates using .mention as a single reference
      mention: discordMention(participants[0].account_id),
      nicknames: participants.map(p => p.nickname), // NEW: full list
      mentions: participants.map(p => discordMention(p.account_id)), // NEW: full list, use this for squad games
      is_squad_game: participants.length > 1 // NEW: true if 2+ squad members were in this match
    });

    // Marathon: the single longest match this week, squad-wide. If it was a
    // shared squad game, ALL participants get mentioned via `.mentions`.
    let marathon = null;
    for (const matchId in matchRecords) {
      const rec = matchRecords[matchId];
      if (!marathon || rec.duration_seconds > marathon.duration_seconds) {
        const min = Math.floor(rec.duration_seconds / 60), sec = rec.duration_seconds % 60;
        marathon = {
          ...buildMentionGroup(rec.participants),
          duration_seconds: rec.duration_seconds,
          duration_formatted: `${min}:${String(sec).padStart(2, "0")}`,
          hero: rec.hero,
          match_id: String(matchId)
        };
      }
    }

    // Speedrun: fastest WON match this week, squad-wide. Only participants
    // who actually won count (guards the rare case where squad members
    // ended up on opposing teams in the same lobby) - only wins count, a
    // fast loss isn't a flex.
    let speedrun = null;
    for (const matchId in matchRecords) {
      const rec = matchRecords[matchId];
      const winners = rec.participants.filter(p => p.won);
      if (winners.length === 0) continue;
      if (!speedrun || rec.duration_seconds < speedrun.duration_seconds) {
        const min = Math.floor(rec.duration_seconds / 60), sec = rec.duration_seconds % 60;
        speedrun = {
          ...buildMentionGroup(winners),
          duration_seconds: rec.duration_seconds,
          duration_formatted: `${min}:${String(sec).padStart(2, "0")}`,
          hero: rec.hero,
          match_id: String(matchId)
        };
      }
    }

    // Power Duo: for every WON shared match, credit every pair of squad
    // members who won it together. Only wins count (same "can't carry a
    // loss" philosophy as squad_carry) - this rewards duo synergy, not
    // just queuing together regardless of outcome.
    for (const matchId in matchRecords) {
      const participants = matchRecords[matchId].participants;
      if (participants.length < 2 || !participants[0].won) continue;
      for (let i = 0; i < participants.length; i++) {
        for (let j = i + 1; j < participants.length; j++) {
          const pairKey = [participants[i].nickname, participants[j].nickname].sort().join("|");
          pairWinCounts[pairKey] = (pairWinCounts[pairKey] || 0) + 1;
        }
      }
    }

    const pairEntries = Object.entries(pairWinCounts);
    const powerDuo = pairEntries.length > 0
      ? (() => {
          const [key, count] = pairEntries.sort((a, b) => b[1] - a[1])[0];
          const [p1, p2] = key.split("|");
          return {
            pair: [p1, p2],
            mentions: [discordMention(NICKNAME_TO_ACCOUNT_ID[p1]), discordMention(NICKNAME_TO_ACCOUNT_ID[p2])],
            wins_together: count
          };
        })()
      : null;

    const oneTrick = oneTrickCandidates.length > 0
      ? oneTrickCandidates.sort((a, b) => b.games - a.games)[0]
      : null;

    const explorer = explorerCandidates.length > 0
      ? explorerCandidates.sort((a, b) => b.distinct_heroes - a.distinct_heroes)[0]
      : null;

    // NEW: Int of the Week - worst KDA game this week, individual stat like
    // Bloodbath. Requires 3+ deaths so a low-participation game (0/0/0)
    // doesn't get flagged as an "int" just for having a low ratio.
    let intOfWeek = null;
    for (const id of squadIds) {
      const games = perPlayerGames[id] || [];
      for (const g of games) {
        if (g.deaths < 3) continue;
        const kda = (g.kills + g.assists) / Math.max(1, g.deaths);
        if (!intOfWeek || kda < intOfWeek.kda_ratio) {
          intOfWeek = {
            nickname: SQUAD_MAP[id], mention: discordMention(id), photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
            kda_ratio: Math.round(kda * 100) / 100, kills: g.kills, deaths: g.deaths, assists: g.assists,
            hero: g.hero, match_id: g.match_id
          };
        }
      }
    }

    // NEW: Coin-Flip Award - highest kill-count variance across this week's
    // games. Needs 2+ games this week to compute meaningfully.
    const variance = (arr) => {
      if (arr.length < 2) return null;
      const mean = arr.reduce((a, b) => a + b, 0) / arr.length;
      return arr.reduce((a, b) => a + (b - mean) ** 2, 0) / arr.length;
    };
    let coinFlip = null;
    for (const id of squadIds) {
      const games = perPlayerGames[id] || [];
      if (games.length < 2) continue;
      const v = variance(games.map(g => g.kills));
      if (v !== null && (!coinFlip || v > coinFlip.variance)) {
        coinFlip = {
          nickname: SQUAD_MAP[id], mention: discordMention(id), photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
          variance: Math.round(v * 10) / 10, games_played: games.length
        };
      }
    }

    // NEW: Stat-Padder Callout - PARTIAL implementation. The original idea
    // needed "high souls, low kills, low objective participation" but
    // per-match objective data doesn't exist in any raw match field we've
    // confirmed - only souls-to-kills ratio is computable. Flagged clearly
    // in the note so this isn't mistaken for the full original concept.
    let statPadder = null;
    for (const id of squadIds) {
      const games = perPlayerGames[id] || [];
      if (games.length === 0) continue;
      const totalNetWorth = games.reduce((a, g) => a + g.net_worth, 0);
      const totalKills = games.reduce((a, g) => a + g.kills, 0);
      const ratio = totalKills > 0 ? totalNetWorth / totalKills : totalNetWorth;
      if (!statPadder || ratio > statPadder.souls_per_kill) {
        statPadder = {
          nickname: SQUAD_MAP[id], mention: discordMention(id), photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
          souls_per_kill: Math.round(ratio), games_played: games.length,
          note: "Based on souls-to-kills ratio only - objective participation data isn't available from match-history, so this is a partial version of the original idea."
        };
      }
    }

    // NEW: Time-of-Day / Day-of-Week performance - EVERY squad member gets
    // an entry (not just a single winner), per your request to add this to
    // the weekly for everyone. Day bucketing uses UTC day boundaries from
    // start_time, which may not line up exactly with Saudi local day
    // boundaries near midnight - worth knowing if a game right at the day
    // edge looks misfiled.
    const DAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
    const timeOfDay = squadIds.map(id => {
      const games = perPlayerGames[id] || [];
      if (games.length === 0) {
        return { nickname: SQUAD_MAP[id], mention: discordMention(id), photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL, has_data: false, best_day: null, worst_day: null };
      }
      const byDay = {};
      for (const g of games) {
        const day = DAY_NAMES[new Date(g.ts * 1000).getUTCDay()];
        if (!byDay[day]) byDay[day] = { games: 0, wins: 0 };
        byDay[day].games++;
        if (g.won) byDay[day].wins++;
      }
      const dayEntries = Object.entries(byDay).map(([day, s]) => ({ day, games: s.games, wins: s.wins, win_rate: Math.round((s.wins / s.games) * 100) }));
      dayEntries.sort((a, b) => b.win_rate - a.win_rate);
      return {
        nickname: SQUAD_MAP[id], mention: discordMention(id), photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
        has_data: true,
        best_day: dayEntries[0] ?? null,
        worst_day: dayEntries[dayEntries.length - 1] ?? null,
        days_played: dayEntries.length
      };
    });

    return {
      bloodbath, marathon, speedrun, one_trick: oneTrick, explorer, power_duo: powerDuo,
      int_of_week: intOfWeek, coin_flip: coinFlip, stat_padder: statPadder, time_of_day: timeOfDay
    };
  } catch (e) {
    // Whole-function safety net - if anything above throws (unexpected
    // upstream shape, etc.), return all-null instead of failing the cron's
    // leaderboard compile step or serving a broken payload.
    log("WEEKLY MOMENTS ERR", e.message);
    return { bloodbath: null, marathon: null, speedrun: null, one_trick: null, explorer: null, power_duo: null, int_of_week: null, coin_flip: null, stat_padder: null, time_of_day: [] };
  }
}

// ============================================================
// NEW: WEEKLY AWARDS (fun highlight categories, not just a leaderboard)
// ============================================================
// weekly_recap_leaderboard above is ONE array sorted by kills_delta - so
// referencing .0 for "biggest climber" or "most commends" would actually
// just show the kills leader again, not the real winner of that category.
// This computes each award independently (its own sort), so every category
// can correctly have a different winner. Pure derived transform of data
// already fetched for weekly_recap_leaderboard - no extra KV reads.
// ============================================================
// NEW: WEAKNESS SPOTLIGHT (per-player, compares recent vs own lifetime norm)
// ============================================================
// Different from every squad leaderboard so far - this is personal, not
// competitive. Compares this week's per-game rate (from the same Friday
// checkpoint diff used elsewhere) against the player's own LIFETIME
// per-game average (from account_stats), and finds whichever stat has
// dropped the most relative to their own historical norm - not a
// comparison against anyone else.
const WEAKNESS_STAT_MAP = [
  { deltaField: "kills_delta", lifetimeField: "kills", label: "Kills" },
  { deltaField: "assists_delta", lifetimeField: "assists", label: "Assists" },
  { deltaField: "hero_damage_delta", lifetimeField: "hero_damage", label: "Hero Damage" },
  { deltaField: "obj_damage_delta", lifetimeField: "obj_damage", label: "Objective Damage" },
  { deltaField: "healing_delta", lifetimeField: "healing", label: "Healing" },
  { deltaField: "souls_delta", lifetimeField: "souls_collected", label: "Souls Collected" }
];

function computeWeaknessSpotlight(accountStats, weeklyDiff) {
  if (!weeklyDiff || !weeklyDiff.has_data || !weeklyDiff.matches_delta || weeklyDiff.matches_delta <= 0) {
    return { ready: false, note: "Needs at least one full week of checkpoint data with matches played that week." };
  }
  if (!accountStats || !accountStats.matches_played) {
    return { ready: false, note: "Missing lifetime account stats." };
  }

  let weakest = null;
  for (const { deltaField, lifetimeField, label } of WEAKNESS_STAT_MAP) {
    const recentPerGame = (weeklyDiff[deltaField] ?? 0) / weeklyDiff.matches_delta;
    const lifetimePerGame = (accountStats[lifetimeField] ?? 0) / accountStats.matches_played;
    if (lifetimePerGame <= 0) continue; // avoid divide-by-zero / meaningless ratio
    const ratio = recentPerGame / lifetimePerGame;
    if (!weakest || ratio < weakest.ratio) {
      weakest = {
        stat: deltaField.replace("_delta", ""),
        label,
        recent_per_game: Math.round(recentPerGame * 10) / 10,
        lifetime_per_game: Math.round(lifetimePerGame * 10) / 10,
        ratio_pct: Math.round(ratio * 100) // e.g. 62 means "62% of your normal rate this week"
      };
    }
  }

  return { ready: true, weakest_stat: weakest };
}

function computeWeeklyAwards(board) {
  const withData = board.filter(m => m.has_data);
  // FIX: was a bare `null` here, which meant every {weeklyapi.response.
  // weekly_awards.X.field} reference in a Discord message showed as raw
  // unresolved placeholder text before the second Friday checkpoint - ugly
  // and confusing. Now returns an explicit ready:false object instead, so
  // the whole section can be gated with one IF check (weekly_awards.ready)
  // and replaced with a friendly "check back Friday" message instead.
  if (withData.length === 0) {
    return {
      ready: false,
      note: "Needs a second Friday 1pm checkpoint to compute a diff - only one checkpoint (or none) has been taken so far."
    };
  }

  const round1 = (n) => Math.round(n * 10) / 10;

  // Derived per-player fields not stored directly, computed from what IS
  // stored (matches_delta, wins_delta, kills_delta, souls_delta).
  const enriched = withData.map(m => {
    const matches = m.matches_delta ?? 0;
    return {
      ...m,
      losses_delta: matches - (m.wins_delta ?? 0),
      win_rate_delta: matches > 0 ? Math.round(((m.wins_delta ?? 0) / matches) * 100) : null, // NEW: rounded to whole %, was a raw float before
      kills_per_game: matches > 0 ? round1((m.kills_delta ?? 0) / matches) : null,   // NEW: rewards quality over volume
      souls_per_game: matches > 0 ? Math.round((m.souls_delta ?? 0) / matches) : null, // NEW: farming efficiency, not just total grind time
      carry_score: (m.kills_delta ?? 0) + (m.assists_delta ?? 0) // NEW: total team-fight contribution
    };
  });

  const topBy = (field, source = enriched) => {
    const sorted = [...source].sort((a, b) => (b[field] ?? -Infinity) - (a[field] ?? -Infinity));
    const winner = sorted[0];
    return winner ? { nickname: winner.nickname, mention: discordMention(winner.account_id), photo_url: winner.photo_url, value: winner[field] ?? 0 } : null;
  };

  const bottomBy = (field, source = enriched) => {
    const sorted = [...source].sort((a, b) => (a[field] ?? Infinity) - (b[field] ?? Infinity));
    const winner = sorted[0];
    return winner ? { nickname: winner.nickname, mention: discordMention(winner.account_id), photo_url: winner.photo_url, value: winner[field] ?? 0 } : null;
  };

  // "Worst win rate" and per-game stats only count players who actually
  // played this week - someone who played 0 games has an undefined rate,
  // not a 0% rate, and shouldn't win/lose a category for a week they
  // weren't even active.
  const playedThisWeek = enriched.filter(m => m.win_rate_delta !== null);

  // NEW: PLAYER OF THE WEEK - a fair overall crown across 7 core categories,
  // using RANKED POINTS instead of a raw weighted sum. A raw sum would be
  // dominated by whichever stat has the biggest numbers (hero_damage/souls
  // are in the thousands, kills/commends are single digits) - ranked points
  // sidesteps that entirely: in each category, 1st place scores (squad
  // size) points, last place scores 1 point, then all 7 categories get
  // summed. This rewards genuinely well-rounded performance instead of
  // whoever happens to play the stat with the biggest raw numbers.
  const crownCategories = ["kills_delta", "assists_delta", "hero_damage_delta", "obj_damage_delta", "healing_delta", "souls_delta", "wins_delta"];
  const squadSize = enriched.length;
  const scores = {}; // nickname -> { total, categoriesWon }
  enriched.forEach(m => { scores[m.nickname] = { total: 0, categoriesWon: 0 }; });

  for (const cat of crownCategories) {
    const sorted = [...enriched].sort((a, b) => (b[cat] ?? -Infinity) - (a[cat] ?? -Infinity));
    sorted.forEach((m, idx) => {
      scores[m.nickname].total += (squadSize - idx);
      if (idx === 0) scores[m.nickname].categoriesWon += 1;
    });
  }

  const crownWinnerNickname = Object.entries(scores).sort((a, b) => b[1].total - a[1].total)[0][0];
  const crownWinnerMember = enriched.find(m => m.nickname === crownWinnerNickname);
  const playerOfTheWeek = {
    nickname: crownWinnerNickname,
    mention: discordMention(crownWinnerMember?.account_id),
    photo_url: crownWinnerMember?.photo_url ?? null,
    categories_won: scores[crownWinnerNickname].categoriesWon,
    total_categories: crownCategories.length
  };

  return {
    ready: true, // NEW: consistent with the ready:false early-return above

    // The headline category - a genuine overall crown, not just "most kills"
    player_of_the_week: playerOfTheWeek,

    mvp: topBy("kills_delta"),               // most kills this week
    biggest_climber: topBy("rank_points_delta"), // most rank points gained this week
    team_medic: topBy("healing_delta"),       // most healing done this week
    top_farmer: topBy("souls_delta"),         // most souls collected this week
    most_feared: topBy("hero_damage_delta"),  // most hero damage dealt this week
    most_respected: topBy("commends_delta"),  // most commends received this week
    grindset: topBy("matches_delta"),         // most matches played this week
    demolitions_expert: topBy("obj_damage_delta"), // most objective damage this week

    // NEW: efficiency categories - reward quality, not just volume/who played the most
    sharpshooter: playedThisWeek.length > 0 ? topBy("kills_per_game", playedThisWeek) : null,  // best kills-per-game average
    goldmine: playedThisWeek.length > 0 ? topBy("souls_per_game", playedThisWeek) : null,        // best souls-per-game average
    carry_of_the_week: topBy("carry_score"),  // highest kills+assists combined

    // Roast categories
    most_losses: topBy("losses_delta"),       // most losses this week
    worst_win_rate: playedThisWeek.length > 0 ? bottomBy("win_rate_delta", playedThisWeek) : null, // lowest win% this week, among those who played
    couch_potato: bottomBy("matches_delta"),  // fewest matches played this week
    biggest_derank: bottomBy("rank_points_delta"), // most rank points LOST this week
    ice_cold: bottomBy("kills_delta")         // fewest kills this week
  };
}

// ============================================================
// NEW: CACHE PEEK (read-only status check, no upstream fetch)
// ============================================================
// Used by the /health endpoint to report fresh/stale/missing per cache key
// WITHOUT triggering a live refresh the way getEndpointData() would. Checks
// memory first, then KV, same order as the real cache-first path, but never
// calls refreshData().
async function peekCacheStatus(key, baseTtlMs, env) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  let payload = memoryCache.get(key);
  let source = "memory";

  if (!payload && kvBinding) {
    try {
      const kvStr = await kvBinding.get(key);
      if (kvStr) { payload = JSON.parse(kvStr); source = "kv"; }
    } catch (e) { /* treat as missing */ }
  }

  if (!payload) return { status: "missing" };

  const ageMs = Date.now() - (payload.expiresAt - baseTtlMs);
  const isExpired = Date.now() > payload.expiresAt;
  return {
    status: isExpired ? "stale" : "fresh",
    source,
    age_seconds: Math.max(0, Math.round(ageMs / 1000)),
    has_error: !!payload.error
  };
}

// ============================================================
// NEW: RANKED-MODE ELIGIBILITY (best-effort, from /match-history)
// ============================================================
// Beta Season 1 gates Ranked behind 60 Standard-mode wins + 15 Standard
// wins on each of 3 heroes (deadlock.wiki, checked live Aug 2026). There's
// no dedicated "eligibility" endpoint in the API, so this is computed from
// whatever /match-history returns for the account.
//
// CONFIRMED from live match-history entries:
//   - Ranked matches carry `ranked_delta`, `ranked_display_badge`, and
//     `ranked_calibration_match` fields (e.g. ranked_delta: 300 - the Aug 2
//     patch's +20% RP change). Non-ranked matches don't carry these at all.
//   - Every match entry carries `brawl_score_team0`, `brawl_score_team1`,
//     and `brawl_avg_round_time_s` fields, all zero on a confirmed non-
//     Street-Brawl match. Street Brawl is round-based (unlike every other
//     mode), so a non-zero round time or score is a strong signal the match
//     WAS Street Brawl - used here to exclude those from standard_wins per
//     your callout that Street Brawl shouldn't count.
// Classification: ranked_delta present => Ranked. Absent + brawl fields
// non-zero => Street Brawl (excluded entirely). Absent + brawl fields zero
// => counted as Standard.
// Two caveats still surfaced in the output:
//   1. Bot Match still isn't distinguishable from Standard with any field
//      confirmed so far - it also lacks ranked_delta and (presumably) has
//      zero brawl fields, so Bot Match wins may still inflate standard_wins
//      if the account plays that mode. No known field to separate it out yet.
//   2. This can only count matches /match-history actually returns. If
//      that endpoint is capped (e.g. last N matches) rather than full
//      account history, these numbers are a LOWER BOUND, not a guarantee -
//      a "counted 45 wins, need 60" player may already be eligible in-game.
function computeRankedEligibility(historyRaw) {
  const matches = Array.isArray(historyRaw) ? historyRaw : (historyRaw?.matches || historyRaw?.history || []);

  let standardWins = 0, rankedWins = 0, calibrationMatches = 0, streetBrawlMatches = 0;
  const heroStandardWins = {};

  for (const m of matches) {
    const won = m.match_result === m.player_team;
    const isRanked = pickField(m, ["ranked_delta", "ranked_display_badge", "ranked_calibration_match"]) !== undefined;

    if (isRanked) {
      if (m.ranked_calibration_match) calibrationMatches++;
      if (won) rankedWins++;
      continue;
    }

    // NEW: Street Brawl detection via the brawl_* fields - excluded from
    // standard_wins entirely per your note that those shouldn't count.
    const isStreetBrawl = (m.brawl_avg_round_time_s > 0) || (m.brawl_score_team0 > 0) || (m.brawl_score_team1 > 0);
    if (isStreetBrawl) {
      streetBrawlMatches++;
      continue;
    }

    // Not Ranked, not Street Brawl -> counted as Standard (see caveat #1:
    // Bot Match wins may still be mixed in here, no confirmed field to
    // split them out yet).
    if (won) {
      standardWins++;
      const heroId = m.hero_id;
      if (heroId != null) heroStandardWins[heroId] = (heroStandardWins[heroId] || 0) + 1;
    }
  }

  const eligibleHeroes = Object.entries(heroStandardWins)
    .filter(([, wins]) => wins >= 15)
    .map(([heroId, wins]) => ({
      hero_id: Number(heroId),
      hero_name: HERO_DICTIONARY[Number(heroId)] || `Unknown (${heroId})`,
      emoji: heroEmojiTag(Number(heroId)), // e.g. "<:dl_pocket:123...>" once IDs are filled, ":dl_pocket:" fallback until then
      wins
    }))
    .sort((a, b) => b.wins - a.wins);

  // NEW: heroes sitting close to the 15-win threshold, flagged separately.
  // CONFIRMED via a real account (elyas): a hero can show as unlocked
  // in-game while sitting at 13/15 here, because /match-history has a
  // small, consistent gap vs a player's true match count (missing ~3
  // matches per account, likely abandoned/edge-case games) - close enough
  // that 1-2 unseen wins on a borderline hero fully explains the mismatch.
  // Rather than flatly saying "not unlocked" for these, they're called out
  // as likely already earned in-game.
  const nearThresholdHeroes = Object.entries(heroStandardWins)
    .filter(([, wins]) => wins >= 13 && wins < 15)
    .map(([heroId, wins]) => ({
      hero_id: Number(heroId),
      hero_name: HERO_DICTIONARY[Number(heroId)] || `Unknown (${heroId})`,
      emoji: heroEmojiTag(Number(heroId)),
      wins,
      wins_needed: 15 - wins
    }))
    .sort((a, b) => b.wins - a.wins);

  // NEW: diagnostic - the actual date range covered by the matches we got
  // back. If oldest_match_date lands suspiciously recent (e.g. after Beta
  // Season 1 launched July 30 2026) despite ?limit=1000 and a player having
  // played long before that, it proves /match-history is capped to a
  // recent window rather than truly returning full account history -
  // explaining why hero_standard_wins can undercount heroes a player
  // qualified on using older matches this endpoint can't see.
  const startTimes = matches.map(m => m.start_time).filter(t => t != null);
  const oldestMatchDate = startTimes.length > 0 ? new Date(Math.min(...startTimes) * 1000).toISOString() : null;
  const newestMatchDate = startTimes.length > 0 ? new Date(Math.max(...startTimes) * 1000).toISOString() : null;

  return {
    standard_wins: standardWins,
    ranked_wins: rankedWins,
    calibration_matches_seen: calibrationMatches,
    street_brawl_matches_excluded: streetBrawlMatches, // NEW: visibility into how many were filtered out
    matches_seen: matches.length,
    oldest_match_date: oldestMatchDate, // NEW: diagnostic
    newest_match_date: newestMatchDate, // NEW: diagnostic
    hero_standard_wins: Object.fromEntries(
      Object.entries(heroStandardWins).map(([id, wins]) => [HERO_DICTIONARY[Number(id)] || id, wins])
    ),
    eligible_heroes: eligibleHeroes,
    near_threshold_heroes: nearThresholdHeroes, // NEW: 13-14 wins - likely already unlocked in-game, see note
    eligible_heroes_names: eligibleHeroes.length > 0
      ? eligibleHeroes.map(h => h.hero_name).join(", ")
      : "None yet", // NEW: ready-to-embed string, no loop needed in BotGhost
    eligible_heroes_display: eligibleHeroes.length > 0
      ? eligibleHeroes.map(h => `${h.emoji} ${h.hero_name}`.trim()).join("  ")
      : "None yet", // NEW: emoji-prefixed version, e.g. ":dl_pocket: Pocket  :dl_victor: Victor"
    ranked_unlocked: standardWins >= 60 && eligibleHeroes.length >= 3,
    wins_needed: Math.max(0, 60 - standardWins),
    heroes_needed: Math.max(0, 3 - eligibleHeroes.length),
    note: `Based on the ${matches.length} matches /match-history returned (may not be full account history - CONFIRMED via a real account that this can undercount by a couple wins on borderline heroes, since a small number of matches per account aren't visible to this endpoint). Street Brawl matches are excluded (${streetBrawlMatches} filtered out). Heroes at 13-14 wins are flagged separately in near_threshold_heroes since they may already be unlocked in-game despite showing under 15 here.`
  };
}

// ============================================================
// RATE LIMITER QUEUE
// ============================================================
function processLocalQueue() {
  if (localActiveRequests >= CONFIG.CONCURRENCY || localQueue.length === 0) return;
  localActiveRequests++;
  
  const { url, resolve, reject } = localQueue.shift();

  fetchAPIWithBackoff(url)
    .then(resolve).catch(reject)
    .finally(() => {
      setTimeout(() => {
        localActiveRequests--;
        processLocalQueue();
      }, CONFIG.REQUEST_DELAY_MS);
    });
}

function enqueueAPIRequest(url) {
  return new Promise((resolve, reject) => {
    localQueue.push({ url, resolve, reject });
    processLocalQueue();
  });
}

// ============================================================
// EXPONENTIAL BACKOFF FETCH
// ============================================================
async function fetchAPIWithBackoff(url) {
  let attempt = 1;
  while (attempt <= (CONFIG.MAX_RETRIES + 1)) {
    try {
      log("FETCH", `API request to ${url} (Attempt ${attempt})`);
      const res = await fetch(url, { headers: API_HEADERS });

      if (res.ok) {
        const data = await res.json();
        if (!data || (Array.isArray(data) && data.length === 0 && !url.includes('match-history')) || Object.keys(data).length === 0) {
          throw new Error("API returned empty data structure");
        }
        return { data, error: null };
      }

      if ([429, 500, 502, 503, 504].includes(res.status) && attempt <= CONFIG.MAX_RETRIES) {
        let baseDelay = attempt === 1 ? 2000 : attempt === 2 ? 5000 : 10000;
        let jitter = Math.random() * (attempt === 1 ? 1000 : attempt === 2 ? 2000 : 5000);
        let delay = baseDelay + jitter;

        const retryAfter = res.headers.get("Retry-After");
        if (retryAfter) delay = Math.max(delay, parseInt(retryAfter, 10) * 1000);

        log("RETRY", `Status ${res.status} on ${url}. Waiting ${Math.round(delay)}ms.`);
        await new Promise(r => setTimeout(r, delay));
        attempt++;
      } else {
        return { data: null, error: res.status };
      }
    } catch (e) {
      log("ERROR", `Fetch failed on ${url}: ${e.message}`);
      if (attempt <= CONFIG.MAX_RETRIES) {
        let baseDelay = attempt === 1 ? 2000 : attempt === 2 ? 5000 : 10000;
        await new Promise(r => setTimeout(r, baseDelay + Math.random() * 2000));
        attempt++;
      } else {
        return { data: null, error: 'MAX_RETRIES_EXCEEDED' };
      }
    }
  }
  return { data: null, error: 'MAX_RETRIES_EXCEEDED' };
}

// ============================================================
// CACHE ARCHITECTURE: Cache-First Data Retrieval
// ============================================================
async function getEndpointData(key, url, baseTtlMs, env, ctx, forceRefresh = false) {
  if (pendingRequests.has(key)) {
    log("DEDUPE", `Awaiting existing promise for ${key}`);
    return pendingRequests.get(key);
  }

  const promise = (async () => {
    const kvBinding = env.DEADLOCK_KV || env.KV;
    let cachedPayload = memoryCache.get(key);
    
    // Layer 2: Check KV ONLY if memory misses
    if (!cachedPayload && kvBinding) {
      try {
        const kvStr = await kvBinding.get(key);
        if (kvStr) {
          cachedPayload = JSON.parse(kvStr);
          memoryCache.set(key, cachedPayload);
          log("CACHE", `KV Hit: ${key}`);
        }
      } catch (e) { log("KV ERROR", e.message); }
    }

    const now = Date.now();
    const ttlMs = getJitteredTTL(baseTtlMs);
    const isExpired = !cachedPayload || now > cachedPayload.expiresAt;
    const isExpiringSoon = cachedPayload && (cachedPayload.expiresAt - now) < (baseTtlMs * CONFIG.REFRESH_WINDOW_PCT);
    const needsRefresh = forceRefresh || isExpired || isExpiringSoon;

    // SCENARIO 1: We have ANY cached data (even if stale, even if it's a cached
    // negative/error result) -> Return immediately. Cache-First!
    if (cachedPayload && !forceRefresh) {
      if (needsRefresh) {
        log("SWR", `Serving ${key} directly, triggering background refresh.`);
        const refreshTask = refreshData(key, url, ttlMs, env, cachedPayload.data).catch(e => log("SWR ERR", e));
        if (ctx && ctx.waitUntil) ctx.waitUntil(refreshTask);
      } else {
        log("CACHE", `Fresh Hit: ${key}`);
      }
      // FIX: propagate the cached error (if this was a negative-cached failure)
      // instead of always reporting error: null on a cache hit.
      return { data: cachedPayload.data, error: cachedPayload.error || null };
    }

    // SCENARIO 2: No data exists at all (or Cron Force Refresh) -> Await physical fetch
    log("FETCH", `Cache Miss or Force. Building ${key}`);
    return refreshData(key, url, ttlMs, env, cachedPayload ? cachedPayload.data : null);
  })();

  pendingRequests.set(key, promise);
  try { return await promise; } finally { pendingRequests.delete(key); }
}

// Background API Fetch & Save to Stores
async function refreshData(key, url, ttlMs, env, staleData) {
  const result = await enqueueAPIRequest(url);
  const kvBinding = env.DEADLOCK_KV || env.KV;

  // Fallback Rule: NEVER return empty/error if we have ANY stale cache
  if (result.error || !result.data) {
    if (staleData) {
      log("FALLBACK", `Serving STALE data for ${key} due to API Error: ${result.error}`);
      return { data: staleData, error: `${result.error} (STALE CACHE)` };
    }

    // FIX: negative-cache the failure briefly. Previously a hard failure with no
    // stale data was never written anywhere, so the very next request for this
    // key (e.g. a permanently-403'ing account) would immediately re-fetch again -
    // on every single incoming Discord/BotGhost request. That ate into the shared
    // CONCURRENCY(8) budget and upstream rate limit, occasionally starving other,
    // healthy accounts and causing THEM to fall back to stale data too.
    const negPayload = { data: null, error: result.error, expiresAt: Date.now() + getJitteredTTL(CONFIG.TTL.ERROR) };
    memoryCache.set(key, negPayload);
    if (kvBinding) {
      try {
        await kvBinding.put(key, JSON.stringify(negPayload), { expirationTtl: 300 });
      } catch (e) { log("KV PUT ERR", e.message); }
    }

    return { data: null, error: result.error };
  }

  const payload = { data: result.data, error: null, expiresAt: Date.now() + ttlMs };
  
  // Instant Memory Write
  memoryCache.set(key, payload);

  // FIX: Background KV Write is now AWAITED. A fire-and-forget put() has no
  // guarantee of completing once the Worker's execution context is torn down -
  // which happens right after the enclosing await chain resolves (e.g. right
  // after Promise.allSettled() in the cron handler, or right after a Response
  // is returned in the fetch handler). Losing writes silently and
  // unpredictably like that is what caused stats to look like they were
  // "changing for no reason": different edge isolates ended up holding
  // different partial views of KV.
  if (kvBinding) {
    try {
      await kvBinding.put(key, JSON.stringify(payload), { expirationTtl: 86400 });
    } catch (e) { log("KV PUT ERR", e.message); }
  }

  return { data: result.data, error: null };
}

// ============================================================
// NEW: MATCH SCOREBOARD BUILDER
// ============================================================
// Live-computed on request (like synergy_lookup), NOT cron-precompiled
// like the squad leaderboards - a specific match_id is a one-off lookup,
// not something that benefits from a scheduled rebuild. Still goes through
// the same cache-first getEndpointData() pipeline for the underlying
// upstream fetch though, so re-requesting the same match_id twice (e.g.
// items lookup + scoreboard lookup on the same recent match) doesn't cost
// a second upstream call.
//
// EXPERIMENTAL FIELD NAMES: the /v1/matches/{id}/metadata endpoint path is
// confirmed working (verified live via the item-purchase-log feature), but
// the exact field names for the full 12-player roster inside that payload
// are NOT yet confirmed - this tries several plausible shapes/names for
// both the roster location and each per-player stat, and always attaches
// the raw payload plus a per-dimension "was this field actually found"
// report so the guesses can be corrected in one pass once you've hit this
// live and shared the raw output.
const SCOREBOARD_DIMENSIONS = [
  { key: "combat", label: "Combat", weight: 0.25, compute: p => (p.kills * 2 + p.assists) / p.durationMin },
  { key: "economy", label: "Economy", weight: 0.15, compute: p => p.net_worth / p.durationMin },
  { key: "survival", label: "Survival", weight: 0.15, compute: p => -(p.deaths / p.durationMin) }, // negated - fewer deaths per minute is better
  { key: "lane", label: "Lane Presence", weight: 0.10, compute: p => (p.last_hits + p.denies * 1.5) / p.durationMin },
  { key: "damage", label: "Damage Dealt", weight: 0.20, compute: p => p.hero_damage / p.durationMin, requiresField: "hero_damage" },
  { key: "support", label: "Objective/Healing", weight: 0.15, compute: p => (p.obj_damage + p.healing) / p.durationMin, requiresField: ["obj_damage", "healing"] }
];

// Deadlock's two real teams (confirmed via deadlock.wiki): The Sapphire
// Flame (blue) and The Amber Hand (orange). NOT YET CONFIRMED: which
// numeric `team` value (0 or 1) the API assigns to which side - this is a
// reasonable default guess. If it comes out backwards once you see it live
// (e.g. your Amber-side games show as "Sapphire Flame"), just swap the two
// values below in one line.
const TEAM_NAMES = { 0: "The Amber Hand", 1: "The Sapphire Flame" };
function teamLabel(team) {
  return TEAM_NAMES[team] ?? (team != null ? `Team ${team}` : "Unknown");
}

function fmtTime(s) {
  if (s == null) return null;
  return `${Math.floor(s / 60)}:${String(Math.floor(s % 60)).padStart(2, "0")}`;
}

// Linear interpolation of a per-player stat between whichever two periodic
// snapshots bracket the target time - the /metadata endpoint's `stats`
// array is a sparse checkpoint log (roughly every 3-5 min, confirmed live:
// 180/360/540/720/900/1200/1500/1800/end), NOT a per-minute time series, so
// there is usually no snapshot sitting exactly on a target second like 600
// (10:00). Interpolating between the two closest real snapshots is far more
// accurate than just grabbing the nearest one.
function interpolateSnapshotField(snapshots, targetS, field) {
  if (!Array.isArray(snapshots) || snapshots.length === 0) return null;
  const sorted = [...snapshots].sort((a, b) => a.time_stamp_s - b.time_stamp_s);
  if (targetS <= sorted[0].time_stamp_s) return sorted[0][field] ?? null;
  for (let i = 0; i < sorted.length - 1; i++) {
    const a = sorted[i], b = sorted[i + 1];
    if (targetS >= a.time_stamp_s && targetS <= b.time_stamp_s) {
      const span = b.time_stamp_s - a.time_stamp_s;
      const frac = span > 0 ? (targetS - a.time_stamp_s) / span : 0;
      const av = a[field] ?? 0, bv = b[field] ?? 0;
      return Math.round(av + frac * (bv - av));
    }
  }
  return sorted[sorted.length - 1][field] ?? null; // target is past the last checkpoint (e.g. match ended before 10:00)
}

// ============================================================
// MATCH OBJECTIVES (Mid-Boss, Soul Urn, lane winners)
// ============================================================
// TERMINOLOGY (confirmed via deadlock.wiki and community guides):
//   - Mid-Boss: neutral jungle boss, first spawns 10:00, respawns 7 min
//     after each kill. Killing it grants the whole team souls + stat
//     buffs and drops a "Rejuvenator" crystal (punch it for revive charges).
//   - Soul Urn: a SEPARATE objective, also first appears at 10:00 (then
//     every ~5 min). Must be carried on foot to the far side of the map to
//     "deliver" it for a team-wide reward. Not the same thing as Mid-Boss.
//   - Lanes: each match randomly assigns 2 of the 4 lanes as 1v1 and the
//     other 2 as 2v2 - so lane winner is a fresh per-match comparison.
//
// DETECTION METHOD (verified against a real live match payload on Aug
// 2026): there is no explicit objective/event-log array in this endpoint's
// response - so Mid-Boss and Urn are instead detected from real, confirmed
// per-player fields: each periodic `stats` snapshot carries a cumulative
// `gold_boss` (souls earned from Mid-Boss kills) and `gold_treasure`
// (souls earned from Urn delivery) value. A step-increase in either
// between two consecutive snapshots means that team earned that reward
// sometime in between - reported as a time WINDOW (bounded by the two
// snapshot timestamps), not an exact second, since the snapshots
// themselves are only taken every 3-5 minutes. This is real signal from
// confirmed fields, just coarser resolution than a true event timestamp.
function detectObjectiveWindows(players, statField, objectiveName) {
  const seen = new Set();
  const events = [];
  for (const p of players) {
    const snaps = Array.isArray(p.raw.stats) ? [...p.raw.stats].sort((a, b) => a.time_stamp_s - b.time_stamp_s) : [];
    for (let i = 1; i < snaps.length; i++) {
      const prev = snaps[i - 1][statField] ?? 0;
      const curr = snaps[i][statField] ?? 0;
      if (curr > prev) {
        // Dedup by (team, window) since every player on the credited team
        // shows the same jump at the same checkpoint pair - without this,
        // a 3-player Mid-Boss kill would report as 3 separate "events".
        const key = `${p.team}|${snaps[i - 1].time_stamp_s}|${snaps[i].time_stamp_s}`;
        if (seen.has(key)) continue;
        seen.add(key);
        const windowFormatted = `${fmtTime(snaps[i - 1].time_stamp_s)}–${fmtTime(snaps[i].time_stamp_s)}`;
        events.push({
          team: p.team,
          team_label: teamLabel(p.team), // NEW
          objective: objectiveName, // NEW: "Mid-Boss" or "Soul Urn"
          window_start_s: snaps[i - 1].time_stamp_s,
          window_end_s: snaps[i].time_stamp_s,
          window_formatted: windowFormatted,
          // NEW: ready-to-post sentence, e.g. "The Amber Hand secured
          // Mid-Boss between 9:00–12:00"
          summary: `${teamLabel(p.team)} secured ${objectiveName} between ${windowFormatted}`
        });
      }
    }
  }
  events.sort((a, b) => a.window_start_s - b.window_start_s);
  return events;
}

// Lane winners: needs BOTH the confirmed `assigned_lane` field AND a
// minute-10 net-worth comparison between whoever shares that lane. Uses
// interpolation (see above) rather than end-of-match totals, which would
// unfairly reflect the whole game instead of just the 10-minute laning phase.
function computeLaneWinners(players) {
  const withLanes = players.filter(p => p.lane != null);
  if (withLanes.length === 0) {
    return { lane_winners: null, note: "No assigned_lane field found on this match's players - lane winners can't be computed." };
  }

  const lanes = {};
  for (const p of withLanes) {
    const netWorthAt10 = interpolateSnapshotField(p.raw.stats, 600, "net_worth");
    if (!lanes[p.lane]) lanes[p.lane] = [];
    lanes[p.lane].push({
      account_id: p.account_id, nickname: SQUAD_MAP[p.account_id] || null,
      hero: HERO_DICTIONARY[p.hero_id] || `Unknown (${p.hero_id})`,
      team: p.team, net_worth_at_10min: netWorthAt10 ?? 0
    });
  }

  const laneWinners = Object.entries(lanes).map(([lane, laners]) => {
    const byTeam = {};
    for (const l of laners) {
      if (!byTeam[l.team]) byTeam[l.team] = { team: l.team, total_net_worth_at_10min: 0, players: [] };
      byTeam[l.team].total_net_worth_at_10min += l.net_worth_at_10min;
      byTeam[l.team].players.push(l);
    }
    const teams = Object.values(byTeam).sort((a, b) => b.total_net_worth_at_10min - a.total_net_worth_at_10min);
    const isSolo = laners.length === 2;
    const margin = teams.length > 1 ? teams[0].total_net_worth_at_10min - teams[1].total_net_worth_at_10min : null;
    const winnerNames = teams[0]?.players.map(p => p.nickname || p.hero).join(" & ") || "Unknown";

    return {
      lane: Number(lane),
      is_solo_lane: isSolo, // 1v1 lane vs a 2v2 lane
      lane_type: isSolo ? "1v1" : "2v2", // NEW: ready-to-display string
      winning_team: teams[0]?.team ?? null,
      winning_team_label: teamLabel(teams[0]?.team), // NEW
      margin, // net worth advantage at the 10-minute mark
      teams,
      // NEW: ready-to-post sentence, e.g. "Lane 2 (2v2): The Sapphire Flame
      // won (teacup & zes), up 1,340 souls at 10:00"
      summary: margin != null
        ? `Lane ${lane} (${isSolo ? "1v1" : "2v2"}): ${teamLabel(teams[0]?.team)} won (${winnerNames}), up ${margin.toLocaleString()} souls at 10:00`
        : `Lane ${lane} (${isSolo ? "1v1" : "2v2"}): not enough data to compare`
    };
  });

  return {
    lane_winners: laneWinners,
    lane_winners_text: laneWinners.length > 0 ? laneWinners.map(l => l.summary).join("\n") : "No lane data available this match.", // NEW: ready-to-paste block
    note: null
  };
}

async function buildMatchScoreboard(matchId, env, ctx) {
  const res = await getEndpointData(
    `matchdetails:${matchId}`,
    `https://api.deadlock-api.com/v1/matches/${matchId}/metadata`,
    CONFIG.TTL.HISTORY, env, ctx, false
  );

  if (res.error) {
    return { error: `/v1/matches/${matchId}/metadata returned an error: ${res.error}. This is the confirmed endpoint path, so an error likely means the match hasn't finished replay-processing yet, or the match_id doesn't exist.` };
  }

  const raw = unwrapPayload(res.data);
  // FIX (confirmed live Aug 2026): the roster and match-level fields live
  // one level deeper, under `match_info`, not at the top level of the
  // payload - this was the actual cause of the earlier "no recognizable
  // player roster array" error. Falls back to `raw` itself if a future
  // API change ever flattens this, so it doesn't hard-break either way.
  const matchInfo = pickField(raw, ["match_info"]) || raw;
  const rosterRaw = pickField(matchInfo, ["players", "match_players", "player_info"]);
  if (!Array.isArray(rosterRaw) || rosterRaw.length === 0) {
    return { error: "Endpoint responded but no recognizable player roster array was found on the match payload, even after unwrapping match_info.", raw };
  }

  const matchDurationS = pickField(matchInfo, ["duration_s", "match_duration_s"]) ?? 0;
  const winningTeam = pickField(matchInfo, ["winning_team", "match_outcome"]);
  const durationMin = Math.max(0.5, matchDurationS / 60);

  const players = rosterRaw.map(p => {
    const accountId = pickField(p, ["account_id", "steam_id", "player_id"]);
    const heroId = pickField(p, ["hero_id"]);
    const snaps = Array.isArray(p.stats) ? [...p.stats].sort((a, b) => a.time_stamp_s - b.time_stamp_s) : [];
    const finalSnap = snaps.length > 0 ? snaps[snaps.length - 1] : null;
    return {
      account_id: accountId != null ? String(accountId) : null,
      hero_id: heroId ?? null,
      team: pickField(p, ["team", "player_team"]) ?? null,
      lane: pickField(p, ["assigned_lane", "lane"]) ?? null, // CONFIRMED field: assigned_lane
      kills: Number(pickField(p, ["kills", "player_kills"])) || 0,
      deaths: Number(pickField(p, ["deaths", "player_deaths"])) || 0,
      assists: Number(pickField(p, ["assists", "player_assists"])) || 0,
      net_worth: Number(pickField(p, ["net_worth"])) || 0,
      last_hits: Number(pickField(p, ["last_hits"])) || 0,
      denies: Number(pickField(p, ["denies"])) || 0,
      hero_level: Number(pickField(p, ["level"])) || 0, // CONFIRMED field: level (not hero_level)
      // CONFIRMED: end-of-match hero damage/healing/boss(objective) damage
      // live in the FINAL periodic stats snapshot as player_damage/
      // player_healing/boss_damage, not as separate top-level fields.
      hero_damage: Number(finalSnap?.player_damage) || 0,
      obj_damage: Number(finalSnap?.boss_damage) || 0,
      healing: Number(finalSnap?.player_healing) || 0,
      durationMin,
      raw: p
    };
  });

  const fieldHasSignal = (field) => players.some(p => p[field] > 0);
  const activeDimensions = SCOREBOARD_DIMENSIONS.filter(dim => {
    if (!dim.requiresField) return true;
    const fields = Array.isArray(dim.requiresField) ? dim.requiresField : [dim.requiresField];
    return fields.some(f => fieldHasSignal(f));
  });
  const totalWeight = activeDimensions.reduce((a, d) => a + d.weight, 0);

  const dimensionZScores = {};
  for (const dim of activeDimensions) {
    dimensionZScores[dim.key] = computeZScores(players.map(dim.compute));
  }

  const scored = players.map((p, idx) => {
    let compositeZ = 0;
    for (const dim of activeDimensions) {
      compositeZ += (dim.weight / totalWeight) * dimensionZScores[dim.key][idx];
    }
    const score = Math.round(50 + compositeZ * 15);
    const squadNickname = SQUAD_MAP[p.account_id] || null;
    const heroName = HERO_DICTIONARY[p.hero_id] || `Unknown (${p.hero_id})`;
    const won = winningTeam != null && p.team != null ? (p.team === winningTeam) : null;

    return {
      account_id: p.account_id,
      is_squad_member: squadNickname !== null,
      nickname: squadNickname,
      mention: squadNickname ? discordMention(p.account_id) : null,
      label: squadNickname || heroName,
      hero: heroName,
      hero_id: p.hero_id,
      hero_emoji: heroEmojiTag(p.hero_id),
      team: p.team,
      team_label: teamLabel(p.team), // NEW: "The Amber Hand" / "The Sapphire Flame"
      lane: p.lane,
      won,
      outcome_label: won === true ? "Won" : won === false ? "Lost" : "Unknown", // NEW
      kills: p.kills, deaths: p.deaths, assists: p.assists,
      kda_line: `${p.kills} / ${p.deaths} / ${p.assists}`,
      net_worth: p.net_worth,
      hero_damage: p.hero_damage,
      healing: p.healing,
      performance_score: score
    };
  });

  scored.sort((a, b) => b.performance_score - a.performance_score);
  scored.forEach((p, idx) => {
    p.rank = idx + 1;
    p.medal = idx === 0 ? "🥇" : idx === 1 ? "🥈" : idx === 2 ? "🥉" : "";
    p.display_line = `${p.medal ? p.medal + " " : `#${p.rank} `}${p.hero_emoji} ${p.label} — ${p.performance_score}`.trim();
  });

  const midbossKills = detectObjectiveWindows(players, "gold_boss", "Mid-Boss");
  const urnEvents = detectObjectiveWindows(players, "gold_treasure", "Soul Urn");
  const { lane_winners: laneWinners, lane_winners_text: laneWinnersText, note: laneNote } = computeLaneWinners(players);

  const notes = [
    "Mid-Boss and Soul Urn are detected from step-increases in each player's gold_boss/gold_treasure totals between periodic snapshots (roughly every 3-5 min) - so window_start_s/window_end_s bound when it happened, not an exact second.",
    "team 0/1 -> The Amber Hand / The Sapphire Flame is a best-guess mapping direction, not yet confirmed live - swap TEAM_NAMES in the worker if it comes out backwards."
  ];
  if (laneNote) notes.push(laneNote);

  // --- BotGhost-ready text blocks ---
  // These need zero further formatting in BotGhost - drop straight into a
  // message's content or an embed description field.
  const mvpEntry = scored[0] || null;
  const podiumText = scored.slice(0, 3).map(p => p.display_line).join("\n");
  const fullScoreboardText = scored.map(p => p.display_line).join("\n");
  const squadOnlyEntries = scored.filter(p => p.is_squad_member);
  const squadScoreboardText = squadOnlyEntries.length > 0
    ? squadOnlyEntries.map(p => `#${p.rank}/12 ${p.hero_emoji} ${p.label} — ${p.performance_score}`).join("\n")
    : "No squad members found in this match.";
  const midbossText = midbossKills.length > 0 ? midbossKills.map(e => e.summary).join("\n") : "No Mid-Boss kills detected this match.";
  const urnText = urnEvents.length > 0 ? urnEvents.map(e => e.summary).join("\n") : "No Soul Urn deliveries detected this match.";

  return {
    match_id: String(matchId),
    match_duration_s: matchDurationS,
    match_duration_formatted: fmtTime(matchDurationS),
    winning_team: winningTeam ?? null,
    winning_team_label: teamLabel(winningTeam), // NEW
    dimensions_used: activeDimensions.map(d => d.label),
    dimensions_skipped: SCOREBOARD_DIMENSIONS.filter(d => !activeDimensions.includes(d)).map(d => d.label),
    scoreboard: scored, // full 12-player array, ranked - use for building custom layouts
    mvp: mvpEntry, // NEW: rank #1 player, direct reference (e.g. {match_scoreboard.mvp.label})
    podium_text: podiumText, // NEW: top 3 only, ready to paste
    scoreboard_text: fullScoreboardText, // NEW: all 12, ready to paste
    squad_scoreboard_text: squadScoreboardText, // NEW: only your squad's members, with their rank out of 12
    midboss_kills: midbossKills, // each entry {team, team_label, window_start_s, window_end_s, window_formatted, objective, summary}
    midboss_text: midbossText, // NEW: ready to paste
    urn_events: urnEvents, // each entry {team, team_label, window_start_s, window_end_s, window_formatted, objective, summary}
    urn_text: urnText, // NEW: ready to paste
    lane_winners: laneWinners, // per-lane winner based on an interpolated 10-minute net worth snapshot, or null - see objectives_notes
    lane_winners_text: laneWinnersText, // NEW: ready to paste
    objectives_notes: notes,
    raw // full raw payload
  };
}


// ============================================================
// PRE-COMPILED LEADERBOARD BUILDERS
// ============================================================
async function buildRankLeaderboard(env, ctx) {
  // Fetch both the new seasonal /rank endpoint and the legacy /card endpoint
  // for every squad member. /card is still needed for photo/backwards-compat
  // fallback, and gets deduped/cached the same as everything else so this
  // doesn't double the live upstream traffic once warm.
  const cardTasks = squadIds.map(id => getEndpointData(`card:${id}`, `https://api.deadlock-api.com/v1/players/${id}/card`, CONFIG.TTL.CARD, env, ctx, false));
  const rankTasks = squadIds.map(id => getEndpointData(`rank:${id}`, `https://api.deadlock-api.com/v1/players/${id}/rank`, CONFIG.TTL.RANK, env, ctx, false));
  // NEW: season peaks are tiny, cron-maintained KV records - read-only here,
  // never written outside the cron job, so this is just a cheap parallel read.
  const kvBinding = env.DEADLOCK_KV || env.KV;
  const peakTasks = squadIds.map(async id => {
    if (!kvBinding) return null;
    try {
      const raw = await kvBinding.get(`season_peak:${id}`);
      return raw ? JSON.parse(raw) : null;
    } catch (e) { return null; }
  });
  const [cardResults, rankResults, peakResults] = await Promise.all([Promise.all(cardTasks), Promise.all(rankTasks), Promise.all(peakTasks)]);

  const leaderboard = cardResults.map((cardRes, index) => {
    const id = squadIds[index];
    const rankRes = rankResults[index];
    const normalized = normalizeRankData(rankRes.data, cardRes.data);

    return {
      account_id: id,
      nickname: SQUAD_MAP[id],
      photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
      ranked_rank: normalized.ranked_rank,
      ranked_subrank: normalized.ranked_subrank,
      rank_points: normalized.rank_points,
      rank_name: normalized.rank_name,
      rank_image: normalized.rank_image,
      rank_score: normalized.rank_score,
      season: normalized.season,
      calibration_status: normalized.calibration_status, // NEW
      win_streak: normalized.win_streak, // NEW
      points_to_next_subrank: normalized.points_to_next_subrank, // NEW
      season_peak: peakResults[index], // NEW: {rank, subrank, rank_points, rank_name, ts} or null if never recorded yet
      rank_source: normalized.source,
      data_fetched: cardRes.data !== null || rankRes.data !== null,
      error_code: rankRes.error || cardRes.error
    };
  });

  leaderboard.sort((a, b) => b.rank_score - a.rank_score);
  leaderboard.forEach((member, idx) => member.position = idx + 1);
  return leaderboard;
}

// ============================================================
// NEW: SEASON PEAK LEADERBOARD (highest rank each player reached this season)
// ============================================================
// Pure KV reads - season_peak:${id} is only ever WRITTEN by the cron job's
// updateSeasonPeak() call, never here. Different from buildRankLeaderboard:
// this shows each player's best-ever standing this season, not their
// current one, so someone who's since deranked can still show their peak.
async function buildSeasonPeakLeaderboard(env, ctx) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  const peaks = await Promise.all(squadIds.map(async id => {
    if (!kvBinding) return null;
    try {
      const raw = await kvBinding.get(`season_peak:${id}`);
      return raw ? JSON.parse(raw) : null;
    } catch (e) { return null; }
  }));

  const board = squadIds.map((id, index) => {
    const peak = peaks[index];
    return {
      account_id: id,
      nickname: SQUAD_MAP[id],
      photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
      has_peak: peak !== null,
      rank: peak?.rank ?? null,
      subrank: peak?.subrank ?? null,
      rank_points: peak?.rank_points ?? null,
      rank_name: peak?.rank_name ?? "No data yet",
      rank_image: peak ? (RANK_BADGE_IMAGES[peak.rank] || null) : null,
      recorded_at: peak?.ts ?? null,
      sort_score: peak?.rank_points ?? -1
    };
  });

  board.sort((a, b) => b.sort_score - a.sort_score);
  board.forEach((member, idx) => member.position = idx + 1);
  return board;
}

// ============================================================
// NEW: WEEKLY MOVEMENT LEADERBOARD (biggest climber, trailing 7 days)
// ============================================================
// Also pure KV reads, off rank_history:${id} (written by the cron job's
// appendRankHistorySnapshot(), see the RANK HISTORY section above). Finds
// the oldest snapshot within the last 7 days as the baseline and compares
// it to the current rank to compute a delta - this is what "who climbed
// the most this week" for a weekly recap post is built from.
const WEEKLY_MOVEMENT_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;

async function buildWeeklyMovementLeaderboard(env, ctx) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  const histories = await Promise.all(squadIds.map(async id => {
    if (!kvBinding) return [];
    try {
      const raw = await kvBinding.get(`rank_history:${id}`);
      const history = raw ? JSON.parse(raw) : [];
      return Array.isArray(history) ? history : [];
    } catch (e) { return []; }
  }));

  const cutoff = Date.now() - WEEKLY_MOVEMENT_WINDOW_MS;

  const board = squadIds.map((id, index) => {
    const history = histories[index];
    if (history.length === 0) {
      return {
        account_id: id, nickname: SQUAD_MAP[id], photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
        has_data: false, current_rank_name: null, points_delta: null, direction: null
      };
    }

    // Baseline = oldest snapshot still within the trailing 7-day window. If
    // every snapshot we have is younger than 7 days (e.g. this feature just
    // launched), fall back to the very first snapshot we have and note that
    // it's a shorter window than a full week.
    const inWindow = history.filter(h => h.ts >= cutoff);
    const baseline = inWindow.length > 0 ? inWindow[0] : history[0];
    const current = history[history.length - 1];

    const baselineScore = (baseline.rank_points ?? (baseline.rank * 100000 + baseline.subrank * 10000));
    const currentScore = (current.rank_points ?? (current.rank * 100000 + current.subrank * 10000));
    const delta = currentScore - baselineScore;

    return {
      account_id: id,
      nickname: SQUAD_MAP[id],
      photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
      has_data: true,
      current_rank_name: current.rank_name,
      baseline_rank_name: baseline.rank_name,
      points_delta: delta,
      direction: delta > 0 ? "up" : delta < 0 ? "down" : "same",
      window_is_full_week: inWindow.length > 0 && baseline.ts <= cutoff + (24 * 60 * 60 * 1000), // rough sanity check
      baseline_ts: baseline.ts,
      current_ts: current.ts
    };
  });

  board.sort((a, b) => (b.points_delta ?? -Infinity) - (a.points_delta ?? -Infinity));
  board.forEach((member, idx) => member.position = idx + 1);
  return board;
}

// ============================================================
// NEW: WEEKLY STATS LEADERBOARD (kills/commends/etc. gained, trailing 7 days)
// ============================================================
// Same trailing-window pattern as buildWeeklyMovementLeaderboard, but off
// stats_history:${id} (daily snapshots, see appendStatsHistorySnapshot
// above) instead of rank_history. Sorted by kills gained this week by
// default since that's the most common "who's been grinding" stat, but
// every field's delta is included per player so a template can reference
// commends_delta, wins_delta, etc. for other squad members directly even
// though the array itself is kills-sorted.
async function buildWeeklyStatsLeaderboard(env, ctx) {
  const kvBinding = env.DEADLOCK_KV || env.KV;
  const histories = await Promise.all(squadIds.map(async id => {
    if (!kvBinding) return [];
    try {
      const raw = await kvBinding.get(`stats_history:${id}`);
      const history = raw ? JSON.parse(raw) : [];
      return Array.isArray(history) ? history : [];
    } catch (e) { return []; }
  }));

  const cutoff = Date.now() - WEEKLY_MOVEMENT_WINDOW_MS; // reuses the same 7-day window constant

  const board = squadIds.map((id, index) => {
    const history = histories[index];
    if (history.length === 0) {
      return {
        account_id: id, nickname: SQUAD_MAP[id], photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
        has_data: false, kills_delta: null, assists_delta: null, hero_damage_delta: null,
        commends_delta: null, wins_delta: null, matches_delta: null
      };
    }

    const inWindow = history.filter(h => h.ts >= cutoff);
    const baseline = inWindow.length > 0 ? inWindow[0] : history[0];
    const current = history[history.length - 1];

    // If baseline and current are the SAME entry (only one snapshot exists
    // total, e.g. day one of this feature), every delta is correctly 0 -
    // not a bug, just "no data old enough yet" reported honestly via
    // window_is_full_week below rather than silently.
    const diff = (field) => (current[field] ?? 0) - (baseline[field] ?? 0);

    return {
      account_id: id,
      nickname: SQUAD_MAP[id],
      photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
      has_data: true,
      kills_delta: diff("kills"),
      assists_delta: diff("assists"),
      hero_damage_delta: diff("hero_damage"),
      obj_damage_delta: diff("obj_damage"),
      healing_delta: diff("healing"),
      souls_delta: diff("souls_collected"),
      commends_delta: diff("commends"),
      wins_delta: diff("games_won"),
      matches_delta: diff("matches_played"),
      window_is_full_week: inWindow.length > 0 && baseline.ts <= cutoff + (24 * 60 * 60 * 1000),
      baseline_date: baseline.date,
      current_date: current.date
    };
  });

  board.sort((a, b) => (b.kills_delta ?? -Infinity) - (a.kills_delta ?? -Infinity));
  board.forEach((member, idx) => member.position = idx + 1);
  return board;
}

// Shared builder for any leaderboard sourced from `/account-stats` - the raw
// fetch/shape is identical, only the sort key differs, so 'stats' and
// 'commends' (and any future account-stats leaderboard) share one function
// instead of duplicating the fetch+map+sort+rank logic.
async function buildAccountStatsLeaderboard(env, ctx, sortFn) {
  const fetchTasks = squadIds.map(id => getEndpointData(`stats:${id}`, `https://api.deadlock-api.com/v1/players/${id}/account-stats`, CONFIG.TTL.STATS, env, ctx, false));
  const results = await Promise.all(fetchTasks);

  const board = results.map((res, index) => {
    const id = squadIds[index];
    return {
      account_id: id,
      nickname: SQUAD_MAP[id],
      mention: discordMention(id),
      photo_url: SQUAD_PHOTOS[id] || DEFAULT_PHOTO_URL,
      data_fetched: res.data !== null,
      error_code: res.error,
      metrics: parseAccountStatsData(res.data)
    };
  });

  board.sort(sortFn);
  board.forEach((member, idx) => member.position = idx + 1);
  return board;
}

// RESTORED: Pulls from `/account-stats` using parseAccountStatsData
function buildStatsLeaderboard(env, ctx) {
  return buildAccountStatsLeaderboard(env, ctx, (a, b) => b.metrics.records.most_kills - a.metrics.records.most_kills);
}

// Commends leaderboard - same account-stats data as buildStatsLeaderboard
// (so it hits the same cache, no extra API calls), just sorted by commends.
function buildCommendsLeaderboard(env, ctx) {
  return buildAccountStatsLeaderboard(env, ctx, (a, b) => b.metrics.commends - a.metrics.commends);
}

// Master function to compile and store leaderboards
async function rebuildAndStoreLeaderboard(type, env, ctx) {
  const key = `leaderboard_compiled:${type}:${CONFIG.CACHE_VERSION}`;
  log("LB REBUILD", `Pre-compiling ${key}...`);
  
  const data = type === 'rank' ? await buildRankLeaderboard(env, ctx)
    : type === 'commends' ? await buildCommendsLeaderboard(env, ctx)
    : type === 'peak' ? await buildSeasonPeakLeaderboard(env, ctx) // NEW
    : type === 'movement' ? await buildWeeklyMovementLeaderboard(env, ctx) // NEW
    : type === 'weekly_stats' ? await buildWeeklyStatsLeaderboard(env, ctx) // NEW
    : type === 'weekly_recap' ? await buildWeeklyRecapLeaderboard(env, ctx) // NEW: exact Friday-checkpoint diff
    : type === 'squad_carry' ? await buildSquadCarryLeaderboard(env, ctx) // NEW: who carried shared squad games this week
    : type === 'kill_record' ? await buildKillRecordLeaderboard(env, ctx) // NEW: true all-time highest kills in any single match
    : type === 'assist_record' ? await buildAssistRecordLeaderboard(env, ctx) // NEW: true all-time highest assists in any single match
    : type === 'badges' ? await buildBadgesLeaderboard(env, ctx) // NEW: achievement/badge counts
    : type === 'badge_directory' ? await buildBadgeDirectory(env, ctx) // NEW: organized by badge, who's earned each one
    : type === 'lineup' ? await buildLineupLeaderboard(env, ctx) // NEW: best 3/4/5/6-stack combos
    : type === 'synergy' ? await buildHeroSynergyLeaderboard(env, ctx) // NEW: best hero-pairing combos
    : type === 'weekly_moments' ? await buildWeeklyMomentsLeaderboard(env, ctx) // NEW: Bloodbath/Marathon/Speedrun/One Trick/Explorer/Power Duo
    : await buildStatsLeaderboard(env, ctx);
  const payload = { data, expiresAt: Date.now() + getJitteredTTL(CONFIG.TTL.LEADERBOARD) };
  
  memoryCache.set(key, payload);
  const kvBinding = env.DEADLOCK_KV || env.KV;
  if (kvBinding) {
    // Awaited - see FIX note in refreshData() for why this matters.
    try {
      await kvBinding.put(key, JSON.stringify(payload), { expirationTtl: 86400 });
    } catch (e) { log("KV LB ERR", e.message); }
  }
  return data;
}

// User-facing leaderboard fetcher (Acts like a static file read)
async function getCachedLeaderboard(type, env, ctx) {
  const key = `leaderboard_compiled:${type}:${CONFIG.CACHE_VERSION}`;
  
  let cachedPayload = memoryCache.get(key);
  const kvBinding = env.DEADLOCK_KV || env.KV;
  
  if (!cachedPayload && kvBinding) {
    try {
      const kvStr = await kvBinding.get(key);
      if (kvStr) {
        cachedPayload = JSON.parse(kvStr);
        memoryCache.set(key, cachedPayload);
      }
    } catch (e) {}
  }

  // Cache First Architecture: If we have ANY compiled data, return it instantly.
  if (cachedPayload) {
    const now = Date.now();
    const isExpired = now > cachedPayload.expiresAt;
    // If it's expired, return stale data to user immediately, but trigger background rebuild
    if (isExpired) {
       const rebuildTask = rebuildAndStoreLeaderboard(type, env, ctx).catch(e=>{});
       if (ctx && ctx.waitUntil) ctx.waitUntil(rebuildTask);
    }
    return cachedPayload.data;
  }

  // Absolute cold start fallback (Should only happen on initial deploy)
  return await rebuildAndStoreLeaderboard(type, env, ctx);
}

// ============================================================
// NEW: HEALTH ENDPOINT HANDLER
// ============================================================
// Read-only diagnostics: last cron run + per-key cache freshness, without
// triggering any live upstream fetches (see peekCacheStatus). Meant for
// checking from Discord/a browser instead of digging through Workers logs.
async function handleHealthRequest(env, RESPONSE_HEADERS) {
  const kvBinding = env.DEADLOCK_KV || env.KV;

  let cronStatus = null;
  if (kvBinding) {
    try {
      const raw = await kvBinding.get("_meta:cron_status");
      if (raw) cronStatus = JSON.parse(raw);
    } catch (e) { /* leave null */ }
  }

  const endpointDefs = [
    ["card", CONFIG.TTL.CARD], ["rank", CONFIG.TTL.RANK], ["stats", CONFIG.TTL.STATS],
    ["mates", CONFIG.TTL.MATES], ["history", CONFIG.TTL.HISTORY]
  ];

  const cachePeeks = await Promise.all(
    squadIds.flatMap(id => endpointDefs.map(async ([label, ttl]) => {
      const status = await peekCacheStatus(`${label}:${id}`, ttl, env);
      return { id, label, ...status };
    }))
  );

  const summary = { fresh: 0, stale: 0, missing: 0 };
  const problems = [];
  for (const p of cachePeeks) {
    summary[p.status] = (summary[p.status] || 0) + 1;
    if (p.status === "missing" || p.has_error) {
      problems.push({ key: `${p.label}:${p.id}`, nickname: SQUAD_MAP[p.id], status: p.status, has_error: !!p.has_error });
    }
  }

  const leaderboardPeeks = await Promise.all(
    ["rank", "stats", "commends", "peak", "movement", "weekly_stats", "weekly_recap", "squad_carry", "weekly_moments", "kill_record", "assist_record", "badges", "badge_directory", "lineup", "synergy"].map(async type => {
      const status = await peekCacheStatus(`leaderboard_compiled:${type}:${CONFIG.CACHE_VERSION}`, CONFIG.TTL.LEADERBOARD, env);
      return { type, ...status };
    })
  );

  const now = Date.now();
  const body = {
    ok: cronStatus ? (cronStatus.error === null && cronStatus.tasks_failed === 0) : null,
    cron: cronStatus ? {
      last_run_ts: cronStatus.last_run_ts,
      last_run_ago_seconds: Math.round((now - cronStatus.last_run_ts) / 1000),
      duration_ms: cronStatus.duration_ms,
      tasks_ok: cronStatus.tasks_ok,
      tasks_failed: cronStatus.tasks_failed,
      error: cronStatus.error,
      players: cronStatus.players
    } : { note: "No cron run recorded yet - either the cron trigger hasn't fired since this feature was deployed, or this Worker has never run its scheduled handler." },
    cache_summary: summary,
    cache_problems: problems,
    leaderboards: leaderboardPeeks,
    cache_version: CONFIG.CACHE_VERSION,
    checked_at: now
  };

  return new Response(JSON.stringify(body), { headers: RESPONSE_HEADERS });
}

// ============================================================
// WORKER EXECUTION MODULE
// ============================================================
export default {
  // --- PROACTIVE BACKGROUND CRON ---
  // The absolute core of the Cache-First architecture. Runs every X minutes.
  async scheduled(event, env, ctx) {
    const cronStartedAt = Date.now();
    log("CRON", "Initiating master background cache refresh...");

    let tasksOk = 0, tasksFailed = 0, cronError = null;

    try {
      // Step 1: Force fetch live data for every endpoint for every player.
      // card + rank are tracked per-player (not just fire-and-forget) because
      // rank history / season peak snapshots below need the normalized result
      // for each specific id, not just "did the whole batch settle".
      const perPlayerTasks = squadIds.map(async id => {
        const [cardRes, rankRes, statsRes] = await Promise.all([
          getEndpointData(`card:${id}`, `https://api.deadlock-api.com/v1/players/${id}/card`, CONFIG.TTL.CARD, env, ctx, true),
          getEndpointData(`rank:${id}`, `https://api.deadlock-api.com/v1/players/${id}/rank`, CONFIG.TTL.RANK, env, ctx, true), // NEW: seasonal rank
          getEndpointData(`stats:${id}`, `https://api.deadlock-api.com/v1/players/${id}/account-stats`, CONFIG.TTL.STATS, env, ctx, true) // moved here (was in otherTasks) so its result can drive the daily stats snapshot below
        ]);

        // NEW: only snapshot/update peak if we actually got usable rank data
        // this run (either endpoint) - skips writing junk snapshots when both
        // upstream calls failed and we're just looking at negative-cached errors.
        if (cardRes.data || rankRes.data) {
          const normalized = normalizeRankData(rankRes.data, cardRes.data);
          await appendRankHistorySnapshot(id, normalized, env);
          await updateSeasonPeak(id, normalized, env);
        }

        // NEW: daily lifetime-stats snapshot, powers buildWeeklyStatsLeaderboard.
        // Only snapshots on success - skips on a failed fetch so a bad day
        // doesn't record a false "0 progress" data point.
        let parsedStats = null;
        if (statsRes.data) {
          parsedStats = parseAccountStatsData(statsRes.data);
          await appendStatsHistorySnapshot(id, parsedStats, env);
          await appendPersonalRecordSnapshot(id, parsedStats.records, env); // NEW: powers rank-up-style alerts when a personal best is broken
        }

        // NEW: exact Friday 1pm checkpoint diff - only runs during that
        // specific hour window (see isWeeklyCheckpointWindow), and only
        // when we have BOTH a usable rank normalization and parsed stats
        // this run, so a partial upstream failure doesn't corrupt the
        // week's checkpoint with incomplete data.
        if (isWeeklyCheckpointWindow() && (cardRes.data || rankRes.data) && parsedStats) {
          const normalizedForCheckpoint = normalizeRankData(rankRes.data, cardRes.data);
          await processWeeklyCheckpoint(id, normalizedForCheckpoint, parsedStats, env);
        }

        // NEW: badge scanning - moved history fetch here (was in otherTasks
        // as fire-and-forget) so the result can be scanned for new
        // achievement unlocks right after fetching, same tracked pattern as
        // card/rank/stats above.
        const historyRes = await getEndpointData(`history:${id}`, `https://api.deadlock-api.com/v1/players/${id}/match-history?limit=${CONFIG.MATCH_HISTORY_LIMIT}`, CONFIG.TTL.HISTORY, env, ctx, true);
        let newBadges = [];
        if (historyRes.data) {
          const matches = Array.isArray(historyRes.data) ? historyRes.data : (historyRes.data?.matches || historyRes.data?.history || []);
          const badgeResult = await scanAndStoreBadges(id, matches, env);
          newBadges = badgeResult?.newlyEarned ?? [];
        }

        return { cardRes, rankRes, statsRes, historyRes, newBadges };
      });

      const otherTasks = [];
      for (const id of squadIds) {
        otherTasks.push(getEndpointData(`mates:${id}`, `https://api.deadlock-api.com/v1/players/${id}/mate-stats`, CONFIG.TTL.MATES, env, ctx, true));
      }

      // Wait for all player data to hit the cache (KV writes are now awaited
      // inside refreshData, so by the time this resolves, KV is actually consistent).
      const settled = await Promise.allSettled([...perPlayerTasks, ...otherTasks]);
      for (const s of settled) {
        if (s.status === "fulfilled") tasksOk++; else tasksFailed++;
      }

      // Step 2: Now that raw data is perfectly fresh in memory (including the
      // rank_history/season_peak snapshots written during Step 1), compile
      // the leaderboards.
      const lbTasks = [
        rebuildAndStoreLeaderboard('rank', env, ctx),
        rebuildAndStoreLeaderboard('stats', env, ctx),
        rebuildAndStoreLeaderboard('commends', env, ctx),
        rebuildAndStoreLeaderboard('peak', env, ctx),     // NEW
        rebuildAndStoreLeaderboard('movement', env, ctx),  // NEW
        rebuildAndStoreLeaderboard('weekly_stats', env, ctx),  // NEW
        rebuildAndStoreLeaderboard('weekly_recap', env, ctx),  // NEW: exact Friday-checkpoint diff
        rebuildAndStoreLeaderboard('squad_carry', env, ctx),  // NEW: who carried shared squad games this week
        rebuildAndStoreLeaderboard('kill_record', env, ctx),  // NEW: true all-time highest kills in any single match
        rebuildAndStoreLeaderboard('assist_record', env, ctx),  // NEW: true all-time highest assists in any single match
        rebuildAndStoreLeaderboard('badges', env, ctx),  // NEW: achievement/badge counts
        rebuildAndStoreLeaderboard('badge_directory', env, ctx),  // NEW: organized by badge, who's earned each one
        rebuildAndStoreLeaderboard('lineup', env, ctx),  // NEW: best 3/4/5/6-stack combos
        rebuildAndStoreLeaderboard('synergy', env, ctx),  // NEW: best hero-pairing combos
        rebuildAndStoreLeaderboard('weekly_moments', env, ctx)  // NEW: Bloodbath/Marathon/Speedrun/One Trick/Explorer/Power Duo
      ];

      const lbSettled = await Promise.allSettled(lbTasks);
      for (const s of lbSettled) {
        if (s.status === "fulfilled") tasksOk++; else tasksFailed++;
      }

      log("CRON", "Master refresh sequence complete.");
    } catch (e) {
      cronError = e.message;
      log("CRON ERROR", e.message);
    }

    // NEW: record cron health to KV so the /health endpoint can report on it
    // without needing live Workers log access.
    const kvBinding = env.DEADLOCK_KV || env.KV;
    if (kvBinding) {
      const status = {
        last_run_ts: cronStartedAt,
        duration_ms: Date.now() - cronStartedAt,
        tasks_ok: tasksOk,
        tasks_failed: tasksFailed,
        error: cronError,
        players: squadIds.length
      };
      try {
        await kvBinding.put("_meta:cron_status", JSON.stringify(status));
      } catch (e) { log("CRON META ERR", e.message); }
    }
  },

  // --- DISCORD / BOTGHOST REQUEST HANDLER ---
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") return new Response(null, { headers: CORS_HEADERS });

    const url = new URL(request.url);
    const accountId = url.searchParams.get("account_id") || url.searchParams.get("accountId");
    const reqType = url.searchParams.get("type");
    // NEW: "rank" added to the default include list so player lookups get
    // the new seasonal rank data automatically without BotGhost needing to
    // pass a new query param.
    // FIX: url.searchParams.get() only returns the FIRST match for a repeated
    // query param. ?include=rank_history&include=season_peak was silently
    // collapsing to just "rank_history" - the second include was dropped
    // without any error, which is exactly the kind of bug that looks like
    // "season_peak isn't working" when it's actually "season_peak was never
    // read". getAll() + join() combines every repeated ?include=/?lb= into
    // one comma-separated string, so both repeated params (?include=a&include=b)
    // and comma-joined params (?include=a,b) work identically.
    const includeParam = url.searchParams.getAll("include").join(",") || "card,history,mates,stats,rank";
    const lbParam = url.searchParams.getAll("lb").join(",") || "rank,stats";

    const fetchCard = includeParam.includes("card");
    const fetchHistory = includeParam.includes("history");
    const fetchMates = includeParam.includes("mates");
    const fetchStats = includeParam.includes("stats");
    const fetchRank = includeParam.includes("rank"); // NEW
    // NEW: opt-in only (not in the default include list) - each of these
    // does extra KV reads or extra compute, so they're only pulled when a
    // BotGhost command explicitly asks for them via ?include=...
    const fetchRankHistory = includeParam.includes("rank_history");
    const fetchSeasonPeak = includeParam.includes("season_peak");
    const fetchEligibility = includeParam.includes("eligibility");
    const fetchPersonalRecords = includeParam.includes("personal_records"); // NEW: opt-in
    const fetchBadges = includeParam.includes("badges"); // NEW: opt-in - per-player badge list, doesn't depend on leaderboard position
    const fetchWeakness = includeParam.includes("weakness"); // NEW: opt-in
    const fetchItems = includeParam.includes("items"); // NEW: experimental, see match_history.items comment
    const fetchLbRank = lbParam.includes("rank");
    const fetchLbStats = lbParam.includes("stats");
    const fetchLbCommends = lbParam.includes("commends");
    const fetchLbPeak = lbParam.includes("peak"); // NEW: opt-in, not in the "rank,stats" default
    const fetchLbMovement = lbParam.includes("movement"); // NEW: opt-in
    const fetchLbWeeklyStats = lbParam.includes("weekly_stats"); // NEW: opt-in
    const fetchLbWeeklyRecap = lbParam.includes("weekly_recap"); // NEW: opt-in
    const fetchLbSquadCarry = lbParam.includes("squad_carry"); // NEW: opt-in
    const fetchLbKillRecord = lbParam.includes("kill_record"); // NEW: opt-in
    const fetchLbAssistRecord = lbParam.includes("assist_record"); // NEW: opt-in
    const fetchLbBadges = lbParam.includes("badges"); // NEW: opt-in
    const fetchLbBadgeDirectory = lbParam.includes("badge_directory"); // NEW: opt-in
    const fetchLbLineup = lbParam.includes("lineup"); // NEW: opt-in
    const fetchLbSynergy = lbParam.includes("synergy"); // NEW: opt-in
    const fetchLbWeeklyMoments = lbParam.includes("weekly_moments"); // NEW: opt-in

    // FIX: Changed default fallback from "full" to "player"
    let mode = reqType ? reqType : (!accountId ? "leaderboard" : "player");

    // Allow Cloudflare Edge network to cache requests natively for 30s 
    // to absorb multiple users hitting identical BotGhost buttons simultaneously.
    const RESPONSE_HEADERS = { 
      ...CORS_HEADERS, 
      "Content-Type": "application/json",
      "Cache-Control": "public, max-age=30, s-maxage=30, stale-while-revalidate=60"
    };

    // NEW: ?type=health doesn't need an account_id and bypasses the normal
    // leaderboard/player pipeline entirely - handle it first and return early.
    if (mode === "health") {
      return await handleHealthRequest(env, RESPONSE_HEADERS);
    }

    // NEW: ?type=synergy_lookup&player_id=X&hero_name=Y - interactive lookup,
    // separate from the normal leaderboard/player pipeline since it takes
    // its own two params instead of account_id.
    //
    // NEW: ?type=match_scoreboard&match_id=X - fair 1-12 performance
    // ranking for one specific match, peer-relative z-scores across
    // combat/economy/survival/lane/damage/support dimensions (see
    // buildMatchScoreboard's comment block for the full rationale). Like
    // synergy_lookup, this is live-computed per request, not a
    // cron-precompiled leaderboard - a match_id is a one-off lookup.
    if (mode === "match_scoreboard") {
      const matchId = url.searchParams.get("match_id");
      if (!matchId || !/^\d+$/.test(matchId)) {
        return new Response(JSON.stringify({ error: "Missing or invalid match_id. Must be a numeric Deadlock match ID." }), { status: 400, headers: RESPONSE_HEADERS });
      }
      const result = await buildMatchScoreboard(matchId, env, ctx);
      if (result.error) {
        return new Response(JSON.stringify(result), { status: 502, headers: RESPONSE_HEADERS });
      }
      return new Response(JSON.stringify(result), { headers: RESPONSE_HEADERS });
    }

    // FIX: hero is now looked up by NAME ONLY, case/space/hyphen-insensitive.
    // `hero_name` is the primary param. `hero_id` is still accepted for any
    // existing BotGhost setups that pass it, but it's ALSO resolved as a
    // name (not a numeric ID) - e.g. `hero_id=Warden` or `hero_id=warden`
    // both work, matching how someone naturally fills in a text/select
    // field. A bare number is no longer required or expected from either
    // param.
    if (mode === "synergy_lookup") {
      const playerId = url.searchParams.get("player_id");
      const heroNameRaw = url.searchParams.get("hero_name") ?? url.searchParams.get("hero_id");
      const heroId = heroNameRaw !== null ? (HERO_NAME_TO_ID[normalizeHeroName(heroNameRaw.trim())] ?? null) : null;

      if (!playerId || !SQUAD_MAP[playerId]) {
        return new Response(JSON.stringify({ error: "Missing or unrecognized player_id. Must be one of the squad's account IDs." }), { status: 400, headers: RESPONSE_HEADERS });
      }
      if (heroId === null) {
        return new Response(JSON.stringify({ error: "Missing or unrecognized hero name. Pass it as hero_name (or hero_id) - any case, with or without spaces/hyphens, e.g. 'Warden', 'warden', 'grey talon', 'grey-talon', 'greytalon' all work." }), { status: 400, headers: RESPONSE_HEADERS });
      }

      const topSynergies = await findTopSynergiesForPlayerHero(env, ctx, playerId, heroId);

      // NEW: pad to a fixed 10 slots so a BotGhost message referencing
      // top_synergies.0 through .9 by index never hits a missing array
      // element (which would render as the raw, unresolved
      // "{synergyapi.response.top_synergies.N.display_line}" placeholder
      // text in the actual Discord message). Missing/empty slots get a
      // friendly explanatory line instead, and the very first slot carries
      // it when there's no data at all so the message never posts blank.
      // NEW: the numbered emojis (1️⃣-🔟) now live server-side, one per
      // ranked row, instead of being hardcoded in the BotGhost message.
      // That was the reason empty/no-data rows still showed a numeral with
      // nothing after it - the template had no way to know a given index
      // was empty. Now rank_emoji is only ever set on rows that actually
      // have a synergy result; every padding row (including the
      // no-data-at-all message row) gets an empty rank_emoji, so nothing
      // renders next to it.
      const NUMBER_EMOJIS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"];

      const NO_DATA_LINE = `No matches yet with 5+ shared games alongside another squad member on ${HERO_DICTIONARY[heroId]} - queue up together a bit more and this will fill in.`;
      const hasSynergies = topSynergies.length > 0;
      const paddedSynergies = Array.from({ length: 10 }, (_, i) => {
        if (topSynergies[i]) {
          const entry = topSynergies[i];
          return {
            ...entry,
            rank_emoji: NUMBER_EMOJIS[i],
            // NEW: number + display_line combined, ready to drop straight
            // into a message with zero BotGhost-side formatting.
            numbered_line: `${NUMBER_EMOJIS[i]} ${entry.display_line}`
          };
        }
        // First empty slot when there's zero real data gets the friendly
        // message (no leading number, since it isn't a ranked result);
        // every other empty slot (or overflow past real data when there
        // IS at least one entry) is just a fully blank line - no number,
        // no text.
        const isFirstAndEmpty = i === 0 && !hasSynergies;
        return {
          partner_nickname: null, partner_mention: null, partner_hero: null,
          partner_hero_id: null, partner_hero_emoji: null,
          your_hero: null, your_hero_id: null, your_hero_emoji: null,
          games: null, wins: null, win_rate: null,
          rank_emoji: "",
          display_line: isFirstAndEmpty ? NO_DATA_LINE : "",
          numbered_line: isFirstAndEmpty ? NO_DATA_LINE : ""
        };
      });

      return new Response(JSON.stringify({
        player_id: playerId,
        nickname: SQUAD_MAP[playerId],
        hero_id: heroId,
        hero_name: HERO_DICTIONARY[heroId],
        hero_emoji: heroEmojiTag(heroId), // NEW: so the header line doesn't need to borrow top_synergies.0's emoji
        has_synergies: hasSynergies, // NEW: true if there's at least one 5+ game pairing
        no_synergies_message: hasSynergies ? null : NO_DATA_LINE, // NEW: same message, for anyone who'd rather gate the whole block with an IF instead of relying on the padded row
        top_synergies: paddedSynergies, // always exactly 10 entries now - safe to reference .0 through .9 unconditionally
        top_synergies_raw: topSynergies // NEW: the real, unpadded list if you want its true length anywhere
      }), { headers: RESPONSE_HEADERS });
    }

    if ((mode === "player" || mode === "full") && !accountId) {
      return new Response(JSON.stringify({ error: "Missing account_id" }), { status: 400, headers: RESPONSE_HEADERS });
    }

    // FIX: accountId was previously passed straight into the upstream API URL
    // with no validation at all (`.../players/${accountId}/card`). Any junk
    // typed into a BotGhost variable would get forwarded verbatim. Steam64/
    // Deadlock account ids are numeric, so reject anything else up front
    // instead of spending a request round-trip on it.
    if (accountId && !/^\d+$/.test(accountId)) {
      return new Response(JSON.stringify({ error: "Invalid account_id" }), { status: 400, headers: RESPONSE_HEADERS });
    }

    // FIX: an unrecognized `type` used to silently fall through and return an
    // almost-empty 200 response. Now it's a clear 400 instead of a payload
    // that looks successful but has nothing useful in it.
    if (!["leaderboard", "player", "full"].includes(mode)) {
      return new Response(JSON.stringify({ error: `Invalid type "${mode}". Expected leaderboard, player, full, health, synergy_lookup, or match_scoreboard.` }), { status: 400, headers: RESPONSE_HEADERS });
    }

    try {
      const payload = {};

      if (accountId) {
        payload.account_id = String(accountId);
        payload.squad = {
          nickname: SQUAD_MAP[String(accountId)] || "Guest Fighter",
          is_member: SQUAD_MAP[String(accountId)] !== undefined,
          photo_url: SQUAD_PHOTOS[String(accountId)] || DEFAULT_PHOTO_URL 
        };
      }

      // Read pre-compiled static leaderboards
      if (mode === "leaderboard" || mode === "full") {
        if (fetchLbRank) payload.leaderboard = await getCachedLeaderboard('rank', env, ctx);
        if (fetchLbStats) payload.stats_leaderboard = await getCachedLeaderboard('stats', env, ctx);
        if (fetchLbCommends) payload.commends_leaderboard = await getCachedLeaderboard('commends', env, ctx);
        if (fetchLbPeak) payload.peak_leaderboard = await getCachedLeaderboard('peak', env, ctx); // NEW
        if (fetchLbMovement) payload.movement_leaderboard = await getCachedLeaderboard('movement', env, ctx); // NEW
        if (fetchLbWeeklyStats) payload.weekly_stats_leaderboard = await getCachedLeaderboard('weekly_stats', env, ctx); // NEW
        if (fetchLbWeeklyRecap) {
          payload.weekly_recap_leaderboard = await getCachedLeaderboard('weekly_recap', env, ctx); // exact Friday-checkpoint diff
          payload.weekly_awards = computeWeeklyAwards(payload.weekly_recap_leaderboard); // NEW: fun per-category winners, each sorted independently
        }
        if (fetchLbSquadCarry) payload.squad_carry_leaderboard = await getCachedLeaderboard('squad_carry', env, ctx); // NEW: who carried shared squad games this week
        if (fetchLbKillRecord) payload.kill_record_leaderboard = await getCachedLeaderboard('kill_record', env, ctx); // NEW: true all-time highest kills in any single match
        if (fetchLbAssistRecord) payload.assist_record_leaderboard = await getCachedLeaderboard('assist_record', env, ctx); // NEW: true all-time highest assists in any single match
        if (fetchLbBadges) payload.badges_leaderboard = await getCachedLeaderboard('badges', env, ctx); // NEW: achievement/badge counts
        if (fetchLbBadgeDirectory) payload.badge_directory = await getCachedLeaderboard('badge_directory', env, ctx); // NEW: organized by badge, who's earned each one
        if (fetchLbLineup) payload.lineup_leaderboard = await getCachedLeaderboard('lineup', env, ctx); // NEW: best 3/4/5/6-stack combos
        if (fetchLbSynergy) payload.synergy_leaderboard = await getCachedLeaderboard('synergy', env, ctx); // NEW: best hero-pairing combos
        if (fetchLbWeeklyMoments) payload.weekly_moments = await getCachedLeaderboard('weekly_moments', env, ctx); // NEW: Bloodbath/Marathon/Speedrun/One Trick/Explorer/Power Duo
      }

      // Read static player metadata from Memory/KV
      if (mode === "player" || mode === "full") {
        let historyRes = { data: null, error: "Skipped" };
        let cardRes = { data: null, error: "Skipped" };
        let mateStatsRes = { data: null, error: "Skipped" };
        let accountStatsRes = { data: null, error: "Skipped" };
        let rankRes = { data: null, error: "Skipped" }; // NEW
        let rankHistoryRaw = null; // NEW
        let seasonPeakRaw = null; // NEW
        let personalRecordsRaw = null; // NEW
        let weeklyDiffRaw = null; // NEW
        let badgesRaw = null; // NEW

        const tasks = [];
        
        // RESTORED: fetchStats separated back to account-stats to regain damage/healing
        // NEW: also fetched when eligibility is requested, since it's computed from this same data.
        // REVERTED: tried forceRefresh=true here to fix "takes 2-4 clicks to
        // see the latest match" - confirmed that did NOT fix it, only added
        // latency with no benefit. That's actually useful information: it
        // rules out our caching layer as the cause. The real explanation is
        // most likely that deadlock-api.com's own /match-history endpoint
        // has some processing delay after a match finishes, before OUR
        // request ever reaches them - no amount of aggressive fetching on
        // our end can work around a delay that exists upstream of us. Back
        // to the faster stale-while-revalidate behavior since forcing live
        // fetches wasn't solving the actual problem.
        if (fetchHistory || fetchEligibility) tasks.push(getEndpointData(`history:${accountId}`, `https://api.deadlock-api.com/v1/players/${accountId}/match-history?limit=${CONFIG.MATCH_HISTORY_LIMIT}`, CONFIG.TTL.HISTORY, env, ctx, false).then(r => historyRes = r));
        if (fetchCard) tasks.push(getEndpointData(`card:${accountId}`, `https://api.deadlock-api.com/v1/players/${accountId}/card`, CONFIG.TTL.CARD, env, ctx, false).then(r => cardRes = r));
        if (fetchMates) tasks.push(getEndpointData(`mates:${accountId}`, `https://api.deadlock-api.com/v1/players/${accountId}/mate-stats`, CONFIG.TTL.MATES, env, ctx, false).then(r => mateStatsRes = r));
        if (fetchStats || fetchWeakness) tasks.push(getEndpointData(`stats:${accountId}`, `https://api.deadlock-api.com/v1/players/${accountId}/account-stats`, CONFIG.TTL.STATS, env, ctx, false).then(r => accountStatsRes = r));
        // NEW: seasonal rank. Fetched whenever card OR rank is requested, since
        // player_card's rank fields below are sourced from it (falling back to
        // /card if it's ever empty).
        if (fetchRank || fetchCard) tasks.push(getEndpointData(`rank:${accountId}`, `https://api.deadlock-api.com/v1/players/${accountId}/rank`, CONFIG.TTL.RANK, env, ctx, false).then(r => rankRes = r));
        // NEW: these are plain KV reads (cron-maintained, never live-fetched
        // here), so they skip the whole getEndpointData pipeline.
        const kvBinding = env.DEADLOCK_KV || env.KV;
        if (fetchRankHistory && kvBinding) tasks.push(kvBinding.get(`rank_history:${accountId}`).then(raw => { rankHistoryRaw = raw ? JSON.parse(raw) : []; }).catch(() => { rankHistoryRaw = []; }));
        if (fetchSeasonPeak && kvBinding) tasks.push(kvBinding.get(`season_peak:${accountId}`).then(raw => { seasonPeakRaw = raw ? JSON.parse(raw) : null; }).catch(() => { seasonPeakRaw = null; }));
        if (fetchPersonalRecords && kvBinding) tasks.push(kvBinding.get(`personal_records_history:${accountId}`).then(raw => { personalRecordsRaw = raw ? JSON.parse(raw) : []; }).catch(() => { personalRecordsRaw = []; }));
        if (fetchWeakness && kvBinding) tasks.push(kvBinding.get(`weekly_diff:${accountId}`).then(raw => { weeklyDiffRaw = raw ? JSON.parse(raw) : null; }).catch(() => { weeklyDiffRaw = null; }));
        if (fetchBadges && kvBinding) tasks.push(kvBinding.get(`badges:${accountId}`).then(raw => { badgesRaw = raw ? JSON.parse(raw) : {}; }).catch(() => { badgesRaw = {}; }));

        await Promise.all(tasks);

        if (fetchStats) {
          payload.account_stats = parseAccountStatsData(accountStatsRes.data);
          payload.account_stats.error_code = accountStatsRes.error;
        }

        // NEW: normalized seasonal rank block, independent of player_card so
        // BotGhost can pull it even if `include` doesn't ask for "card".
        let normalizedRank = null;
        if (fetchRank || fetchCard) {
          normalizedRank = normalizeRankData(rankRes.data, cardRes.data);
          if (fetchRank) {
            payload.player_rank = {
              ranked_rank: normalizedRank.ranked_rank,
              ranked_subrank: normalizedRank.ranked_subrank,
              rank_points: normalizedRank.rank_points,
              rank_name: normalizedRank.rank_name,
              rank_image: normalizedRank.rank_image,
              season: normalizedRank.season,
              calibrated: normalizedRank.calibrated,
              calibration_status: normalizedRank.calibration_status,
              leaderboard_rank: normalizedRank.leaderboard_rank,
              last_match_delta: normalizedRank.last_match_delta, // points gained/lost on most recent ranked match, e.g. +150
              win_streak: normalizedRank.win_streak,
              win_streak_before_last_match: normalizedRank.win_streak_before_last_match, // NEW: for verification/debugging
              used_demotion_protection: normalizedRank.used_demotion_protection,
              points_into_subrank: normalizedRank.points_into_subrank, // NEW: e.g. 125 pts into current subrank
              points_to_next_subrank: normalizedRank.points_to_next_subrank, // NEW: e.g. 875 pts to rank up
              next_subrank_cost: normalizedRank.next_subrank_cost, // NEW: 1000 normally, 2000 crossing into a new rank
              is_percentile_tier: normalizedRank.is_percentile_tier, // NEW: true for Eternus - progress-to-next isn't meaningful there
              progress_pct: normalizedRank.progress_pct, // NEW: e.g. 37
              progress_bar: normalizedRank.progress_bar, // NEW: e.g. "▰▰▰▱▱▱▱▱▱▱"
              source: normalizedRank.source, // "rank_endpoint" or "card_endpoint_fallback" - check this if numbers look off
              raw: normalizedRank.raw,       // full raw /rank payload for verifying field names once live
              error_code: rankRes.error
            };
          }
        }

        // NEW: rank movement history + trend arrow, opt-in via include=rank_history
        if (fetchRankHistory) {
          const history = rankHistoryRaw || [];
          payload.rank_history = {
            entries: history.slice(-20), // most recent 20 changes, oldest first
            trend: computeRankTrend(history),
            total_snapshots: history.length
          };
        }

        // NEW: highest rank reached this season, opt-in via include=season_peak
        if (fetchSeasonPeak) {
          payload.season_peak = seasonPeakRaw; // null if the cron hasn't recorded one yet for this player
        }

        // NEW: personal record change history + trend, opt-in via include=personal_records
        if (fetchPersonalRecords) {
          const history = personalRecordsRaw || [];
          payload.personal_records = {
            entries: history.slice(-20), // most recent 20 changes, oldest first
            trend: computePersonalRecordTrend(history), // { changed, broken_records, emoji, text }
            total_snapshots: history.length
          };
        }

        // NEW: personal weakness spotlight - this week's rate vs own lifetime
        // norm, opt-in via include=weakness. Needs both lifetime account
        // stats and a completed weekly checkpoint diff, so it stays not-ready
        // until the same 2-Friday timeline as the rest of the weekly system.
        if (fetchWeakness) {
          const parsedForWeakness = accountStatsRes.data ? parseAccountStatsData(accountStatsRes.data) : null;
          payload.weakness_spotlight = computeWeaknessSpotlight(parsedForWeakness, weeklyDiffRaw);
        }

        // NEW: this player's own badge list, opt-in via include=badges.
        // Independent of leaderboard position (unlike badges_leaderboard,
        // which is sorted by count and shifts over time) - always this
        // specific account_id's data.
        if (fetchBadges) {
          const earned = badgesRaw || {};
          const earnedList = BADGE_DEFINITIONS
            .filter(def => earned[def.id])
            .map(def => ({ id: def.id, name: def.name, emoji: def.emoji, desc: def.desc, ...earned[def.id] }));
          const missingList = BADGE_DEFINITIONS
            .filter(def => !earned[def.id])
            .map(def => ({ id: def.id, name: def.name, emoji: def.emoji, desc: def.desc }));

          payload.badges = {
            earned: earnedList,
            missing: missingList,
            badge_count: earnedList.length,
            total_badges: BADGE_DEFINITIONS.length,
            earned_text: earnedList.length > 0 ? earnedList.map(b => `${b.emoji} ${b.name}`).join(", ") : "None yet"
          };
        }

        // NEW: best-effort Ranked-mode unlock progress, opt-in via include=eligibility
        if (fetchEligibility) {
          payload.ranked_eligibility = computeRankedEligibility(historyRes.data);
        }

        if (fetchCard) {
          let cardData = cardRes.data || {};
          if (cardData.response) cardData = cardData.response;
          else if (Array.isArray(cardData) && cardData.length > 0) cardData = cardData[0];

          let featuredHeroId = null, featuredHeroKills = 0, featuredHeroWins = 0;
          if (cardData.slots && Array.isArray(cardData.slots)) {
            const heroSlot = cardData.slots.find(slot => slot.hero !== null);
            if (heroSlot?.hero) {
              featuredHeroId = heroSlot.hero.id;
              featuredHeroKills = heroSlot.hero.kills || 0;
              featuredHeroWins = heroSlot.hero.wins || 0;
            }
          }

          // NEW: rank fields on player_card now come from normalizeRankData
          // (seasonal /rank, falling back to /card) instead of reading
          // cardData.ranked_* directly - keeps this block in sync with the
          // leaderboard and with payload.player_rank above.
          const rankedRank = normalizedRank ? normalizedRank.ranked_rank : (Number(cardData.ranked_rank) || 0);
          const rankedSubrank = normalizedRank ? normalizedRank.ranked_subrank : (Number(cardData.ranked_subrank) || 0);
          const rankName = normalizedRank ? normalizedRank.rank_name : (rankedRank === 0 ? "Obscurus" : `${RANK_NAMES[rankedRank] || "Unknown"} ${rankedSubrank}`);
          const rankImage = normalizedRank ? normalizedRank.rank_image : (RANK_BADGE_IMAGES[rankedRank] || null);

          payload.player_card = {
            account_id: cardData.account_id || Number(accountId),
            ranked_rank: rankedRank,
            ranked_subrank: rankedSubrank,
            rank_points: normalizedRank ? normalizedRank.rank_points : null,
            rank_name: rankName,
            rank_image: rankImage,
            rank_source: normalizedRank ? normalizedRank.source : "card_endpoint_fallback",
            featured_hero: HERO_DICTIONARY[featuredHeroId] || `Unknown (${featuredHeroId})`,
            featured_hero_id: featuredHeroId,
            featured_hero_won: featuredHeroId ? (HERO_WON[featuredHeroId] || null) : null,
            featured_hero_lost: featuredHeroId ? (HERO_LOST[featuredHeroId] || null) : null,
            featured_hero_kills: featuredHeroKills,
            featured_hero_wins: featuredHeroWins,
            raw: cardData,
            error_code: cardRes.error
          };
        }

        if (fetchMates) {
          const mateStatsRaw = mateStatsRes.data || [];
          payload.mate_stats = {
            summary: mateStatsRaw
              .filter(mate => SQUAD_MAP[String(mate.mate_id)] !== undefined)
              .map(mate => {
                const wins = mate.wins || 0;
                const matchesPlayed = mate.matches_played || 0;
                const winRate = matchesPlayed > 0 ? ((wins / matchesPlayed) * 100).toFixed(2) : "0.00";
                return {
                  mate_id: mate.mate_id, mate_name: SQUAD_MAP[String(mate.mate_id)],
                  wins, matches_played: matchesPlayed, losses: matchesPlayed - wins, win_rate: `${winRate}%`
                };
              }),
            raw_count: mateStatsRaw.length,
            error_code: mateStatsRes.error
          };
        }

        if (fetchHistory) {
          const matches = Array.isArray(historyRes.data) ? historyRes.data : (historyRes.data?.matches || historyRes.data?.history || []);
          if (!matches.length) {
            payload.match_history = { has_matches: false, items: [], error_code: historyRes.error };
          } else {
            const recentMatch = matches[0];

            // NEW: last 5 games, each with FULL match detail (hero, K/D/A,
            // outcome, net worth, duration, ranked flag) via the shared
            // summarizeMatch() helper - not just win/loss. Zero extra API
            // calls, since `matches` already has everything needed. This is
            // what powers a "click a button to switch which of your last 5
            // games is shown" UI: all 5 full datasets are already in this
            // one response, so a button just needs to swap which
            // last_5_detailed.N fields the message displays.
            const last5Full = matches.slice(0, 5).map(summarizeMatch);
            const last5WinCount = last5Full.filter(m => m.won).length;
            const last5Emoji = last5Full.map(m => m.won ? "🟢" : "🔴").join(" ");
            const last5Letters = last5Full.map(m => m.won ? "W" : "L").join(" ");

            // FIX: the previous version of this guessed at
            // /v1/matches/${match_id} with no success - it was hitting the
            // wrong path entirely. The real endpoint, confirmed from the
            // Deadlock API project's own documentation (its endpoint list
            // explicitly names "/matches/{id}/metadata - Detailed match
            // information"), is /v1/matches/${match_id}/metadata. This is
            // the same detailed-match data source tracklock/statlocker/
            // deadlocklabs use for their item builds and per-match stats.
            // CONFIRMED (Aug 2026 live payload): endpoint path, roster
            // location (nested under match_info), and the items array shape
            // are all verified against a real response. Each raw item entry
            // is {game_time_s, item_id, upgrade_id, sold_time_s, flags,
            // imbued_ability_id, upgrade_info} - critically, the SAME
            // item_id can appear multiple times as its upgrade_id changes
            // (Deadlock's item-tier upgrade system), so a naive list would
            // show duplicates. This dedupes to the final build: one entry
            // per item_id (its most recent state), excluding any item whose
            // latest record shows it was sold with no later repurchase.
            let matchItems = [];
            let itemsError = null;
            let rawMatchDetails = null;
            if (fetchItems) {
              try {
                const itemsRes = await getEndpointData(
                  `matchdetails:${recentMatch.match_id}`,
                  `https://api.deadlock-api.com/v1/matches/${recentMatch.match_id}/metadata`,
                  CONFIG.TTL.HISTORY, env, ctx, false
                );
                if (itemsRes.error) {
                  itemsError = `/v1/matches/${recentMatch.match_id}/metadata returned an error: ${itemsRes.error}. This is the confirmed endpoint path, so an error here likely means the match isn't processed yet (replay parsing can lag behind match completion) rather than a wrong URL.`;
                } else {
                  const rawPayload = unwrapPayload(itemsRes.data);
                  rawMatchDetails = rawPayload;
                  const matchInfo = pickField(rawPayload, ["match_info"]) || rawPayload; // FIX: roster is nested under match_info
                  const playersArr = pickField(matchInfo, ["players", "match_players", "player_info"]) || null;
                  const thisPlayer = Array.isArray(playersArr)
                    ? playersArr.find(p => String(p.account_id) === String(accountId))
                    : null;
                  const rawItems = Array.isArray(thisPlayer?.items) ? thisPlayer.items : null;

                  if (Array.isArray(rawItems)) {
                    const { map: itemAssetMap } = await getItemAssetsMap(env, ctx);

                    // Keep only the LATEST record per item_id (chronological
                    // by game_time_s), which reflects its final upgrade tier
                    // for the match - then drop anything whose latest record
                    // shows it was sold and never rebought afterward.
                    const latestByItemId = new Map();
                    for (const it of [...rawItems].sort((a, b) => (a.game_time_s ?? 0) - (b.game_time_s ?? 0))) {
                      const itemId = pickField(it, ["item_id"]);
                      if (itemId == null) continue;
                      latestByItemId.set(itemId, it); // later entries overwrite earlier ones - Map preserves insertion order
                    }

                    matchItems = [...latestByItemId.entries()]
                      .filter(([, it]) => !(it.sold_time_s > 0)) // currently sold = not in final build
                      .map(([itemId, it]) => {
                        const asset = itemAssetMap[itemId];
                        const boughtAtS = it.game_time_s ?? null;
                        return {
                          item_id: itemId,
                          item_name: asset?.name ?? `Unknown Item (${itemId})`,
                          item_image: asset?.image ?? null,
                          item_cost: asset?.cost ?? null,
                          item_emoji: itemEmojiTag(itemId), // "" until you add that item_id to ITEM_EMOJIS
                          bought_at_s: boughtAtS,
                          bought_at_formatted: boughtAtS != null ? fmtTime(boughtAtS) : null,
                          raw: it
                        };
                      })
                      .sort((a, b) => (a.bought_at_s ?? 0) - (b.bought_at_s ?? 0));
                  } else if (!Array.isArray(playersArr)) {
                    itemsError = "Match payload responded but no player roster array was found under match_info - the shape may have changed since this was last verified.";
                  } else if (!thisPlayer) {
                    itemsError = `This account (${accountId}) wasn't found in that match's roster - double check the match_id belongs to this player.`;
                  } else {
                    itemsError = "This player's roster entry has no items array - see raw_match_details to inspect the real shape.";
                  }
                }
              } catch (e) {
                itemsError = `Item lookup failed: ${e.message}`;
              }
            }


            payload.match_history = {
              has_matches: true,
              ...summarizeMatch(recentMatch), // most-recent-match fields stay at the top level, unchanged shape for existing templates
              last_5_results: last5Full.map(m => ({ match_id: m.match_id, hero: m.hero, hero_id: m.hero_id, won: m.won })), // kept lightweight, same shape as before, for anything already using it
              last_5_detailed: last5Full, // NEW: full stats per game - last_5_detailed.0 through .4, most recent first
              last_5_emoji: last5Emoji, // e.g. "🟢 🟢 🔴 🟢 🔴"
              last_5_letters: last5Letters, // e.g. "W W L W L"
              last_5_record: `${last5WinCount}W - ${last5Full.length - last5WinCount}L`, // e.g. "3W - 2L"
              items: matchItems, // EXPERIMENTAL field NAMES (endpoint path is confirmed) - only populated when include=items
              items_build_text: matchItems.length > 0 ? matchItems.map(i => i.item_name).join(", ") : "No item data", // NEW: ready-to-embed string, no loop needed in BotGhost
              // NEW: emoji row for the build, one per purchased item, in
              // purchase order. Leading "#" triggers Discord's markdown-
              // header rendering, which displays emoji noticeably larger -
              // confirmed by testing in an actual embed. Items without an
              // ITEM_EMOJIS entry yet just contribute nothing (not a
              // placeholder box), so the row degrades gracefully as you
              // fill the table in over time rather than looking broken.
              items_build_emoji_line: matchItems.some(i => i.item_emoji)
                ? `# ${matchItems.map(i => i.item_emoji).filter(Boolean).join(" ")}`
                : "",
              items_missing_emoji_count: matchItems.filter(i => i.item_id != null && !i.item_emoji).length, // NEW: how many purchased items still need an ITEM_EMOJIS entry - handy while you're filling the table in
              items_error: fetchItems ? itemsError : "Not requested - add include=items to try this",
              raw_match_details: fetchItems ? rawMatchDetails : undefined, // full guessed-endpoint response, so you can see the real shape if items came back empty
              error_code: null
            };
          }
        }
      }

      return new Response(JSON.stringify(payload), { headers: RESPONSE_HEADERS });

    } catch (error) {
      return new Response(
        JSON.stringify({ error: "Worker Exception", message: error.message, stack: error.stack }),
        { status: 500, headers: RESPONSE_HEADERS }
      );
    }
  }
};
