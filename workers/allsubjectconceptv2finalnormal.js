require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.CONCEPT_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_LIMIT || "30", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_LOCK_TTL_MIN || "15", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `concept-normalizer-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ⛔ SUBJECTS TO IGNORE
const EXCLUDED_SUBJECTS = [
  "Community Medicine",
  "Forensic Medicine"
];

// ✅ LOG CONTROL (IMPORTANT)
let lastRemaining = null;

// ─────────────────────────────────────────────
// PROMPT (UNCHANGED)
// ─────────────────────────────────────────────
function buildPrompt(conceptText) {
  return `
You are a STRICT CONTENT NORMALIZATION AND PACKING ENGINE.

Your job is ONLY to:
- Remove instructional / prompt / formatting noise accidentally included in content
- Preserve ONLY genuine medical subject content EXACTLY as written
- Pack the cleaned content into a renderer-safe fixed JSON structure

Return ONE valid JSON object ONLY.

CONTENT:
${conceptText}
`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i
    .test(String(e?.message || e));
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }]
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 2) {
      await sleep(600 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// SAFE JSON PARSE
// ─────────────────────────────────────────────
function safeParseJson(raw) {
  const txt = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "");

  const match = txt.match(/\{[\s\S]*\}/);
  if (!match) throw new Error("❌ No JSON object found");

  return JSON.parse(match[0]);
}

// ─────────────────────────────────────────────
// CLAIM ROWS (CLEAN LOGGING)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1️⃣ Clear expired locks
  await supabase
    .from("all_subjects_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .lt("concept_lock_at", cutoff);

  // 2️⃣ Count remaining (LOG ONLY IF CHANGED)
  const { count: remaining, error: countErr } = await supabase
    .from("all_subjects_raw")
    .select("id", { count: "exact", head: true })
    .not("concept", "is", null)
    .is("concept_v2_final", null)
    .not(
      "subject",
      "in",
      `(${EXCLUDED_SUBJECTS.map(s => `"${s}"`).join(",")})`
    );

  if (countErr) throw countErr;

  if (remaining !== lastRemaining) {
    console.log(`📊 Remaining rows to normalize: ${remaining ?? 0}`);
    lastRemaining = remaining;
  }

  // 3️⃣ Fetch rows
  const { data: rows, error } = await supabase
    .from("all_subjects_raw")
    .select("id, concept")
    .not("concept", "is", null)
    .is("concept_v2_final", null)
    .is("concept_lock", null)
    .not(
      "subject",
      "in",
      `(${EXCLUDED_SUBJECTS.map(s => `"${s}"`).join(",")})`
    )
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // 4️⃣ Lock rows
  const { data: locked, error: err2 } = await supabase
    .from("all_subjects_raw")
    .update({
      concept_lock: WORKER_ID,
      concept_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("concept_lock", null)
    .select("id, concept");

  if (err2) throw err2;

  if (locked.length) {
    console.log(`⚙️ Claimed ${locked.length} rows`);
  }

  return locked || [];
}

// ─────────────────────────────────────────────
// CLEAR LOCKS
// ─────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("all_subjects_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  let raw = await callOpenAI(buildPrompt(row.concept));
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(buildPrompt(row.concept));
    parsed = safeParseJson(raw);
  }

  await supabase
    .from("all_subjects_raw")
    .update({
      concept_v2_final: parsed,
      concept_lock: null,
      concept_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧹 CONCEPT NORMALIZER STARTED | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
          batch.map(processRow)
        );

        results.forEach((res, idx) => {
          if (res.status === "fulfilled") {
            console.log("   ✅ concept_v2_final generated");
          } else {
            console.error(`   ❌ Failed row ${batch[idx].id}`, res.reason);
            clearLocks([batch[idx].id]);
          }
        });
      }
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
