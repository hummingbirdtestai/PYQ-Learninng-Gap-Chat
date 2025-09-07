require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.CONCEPT_LATEX_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_LATEX_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_LATEX_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_LATEX_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_LATEX_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `conceptlatex-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(conceptJson) {
  const compact = JSON.stringify(conceptJson);
  return `
Take the JSON below. Output the same JSON. 
Do not change keys, structure, or Markdown. 
Only add KaTeX/LaTeX ($…$) for all formulas, subscripts, superscripts, charges, angles. 
Keep all other text as is.

INPUT JSON:
${compact}
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

function safeParseObject(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "")
    .replace(/,\s*}/g, "}")
    .replace(/,\s*]/g, "]");

  try {
    return JSON.parse(cleaned);
  } catch (e) {
    console.error("❌ JSON parse error. Raw snippet:", cleaned.slice(0, 200));
    throw e;
  }
}

// ---------- Locking ----------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // free stale locks
  await supabase
    .from("concepts_vertical")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .is("concept_latex_json", null)
    .lt("mcq_lock_at", cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, concept_json")
    .is("concept_latex_json", null)
    .is("mcq_lock", null)
    .order("vertical_id", { ascending: true })
    .limit(limit);

  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.vertical_id);

  const { data: locked, error: e2 } = await supabase
    .from("concepts_vertical")
    .update({
      mcq_lock: WORKER_ID,
      mcq_lock_at: new Date().toISOString()
    })
    .in("vertical_id", ids)
    .is("concept_latex_json", null)
    .is("mcq_lock", null)
    .select("vertical_id, concept_json");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concepts_vertical")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .in("vertical_id", ids);
}

// ---------- Process one row ----------
async function processRow(row) {
  const prompt = buildPrompt(row.concept_json);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const parsed = safeParseObject(raw);

  // ✅ Directly save parsed JSON into concept_latex_json
  const { error } = await supabase
    .from("concepts_vertical")
    .update({ concept_latex_json: parsed })
    .eq("vertical_id", row.vertical_id);

  if (error) {
    console.error(`❌ Update error for vertical_id=${row.vertical_id}:`, error.message);
    await clearLocks([row.vertical_id]);
    return { updated: 0, total: 1 };
  }

  await clearLocks([row.vertical_id]);
  return { updated: 1, total: 1 };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 ConceptLatex Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);

      const results = await Promise.allSettled(claimed.map(row => processRow(row)));

      let updated = 0;
      results.forEach((r, idx) => {
        if (r.status === "fulfilled") {
          console.log(`   row ${idx + 1}: updated ${r.value.updated}/${r.value.total}`);
          updated += r.value.updated;
        } else {
          console.error(`   row ${idx + 1} error:`, r.reason.message || r.reason);
          clearLocks([claimed[idx].vertical_id]);
        }
      });

      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
