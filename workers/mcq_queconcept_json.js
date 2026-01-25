require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_LIMIT || "150", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_LOOP_SLEEP_MS || "200", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_LOCK_TTL_MIN || "10", 10);

const WORKER_ID =
  process.env.WORKER_ID ||
  `mcq-hyf-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — UNCHANGED)
// ─────────────────────────────────────────────
function buildPrompt(mcqJsonText) {
  return `
This is NEETPG PYQ , create 5 HYFs must to remember that are frequently tested in NEETPG Exam and Mnemonic to recall in exam related to this , and Synoptic Revision Tables . give them as JSON with 3 KEYS , concept , mnemonic ,tables .Make mnemonic more easier to remember with Indian context and also popular mnemonics seen in First AID for USMLE Books , USMLE Question Banks ⁠ Format rules for tables ⁠ — Synoptic Revision Tables Each table MUST be formatted EXACTLY as: { "title": "Table X — Clear Exam Purpose", "markdown": "| Column A | Column B | Column C |\n|---|---:|---|\n| Row | Data | Data |" } STRICT TABLE RULES (NON-NEGOTIABLE): •⁠ ⁠Pure markdown table only •⁠ ⁠Header row + separator row mandatory •⁠ ⁠❌ No blank lines inside table •⁠ ⁠❌ No bullets or line breaks inside cells •⁠ ⁠Inline markdown allowed inside cells •⁠ ⁠2–4 columns ONLY •⁠ ⁠Every table must enable *MCQ elimination* Recommended table intents: •⁠ ⁠Differentiation •⁠ ⁠Classification / staging •⁠ ⁠Investigation → next step •⁠ ⁠Risk → consequence mapping Use Markdown for Bold and italic of key words and Unicode for Symbols superscripts , subscripts , math and equations in the content in all the keys

INPUT:
${mcqJsonText}
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
  if (typeof prompt !== "string" || !prompt.trim()) {
    throw new Error("❌ Invalid prompt");
  }

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
// SAFE JSON PARSE (OBJECT)
// ─────────────────────────────────────────────
function safeParseJson(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "");

  const match = cleaned.match(/\{[\s\S]*\}/);
  if (!match) throw new Error("❌ No JSON object found");

  return JSON.parse(match[0]);
}

// ─────────────────────────────────────────────
// CLAIM ROWS
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Clear expired locks
  await supabase
    .from("mcq_reconstruction_queue")
    .update({
      updated_mcq_json_lock: null,
      updated_mcq_json_lock_at: null
    })
    .lt("updated_mcq_json_lock_at", cutoff);

  // Fetch eligible rows
  const { data, error } = await supabase
    .from("mcq_reconstruction_queue")
    .select("id, updated_mcq_json")
    .not("updated_mcq_json", "is", null)
    .is("updated_concept_json", null)
    .is("updated_mcq_json_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);

  const { data: locked, error: err2 } = await supabase
    .from("mcq_reconstruction_queue")
    .update({
      updated_mcq_json_lock: WORKER_ID,
      updated_mcq_json_lock_at: new Date().toISOString(),
      status: "processing"
    })
    .in("id", ids)
    .is("updated_mcq_json_lock", null)
    .select("id, updated_mcq_json");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// PROCESS ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const mcqText = JSON.stringify(row.updated_mcq_json, null, 2);

  let raw = await callOpenAI(buildPrompt(mcqText));
  let parsed;

  try {
    parsed = safeParseJson(raw);
  } catch {
    raw = await callOpenAI(buildPrompt(mcqText));
    parsed = safeParseJson(raw);
  }

  await supabase
    .from("mcq_reconstruction_queue")
    .update({
      updated_concept_json: parsed,
      updated_mcq_json_lock: null,
      updated_mcq_json_lock_at: null,
      status: "completed",
      updated_at: new Date().toISOString()
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 MCQ → HYF+MNEMONIC WORKER STARTED | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
          batch.map(processRow)
        );

        results.forEach((res, idx) => {
          if (res.status === "fulfilled") {
            console.log("   ✅ concept+mnemonic generated");
          } else {
            console.error(`   ❌ Failed row ${batch[idx].id}`, res.reason);
          }
        });
      }
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
