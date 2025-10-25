require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.MCQ_REPAIR_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_REPAIR_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_REPAIR_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_REPAIR_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_REPAIR_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `mcq-repair-worker-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(mcqJson) {
  return `
THIS IS A MCQ which is malformed in the stem truncating with "Which of the following ...?".
Rephrase the stem naturally in a NEET-PG clinical vignette format and maintain the same **MarkUp**, _Unicode_ format, and field structure.

Recreate the MCQ JSON in the same schema with better phrasing:
[
  {
    "mcq_key": "mcq_1",
    "stem": "...",
    "options": {"A": "...","B": "...","C": "...","D": "..."},
    "correct_answer": "A",
    "high_yield_facts": "...",
    "learning_gap": "..."
  }
]

🎯 STRICT OUTPUT RULES:
• Output valid JSON only, no markdown fences or prose.
• Preserve existing structure and keys.
• Only rephrase truncated or malformed **stem** sentences.
• Keep all Unicode, sub/superscripts, symbols (↑ ↓ α β μ Δ etc.) intact.

Original MCQ JSON:
${JSON.stringify(mcqJson)}
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(String(e?.message || e));
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "text" },
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

function safeParseJson(raw, id) {
  try {
    const cleaned = raw
      .trim()
      .replace(/^```json\s*/i, "")
      .replace(/^```/, "")
      .replace(/```$/, "");
    return JSON.parse(cleaned);
  } catch (err) {
    const preview = raw ? raw.slice(0, 200).replace(/\n/g, "\\n") : "";
    console.error("❌ Failed to parse JSON for id", id, err.message);
    console.error("Raw:", preview);
    throw new Error("Failed to parse JSON for id=" + id + ": " + err.message);
  }
}

// ---------- Locks ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();
  await supabase
    .from("mock_test_mcqs_flattened")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .is("mcq_json_cleaned", null)
    .lt("mcq_lock_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();
  const { data, error } = await supabase
    .from("mock_test_mcqs_flattened")
    .select("id, mcq_json")
    .eq("select", true)
    .is("mcq_lock", null)
    .is("mcq_json_cleaned", null)
    .limit(limit);

  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map((r) => r.id);
  const { data: locked, error: e2 } = await supabase
    .from("mock_test_mcqs_flattened")
    .update({
      mcq_lock: WORKER_ID,
      mcq_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .eq("select", true)
    .is("mcq_json_cleaned", null)
    .is("mcq_lock", null)
    .select("id, mcq_json");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("mock_test_mcqs_flattened")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .in("id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const prompt = buildPrompt(row.mcq_json);
  const raw = await callOpenAI(prompt);
  const jsonOut = safeParseJson(raw, row.id);

  const { error } = await supabase
    .from("mock_test_mcqs_flattened")
    .update({
      mcq_json_cleaned: jsonOut,
      mcq_lock: null,
      mcq_lock_at: null,
      select: false, // ✅ mark processed
    })
    .eq("id", row.id);

  if (error) throw new Error("Update failed id=" + row.id + ": " + error.message);
  return { updated: 1 };
}

async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    chunks.push(rows.slice(i, i + BATCH_SIZE));
  }

  let updated = 0;
  for (const chunk of chunks) {
    const results = await Promise.allSettled(chunk.map(processRow));
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "fulfilled") {
        updated += r.value.updated;
      } else {
        console.error(r.reason?.message || r.reason);
        await clearLocks([chunk[i].id]);
      }
    }
  }
  return updated;
}

// ---------- Main Loop ----------
(async function main() {
  console.log("🧩 MCQ Repair Worker", WORKER_ID, "| model =", MODEL);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log("⚙️ claimed =", claimed.length);
      const updated = await processBatch(claimed);
      console.log("✅ updated =", updated, "of", claimed.length);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
