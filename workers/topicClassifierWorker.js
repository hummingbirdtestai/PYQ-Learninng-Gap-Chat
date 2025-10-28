// /workers/topicClassifierWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.SUBJECT_IMAGE_MCQ_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.SUBJECT_IMAGE_MCQ_LIMIT || "50", 10);
const BATCH_SIZE   = parseInt(process.env.SUBJECT_IMAGE_MCQ_BLOCK_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.SUBJECT_IMAGE_MCQ_SLEEP_MS || "1000", 10);
const LOCK_TTL_MIN = parseInt(process.env.SUBJECT_IMAGE_MCQ_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `topic-classifier-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ─────────────────────────────────────────────
// PROMPT BUILDER
// ─────────────────────────────────────────────
function buildPrompt(phaseJson) {
  const compact = JSON.stringify(phaseJson);
  return `
You are a **NEET-PG Paper Setter with 30 years of experience**.

Classify the MCQ below into a **High-Yield Topic Name** in **≤ 2 words**.  
If it is longer, output an **acronym** (abbreviation).  

**Rules:**  
- Output ONLY valid JSON: { "topic_name": "..." }  
- Base classification purely on the MCQ’s content.  
- Keep the topic concise, human-readable, and NEET-PG style.

INPUT MCQ JSON:
${compact}
`.trim();
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(String(e?.message || e));
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
    console.error("❌ JSON parse error. Snippet:", cleaned.slice(0, 200));
    throw e;
  }
}

// ─────────────────────────────────────────────
// LOCKING SYSTEM
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // Free stale locks
  await supabase
    .from("mock_tests_phases")
    .update({ topic_lock: null, topic_lock_at: null })
    .lt("topic_lock_at", cutoff);

  // Get unprocessed rows
  const { data: candidates, error: e1 } = await supabase
    .from("mock_tests_phases")
    .select("id, phase_json")
    .is("topic_name", null)
    .is("topic_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map((r) => r.id);

  // Apply lock
  const { data: locked, error: e2 } = await supabase
    .from("mock_tests_phases")
    .update({
      topic_lock: WORKER_ID,
      topic_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("topic_name", null)
    .select("id, phase_json");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("mock_tests_phases")
    .update({ topic_lock: null, topic_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.phase_json);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const result = safeParseObject(raw);

  const topic = result.topic_name?.trim();
  if (!topic) throw new Error("No topic_name returned");

  await supabase
    .from("mock_tests_phases")
    .update({
      topic_name: topic,
      topic_lock: null,
      topic_lock_at: null,
      updated_at: new Date().toISOString(),
    })
    .eq("id", row.id);

  return { updated: 1 };
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧵 TopicClassifier Worker ${WORKER_ID} | model=${MODEL} | limit=${LIMIT}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} rows`);

      const results = await Promise.allSettled(claimed.map((r) => processRow(r)));

      let updated = 0;
      results.forEach((res, i) => {
        if (res.status === "fulfilled") {
          console.log(`✅ Row ${i + 1}: topic updated`);
          updated += res.value.updated;
        } else {
          console.error(`❌ Row ${i + 1} error:`, res.reason.message || res.reason);
          clearLocks([claimed[i].id]);
        }
      });

      console.log(`🌀 Batch complete — updated=${updated}/${claimed.length}`);
    } catch (err) {
      console.error("💥 Loop error:", err.message || err);
      await sleep(2000);
    }
  }
})();
