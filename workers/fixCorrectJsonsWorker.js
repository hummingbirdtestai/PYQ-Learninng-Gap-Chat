// workers/fixCorrectJsonsWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.CORRECT_JSONS_V2_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CORRECT_JSONS_V2_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.CORRECT_JSONS_V2_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.CORRECT_JSONS_V2_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CORRECT_JSONS_V2_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `fix-correctjsons-v2-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(conversationJson) {
  return `
You are a JSON fixer. Input is a JSON with key "HYFs", each having an array "MCQs".

Rules:
- For each MCQ: if "correct_answer" is missing, add it.
- Derive correct answer (A/B/C/D) from options + feedback.
- If "correct_answer" exists, keep it.
- Do not change any other content.
- Output valid JSON only.

Fix this JSON:
${JSON.stringify(conversationJson)}
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "json_object" }
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(500 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

function safeParseJson(raw, verticalId) {
  try {
    return JSON.parse(
      raw.trim()
        .replace(/^```json\s*/i, "")
        .replace(/^```/, "")
        .replace(/```$/, "")
    );
  } catch (e) {
    console.error(`❌ JSON parse failed for vertical_id=${verticalId}. Raw output:`, raw.slice(0, 300));
    throw e;
  }
}

// ---------- Locking ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();
  await supabase
    .from("concepts_vertical")
    .update({ conversation_lock: null, conversation_lock_at: null })
    .eq("missing_correct_answer_in_correct_jsons", true)
    .is("correct_jsons_v2", null)
    .lt("conversation_lock_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();

  const { data: candidates, error } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, correct_jsons")
    .eq("missing_correct_answer_in_correct_jsons", true)
    .is("correct_jsons_v2", null)
    .is("conversation_lock", null)
    .order("vertical_id", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.vertical_id);
  const { data: locked, error: e2 } = await supabase
    .from("concepts_vertical")
    .update({
      conversation_lock: WORKER_ID,
      conversation_lock_at: new Date().toISOString(),
    })
    .in("vertical_id", ids)
    .eq("missing_correct_answer_in_correct_jsons", true)
    .is("correct_jsons_v2", null)
    .is("conversation_lock", null)
    .select("vertical_id, correct_jsons");

  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concepts_vertical")
    .update({ conversation_lock: null, conversation_lock_at: null })
    .in("vertical_id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const original = row.correct_jsons;
  if (!original?.HYFs) {
    console.error(`⚠️ Skipping vertical_id=${row.vertical_id} (no HYFs)`);
    return { updated: 0, total: 1 };
  }

  let fixedHYFs = [];

  for (let i = 0; i < original.HYFs.length; i++) {
    const hyf = original.HYFs[i];
    const prompt = buildPrompt({ HYFs: [hyf] });
    console.log(`   → HYF ${i + 1}/${original.HYFs.length}, length=${prompt.length}`);

    let raw;
    try {
      raw = await callOpenAI(prompt);
    } catch (e) {
      console.error(`❌ OpenAI call failed for vertical_id=${row.vertical_id}, HYF=${i + 1}`, e.message);
      fixedHYFs.push(hyf); // fallback
      continue;
    }

    if (!raw) {
      console.error(`⚠️ Empty response for vertical_id=${row.vertical_id}, HYF=${i + 1}`);
      fixedHYFs.push(hyf); // fallback
      continue;
    }

    try {
      const parsed = safeParseJson(raw, row.vertical_id);
      if (parsed?.HYFs?.[0]) {
        fixedHYFs.push(parsed.HYFs[0]);
      } else {
        console.warn(`⚠️ Parsed JSON invalid for vertical_id=${row.vertical_id}, HYF=${i + 1}`);
        fixedHYFs.push(hyf); // fallback
      }
    } catch (e) {
      console.error(`❌ Parse error for vertical_id=${row.vertical_id}, HYF=${i + 1}`, e.message);
      fixedHYFs.push(hyf); // fallback
    }
  }

  const fixedJson = { HYFs: fixedHYFs };

  const { error: upErr } = await supabase
    .from("concepts_vertical")
    .update({
      correct_jsons_v2: fixedJson,
      conversation_lock: null,
      conversation_lock_at: null,
    })
    .eq("vertical_id", row.vertical_id);

  if (upErr) {
    const preview = JSON.stringify(fixedJson).slice(0, 200);
    throw new Error(`Update failed for vertical_id=${row.vertical_id}: ${upErr.message}. Preview: ${preview}`);
  }

  return { updated: 1, total: 1 };
}

// ---------- Batch ----------
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
        console.error(`   row ${i + 1} error:`, r.reason?.message || r.reason);
        await clearLocks([chunk[i].vertical_id]);
      }
    }
  }
  return updated;
}

// ---------- Main ----------
(async function main() {
  console.log(`🧵 Correct JSONs v2 Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      const updated = await processBatch(claimed);
      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
