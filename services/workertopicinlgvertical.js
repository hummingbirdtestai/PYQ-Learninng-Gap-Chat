require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

/* -------- Settings -------- */
const MODEL        = process.env.CLASSIFY_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.TOPIC_LIMIT || "180", 10);
const BLOCK_SIZE   = parseInt(process.env.TOPIC_BLOCK_SIZE || "60", 10);
const SLEEP_MS     = parseInt(process.env.TOPIC_LOOP_SLEEP_MS || "800", 10);
const LOCK_TTL_MIN = parseInt(process.env.TOPIC_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `lg-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

/* -------- Prompt Template -------- */
const PROMPT_TEMPLATE = `
You are an expert NEETPG & USMLE medical teacher with 20+ years of experience.  
I will give you a list of raw MCQ texts (from the learning_gap_vertical table, column mcq_json).

Your task:
- For EACH input row, identify the **single most relevant high-yield topic**.  
- The topic must be **exactly 1–3 words**, like textbook headings (e.g., "Myocardial Infarction", "Elbow Dislocation").  
- Be consistent: always use the same canonical phrase (never synonyms).  
- If drug-related → exact drug/class (e.g., "Metformin").  
- Output one topic **per line**, in the same order as inputs.  
- Do not output numbers, JSON, extra text, or explanations — only plain topic names, one per line.
`;

/* -------- Helpers -------- */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function asyncPool(limit, items, iter) {
  const out = [];
  const exec = [];
  for (const it of items) {
    const p = Promise.resolve().then(() => iter(it));
    out.push(p);
    const e = p.then(() => exec.splice(exec.indexOf(e), 1));
    exec.push(e);
    if (exec.length >= limit) await Promise.race(exec);
  }
  return Promise.allSettled(out);
}

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|rate limit|temporar|unavailable|ECONNRESET/i.test(s);
}

/* -------- OpenAI Call -------- */
async function callOpenAI(inputText, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [
        { role: "system", content: PROMPT_TEMPLATE },
        { role: "user", content: inputText }
      ]
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(inputText, attempt + 1);
    }
    throw e;
  }
}

/* -------- Locking & Claiming -------- */
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  await supabase
    .from("learning_gap_vertical")
    .update({ topic_lock: null, topic_locked_at: null })
    .lt("topic_locked_at", cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from("learning_gap_vertical")
    .select("id, mcq_json")
    .is("topic", null)
    .is("topic_lock", null)
    .not("mcq_json", "is", null)
    .order("id", { ascending: true })
    .limit(limit * 3);

  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  const { data: locked, error: e2 } = await supabase
    .from("learning_gap_vertical")
    .update({
      topic_lock: WORKER_ID,
      topic_locked_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("topic", null)
    .is("topic_lock", null)
    .select("id, mcq_json");

  if (e2) throw e2;

  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("learning_gap_vertical")
    .update({ topic_lock: null, topic_locked_at: null })
    .in("id", ids);
}

/* -------- Per-Row Processor -------- */
async function processRow(row) {
  try {
    const raw = await callOpenAI(
      typeof row.mcq_json === "string" ? row.mcq_json : JSON.stringify(row.mcq_json)
    );

    console.log("🔎 Raw GPT output for row", row.id, ":", raw);

    // take the first non-empty line as topic
    const topicGuess = raw.split("\n").map(l => l.trim()).filter(Boolean)[0] || "General";

    const { error: upErr } = await supabase
      .from("learning_gap_vertical")
      .update({
        topic: topicGuess,
        topic_lock: null,
        topic_locked_at: null
      })
      .eq("id", row.id);

    if (upErr) throw upErr;

    return { ok: true, id: row.id };
  } catch (e) {
    console.error("❌ processRow failed", row.id, e.message);
    await clearLocks([row.id]);
    return { ok: false, id: row.id, error: e.message || String(e) };
  }
}

/* -------- Batch Processor -------- */
async function classifyAndUpdate(batch) {
  // break into blocks for GPT
  for (let i = 0; i < batch.length; i += BLOCK_SIZE) {
    const slice = batch.slice(i, i + BLOCK_SIZE);
    const inputs = slice.map(r => 
      typeof r.mcq_json === "string" ? r.mcq_json : JSON.stringify(r.mcq_json)
    );

    try {
      const resp = await callOpenAI(inputs.join("\n"));
      const topics = resp.split("\n").map(t => t.trim()).filter(Boolean);

      for (let j = 0; j < slice.length; j++) {
        const id = slice[j].id;
        const topic = topics[j] || "General";

        const { error } = await supabase
          .from("learning_gap_vertical")
          .update({
            topic,
            topic_lock: null,
            topic_locked_at: null
          })
          .eq("id", id);

        if (error) {
          console.error(`❌ Failed to update topic for ID: ${id}`, error);
        } else {
          console.log(`✅ Updated topic for ID: ${id} → ${topic}`);
        }
      }
    } catch (err) {
      console.error("❌ classifyAndUpdate error:", err.message);
    }
    await sleep(SLEEP_MS);
  }
}

/* -------- Main Loop -------- */
(async function main() {
  console.log(`🧵 Topic Worker ${WORKER_ID} | model=${MODEL} | limit=${LIMIT} | block=${BLOCK_SIZE} | ttl=${LOCK_TTL_MIN}m`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        console.log("😴 No rows found, sleeping...");
        await sleep(SLEEP_MS * 10);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      await classifyAndUpdate(claimed);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
