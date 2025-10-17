// workers/mentorUnicodeMultiWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const MODEL        = process.env.MENTOR_INTRO_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MENTOR_INTRO_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.MENTOR_INTRO_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.MENTOR_INTRO_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.MENTOR_INTRO_LOCK_TTL_MIN || "15", 10);
const SUBJECT      = process.env.MENTOR_INTRO_SUBJECT || "NEET-PG"; // optional filter if you later partition by subject
const WORKER_ID    = process.env.WORKER_ID || `mentor-intro-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(phaseJson) {
  return `
You are a passionate NEET-PG mentor with 30 years’ teaching experience.  
Given a phase_json (concept / explanation / MCQ), write a 3-sentence empathetic motivational intro in simple dialogue style that sounds like a friendly teacher.  
Keep it natural, ≤ 60 words, include authentic NEET-PG tone + emojis, and highlight its exam importance.  
Use Unicode markup for **bold**, *italic*, subscripts, superscripts, and symbols (no LaTeX).  

🔹 Important: If phase_json is of type **MCQ**, do **NOT** reveal, hint, or even indirectly suggest the correct answer or the reasoning that leads to it — only motivate, guide focus, or frame the mindset needed to attempt it.  
🔹 Output strictly as JSON: { "mentor_intro": "<your message>" }

Phase JSON:
${JSON.stringify(phaseJson)}
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function isRetryable(e) {
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(String(e?.message || e));
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "text" }
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
    const cleaned = raw.trim()
      .replace(/^```json\s*/i, "")
      .replace(/^```/, "")
      .replace(/```$/, "");
    return JSON.parse(cleaned);
  } catch (err) {
    throw new Error(`❌ Failed to parse JSON for id=${id}: ${err.message}. Raw: ${raw.slice(0,200)}`);
  }
}

// ---------- Locks ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();
  await supabase
    .from("phase_json")
    .update({ mentor_lock: null, mentor_lock_at: null })
    .is("mentor_reply", null)
    .lt("mentor_lock_at", cutoff);
}

async function claimRows(limit) {
  await freeStaleLocks();
  const { data, error } = await supabase
    .from("phase_json")
    .select("id, phase_json")
    .is("mentor_reply", null)
    .is("mentor_lock", null)
    .limit(limit);
  if (error) throw error;
  if (!data?.length) return [];

  const ids = data.map(r => r.id);
  const { data: locked, error: e2 } = await supabase
    .from("phase_json")
    .update({ mentor_lock: WORKER_ID, mentor_lock_at: new Date().toISOString() })
    .in("id", ids)
    .is("mentor_reply", null)
    .is("mentor_lock", null)
    .select("id, phase_json");
  if (e2) throw e2;
  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("phase_json")
    .update({ mentor_lock: null, mentor_lock_at: null })
    .in("id", ids);
}

// ---------- Process ----------
async function processRow(row) {
  const prompt = buildPrompt(row.phase_json);
  const raw = await callOpenAI(prompt);
  const jsonOut = safeParseJson(raw, row.id);

  if (!jsonOut || typeof jsonOut !== "object" || !jsonOut.mentor_intro) {
    throw new Error(`❌ Expected object with mentor_intro key for id=${row.id}`);
  }

  const { error } = await supabase
    .from("phase_json")
    .update({
      mentor_reply: jsonOut,
      mentor_lock: null,
      mentor_lock_at: null
    })
    .eq("id", row.id);

  if (error) throw new Error(`Update failed id=${row.id}: ${error.message}`);
  return { updated: 1 };
}

async function processBatch(rows) {
  const chunks = [];
  for (let i = 0; i < rows.length; i += BATCH_SIZE) chunks.push(rows.slice(i, i + BATCH_SIZE));

  let updated = 0;
  for (const chunk of chunks) {
    const results = await Promise.allSettled(chunk.map(processRow));
    for (let i = 0; i < results.length; i++) {
      const r = results[i];
      if (r.status === "fulfilled") updated += r.value.updated;
      else {
        console.error(r.reason?.message || r.reason);
        await clearLocks([chunk[i].id]);
      }
    }
  }
  return updated;
}

// ---------- Main ----------
(async function main() {
  console.log(`🧵 Mentor Intro MultiWorker ${WORKER_ID} | model=${MODEL}`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      const updated = await processBatch(claimed);
      console.log(`✅ updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
