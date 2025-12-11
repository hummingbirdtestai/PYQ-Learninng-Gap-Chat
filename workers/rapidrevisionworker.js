require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

//──────────────────────────────────────────────
// SETTINGS
//──────────────────────────────────────────────
const MODEL        = process.env.HY_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.HY_LIMIT || "30", 10);
const SLEEP_MS     = parseInt(process.env.HY_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.HY_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `video-phase-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

//──────────────────────────────────────────────
// PROMPT BUILDER (topic → phase_concept)
//──────────────────────────────────────────────
function buildPrompt(topic) {
  return `
Create ultra–high-yield NEET-PG revision content of 30-point rapid-revision output for that topic with these strict rules: • Output ONLY one code block containing pure markup text. • Start directly with the subject content. • Do NOT add headings like introduction/conclusion. • Do NOT use any meta-expressions (no mentions of videos, reels, TTS, scripts). • • NO tables. • Tone = rapid-revision, crisp, memory-anchoring. • Length = 250–320 words. • Include ONLY must-memorise NEET-PG facts. • Begin immediately with high-yield teaching points. • Give EXACTLY **30 points**, each **<6 words**, high-yield, repeat-asked, tricky. • Final answer must be ONLY the code block — no explanations. Use Unicode for Super Scripts , Subscripts , Symbols , Math , emojis , Bullets and MarkUp to High light Bold and Italic of important Key words in High Yiedl facts List and also Table • Highlight key words using ONLY GitHub-Flavored Markdown: - Bold = **text** - Italic = *text* - Bold+Italic = ***text*** • Do NOT use any underscore formatting (no _text_, __text__, **_text_**). Give the entire content as MarkUp Code BlocK

TOPIC:
${topic}
`.trim();
}

//──────────────────────────────────────────────
// HELPERS
//──────────────────────────────────────────────
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

function isRetryable(e) {
  const m = String(e?.message || e);
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(m);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages,
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (err) {
    if (isRetryable(err) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw err;
  }
}

//──────────────────────────────────────────────
// CLAIM VIDEO ROWS (phase_concept IS NULL)
//──────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // 1. Release stale locks
  await supabase
    .from("video_table")
    .update({ phase_concept_lock: null, phase_concept_lock_at: null })
    .lt("phase_concept_lock_at", cutoff);

  // 2. Fetch unprocessed unlocked rows
  const { data: rows, error } = await supabase
    .from("video_table")
    .select("id, topic")
    .is("phase_concept", null)
    .is("phase_concept_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // 3. Lock them
  const { data: locked, error: lockErr } = await supabase
    .from("video_table")
    .update({
      phase_concept_lock: WORKER_ID,
      phase_concept_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .select("id, topic");

  if (lockErr) throw lockErr;
  return locked || [];
}

//──────────────────────────────────────────────
// CLEAR LOCK (per-row)
//──────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;

  await supabase
    .from("video_table")
    .update({
      phase_concept_lock: null,
      phase_concept_lock_at: null,
    })
    .in("id", ids);
}

//──────────────────────────────────────────────
// PROCESS ONE ROW
//──────────────────────────────────────────────
async function processRow(row) {
  const prompt = buildPrompt(row.topic);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);

  // Save output
  await supabase
    .from("video_table")
    .update({
      phase_concept: raw,
      phase_concept_lock: null,
      phase_concept_lock_at: null,
    })
    .eq("id", row.id);

  return { updated: 1 };
}

//──────────────────────────────────────────────
// MAIN LOOP
//──────────────────────────────────────────────
(async function main() {
  console.log(`🎞️ Video Concept Worker Started | model=${MODEL} | worker=${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} video rows`);

      const results = await Promise.allSettled(
        claimed.map((row) => processRow(row))
      );

      let success = 0;

      results.forEach((res, i) => {
        if (res.status === "fulfilled") {
          console.log(`   ✅ Processed Video Topic #${i + 1}`);
          success += res.value.updated;
        } else {
          console.error(`   ❌ Error in row #${i + 1}:`, res.reason);
          clearLocks([claimed[i].id]);
        }
      });

      console.log(`🔁 Batch finished → saved=${success}/${claimed.length}`);
    } catch (err) {
      console.error("❌ Loop Error:", err);
      await sleep(1000);
    }
  }
})();
