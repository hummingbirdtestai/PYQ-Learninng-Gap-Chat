require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.FEED_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.FEED_LIMIT || "50", 10);
const SLEEP_MS     = parseInt(process.env.FEED_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.FEED_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID || `concept-feed-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ─────────────────────────────────────────────
// PROMPT BUILDER
// ─────────────────────────────────────────────
function buildPrompt(conceptObj) {
  return `
You are an expert NEET-PG content creator.

Produce ONLY a valid JSON object with the keys:
- "Keyword"
- "image_description"
- "post_content"

The value of "post_content" MUST follow EXACTLY this structure and Markdown formatting:

1. A main title in the format:
   **${conceptObj.Concept} — High-Yield Image Anatomy for NEET-PG**

2. A 2–3 line intro paragraph explaining why this image is frequently tested.

3. A section titled:
   ### Key Identifiers
   Followed by 2–3 bullet points using “- ” as bullet markers.

4. A section titled:
   ### Must-Know Exam Points
   Followed by exactly 3 bullet points using “- ” markers.

5. A horizontal rule:
   ---

6. A section titled:
   ### 📌 NEET-PG PYQ (Image-Based)
   Include:
   - A bold **Q.** question in one paragraph.
   - A bold **A.** answer in the next line.
   - One short line: **Concept Tested:** <explanation>

7. Another horizontal rule:
   ---

8. A section titled:
   ### Exam Tip
   One crisp line giving a recall trick.

STYLE RULES:
- This exact section order is mandatory.
- Bullet format must match the example exactly.
- Headings MUST use ###.
- Use Markdown **bold**, *italic* and Unicode (superscripts, subscripts, symbols, emojis).
- Tone: factual, concise, clinical.
- Absolutely NO emotional or motivational language.
- No extra commentary outside JSON.

CONCEPT INPUT:
${JSON.stringify(conceptObj)}
`.trim();
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(
    String(e?.message || e)
  );
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(500 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

function safeParseObject(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json/i, "")
    .replace(/^```/, "")
    .replace(/```$/, "")
    .replace(/,\s*}/g, "}")
    .replace(/,\s*]/g, "]");

  try {
    return JSON.parse(cleaned);
  } catch (err) {
    console.error("❌ JSON Parse Error EXTRACT:", cleaned.slice(0, 300));
    throw err;
  }
}

// ─────────────────────────────────────────────
// LOCKING SYSTEM
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();

  // Release expired locks
  await supabase
    .from("concept_phase_final")
    .update({ mentor_lock: null, mentor_lock_at: null })
    .lt("mentor_lock_at", cutoff);

  // Fetch rows to process
  const { data: rows, error } = await supabase
    .from("concept_phase_final")
    .select("id, phase_json")
    .eq("phase_type", "concept")
    .is("feed", null)
    .is("mentor_lock", null)
    .order("react_order_final", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // Lock them
  const { data: locked, error: err2 } = await supabase
    .from("concept_phase_final")
    .update({
      mentor_lock: WORKER_ID,
      mentor_lock_at: new Date().toISOString(),
    })
    .in("id", ids)
    .is("feed", null)
    .is("mentor_lock", null)
    .select("id, phase_json");

  if (err2) throw err2;

  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concept_phase_final")
    .update({ mentor_lock: null, mentor_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS SINGLE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const conceptObj = row.phase_json;

  const prompt = buildPrompt(conceptObj);
  const raw = await callOpenAI([{ role: "user", content: prompt }]);
  const parsed = safeParseObject(raw);

  await supabase
    .from("concept_phase_final")
    .update({ feed: parsed })
    .eq("id", row.id);

  await clearLocks([row.id]);

  return { updated: 1 };
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🟩 Concept Feed Worker Started | worker=${WORKER_ID} | model=${MODEL}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} concept rows`);

      const results = await Promise.allSettled(
        claimed.map(row => processRow(row))
      );

      let updated = 0;
      results.forEach((r, idx) => {
        if (r.status === "fulfilled") {
          console.log(`   ✅ Processed row #${idx + 1}`);
          updated += r.value.updated;
        } else {
          console.error(`   ❌ Error row #${idx + 1}:`, r.reason);
          clearLocks([claimed[idx].id]);
        }
      });

      console.log(`🔁 Batch complete → updated=${updated}/${claimed.length}`);

    } catch (e) {
      console.error("Loop error:", e);
      await sleep(1000);
    }
  }
})();
