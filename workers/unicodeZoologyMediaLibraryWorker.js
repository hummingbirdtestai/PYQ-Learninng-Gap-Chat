// workers/unicodeZoologyMediaLibraryWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.MEDIA_UNICODE_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MEDIA_UNICODE_LIMIT || "200", 10);
const BATCH_SIZE   = parseInt(process.env.MEDIA_UNICODE_BATCH_SIZE || "10", 10);
const SLEEP_MS     = parseInt(process.env.MEDIA_UNICODE_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.MEDIA_UNICODE_LOCK_TTL_MIN || "15", 10);
const SUBJECT_FILTER = "Zoology";   // ✅ Hard-locked to Zoology
const WORKER_ID    = process.env.WORKER_ID || `zoology-media-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(conceptJson) {
  const compact = JSON.stringify(conceptJson);
  return `
You are a 40-year experienced NEET Zoology teacher. 
From the Concept JSON below, create **3 Google Image searches** and **3 YouTube video searches**, each with a short description.

Rules:
- Output JSON only → { "uuid": "...", "ImageSearches": [...], "YouTubeSearches": [...] } 
- Use the same **uuid** provided in the input.
- Each item → { "Search": "", "Description": "" } 
- Use **Markdown** for emphasis of *bold*, *italic* and **Unicode** for formulas, subscripts, superscripts, bond angles. 
- Each search must cover a **unique high-yield aspect** (no repetition).

Concept JSON:
${compact}
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
      max_completion_tokens: 1500,
      messages: [{ role: "user", content: prompt }],
      response_format: {
        type: "json_schema",
        json_schema: {
          name: "media_library",
          schema: {
            type: "object",
            properties: {
              uuid: { type: "string" },
              ImageSearches: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    Search: { type: "string" },
                    Description: { type: "string" }
                  },
                  required: ["Search", "Description"],
                  additionalProperties: false
                },
                minItems: 3,
                maxItems: 3
              },
              YouTubeSearches: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    Search: { type: "string" },
                    Description: { type: "string" }
                  },
                  required: ["Search", "Description"],
                  additionalProperties: false
                },
                minItems: 3,
                maxItems: 3
              }
            },
            required: ["uuid", "ImageSearches", "YouTubeSearches"],
            additionalProperties: false   // ✅ must be false at root level
          },
          strict: true
        }
      }
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

function safeParseJson(raw) {
  const cleaned = raw.trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "");
  return JSON.parse(cleaned);
}

// ---------- Locking ----------
async function freeStaleLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();
  let q = supabase
    .from("concepts_vertical")
    .update({ conversation_lock: null, conversation_lock_at: null })
    .is("media_library_unicode", null)
    .lt("conversation_lock_at", cutoff);

  if (SUBJECT_FILTER) q = q.eq("subject_name", SUBJECT_FILTER);
  await q;
}

async function claimRows(limit) {
  await freeStaleLocks();

  let q = supabase
    .from("concepts_vertical")
    .select("vertical_id, concept_json_unicode")
    .not("concept_json_unicode", "is", null)
    .is("media_library_unicode", null)
    .is("conversation_lock", null)
    .order("vertical_id", { ascending: true })
    .limit(limit);

  if (SUBJECT_FILTER) q = q.eq("subject_name", SUBJECT_FILTER);

  const { data: candidates, error } = await q;
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
    .is("media_library_unicode", null)
    .is("conversation_lock", null)
    .select("vertical_id, concept_json_unicode");

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
  const prompt = buildPrompt(row.concept_json_unicode);
  const raw = await callOpenAI(prompt);
  const jsonOut = safeParseJson(raw);

  const { error: upErr } = await supabase
    .from("concepts_vertical")
    .update({
      media_library_unicode: jsonOut,
      conversation_lock: null,
      conversation_lock_at: null
    })
    .eq("vertical_id", row.vertical_id);

  if (upErr) {
    const preview = JSON.stringify(jsonOut).slice(0, 200);
    throw new Error(`Update failed for vertical_id=${row.vertical_id}: ${upErr.message}. Preview: ${preview}`);
  }
  return { updated: 1, total: 1 };
}

// ---------- Main ----------
(async function main() {
  console.log(`🧵 Zoology Media Library Worker ${WORKER_ID} | model=${MODEL} | claim=${LIMIT} | batch=${BATCH_SIZE}`);
  while (true) {
    try {
      const claimed = await claimRows(LIMIT);
      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }
      console.log(`⚙️ claimed=${claimed.length}`);
      const results = await Promise.allSettled(claimed.map(processRow));
      let updated = 0;
      results.forEach((r, idx) => {
        if (r.status === "fulfilled") updated += r.value.updated;
        else {
          console.error(`   row ${idx + 1} error:`, r.reason?.message || r.reason);
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
