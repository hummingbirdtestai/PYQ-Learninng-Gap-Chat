// workers/conversationWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const CONVERSATION_MODEL        = process.env.CONVERSATION_MODEL || "gpt-5";
const CONVERSATION_LIMIT        = parseInt(process.env.CONVERSATION_LIMIT || "50", 10);
const CONVERSATION_BLOCK_SIZE   = parseInt(process.env.CONVERSATION_BLOCK_SIZE || "10", 10);
const CONVERSATION_SLEEP_MS     = parseInt(process.env.CONVERSATION_LOOP_SLEEP_MS || "800", 10);
const CONVERSATION_LOCK_TTL_MIN = parseInt(process.env.CONVERSATION_LOCK_TTL_MIN || "15", 10);
const WORKER_ID                 = process.env.WORKER_ID || `conv-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(conceptJson) {
  const raw = JSON.stringify(conceptJson, null, 2);

  return `
You are an Expert Chemistry NEET Coaching Guru with 40 years of experience.  
You know the challenges of students with reading difficulties, comprehension disabilities, writing struggles, slow learning, and weak English vocabulary.  

When teaching such a student:  
- Do NOT bombard with heavy language.  
- Teach like a STORY, slowly, with empathy.  
- Use anecdotes and simple real-life examples to ignite curiosity.  
- Highlight how a NEET examiner may frame a question from this concept, and clearly give the correct answer.  
- Show where this concept is useful while solving NEET MCQs.  

🎯 Output Format Requirements (MUST FOLLOW STRICTLY):  
- Output must be a **single JSON object**.  
- Keys: "uuid", "ConceptHeading", "Conversation".  
- "Conversation" is an array of objects, each object has:  
  - "role" = "teacher" | "student" | "examiner_hint" | "teacher_summary"  
  - "text" = dialogue line.  
- Use **Markdown bold** for important terms, formulas, keywords.  
- Use **emojis and icons** richly (👨‍🏫, 👦, ⚡, 📌, 🎯, 💡, etc.).  
- Keep JSON **cleaner** (no meta-labels like “Teacher continues like a story”).  
- Ensure content is **verbatim, exam-friendly, and NEET-relevant**.  
- The JSON must be **valid and directly renderable** in a React Native Web dark-mode app with ChatGPT font on mobile.  

👉 Input Concept JSON:  
${raw}
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
      model: CONVERSATION_MODEL,
      messages
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

function safeParseJSON(raw) {
  const cleaned = raw
    .trim()
    .replace(/^```json\s*/i, "")
    .replace(/^```/i, "")
    .replace(/```$/i, "");

  try {
    return JSON.parse(cleaned);
  } catch (e) {
    console.error("❌ JSON parse error. Raw snippet:", cleaned.slice(0, 250));
    throw e;
  }
}

// ---------- Locking ----------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - CONVERSATION_LOCK_TTL_MIN * 60 * 1000).toISOString();

  // free stale locks
  await supabase
    .from("concepts_vertical")
    .update({ conversation_lock: null, conversation_lock_at: null })
    .is("conversation", null)
    .lt("conversation_lock_at", cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from("concepts_vertical")
    .select("vertical_id, concept_json")
    .not("concept_json", "is", null)
    .is("conversation", null)
    .order("vertical_id", { ascending: true })
    .limit(limit);
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.vertical_id);

  const { data: locked, error: e2 } = await supabase
    .from("concepts_vertical")
    .update({
      conversation_lock: WORKER_ID,
      conversation_lock_at: new Date().toISOString()
    })
    .in("vertical_id", ids)
    .is("conversation", null)
    .is("conversation_lock", null)
    .select("vertical_id, concept_json");
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

// ---------- Process one block ----------
async function processBlock(block) {
  const updates = [];

  for (const row of block) {
    try {
      const prompt = buildPrompt(row.concept_json);
      const raw = await callOpenAI([{ role: "user", content: prompt }]);
      const obj = safeParseJSON(raw);

      // enforce uuid
      if (!obj.uuid) {
        obj.uuid = row.concept_json?.uuid || uuidv4();
      }

      updates.push({ id: row.vertical_id, data: { conversation: obj } });
    } catch (e) {
      console.error(`❌ Error processing row ${row.vertical_id}:`, e.message || e);
      await clearLocks([row.vertical_id]);
    }
  }

  for (const u of updates) {
    const { error: upErr } = await supabase
      .from("concepts_vertical")
      .update(u.data)
      .eq("vertical_id", u.id);
    if (upErr) throw upErr;
  }

  await clearLocks(block.map(r => r.vertical_id));
  return { updated: updates.length, total: block.length };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 Conversation Worker ${WORKER_ID} | model=${CONVERSATION_MODEL} | claim=${CONVERSATION_LIMIT} | block=${CONVERSATION_BLOCK_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(CONVERSATION_LIMIT);
      if (!claimed.length) {
        await sleep(CONVERSATION_SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);
      let updated = 0;
      for (let i = 0; i < claimed.length; i += CONVERSATION_BLOCK_SIZE) {
        const block = claimed.slice(i, i + CONVERSATION_BLOCK_SIZE);
        try {
          const r = await processBlock(block);
          updated += r.updated;
          console.log(`   block ${i / CONVERSATION_BLOCK_SIZE + 1}: updated ${r.updated}/${r.total}`);
        } catch (e) {
          console.error("   block error:", e.message || e);
          await clearLocks(block.map(r => r.vertical_id));
        }
      }

      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
