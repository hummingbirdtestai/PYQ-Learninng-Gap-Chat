// workers/conceptWorker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");
const { v4: uuidv4 } = require("uuid");

// ---------- Settings ----------
const CONCEPT_MODEL        = process.env.CONCEPT_MODEL || "gpt-5";
const CONCEPT_LIMIT        = parseInt(process.env.CONCEPT_LIMIT || "100", 10);   // claim per loop
const CONCEPT_BLOCK_SIZE   = parseInt(process.env.CONCEPT_BLOCK_SIZE || "20", 10); // batch per OpenAI call
const CONCEPT_SLEEP_MS     = parseInt(process.env.CONCEPT_LOOP_SLEEP_MS || "800", 10);
const CONCEPT_LOCK_TTL_MIN = parseInt(process.env.CONCEPT_LOCK_TTL_MIN || "15", 10);
const WORKER_ID            = process.env.WORKER_ID || `concept-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt Builder ----------
function buildPrompt(conceptRaw) {
  return `
You are a 20-years experienced NEET Chemistry teacher.  
I will give you raw study material that contains multiple chemistry concepts.  

Your task is to **reorganize the text into structured JSON**.

### Rules:
1. Output must be a **valid JSON array** only.  
2. Each object must have exactly two keys:  
   - "Concept" = short title of the concept (from the heading or main idea).  
   - "Explanation" = the detailed explanation for that concept.  
3. Use **Markdown bold** to highlight:  
   - Important terms (Carbon, Nitrogen, Oxygen, Hybridisation, Isomerism, etc.)  
   - **Formulas, equations, numbers, years, and scientist names**.  
   - Any **keywords** that are high-yield for NEET.  
4. Do not skip any concept. Each must become its own JSON object.  
5. Preserve the order of concepts exactly as in the input.  
6. Keep explanations **precise, clear, and exam-oriented**, as if preparing high-yield notes for NEET.  

### Example Input:
Estimation of Carbon – By Liebig’s method, weight of carbon dioxide (w₁) is measured. Formula: %C = (w₁ × 12 × 100) / 44 × W

Estimation of Hydrogen – By Liebig’s method, weight of water (w₂) is measured. Formula: %H = (w₂ × 2 × 100) / 18 × W

### Example Output:
[
  {
    "Concept": "**Estimation of Carbon**",
    "Explanation": "By **Liebig’s method**, weight of **carbon dioxide (w₁)** is measured. Useful equation: **%C = (w₁ × 12 × 100) / 44 × W**"
  },
  {
    "Concept": "**Estimation of Hydrogen**",
    "Explanation": "By **Liebig’s method**, weight of **water (w₂)** is measured. Useful equation: **%H = (w₂ × 2 × 100) / 18 × W**"
  }
]

${conceptRaw}
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
      model: CONCEPT_MODEL,
      messages,
      temperature: 0
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

// ---------- Locking & Claim ----------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - CONCEPT_LOCK_TTL_MIN * 60 * 1000).toISOString();

  // free stale locks
  await supabase
    .from("concept_bank")
    .update({ concept_lock: null, concept_lock_at: null })
    .is("concept_1", null) // free only if unprocessed
    .lt("concept_lock_at", cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from("concept_bank")
    .select("concept_id, concept_raw")
    .is("concept_1", null)
    .order("concept_id", { ascending: true })
    .limit(limit);
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.concept_id);

  const { data: locked, error: e2 } = await supabase
    .from("concept_bank")
    .update({ concept_lock: WORKER_ID, concept_lock_at: new Date().toISOString() })
    .in("concept_id", ids)
    .is("concept_1", null)
    .is("concept_lock", null)
    .select("concept_id, concept_raw");
  if (e2) throw e2;

  return locked || [];
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("concept_bank")
    .update({ concept_lock: null, concept_lock_at: null })
    .in("concept_id", ids);
}

// ---------- Process one block ----------
async function processBlock(block) {
  const updates = [];

  for (const row of block) {
    try {
      const prompt = buildPrompt(row.concept_raw);
      const raw = await callOpenAI([{ role: "user", content: prompt }]);

      // Clean and parse JSON
      const cleaned = raw.trim().replace(/^```json\n?|```$/g, "");
      const arr = JSON.parse(cleaned);

      // Build update object for supabase
      const updateData = {};
      arr.slice(0, 30).forEach((obj, idx) => {
        updateData[`concept_${idx + 1}`] = {
          uuid: uuidv4(),
          Concept: obj.Concept,
          Explanation: obj.Explanation
        };
      });

      updates.push({ id: row.concept_id, data: updateData });
    } catch (e) {
      console.error(`❌ Error processing row ${row.concept_id}:`, e.message || e);
      await clearLocks([row.concept_id]);
    }
  }

  // Bulk update supabase
  for (const u of updates) {
    const { error: upErr } = await supabase
      .from("concept_bank")
      .update(u.data)
      .eq("concept_id", u.id);
    if (upErr) throw upErr;
  }

  await clearLocks(block.map(r => r.concept_id));
  return { updated: updates.length, total: block.length };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 Concept Worker ${WORKER_ID} | model=${CONCEPT_MODEL} | claim=${CONCEPT_LIMIT} | block=${CONCEPT_BLOCK_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(CONCEPT_LIMIT);
      if (!claimed.length) {
        await sleep(CONCEPT_SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);
      let updated = 0;
      for (let i = 0; i < claimed.length; i += CONCEPT_BLOCK_SIZE) {
        const block = claimed.slice(i, i + CONCEPT_BLOCK_SIZE);
        try {
          const r = await processBlock(block);
          updated += r.updated;
          console.log(`   block ${i / CONCEPT_BLOCK_SIZE + 1}: updated ${r.updated}/${r.total}`);
        } catch (e) {
          console.error("   block error:", e.message || e);
          await clearLocks(block.map(r => r.concept_id));
        }
      }

      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error("Loop error:", e.message || e);
      await sleep(1000);
    }
  }
})();
