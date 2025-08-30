import { createClient } from "@supabase/supabase-js";
import OpenAI from "openai";
import pg from "pg";

const required = [
  "SUPABASE_URL",
  "SUPABASE_SERVICE_ROLE_KEY",
  "OPENAI_API_KEY"
];
required.forEach((k) => {
  if (!process.env[k]) {
    throw new Error(`❌ Missing env var: ${k}`);
  } else {
    console.log(`✅ Loaded ${k}`);
  }
});

// --- ENV Vars ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const GEN_MODEL = process.env.GEN_MODEL || "gpt-5-mini";
const GEN_LIMIT = parseInt(process.env.GEN_LIMIT || "120", 10);
const GEN_CONCURRENCY = parseInt(process.env.GEN_CONCURRENCY || "5", 10);
const GEN_LOOP_SLEEP_MS = parseInt(process.env.GEN_LOOP_SLEEP_MS || "1000", 10);
const GEN_LOCK_TTL_MIN = parseInt(process.env.GEN_LOCK_TTL_MIN || "30", 10); // minutes
const WORKER_ID = process.env.WORKER_ID || "worker-1";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
const pgClient = new pg.Client({ connectionString: `${SUPABASE_URL}/postgres?apikey=${SUPABASE_SERVICE_ROLE_KEY}` });
await pgClient.connect();

const PROMPT_TEMPLATE = `You are a NEETPG & USMLE expert medical teacher with 20 years of experience.

### Task
From the RAW MCQ text provided as input, create exactly 20 Questions and Answers for NEETPG/USMLE preparation.

### Rules
1. Output must be a **valid JSON array only**.  
2. Each Question and Answer must be a JSON object with exactly these keys:  
   - "question": Rewrite the MCQ into a **3–4 sentence clinical vignette style active recall question** (standard of NBME, Amboss, UWorld, USMLERx). Must be **exam-oriented, very specific, and high-yield**.  
   - "answer": Provide the **precise, high-yield answer** (not generic).  
3. Do **NOT format as MCQs**. Each must be in **Question–Answer format**.  
4. Use **Markdown bold** to highlight important words, numbers, anatomical structures, diseases, drugs, and exam-relevant facts in both the **question** and the **answer**.  
5. Do not include UUIDs. Supabase will auto-generate them.  
6. Do not include any text outside the JSON (no commentary, no explanations, no headings).  
7. Questions must **not be repeated** — instead, create **recursive remediation questions** that expand into the most relevant connected high-yield concepts that logically follow from the primary MCQ, as appropriate for the subject.  
8. If a **clinical vignette is not possible** (low-yield recall fact), frame a **direct NEETPG-style high-yield Q&A** instead.  
9. Ensure all questions and answers are **unique, non-repetitive, and exam-style**.  
10. Start with the **primary fact tested in the raw MCQ**, and then branch into **18–19 progressively related high-yield concepts** for active recall and spaced repetition.  
11. Maintain the same JSON structure strictly for every output.  

### Output JSON Format Example

[
  {
    "question": "",
    "answer": ""
  }
]
`;

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchAndLockBatch() {
  const { rows } = await pgClient.query(
    `
    UPDATE mcq_bank
    SET lg_lock = $1, lg_locked_at = now()
    WHERE id IN (
      SELECT id FROM mcq_bank
      WHERE lg_flashcard IS NULL
        AND (lg_lock IS NULL OR lg_locked_at < now() - ($2 || ' minutes')::interval)
      ORDER BY created_at
      FOR UPDATE SKIP LOCKED
      LIMIT $3
    )
    RETURNING id, mcq;
    `,
    [WORKER_ID, GEN_LOCK_TTL_MIN, GEN_LIMIT]
  );
  return rows;
}

async function processBatch() {
  const rows = await fetchAndLockBatch();
  if (rows.length === 0) {
    console.log(`[${WORKER_ID}] No rows to process.`);
    return;
  }
  console.log(`[${WORKER_ID}] Picked ${rows.length} rows`);

  for (let i = 0; i < rows.length; i += GEN_CONCURRENCY) {
    const chunk = rows.slice(i, i + GEN_CONCURRENCY);
    await Promise.all(
      chunk.map(async (row) => {
        try {
          const completion = await openai.chat.completions.create({
            model: GEN_MODEL,
            messages: [
              { role: "system", content: PROMPT_TEMPLATE },
              { role: "user", content: row.mcq },
            ],
            temperature: 0,
          });

          const jsonStr = completion.choices[0].message.content.trim();
          let parsed;
          try {
            parsed = JSON.parse(jsonStr);
          } catch {
            console.error(`[${WORKER_ID}] Invalid JSON for id=${row.id}`);
            return;
          }

          const { error: updateError } = await supabase
            .from("mcq_bank")
            .update({
              lg_flashcard: parsed,
              lg_lock: null,
              lg_locked_at: null,
            })
            .eq("id", row.id);

          if (updateError) {
            console.error(`[${WORKER_ID}] Update error id=${row.id}`, updateError);
          } else {
            console.log(`[${WORKER_ID}] ✅ Updated id=${row.id}`);
          }
        } catch (err) {
          console.error(`[${WORKER_ID}] GPT error id=${row.id}`, err);
          // release lock so it can be retried
          await supabase
            .from("mcq_bank")
            .update({ lg_lock: null, lg_locked_at: null })
            .eq("id", row.id);
        }
      })
    );
    await sleep(GEN_LOOP_SLEEP_MS);
  }
}

async function main() {
  while (true) {
    await processBatch();
    await sleep(GEN_LOOP_SLEEP_MS);
  }
}

main();
