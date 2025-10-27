// /app/workers/mcq_generator_worker.js
require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ---------- Settings ----------
const MODEL        = process.env.MCQ_GEN_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.MCQ_GEN_LIMIT || "100", 10);
const BATCH_SIZE   = parseInt(process.env.MCQ_GEN_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.MCQ_GEN_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_GEN_LOCK_TTL_MIN || "15", 10);

// ✅ UPDATED HERE — narrowed to only one subject, matching DB spelling exactly
const SUBJECTS     = ["Obstetrics and Gynaecology"];

const WORKER_ID    = process.env.WORKER_ID || 
  `mcq-gen-worker-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Prompt ----------
function buildPrompt(concept) {
  return `
You are a **NEET-PG Exam paper setter with 30 years of experience**, deeply familiar with actual NEET-PG PYQs and question framing patterns seen in **AMBOSS, UWorld, NBME, and FIRST AID**.

From the following Concept & Explanation, create **exactly 1 MCQ** (moderate-to-severe difficulty, NEET-PG / NBME style).  
Each MCQ must be a **clinical case vignette** testing the most **high-yield concept** from the explanation.

🎯 STRICT OUTPUT RULES — Output valid JSON only (no Markdown, no array, no text outside).  
The output must be **one single JSON object** like this example:

{
  "stem": "A 12-year-old girl presents for school medical clearance but has no vaccination records. On exam there is a healed 4–5 mm scar over the left deltoid consistent with prior intradermal injection. Parents report the child received a vaccine as a neonate but cannot recall which one. Regarding the observed scar and BCG vaccination, **which of the following...?**",
  "mcq_key": "mcq_3",
  "options": {
    "A": "Presence of the deltoid scar excludes the need for any further TB testing because it proves lifelong immunity.",
    "B": "Absence of a scar reliably excludes prior BCG vaccination and indicates the child was not immunized.",
    "C": "A healed intradermal scar is commonly used as evidence of prior *BCG* vaccination, but absence of a scar does not definitively exclude prior vaccination.",
    "D": "A BCG scar indicates vaccine failure and predicts higher risk of severe childhood TB."
  },
  "learning_gap": "💡 Trap: Interpreting the scar too rigidly. Do not assume absence of scar = no vaccination, nor assume scar = complete protection. Use scar as supportive history; combine with exposure, symptoms, and testing when deciding further evaluation.",
  "correct_answer": "C",
  "high_yield_facts": "✅ Typical local reaction: intradermal papule → ulcer → healed **scar**. The scar is **commonly used as evidence** of prior BCG, but **no scar does not reliably rule out** past vaccination. Scar presence ≠ guaranteed sterilizing immunity."
}

🧩 Guidelines:
• Each stem must form a complete, self-contained clinical vignette ending naturally with “Which of the following…?”  
• Avoid “EXCEPT” or “All of the following”.  
• Include patient details, investigations, and clues.  
• Use crisp professional exam tone — no AI or textbook verbosity.  
• Correct answer must be a single letter A–D.  

Concept JSON:
${JSON.stringify(concept)}
`.trim();
}

// ---------- Helpers ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|ETIMEDOUT|429|temporar|unavailable|ECONNRESET/i.test(String(e?.message || e));
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "text" },
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(prompt, attempt + 1);
