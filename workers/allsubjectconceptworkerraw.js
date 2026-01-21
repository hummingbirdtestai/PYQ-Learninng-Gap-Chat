require("dotenv").config();
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────
const MODEL        = process.env.CONCEPT_MODEL || "gpt-5-mini";
const LIMIT        = parseInt(process.env.CONCEPT_LIMIT || "30", 10);
const BATCH_SIZE   = parseInt(process.env.CONCEPT_BATCH_SIZE || "5", 10);
const SLEEP_MS     = parseInt(process.env.CONCEPT_LOOP_SLEEP_MS || "500", 10);
const LOCK_TTL_MIN = parseInt(process.env.CONCEPT_LOCK_TTL_MIN || "15", 10);
const WORKER_ID    = process.env.WORKER_ID ||
  `concept-all-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ─────────────────────────────────────────────
// CONCEPT PROMPT (USE AS-IS)
// ─────────────────────────────────────────────
function buildPrompt(question) {
  return `
You are an 30 Years experienced Undergraduate MBBS Teacher expert in Preparing MBBS Students to do Last Minute revision for University MBBS exams Theory Papers. MBBS Students needed Bullet Points Summary Notes to answer this Question as Buzzword style for Active Recall and Spaced Repettion to write Answers in Exam. 1) Central Concepts fundament to understanding of the Topic 2) Give a **5 Clinical Case Vignettes** on emphasis of: • Clinical History • Physical Examination • Investigations • Differential • Treatment (first part should reflect the expected depth and standard) 3) Then give **25 Most High Yield points** like Buzz words, one need to remember for USMLE , NEETPG , MRCP Exam. • gIVE EACH hyf as a short sentence max **6 Words long** • Bold and Italic with Unicode important words that need instant memory. • Cover most tested Exam points and cover all within that 25. • Make **25 Points**, highly exam oriented and Cover all. IMPORTANT FORMATTING RULES FOR HIGH-YIELD FACTS (STRICT): a) In **High-Yield Facts**, ***bold + italic highlighting must be limited to a maximum of 1–2 words only per point***. b) Do **NOT** apply bold-italic formatting to the entire sentence in High-Yield Facts — only the **single most recall-critical keyword or phrase**. c) **DO NOT wrap the final output in triple backticks when passing the content to a rendering component**; the content must be plain Markdown text without outer code fences. 4) Include **Synoptic Summary Tables** for rapid exam revision. The content should be: 1) **Central Concepts** 2) **5 Clinical Case Vignettes** 2) **25 High-Yield Facts** 3) **Synoptic Summary Tables** Give the output *strictly in Markdown code blocks* with Unicode symbols. In the output, explicitly *bold and italicize* all important: • key words • clinical terms • diseases • signs • investigations • headings for emphasis using proper Markdown (e.g., **bold**, *italic*). Use: • headings • *bold* • *italic* • arrows (→, ↑, ↓) • subscripts / superscripts (₁, ₂, ³, ⁺, ⁻) • Greek letters • emojis (💡🫀🫁🧠⚕📘) naturally throughout for visual clarity. Do *NOT* output as JSON. Do *NOT* add any titles or headers beyond the **2 sections** I specify. Output ONLY those **2 sections exactly as numbered**. Dont mention USMLE Style of AMBOSS, USMLE World , NBME. Output as **ONE single Markdown code block**.

QUESTION:
${question}
`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(s);
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }]
    });
    return resp.choices?.[0]?.message?.content || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 2) {
      await sleep(600 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS (concept IS NULL)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Clear expired locks
  await supabase
    .from("all_subjects_raw")
    .update({ concept_lock: null, concept_lock_at: null })
    .lt("concept_lock_at", cutoff);

  // Fetch rows
  const { data: rows, error } = await supabase
    .from("all_subjects_raw")
    .select("id, question")
    .is("concept", null)
    .is("concept_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) re
