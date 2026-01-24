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

const WORKER_ID =
  process.env.WORKER_ID ||
  `mcq-anatomy-topic-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — DO NOT TOUCH)
// ─────────────────────────────────────────────
function buildPrompt(mcqText) {
  return `
You classify NEET-PG Anatomy PYQ MCQs.

Your ONLY output is the value to be written into the column:
new_topic TEXT

RULES (STRICT):
- Output EXACTLY ONE topic.
- The topic MUST be copied EXACTLY from the allowed list below.
- Do NOT explain.
- Do NOT add quotes, punctuation, or extra text.
- Do NOT invent or modify topics.
- If multiple fit, choose the ONE examiners would use.
- If unsure, choose the BEST possible match (never blank).

OUTPUT:
<one topic name only>

ALLOWED TOPICS (ONLY THESE):

Osteology
Arthrology
Myology
Anatomy terminology
Anatomical planes
Upper limb – shoulder
Upper limb – arm
Upper limb – forearm
Upper limb – hand
Brachial plexus
Lower limb – hip
Lower limb – thigh
Lower limb – leg
Lower limb – foot
Lumbosacral plexus
Thoracic wall
Mediastinum
Heart anatomy
Coronary arteries
Lung anatomy
Pleura
Diaphragm
Abdominal wall
Inguinal canal
Peritoneum
Stomach anatomy
Small intestine
Large intestine
Liver anatomy
Portal vein
Pancreas anatomy
Spleen anatomy
Kidney anatomy
Ureter anatomy
Urinary bladder
Male genital organs
Female genital organs
Pelvic floor
Head and neck – triangles
Thyroid anatomy
Pharynx anatomy
Larynx anatomy
Nose and sinuses
Orbit anatomy
Cranial nerves
Brainstem anatomy
Cerebellum
Basal ganglia
Internal capsule
Ventricular system
Spinal cord
Meninges
CSF circulation
Blood supply of brain
Autonomic nervous system
Sympathetic chain
Parasympathetic system
Endocrine glands
Pituitary anatomy
Pineal gland
Adrenal anatomy
Thyroid blood supply
Lymphatic system
Thoracic duct
Surface anatomy
Radiological anatomy
Developmental anatomy
Pharyngeal arches
Embryonic folding
Congenital anomalies
Hernias anatomy
Applied anatomy – upper limb
Applied anatomy – lower limb
Applied anatomy – abdomen
Applied anatomy – thorax
Applied anatomy – head neck
Neuroanatomy lesions
Vascular anatomy
Cross-sectional anatomy
Case-based anatomy

MCQ:
${mcqText}
`;
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  return /timeout|429|temporar|unavailable|ECONNRESET|ETIMEDOUT/i
    .test(String(e?.message || e));
}

async function callOpenAI(prompt, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: MODEL,
      messages: [{ role: "user", content: prompt }]
    });
    return resp.choices?.[0]?.message?.content?.trim() || "";
  } catch (e) {
    if (isRetryable(e) && attempt <= 2) {
      await sleep(600 * attempt);
      return callOpenAI(prompt, attempt + 1);
    }
    throw e;
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS (ANATOMY ONLY)
// ─────────────────────────────────────────────
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60000).toISOString();

  // Clear expired locks
  await supabase
    .from("mcq_analysis")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .lt("mcq_lock_at", cutoff);

  // Fetch eligible rows
  const { data: rows, error } = await supabase
    .from("mcq_analysis")
    .select("id, mcq")
    .eq("subject", "Anatomy")
    .not("mcq", "is", null)
    .is("new_topic", null)
    .is("mcq_lock", null)
    .order("created_at", { ascending: true })
    .limit(limit);

  if (error) throw error;
  if (!rows?.length) return [];

  const ids = rows.map(r => r.id);

  // Lock rows
  const { data: locked, error: err2 } = await supabase
    .from("mcq_analysis")
    .update({
      mcq_lock: WORKER_ID,
      mcq_lock_at: new Date().toISOString()
    })
    .in("id", ids)
    .is("mcq_lock", null)
    .select("id, mcq");

  if (err2) throw err2;
  return locked || [];
}

// ─────────────────────────────────────────────
// CLEAR LOCKS
// ─────────────────────────────────────────────
async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from("mcq_analysis")
    .update({ mcq_lock: null, mcq_lock_at: null })
    .in("id", ids);
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────
async function processRow(row) {
  const topic = await callOpenAI(buildPrompt(row.mcq));

  if (!topic) {
    throw new Error("❌ Empty topic returned");
  }

  await supabase
    .from("mcq_analysis")
    .update({
      new_topic: topic,
      mcq_lock: null,
      mcq_lock_at: null
    })
    .eq("id", row.id);

  return true;
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────
(async function main() {
  console.log(`🧠 ANATOMY MCQ TOPIC CLASSIFIER STARTED | ${WORKER_ID}`);

  while (true) {
    try {
      const claimed = await claimRows(LIMIT);

      if (!claimed.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(`⚙️ Claimed ${claimed.length} MCQs`);

      for (let i = 0; i < claimed.length; i += BATCH_SIZE) {
        const batch = claimed.slice(i, i + BATCH_SIZE);

        const results = await Promise.allSettled(
          batch.map(processRow)
        );

        results.forEach((res, idx) => {
          if (res.status === "fulfilled") {
            console.log("   ✅ topic classified");
          } else {
            console.error(`   ❌ Failed row ${batch[idx].id}`, res.reason);
            clearLocks([batch[idx].id]);
          }
        });
      }
    } catch (e) {
      console.error("❌ Worker loop error:", e);
      await sleep(1000);
    }
  }
})();
