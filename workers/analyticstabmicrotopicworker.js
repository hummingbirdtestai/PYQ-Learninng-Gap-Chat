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
  `mcq-pediatrics-topic-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (STRICT — DO NOT MODIFY)
// ─────────────────────────────────────────────
function buildPrompt(mcqText) {
  return `
You classify NEET-PG Pediatrics PYQ MCQs.

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

Growth and development
Milestones
Growth charts
Neonatal physiology
Neonatal resuscitation
Low birth weight
Prematurity
Neonatal jaundice
Neonatal sepsis
Birth asphyxia
Infant nutrition
Breastfeeding
Complementary feeding
PEM
Vitamin deficiencies
Immunization schedule
Vaccine storage
Common childhood infections
ARI
Diarrheal diseases
Tuberculosis in children
Pediatric HIV
Pediatric anemia
Thalassemia
Pediatric leukemia
Bleeding disorders child
Congenital heart disease
Cyanotic CHD
Acyanotic CHD
Rheumatic heart disease
Pediatric asthma
Wheezing disorders
Pediatric pneumonia
Bronchiolitis
Pediatric epilepsy
Febrile seizures
Developmental delay
Cerebral palsy
Pediatric meningitis
Encephalitis
Pediatric nephrotic syndrome
Pediatric UTI
Pediatric GI bleeding
Malabsorption
Celiac disease
Pediatric liver disease
Pediatric diabetes
Inborn errors metabolism
Genetic syndromes
Dysmorphic child
Pediatric endocrinology
Growth hormone disorders
Puberty disorders
Pediatric emergencies
Shock in children
Poisoning in children
Snake bite children
Adolescent health
Behavioral problems
ADHD
Autism spectrum disorder
Child abuse
Pediatric dermatology
Pediatric rheumatology
Pediatric oncology
Pediatric surgery basics
Neonatal screening
Case-based pediatrics
Integrated pediatric care
Preventive pediatrics

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
// CLAIM ROWS (PEDIATRICS ONLY)
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
    .eq("subject", "Pediatrics")
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
  console.log(`🧠 PEDIATRICS MCQ TOPIC CLASSIFIER STARTED | ${WORKER_ID}`);

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
