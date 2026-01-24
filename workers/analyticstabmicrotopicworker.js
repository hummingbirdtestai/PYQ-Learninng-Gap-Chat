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
  `mcq-bio-topic-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — DO NOT TOUCH)
// ─────────────────────────────────────────────
function buildPrompt(mcqText) {
  return `
You classify NEET-PG Biochemistry PYQ MCQs.

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

Cell membrane
Transport mechanisms
Carbohydrate digestion
Glycolysis
TCA cycle
Electron transport chain
Oxidative phosphorylation
Glycogen metabolism
Gluconeogenesis
HMP shunt
Fructose metabolism
Galactose metabolism
Diabetes mellitus
Hypoglycemia
Lipid digestion
Fatty acid oxidation
Fatty acid synthesis
Ketone bodies
Cholesterol metabolism
Lipoproteins
Atherosclerosis
Protein digestion
Amino acid metabolism
Transamination
Deamination
Urea cycle
Inborn errors metabolism
Phenylketonuria
Maple syrup urine disease
Porphyria
Heme synthesis
Heme degradation
Hemoglobin structure
Oxygen dissociation curve
Acid–base balance
Enzymes
Enzyme kinetics
Enzyme inhibition
Vitamins
Fat-soluble vitamins
Water-soluble vitamins
Vitamin deficiencies
Minerals
Calcium metabolism
Phosphate metabolism
Iron metabolism
Copper metabolism
Zinc metabolism
Free radicals
Antioxidants
Reactive oxygen species
DNA structure
RNA structure
DNA replication
DNA repair
Transcription
Translation
Genetic code
Mutations
Epigenetics
Recombinant DNA
PCR
Blotting techniques
Chromatography
Spectrophotometry
Electrophoresis
Clinical enzymes
Liver function tests
Renal function tests
Thyroid function tests
Cardiac markers
Tumor markers
Plasma proteins
Acute phase reactants
Immunoglobulins
Complement proteins
Blood buffers
Detoxification reactions
Cytochrome P450
Xenobiotics
Nutrition
Balanced diet
PEM
Obesity
Starvation
Metabolic syndrome
Hormone receptors
Second messengers
Signal transduction
Nitric oxide
Eicosanoids
Prostaglandins
Leukotrienes
Nitrogen balance
Glycoproteins
Proteoglycans
Collagen synthesis
Elastin
Lab errors
Case-based biochemistry

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
// CLAIM ROWS (BIOCHEMISTRY ONLY)
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
    .eq("subject", "Biochemistry")
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
  console.log(`🧠 BIOCHEMISTRY MCQ TOPIC CLASSIFIER STARTED | ${WORKER_ID}`);

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
