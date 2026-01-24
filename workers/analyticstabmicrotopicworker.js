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
  `mcq-micro-topic-${process.pid}-${Math.random().toString(36).slice(2,6)}`;

// ─────────────────────────────────────────────
// PROMPT (USE AS-IS — DO NOT TOUCH)
// ─────────────────────────────────────────────
function buildPrompt(mcqText) {
  return `
You classify NEET-PG Microbiology PYQ MCQs.

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

ALLOWED TOPICS (ONLY THESE 150):

Louis Pasteur
Koch’s postulates
Sterilization methods
Disinfection levels
Antiseptics
Culture media
Enriched media
Transport media
Anaerobic culture
Bacterial spores
Bacterial toxins
Endotoxins vs exotoxins
Normal flora
Biofilms
Nosocomial infections
Innate immunity
Adaptive immunity
Antigen structure
Antibody structure
IgG
IgA
IgM
IgE
Complement system
Hypersensitivity reactions
Autoimmunity
Immunodeficiency disorders
Cytokines
Interferons
Vaccines
Staphylococcus aureus
Coagulase-negative staphylococci
MRSA
Streptococcus pyogenes
Streptococcus pneumoniae
Viridans streptococci
Enterococcus
Corynebacterium diphtheriae
Listeria monocytogenes
Bacillus anthracis
Bacillus cereus
Clostridium perfringens
Clostridium tetani
Clostridium botulinum
Actinomycosis
Nocardia
Neisseria gonorrhoeae
Neisseria meningitidis
Meningococcal vaccine
Escherichia coli
Klebsiella pneumoniae
Proteus
Salmonella typhi
Shigella
Vibrio cholerae
Vibrio parahaemolyticus
Campylobacter jejuni
Helicobacter pylori
Pseudomonas aeruginosa
Legionella
Yersinia pestis (Plague)
Bordetella pertussis
Haemophilus influenzae
Mycoplasma
Chlamydia trachomatis
Chlamydia pneumoniae
Lymphogranuloma venereum
Granuloma inguinale
Rickettsial diseases
Coxiella burnetii
Leptospira
Borrelia
Mycobacterium tuberculosis
Atypical mycobacteria
Tuberculin test
IGRA
GeneXpert
Herpes simplex virus
Varicella zoster virus
Cytomegalovirus
Epstein–Barr virus
Poxvirus
Parvovirus
Hepatitis B
Hepatitis A
Hepatitis C
Poliovirus
Rabies virus
Influenza virus
Dengue virus
Japanese encephalitis
Rotavirus
HIV
Candida albicans
Oral candidiasis
Aspergillus
Cryptococcus neoformans
Mucormycosis
Pneumocystis jirovecii
Entamoeba histolytica
Amoebiasis
Giardia lamblia
Giardiasis
Trichomonas vaginalis
Plasmodium falciparum
Plasmodium malariae
Malaria
Toxoplasma gondii
Toxoplasmosis
Cryptosporidium
Leishmania
Kala-azar
Ascaris lumbricoides
Hookworm
Ancylostoma braziliense
Strongyloides stercoralis
Trichuris trichiura
Taenia solium
Taenia saginata
Echinococcus granulosus
Hydatid cyst
Schistosoma haematobium
Schistosomiasis
Wuchereria bancrofti
Microscopy
Staining methods
Acid-fast staining
Culture techniques
Serological tests
ELISA
Complement fixation test
Precipitation test
Agglutination test
PCR
Diarrheal diseases
Sexually transmitted infections
CNS infections
Respiratory infections
Opportunistic infections
Zoonotic diseases
Food poisoning
Scombroid poisoning
Catheter-associated UTI
Antibiotic resistance
ESBL
Carbapenem resistance
Biosecurity
Emerging infections
Pandemic pathogens
Image-based microbiology

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
// CLAIM ROWS (MICROBIOLOGY ONLY)
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
    .eq("subject", "Microbiology")
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
  let topic = await callOpenAI(buildPrompt(row.mcq));

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
  console.log(`🧠 MICROBIOLOGY MCQ TOPIC CLASSIFIER STARTED | ${WORKER_ID}`);

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
