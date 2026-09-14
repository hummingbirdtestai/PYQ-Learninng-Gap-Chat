require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────

const MODEL =
  process.env.NEETPG_PYT_MODEL ||
  "gpt-5-mini";

const LIMIT = parseInt(
  process.env.NEETPG_PYT_LIMIT || "10",
  10
);

const BATCH_SIZE = parseInt(
  process.env.NEETPG_PYT_BATCH_SIZE || "5",
  10
);

const SLEEP_MS = parseInt(
  process.env.NEETPG_PYT_LOOP_SLEEP_MS || "300",
  10
);

const LOCK_TTL_MIN = parseInt(
  process.env.NEETPG_PYT_LOCK_TTL_MIN || "30",
  10
);

const TABLE = "neetpg_pyt_source";

const INPUT_COL = "pyq_content";
const OUTPUT_COL = "jsonb_output";

const LOCK_COL = "generation_lock";
const LOCK_AT = "generation_locked_at";

console.log(
  "🚀 NEETPG PYT FLASHCARD WORKER STARTED"
);

console.log(
  `⚙️ Model=${MODEL} | Pickup=${LIMIT} | Concurrent=${BATCH_SIZE}`
);

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// Paste your supplied prompt inside backticks.
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`

Export response as a Word file
Export response as a PDF file

NEET-PG PYQ TO CLINICAL FLASHCARD GENERATOR ROLE, PURPOSE & STANDARD Act as an elite Indian postgraduate medical-exam educator for NEET-PG. Convert supplied PYQs from any of the 19 NEETPG subjects into ultra-dense active-recall flashcards. Target only postgraduate licensing-exam depth. Prioritise discriminating, repeatedly tested, decision-changing facts—not definitions, superficial symptoms, generic epidemiology, or basic textbook recall. REQUIRED OUTPUT Output only under these two headings: Unique NEETPG PYQ Flashcards Future High-Yield NEETPG Flashcards Use only this format: Q → [Clinical vignette, maximum 30 words] A → [Exactly 1–3 words] [NEETPG Year] For future predictions, append exactly: [Future MCQ] Do not include MCQ options, explanations, rationale, references, summaries, tables, introductions, conclusions, apologies, or extra teaching notes. PYQ CONVERSION RULES Treat every supplied PYQ as a mandatory source record. Do not omit any unique examinable concept. Remove only genuine conceptual duplicates. When duplicates have different exam tags, retain one best clinical variant and append all supplied tags exactly as given. Perform a silent source audit before finalising: Unique retained concepts + merged duplicate groups must account for every supplied PYQ. Preserve every supplied NEETPG tag exactly as written. Retain the source answer-key concept. If an old PYQ tests a historically accepted but now obsolete test, terminology, classification, or legal provision, preserve it only as a tagged PYQ; never reproduce it in Future MCQs. If a supplied answer key conflicts with current accepted science, retain the tagged historical concept only when clearly exam-relevant; do not propagate the outdated fact into predictions. VIGNETTE CONSTRUCTION RULES Every question must be a maximum of 30 words, excluding the exam tag. Every question must contain: age and sex; a clinical, procedural, autopsy, laboratory, anatomical, or epidemiological context; one objective finding: laboratory value, imaging, biopsy, microscopy, ECG, operative finding, autopsy finding, or statutory fact; one additional discriminating clue: timing, anatomical site, exposure, risk factor, morphology, physical sign, or clinical progression. Use clinically realistic facts only. Never invent a laboratory value, investigation, age, duration, dose, morphology, or buzzword merely to make a stem sound advanced. Prefer exact measurable data over vague wording. Use precise thresholds, timings, dosages, stages, gestational ages, legal periods, scores, and units whenever they are examinable. Every stem must produce one unequivocally correct answer. If more than one answer is possible, add a discriminating detail until only one answer remains. Never reveal the answer in the stem. Do not include the diagnosis, eponym, drug name, gene, direct synonym, or defining label that makes the answer obvious. Use buzzwords only when they genuinely discriminate the answer. Do not add decorative, irrelevant, misleading, or artificial clues. Do not call a single isolated sign, test, or finding “diagnostic,” “confirmatory,” or “gold standard” unless this is genuinely accepted in current standard references. Avoid negative stems. Use EXCEPT, NOT, false, or least likely only when unavoidable; capitalise the negative word and test only one negative concept. ANSWER RULES Every answer must contain exactly 1–3 words. The answer must be one singular, specific entity only: diagnosis, drug, antidote, investigation, mutation, anatomical structure, named sign, numerical metric, mechanism, or management step. Answers must contain no explanation, punctuation, brackets, slash, “and,” “or,” alternatives, qualifiers, or conversational wording. Never use non-specific answers such as “No,” “Supportive care,” “Further evaluation,” “Depends,” or “Clinical correlation.” FUTURE HIGH-YIELD MCQ RULES Generate only highly testable, specific, postgraduate-level future facts. Do not repeat or merely paraphrase a PYQ. Future cards must test a higher-order adjacent concept, such as: next best management step; definitive treatment; drug of choice; antidote; investigation of choice; confirmatory or gold-standard test; exact diagnostic cutoff; staging criterion; contraindication; adverse drug effect; drug mechanism; gene mutation; immunohistochemical marker; pathognomonic microscopy; named sign; complication; legal timeline; difficult differential discriminator. Do not generate simple diagnosis cards unless the diagnosis requires a rare morphology, named sign, gene mutation, pathognomonic histology, or difficult discriminating differential. For drug questions, specify sufficient context to distinguish first-line therapy, rescue therapy, prophylaxis, antidote, definitive therapy, and drug of choice. For investigation questions, distinguish clearly between screening test, initial investigation, investigation of choice, confirmatory test, and gold standard. Never use these terms interchangeably. For calculation-based facts, independently validate formula, units, timing, cutoff, numerator, denominator, and age adjustment before generating the card. CURRENT-STANDARD ACCURACY RULES Use only current accepted standards for Future MCQs: Medicine: Harrison and current specialty guidelines. Pathology: Robbins, WHO classifications, current TNM. Surgery: Bailey & Love and current guideline-based management. Obstetrics/Gynaecology: current ACOG, RCOG, FIGO, WHO guidance. Paediatrics: current AAP and WHO guidance. Psychiatry: DSM-5-TR. Forensic/PSM: BNS, BNSS, BSA, and current Indian public-health guidance. Pharmacology: current standard drug recommendations and toxicity profiles. Never create conflicting answers across flashcards. If classical and updated criteria differ, use the current standard in Future MCQs. Exclude controversial, weakly evidenced, region-specific, obsolete, or inconsistently sourced facts from Future MCQs. SUBJECT-ADAPTATION RULE Adapt the tested discriminator to the subject: Anatomy: nerve lesion, artery, embryological derivative, compartment, imaging anatomy. Physiology: mechanism, graph, receptor, clearance, acid-base or haemodynamic calculation. Biochemistry: enzyme defect, cofactor, inheritance, metabolite accumulation. Pathology: morphology, mutation, immunohistochemistry, prognostic marker. Pharmacology: DOC, antidote, toxicity, mechanism, interaction. Microbiology: diagnostic test, culture medium, virulence factor, prophylaxis. PSM: programme target, vaccine schedule, indicator, outbreak action. Forensic: injury interpretation, toxicology, postmortem finding, statutory provision. Clinical subjects: next step, investigation, treatment, complication, staging, differential. FINAL SILENT QUALITY CHECK Before output, verify that: every unique PYQ is represented once; every question is ≤30 words; every question has two discriminating clues; every answer is exactly 1–3 words; every answer is singular and punctuation-free; no stem leaks its answer; no two flashcards conflict; every supplied tag is retained exactly; every Future MCQ is specific, non-generic, current, and non-repetitive. dont mention (Future MCQ) give serial number to each Q ---> A seperately starting from 1 for each of the two Sections The Subject and Topic/PYT are supplied externally with the PYQ content. Never infer, rename, shorten, expand, reclassify, merge, split, or create a topic from the PYQ content. Every supplied PYQ belongs to the supplied Topic/PYT and must remain under that exact Topic/PYT. Process only the PYQs inside the supplied topic. Do not move a PYQ to another topic even if it appears medically related to another topic. The worker's task is to generate flashcards from the supplied PYQs, NOT to classify the PYQs into topics. JSON OUTPUT FORMAT Return only valid JSON. Do not include markdown fences, headings, explanations, comments, introductions, conclusions, or any text outside the JSON. The Subject, Topic Serial Number, Topic/PYT and Number of Times Asked are supplied only as reference context. Do NOT include them in the JSON output. Use this exact structure: { "cards": [ { "card_type": "PYQ", "question": "string", "answer": "string", "neetpg_years": ["2015", "2012"] }, { "card_type": "FUTURE", "question": "string", "answer": "string", "neetpg_years": [] } ] }

`.trim();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT ===
    "PASTE YOUR COMPLETE SYSTEM PROMPT HERE"
) {
  throw new Error(
    "Paste the complete system prompt into SYSTEM_PROMPT"
  );
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────

const sleep = (ms) =>
  new Promise((resolve) =>
    setTimeout(resolve, ms)
  );

function isRetryable(error) {
  const status = Number(error?.status);

  return (
    status === 408 ||
    status === 409 ||
    status === 429 ||
    status >= 500 ||
    /timeout|temporar|unavailable|ECONNRESET|ETIMEDOUT/i.test(
      String(error?.message || error)
    )
  );
}

function countWords(value) {
  return String(value)
    .trim()
    .split(/\s+/u)
    .filter(Boolean)
    .length;
}

// ─────────────────────────────────────────────
// BUILD USER INPUT
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    "REFERENCE CONTEXT:",
    `Subject: ${row.subject}`,
    `Topic Serial Number: ${row.serial_number}`,
    `Exact Topic/PYT: ${row.topic}`,
    `Number of Times Asked: ${row.number_of_times_asked}`,
    "",
    "PYQ CONTENT:",
    row.pyq_content
  ].join("\n");
}

// ─────────────────────────────────────────────
// CALL OPENAI
// ─────────────────────────────────────────────

async function callOpenAI(
  row,
  attempt = 1
) {
  try {
    const response =
      await openai.chat.completions.create({
        model: MODEL,

        response_format: {
          type: "json_object"
        },

        messages: [
          {
            role: "system",
            content: SYSTEM_PROMPT
          },
          {
            role: "user",
            content: buildUserInput(row)
          }
        ]
      });

    const output =
      response.choices?.[0]?.message
        ?.content?.trim();

    if (!output) {
      throw new Error(
        "OpenAI returned an empty response"
      );
    }

    return output;
  } catch (error) {
    if (
      isRetryable(error) &&
      attempt <= 2
    ) {
      const delay =
        1000 * attempt;

      console.warn(
        `⚠️ OpenAI retry ${attempt}/2 after ${delay} ms`
      );

      await sleep(delay);

      return callOpenAI(
        row,
        attempt + 1
      );
    }

    throw error;
  }
}

// ─────────────────────────────────────────────
// PARSE AND VALIDATE JSON
// ─────────────────────────────────────────────

function parseGeneratedOutput(rawOutput) {
  const cleaned = rawOutput
    .trim()
    .replace(
      /^\s*```(?:json)?\s*/i,
      ""
    )
    .replace(
      /\s*```\s*$/i,
      ""
    )
    .trim();

  let parsed;

  try {
    parsed = JSON.parse(cleaned);
  } catch (error) {
    throw new Error(
      `Model returned invalid JSON: ${error.message}`
    );
  }

  if (
    !parsed ||
    typeof parsed !== "object" ||
    Array.isArray(parsed)
  ) {
    throw new Error(
      "Output must be one JSON object"
    );
  }

  if (
    !Array.isArray(parsed.cards) ||
    parsed.cards.length === 0
  ) {
    throw new Error(
      "Output must contain a non-empty cards array"
    );
  }

  let pyqCount = 0;
  let futureCount = 0;

  const normalizedCards =
    parsed.cards.map(
      (card, index) => {
        if (
          !card ||
          typeof card !== "object" ||
          Array.isArray(card)
        ) {
          throw new Error(
            `Card ${index + 1} must be an object`
          );
        }

        const cardType = String(
          card.card_type || ""
        )
          .trim()
          .toUpperCase();

        const question = String(
          card.question || ""
        ).trim();

        const answer = String(
          card.answer || ""
        ).trim();

        const neetpgYears =
          card.neetpg_years;

        if (
          !["PYQ", "FUTURE"].includes(
            cardType
          )
        ) {
          throw new Error(
            `Card ${index + 1} has invalid card_type`
          );
        }

        if (!question) {
          throw new Error(
            `Card ${index + 1} has no question`
          );
        }

        if (!answer) {
          throw new Error(
            `Card ${index + 1} has no answer`
          );
        }

        if (
          countWords(question) > 30
        ) {
          throw new Error(
            `Card ${index + 1} question exceeds 30 words`
          );
        }

        const answerWordCount =
          countWords(answer);

        if (
          answerWordCount < 1 ||
          answerWordCount > 3
        ) {
          throw new Error(
            `Card ${index + 1} answer must contain 1–3 words`
          );
        }

        if (
          !Array.isArray(neetpgYears)
        ) {
          throw new Error(
            `Card ${index + 1} neetpg_years must be an array`
          );
        }

        const normalizedYears =
          neetpgYears.map((year) =>
            String(year).trim()
          );

        if (
          normalizedYears.some(
            (year) => !year
          )
        ) {
          throw new Error(
            `Card ${index + 1} contains an empty NEETPG year`
          );
        }

        if (
          cardType === "PYQ" &&
          normalizedYears.length === 0
        ) {
          throw new Error(
            `PYQ card ${index + 1} has no NEETPG year`
          );
        }

        if (
          cardType === "FUTURE" &&
          normalizedYears.length !== 0
        ) {
          throw new Error(
            `FUTURE card ${index + 1} must have an empty neetpg_years array`
          );
        }

        if (cardType === "PYQ") {
          pyqCount += 1;
        }

        if (cardType === "FUTURE") {
          futureCount += 1;
        }

        return {
          card_type: cardType,
          question,
          answer,
          neetpg_years:
            normalizedYears
        };
      }
    );

  if (pyqCount === 0) {
    throw new Error(
      "Output contains no PYQ cards"
    );
  }

  if (futureCount === 0) {
    throw new Error(
      "Output contains no FUTURE cards"
    );
  }

  return {
    jsonOutput: {
      cards: normalizedCards
    },
    totalCards:
      normalizedCards.length,
    pyqCount,
    futureCount
  };
}

// ─────────────────────────────────────────────
// RELEASE EXPIRED LOCKS
// ─────────────────────────────────────────────

async function releaseExpiredLocks() {
  const cutoff = new Date(
    Date.now() -
      LOCK_TTL_MIN * 60 * 1000
  ).toISOString();

  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT]: null
    })
    .eq(LOCK_COL, true)
    .lt(LOCK_AT, cutoff)
    .is(OUTPUT_COL, null);

  if (error) {
    throw new Error(
      `Failed to release expired locks: ${error.message}`
    );
  }
}

// ─────────────────────────────────────────────
// CLAIM ROWS
// No RPC required.
// ─────────────────────────────────────────────

async function claimRows(limit) {
  await releaseExpiredLocks();

  const {
    data: availableRows,
    error: selectError
  } = await supabase
    .from(TABLE)
    .select(`
      id,
      subject,
      serial_number,
      topic,
      number_of_times_asked,
      pyq_content
    `)
    .not("topic", "is", null)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .eq(LOCK_COL, false)
    .order(
      "subject",
      { ascending: true }
    )
    .order(
      "serial_number",
      { ascending: true }
    )
    .limit(limit);

  if (selectError) {
    throw new Error(
      `Failed to select rows: ${selectError.message}`
    );
  }

  if (!availableRows?.length) {
    return [];
  }

  const rowIds =
    availableRows.map(
      (row) => row.id
    );

  const lockedAt =
    new Date().toISOString();

  const {
    data: lockedRows,
    error: lockError
  } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: true,
      [LOCK_AT]: lockedAt
    })
    .in("id", rowIds)
    .eq(LOCK_COL, false)
    .is(OUTPUT_COL, null)
    .select(`
      id,
      subject,
      serial_number,
      topic,
      number_of_times_asked,
      pyq_content
    `);

  if (lockError) {
    throw new Error(
      `Failed to lock rows: ${lockError.message}`
    );
  }

  return lockedRows || [];
}

// ─────────────────────────────────────────────
// RELEASE FAILED ROW
// ─────────────────────────────────────────────

async function releaseFailedRow(rowId) {
  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT]: null
    })
    .eq("id", rowId)
    .eq(LOCK_COL, true)
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `❌ Failed to release row ${rowId}:`,
      error.message
    );
  }
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────

async function processRow(row) {
  try {
    console.log(
      `🧠 Generating | ${row.subject} | ${row.serial_number} | ${row.topic}`
    );

    const rawOutput =
      await callOpenAI(row);

    const {
      jsonOutput,
      totalCards,
      pyqCount,
      futureCount
    } = parseGeneratedOutput(
      rawOutput
    );

    const {
      data: savedRows,
      error: updateError
    } = await supabase
      .from(TABLE)
      .update({
        [OUTPUT_COL]: jsonOutput,
        [LOCK_COL]: false,
        [LOCK_AT]: null
      })
      .eq("id", row.id)
      .eq(LOCK_COL, true)
      .is(OUTPUT_COL, null)
      .select("id");

    if (updateError) {
      throw new Error(
        `Failed to save JSON output: ${updateError.message}`
      );
    }

    if (!savedRows?.length) {
      throw new Error(
        "Output was not saved because the lock changed"
      );
    }

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | Total=${totalCards} | PYQ=${pyqCount} | Future=${futureCount}`
    );
  } catch (error) {
    console.error(
      `❌ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}:`,
      error?.message || error
    );

    await releaseFailedRow(
      row.id
    );
  }
}

// ─────────────────────────────────────────────
// PROCESS ROWS IN BATCHES
// ─────────────────────────────────────────────

async function processRowsInBatches(
  rows
) {
  for (
    let index = 0;
    index < rows.length;
    index += BATCH_SIZE
  ) {
    const batch = rows.slice(
      index,
      index + BATCH_SIZE
    );

    await Promise.allSettled(
      batch.map(
        (row) => processRow(row)
      )
    );
  }
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────

(async function main() {
  console.log(
    "🧠 NEETPG PYT FLASHCARD WORKER RUNNING"
  );

  while (true) {
    try {
      const rows =
        await claimRows(LIMIT);

      if (!rows.length) {
        await sleep(SLEEP_MS);
        continue;
      }

      console.log(
        `📥 Claimed ${rows.length} topic(s)`
      );

      await processRowsInBatches(
        rows
      );
    } catch (error) {
      console.error(
        "❌ Worker loop error:",
        error?.message || error
      );

      await sleep(
        Math.max(
          SLEEP_MS,
          2000
        )
      );
    }
  }
})();
