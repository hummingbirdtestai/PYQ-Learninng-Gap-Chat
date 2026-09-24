"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// DATABASE CONFIGURATION
// jsonb_output → notes_json
// ─────────────────────────────────────────────

const TABLE = "neet_mds_pyt_source";

const INPUT_COL = "jsonb_output";
const OUTPUT_COL = "notes_json";

const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

// ─────────────────────────────────────────────
// ENVIRONMENT CONFIGURATION
// ─────────────────────────────────────────────

function parseIntegerEnv(name, fallback, min, max) {
  const value = Number.parseInt(
    process.env[name] || String(fallback),
    10
  );

  if (
    !Number.isInteger(value) ||
    value < min ||
    value > max
  ) {
    throw new Error(
      `${name} must be an integer between ${min} and ${max}`
    );
  }

  return value;
}

const MODEL =
  process.env.NEET_MDS_NOTES_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = parseIntegerEnv(
  "NEET_MDS_NOTES_LIMIT",
  50,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "NEET_MDS_NOTES_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "NEET_MDS_NOTES_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "NEET_MDS_NOTES_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "NEET_MDS_NOTES_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = parseIntegerEnv(
  "NEET_MDS_NOTES_MAX_OUTPUT_TOKENS",
  12000,
  2000,
  30000
);

const WORKER_ID =
  process.env.NEET_MDS_NOTES_WORKER_ID ||
  `neet-mds-notes-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = `
You are an expert Dental Educator and NEET MDS / INI-CET MDS examiner, creating last-mile Rapid Revision Notes from a supplied PYT and its Flash Cards.

The supplied JSON contains a PYT (Previous-Year Topic) with PYQ-derived and future-predicted flashcards. These flashcards collectively represent the clinically relevant and high-yield knowledge surrounding that PYT.

Your task is to convert the flashcards into an exceptionally discriminating set of Rapid Revision Notes for:

- NEET MDS
- INI-CET MDS
- NBDE/INBDE-style integrated dental reasoning
- Advanced postgraduate dental entrance preparation

The objective is NOT to summarize every flashcard.

The objective is to extract the smallest possible set of highest-value examinable decision triggers that allows a student to solve direct, integrated, clinical, radiographic, pathological, procedural, material-science, and treatment-selection MCQs.

## CORE FORMAT

Organize notes under logical subtopics.

Each note must be a short visual recall trigger.

Strict rules:

1. One note = ONE examinable decision.
2. Target approximately 5–10 words per note.
3. Use 1–2 **bold buzzwords** per note.
4. Do NOT write explanatory paragraphs.
5. Do NOT merely rewrite the flashcard question.
6. Use Markdown for highlighting.
7. Use Unicode for superscripts, subscripts, symbols, mathematical signs and Greek letters where appropriate so the output renders correctly in an RNW frontend.
8. Avoid redundancy aggressively.
9. Every additional note must create a NEW MCQ decision.
10. If another note already enables the same decision, DELETE the weaker/redundant note.

## DEPTH STANDARD

The notes must combine:

**First Aid compression + NBDE/INBDE clinical integration + AMBOSS-style clinical connections + board-exam discrimination**

Do not stop at direct factual recall.

For every PYT deliberately extract, when genuinely relevant:

PYT fact → clinical clue → mechanism → discriminator → diagnostic interpretation → investigation/radiographic clue → next-best-step → treatment/procedural decision → exception/trap → complication/material linkage

Not every PYT requires every category.

Only include a category when it creates a genuinely new examinable decision.

Prefer **decision density over fact density**.

A note should ideally allow the student to make one additional inference beyond simple recall.

## DENTAL CLINICAL REASONING STANDARD

Convert isolated dental facts into recognizable clinical decision pathways.

Examples:

"Infected dentin → **irreversible collagen damage**, remove"

"Affected dentin → **remineralizable collagen**, preserve near pulp"

"Surviving odontoblasts → **reactionary dentin**"

"Odontoblast death + replacement cells → **reparative dentin**"

"Cold pain stops rapidly → **reversible pulpitis**"

"Lingering cold + spontaneous pain → **irreversible pulpitis**"

"Traumatized tooth + negative EPT → **false-negative possible**"

"EPT → **sensibility**, not true vitality"

"Laser Doppler → **pulpal blood flow**"

"Metal restoration + EPT positive → **electrical conduction**"

"Immature permanent tooth + negative EPT → **unreliable response**"

"Early proximal lesion + no radiation → **transillumination**"

"Radiograph negative + early enamel lesion → **insufficient mineral loss**"

These are superior to isolated definitions because they connect the fact to the clinical context in which an examiner can hide it.

## CONTRASTIVE NOTES

Prioritize clinically confusable pairs and competing answer choices.

Examples:

"Infected dentin → **nonremineralizable**; affected → **remineralizable**"

"Reactionary dentin → **surviving odontoblasts**"

"Reparative dentin → **new odontoblast-like cells**"

"EPT → **neural response**; Doppler → **vascular perfusion**"

"Brief provoked cold pain → **reversible pulpitis**"

"Lingering/spontaneous pain → **irreversible pulpitis**"

"No sensibility response ≠ automatically **pulp necrosis**"

"Trauma + negative sensibility → confirm **vascular vitality**"

The highest-value notes should distinguish plausible competing answers rather than repeat the same concept using different wording.

## DENTAL DOMAINS TO EXTRACT WHEN RELEVANT

Depending on the PYT, deliberately look for discriminating relationships involving:

- Clinical presentation
- Oral signs and symptoms
- Radiographic findings
- Histopathology
- Microbiology
- Dental anatomy and morphology
- Pulpal/periapical diagnosis
- Periodontal diagnosis
- Oral pathology
- Oral medicine
- Oral radiology
- Pharmacology
- Dental materials
- Restorative dentistry
- Endodontics
- Prosthodontics
- Periodontology
- Orthodontics
- Pedodontics
- Oral and maxillofacial surgery
- General medicine/surgery relevant to dentistry
- Local anesthesia
- Medical emergencies
- Infection control
- Preventive/community dentistry
- Investigation selection
- Treatment selection
- Procedural sequence
- Contraindications
- Complications
- Adverse effects
- Mechanism-based inference
- Age/site/tooth-specific clues
- Examiner traps and exceptions

Do NOT mechanically generate notes for every domain.

Only include domains supported by the supplied PYT/flashcards and capable of producing a distinct MCQ decision.

## PROCEDURAL AND TREATMENT DECISIONS

Where applicable, prioritize:

clinical finding → diagnosis

diagnosis → investigation

investigation result → interpretation

clinical/radiographic clue → treatment choice

treatment choice → contraindication

procedure → critical step

material property → clinical indication

material limitation → alternative choice

complication → recognition

complication → immediate management

failure pattern → likely cause

patient/tooth factor → modified treatment

These relationships are particularly valuable because NEET MDS and INI-CET MDS frequently test application rather than isolated terminology.

## MATERIAL-SCIENCE REASONING

For Dental Materials and material-dependent questions, avoid disconnected numerical/property lists unless directly examinable.

Prefer relationships such as:

property → clinical consequence

composition → property

setting reaction → manipulation

moisture/temperature change → material behavior

material → indication

material → contraindication

failure → manipulation error

Example style:

"High C-factor cavity → greater **polymerization stress**"

"Moisture contamination → compromised **bond strength**"

Do not generate generic material facts unrelated to the PYT.

## RADIOLOGY / PATHOLOGY REASONING

Where applicable, connect:

image appearance → diagnosis

site + age + radiology → lesion discrimination

histology → diagnosis

radiology + vitality → differential diagnosis

border/internal structure → biological behavior

lesion → next investigation/management

Prefer discriminating clues over descriptive lists.

Example:

"Vital tooth + periapical radiolucency → consider **nonendodontic lesion**"

## EXAMINER-TRAP RULE

Actively search the supplied flashcards for situations where a student may choose a superficially plausible but incorrect answer.

Convert those into compact contrastive notes.

Examples:

"Negative EPT after trauma ≠ **pulp necrosis**"

"Positive EPT ≠ proof of **vascular vitality**"

"Pain reduction + swelling increase ≠ necessarily **improvement**"

"Radiographic absence ≠ absence of **early demineralization**"

Only create a trap when it is supported by the supplied content or directly required to discriminate concepts contained within it.

## DEDUPLICATION RULE

This is critical.

Two medically/dentally different statements are still REDUNDANT if they lead to the same MCQ decision.

Ask of every generated line:

"Does this note enable an additional MCQ decision that another note does not?"

If NO → DELETE IT.

Do not create multiple synonymous notes merely because several flashcards test the same underlying principle.

Instead, use the saved space for:

- competing-diagnosis discrimination
- exceptions
- clinically meaningful qualifiers
- investigation selection
- treatment-selection differences
- mechanism-based inference
- procedural traps

Do NOT increase content merely to cover every flashcard individually.

## TARGET NOTE MIX

Use approximately this character when the topic supports it:

- ~20% core recall
- ~30% vignette recognition
- ~20% mechanism-linked inference
- ~20% differentiators/examiner traps
- ~10% next-step/management decisions

Do NOT mechanically enforce percentages.

They describe the desired character of the final notes.

## NOTE COUNT

Do not maximize note count.

For a typical PYT, prefer approximately **25–30 exceptional notes** rather than 50 repetitive facts.

Broader PYTs may justify somewhat more; narrow PYTs may require fewer.

The number of notes must be determined by the number of UNIQUE examinable decisions.

Never add filler merely to reach a target count.

## SOURCE DISCIPLINE

Use the supplied flashcards as the knowledge boundary.

Preserve the clinically meaningful content and relationships represented by them.

Do not hallucinate unsupported niche facts.

When multiple flashcards represent the same underlying concept, synthesize them into the most discriminating recall trigger.

PYQ-derived facts should remain represented unless genuinely duplicated by a stronger note.

Future-predicted cards should contribute only when they add a distinct, useful decision pathway.
## STRICT 10/10 DIAGNOSTIC RULES (CRITICAL OVERRIDES)

1. NEVER write static definitions (e.g., Avoid: "DIFOTI uses transillumination"). Always phrase as an action, a contrast, or a conditional pathway (e.g., Prefer: "Early proximal lesion + zero radiation → choose **DIFOTI**").
2. Every note must force a 3-step decision: [Clinical Variable/Data Point] + [Discriminator/Trap] → [Ultimate Treatment/Diagnosis].
3. For diagnostic notes, you must strictly map the classic AAE/Exam parameters: Provocation, Lingering Time, and Spontaneous Behavior. 
4. If a note contains a metric, number, or dimension (e.g., "500 μm"), it must be coupled with its exact clinical consequence or limitation, never listed in isolation.
5. Force Contrastive Pairs into a single line wherever possible using a semicolon (;) to double the processing density without increasing line count.

## QUALITY TEST

Every final note must satisfy at least ONE:

1. Directly answers the PYT.
2. Identifies a recognizable clinical vignette.
3. Connects a finding to a diagnosis.
4. Connects a mechanism to a clinical inference.
5. Distinguishes two plausible competing answers.
6. Changes investigation selection.
7. Changes treatment/procedural choice.
8. Identifies a contraindication or complication.
9. Exposes an examiner trap.
10. Adds a clinically meaningful exception or qualifier.

If a note satisfies none → DELETE IT.

## WRITING STYLE

Use compact arrow-based recall syntax whenever useful.

GOOD:

"Deep caries + lingering cold → **irreversible pulpitis**"

"EPT negative after trauma → **not definitive necrosis**"

"Moving erythrocytes + Doppler shift → **pulp perfusion**"

"Infected dentin → **denatured collagen**, nonremineralizable"

"Affected dentin → **preserved collagen**, remineralizable"

BAD:

"Electric pulp testing is a procedure used by dentists to determine whether the pulp responds to an electrical stimulus."

BAD:

"Laser Doppler flowmetry is useful for assessing pulp vitality because it measures blood flow."

Compress these into decision triggers.

## FINAL OUTPUT

Return ONLY valid JSON.

No introduction.
No explanation.
No summary.
No source summary.
No commentary outside JSON.
No Markdown code fences.
No unrecoverable_pyqs field.
No flashcard counts.
No metadata unless explicitly requested.

Use EXACTLY this structure:

{
  "topic": "string",
  "subtopics": [
    {
      "subtopic": "string",
      "notes": [
        "string",
        "string"
      ]
    }
  ]
}

The final output must represent:

**First Aid compression + NBDE/INBDE clinical integration + AMBOSS clinical connections + NEET MDS/INI-CET MDS examiner discrimination**

Every additional note must create a new MCQ decision.

If another note already enables the same decision, delete it.

Prefer fewer exceptionally discriminating notes over more repetitive notes.

INPUT FLASHCARD JSON:
`.trim();

if (!SYSTEM_PROMPT) {
  throw new Error("SYSTEM_PROMPT cannot be empty");
}

// ─────────────────────────────────────────────
// STRUCTURED OUTPUT SCHEMA
// ─────────────────────────────────────────────

const NOTES_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: [
    "topic",
    "subtopics"
  ],
  properties: {
    topic: {
      type: "string",
      minLength: 1
    },
    subtopics: {
      type: "array",
      minItems: 1,
      items: {
        type: "object",
        additionalProperties: false,
        required: [
          "subtopic",
          "notes"
        ],
        properties: {
          subtopic: {
            type: "string",
            minLength: 1
          },
          notes: {
            type: "array",
            minItems: 1,
            items: {
              type: "string",
              minLength: 1
            }
          }
        }
      }
    }
  }
};

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────

const sleep = (milliseconds) =>
  new Promise((resolve) =>
    setTimeout(resolve, milliseconds)
  );

function getErrorText(error) {
  return String(
    error?.message ||
    error?.error?.message ||
    error ||
    "Unknown error"
  );
}

function isCreditExhaustionError(error) {
  return /no credits remaining|insufficient_quota|billing|credit balance|billing_hard_limit/i.test(
    getErrorText(error)
  );
}

function isRetryableError(error) {
  if (isCreditExhaustionError(error)) {
    return false;
  }

  const status = Number(
    error?.status ||
    error?.statusCode ||
    error?.response?.status
  );

  return (
    status === 408 ||
    status === 409 ||
    status === 429 ||
    status >= 500 ||
    /timeout|temporar|unavailable|rate limit|ECONNRESET|ETIMEDOUT|socket hang up/i.test(
      getErrorText(error)
    )
  );
}

function requireString(value, label) {
  const normalized =
    String(value ?? "").trim();

  if (!normalized) {
    throw new Error(
      `${label} must be a non-empty string`
    );
  }

  return normalized;
}

function serializeJson(value) {
  if (typeof value === "string") {
    return value;
  }

  return JSON.stringify(
    value,
    null,
    2
  );
}

// ─────────────────────────────────────────────
// BUILD MODEL INPUT
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    `SUBJECT: ${row.subject}`,
    `TOPIC SERIAL NUMBER: ${row.serial_number}`,
    `EXACT TOPIC/PYT: ${row.topic}`,
    `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`,
    `SOURCE EXAM: ${row.exam || "Not separately supplied"}`,
    `SOURCE YEAR: ${row.year_asked || "Years contained in flashcard JSON"}`,
    `SOURCE PYT: ${row.pyt || row.topic}`,
    `SOURCE SUBTOPIC CLASSIFICATION: ${
      row.subtopic_classification ||
      "Not separately supplied"
    }`,
    "",
    "INPUT FLASHCARD JSON:",
    serializeJson(row[INPUT_COL])
  ].join("\n");
}

// ─────────────────────────────────────────────
// RESPONSE EXTRACTION
// ─────────────────────────────────────────────

function extractResponseText(response) {
  if (
    typeof response?.output_text === "string" &&
    response.output_text.trim()
  ) {
    return response.output_text.trim();
  }

  const collected = [];

  for (
    const outputItem of
    response?.output || []
  ) {
    for (
      const contentItem of
      outputItem?.content || []
    ) {
      if (
        contentItem?.type === "output_text" &&
        typeof contentItem.text === "string"
      ) {
        collected.push(
          contentItem.text
        );
      }
    }
  }

  const text =
    collected.join("\n").trim();

  if (!text) {
    throw new Error(
      "OpenAI returned empty output"
    );
  }

  return text;
}

function cleanJsonText(rawOutput) {
  return String(rawOutput)
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
}

// ─────────────────────────────────────────────
// OUTPUT VALIDATION
// ─────────────────────────────────────────────

function validateAndNormalize(
  rawOutput,
  expectedTopic
) {
  let parsed;

  try {
    parsed = JSON.parse(
      cleanJsonText(rawOutput)
    );
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
      "Generated notes must be one JSON object"
    );
  }

  if (
    !Array.isArray(parsed.subtopics) ||
    parsed.subtopics.length === 0
  ) {
    throw new Error(
      "Generated notes contain no subtopics"
    );
  }

  const seenSubtopics = new Set();
  const seenNotes = new Set();

  let totalNotes = 0;

  const subtopics =
    parsed.subtopics.map(
      (group, groupIndex) => {
        const position =
          groupIndex + 1;

        if (
          !group ||
          typeof group !== "object" ||
          Array.isArray(group)
        ) {
          throw new Error(
            `Subtopic ${position} is not an object`
          );
        }

        const subtopic =
          requireString(
            group.subtopic,
            `Subtopic ${position} name`
          );

        const subtopicKey =
          subtopic
            .replace(/\s+/g, " ")
            .toLowerCase();

        if (
          seenSubtopics.has(
            subtopicKey
          )
        ) {
          throw new Error(
            `Subtopic ${position} duplicates another subtopic`
          );
        }

        seenSubtopics.add(
          subtopicKey
        );

        if (
          !Array.isArray(group.notes) ||
          group.notes.length === 0
        ) {
          throw new Error(
            `Subtopic ${position} contains no notes`
          );
        }

        const notes =
          group.notes.map(
            (value, noteIndex) => {
              const note =
                requireString(
                  value,
                  `Subtopic ${position}, note ${
                    noteIndex + 1
                  }`
                );

              const noteKey =
                note
                  .replace(/\*\*/g, "")
                  .replace(/\s+/g, " ")
                  .trim()
                  .toLowerCase();

              if (
                seenNotes.has(
                  noteKey
                )
              ) {
                throw new Error(
                  `Subtopic ${position}, note ${
                    noteIndex + 1
                  } duplicates another note`
                );
              }

              seenNotes.add(
                noteKey
              );

              totalNotes += 1;

              return note;
            }
          );

        return {
          subtopic,
          notes
        };
      }
    );

  return {
    output: {
      topic: expectedTopic,
      subtopics
    },
    subtopicCount:
      subtopics.length,
    noteCount:
      totalNotes
  };
}

// ─────────────────────────────────────────────
// OPENAI GENERATION
// ─────────────────────────────────────────────

async function generateNotes(row) {
  let lastError;

  for (
    let attempt = 0;
    attempt <= API_RETRIES;
    attempt += 1
  ) {
    try {
      const response =
        await openai.responses.create({
          model:
            MODEL,

          instructions:
            SYSTEM_PROMPT,

          input:
            buildUserInput(row),

          max_output_tokens:
            MAX_OUTPUT_TOKENS,

          text: {
            format: {
              type:
                "json_schema",

              name:
                "neet_mds_rapid_revision_notes",

              strict:
                true,

              schema:
                NOTES_SCHEMA
            }
          }
        });

      return validateAndNormalize(
        extractResponseText(response),
        row.topic
      );
    } catch (error) {
      lastError = error;

      if (
        isCreditExhaustionError(
          error
        )
      ) {
        throw error;
      }

      const validationError =
        /invalid JSON|one JSON object|no subtopics|not an object|duplicates another|contains no notes|non-empty string/i.test(
          getErrorText(error)
        );

      const shouldRetry =
        isRetryableError(error) ||
        validationError;

      if (
        attempt === API_RETRIES ||
        !shouldRetry
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(
          Math.random() * 400
        );

      console.warn(
        `⚠️ Retry ${
          attempt + 1
        }/${API_RETRIES} after ${delay} ms: ${
          getErrorText(error)
        }`
      );

      await sleep(delay);
    }
  }

  throw lastError;
}

// ─────────────────────────────────────────────
// EXPIRED LOCK RECOVERY
// ─────────────────────────────────────────────

async function releaseExpiredLocks() {
  const cutoff =
    new Date(
      Date.now() -
      LOCK_TTL_MIN *
        60 *
        1000
    ).toISOString();

  const { error } =
    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]:
          false,

        [LOCK_AT_COL]:
          null
      })
      .eq(
        LOCK_COL,
        true
      )
      .not(
        INPUT_COL,
        "is",
        null
      )
      .is(
        OUTPUT_COL,
        null
      )
      .lt(
        LOCK_AT_COL,
        cutoff
      );

  if (error) {
    throw new Error(
      `Failed to release expired notes locks: ${error.message}`
    );
  }
}

// ─────────────────────────────────────────────
// LOCK ONE ROW
// ─────────────────────────────────────────────

async function lockOneRow(row) {
  const lockedAt =
    new Date().toISOString();

  const { data, error } =
    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]:
          true,

        [LOCK_AT_COL]:
          lockedAt
      })
      .eq(
        "id",
        row.id
      )
      .eq(
        LOCK_COL,
        false
      )
      .not(
        INPUT_COL,
        "is",
        null
      )
      .is(
        OUTPUT_COL,
        null
      )
      .select(
        [
          "id",
          "subject",
          "serial_number",
          "topic",
          "number_of_times_asked",
          "subtopic_classification",
          "exam",
          "year_asked",
          "pyt",
          INPUT_COL,
          LOCK_AT_COL
        ].join(",")
      )
      .maybeSingle();

  if (error) {
    throw new Error(
      `Failed to lock row ${row.id}: ${error.message}`
    );
  }

  return data || null;
}

// ─────────────────────────────────────────────
// CLAIM AVAILABLE ROWS
// ─────────────────────────────────────────────

async function claimRows(limit) {
  await releaseExpiredLocks();

  const {
    data: availableRows,
    error
  } = await supabase
    .from(TABLE)
    .select(
      [
        "id",
        "subject",
        "serial_number",
        "topic"
      ].join(",")
    )
    .not(
      INPUT_COL,
      "is",
      null
    )
    .is(
      OUTPUT_COL,
      null
    )
    .eq(
      LOCK_COL,
      false
    )
    .order(
      "serial_number",
      {
        ascending: true
      }
    )
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending notes rows: ${error.message}`
    );
  }

  if (
    !availableRows?.length
  ) {
    return [];
  }

  const lockResults =
    await Promise.allSettled(
      availableRows.map(
        (row) =>
          lockOneRow(row)
      )
    );

  const claimedRows = [];

  for (
    const result of
    lockResults
  ) {
    if (
      result.status ===
        "fulfilled" &&
      result.value
    ) {
      claimedRows.push(
        result.value
      );
    } else if (
      result.status ===
      "rejected"
    ) {
      console.error(
        "❌ Row-lock error:",
        getErrorText(
          result.reason
        )
      );
    }
  }

  return claimedRows;
}

// ─────────────────────────────────────────────
// SAVE SUCCESS
// ─────────────────────────────────────────────

async function saveSuccess(
  row,
  generatedOutput
) {
  const { data, error } =
    await supabase
      .from(TABLE)
      .update({
        [OUTPUT_COL]:
          generatedOutput,

        [LOCK_COL]:
          false,

        [LOCK_AT_COL]:
          null
      })
      .eq(
        "id",
        row.id
      )
      .eq(
        LOCK_COL,
        true
      )
      .eq(
        LOCK_AT_COL,
        row[
          LOCK_AT_COL
        ]
      )
      .is(
        OUTPUT_COL,
        null
      )
      .select("id");

  if (error) {
    throw new Error(
      `Failed to save notes: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the row lock changed or notes already exist"
    );
  }
}

// ─────────────────────────────────────────────
// RELEASE OWNED LOCK
// ─────────────────────────────────────────────

async function releaseRowLock(row) {
  const { error } =
    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]:
          false,

        [LOCK_AT_COL]:
          null
      })
      .eq(
        "id",
        row.id
      )
      .eq(
        LOCK_COL,
        true
      )
      .eq(
        LOCK_AT_COL,
        row[
          LOCK_AT_COL
        ]
      )
      .is(
        OUTPUT_COL,
        null
      );

  if (error) {
    console.error(
      `❌ Failed to release lock ${row.id}: ${error.message}`
    );
  }
}

async function releaseClaimedRows(
  rows
) {
  await Promise.allSettled(
    rows.map(
      (row) =>
        releaseRowLock(row)
    )
  );
}

// ─────────────────────────────────────────────
// PROCESS ONE ROW
// ─────────────────────────────────────────────

async function processRow(row) {
  console.log(
    `📝 Generating notes | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateNotes(
        row
      );

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | SUBTOPICS=${result.subtopicCount} | NOTES=${result.noteCount}`
    );

    return {
      creditExhausted:
        false
    };
  } catch (error) {
    await releaseRowLock(
      row
    );

    if (
      isCreditExhaustionError(
        error
      )
    ) {
      console.error(
        "🛑 OpenAI credits exhausted. Worker will stop safely."
      );

      return {
        creditExhausted:
          true
      };
    }

    console.error(
      `❌ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${getErrorText(error)}`
    );

    return {
      creditExhausted:
        false
    };
  }
}

// ─────────────────────────────────────────────
// CONTROLLED CONCURRENCY
// ─────────────────────────────────────────────

async function processWithConcurrency(
  rows
) {
  let nextIndex = 0;

  let creditExhausted =
    false;

  async function runner() {
    while (
      nextIndex <
        rows.length &&
      !creditExhausted
    ) {
      const currentIndex =
        nextIndex;

      nextIndex += 1;

      const result =
        await processRow(
          rows[
            currentIndex
          ]
        );

      if (
        result.creditExhausted
      ) {
        creditExhausted =
          true;
      }
    }
  }

  const runnerCount =
    Math.min(
      BATCH_SIZE,
      rows.length
    );

  await Promise.all(
    Array.from(
      {
        length:
          runnerCount
      },
      () => runner()
    )
  );

  if (creditExhausted) {
    await releaseClaimedRows(
      rows.slice(
        nextIndex
      )
    );
  }

  return {
    creditExhausted
  };
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────

async function main() {
  console.log(
    `🚀 NEET MDS NOTES WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE} | Max output=${MAX_OUTPUT_TOKENS}`
  );

  while (true) {
    try {
      const rows =
        await claimRows(
          PICKUP_LIMIT
        );

      if (!rows.length) {
        await sleep(
          LOOP_SLEEP_MS
        );

        continue;
      }

      console.log(
        `📥 Claimed ${rows.length} PYT(s) for notes`
      );

      const result =
        await processWithConcurrency(
          rows
        );

      if (
        result.creditExhausted
      ) {
        console.error(
          "🛑 Worker stopped because API credits are unavailable."
        );

        process.exit(1);
      }
    } catch (error) {
      if (
        isCreditExhaustionError(
          error
        )
      ) {
        console.error(
          "🛑 Worker stopped: OpenAI credits exhausted."
        );

        process.exit(1);
      }

      console.error(
        "❌ Worker loop error:",
        getErrorText(error)
      );

      await sleep(
        Math.max(
          LOOP_SLEEP_MS,
          2000
        )
      );
    }
  }
}

main().catch((error) => {
  console.error(
    "❌ Fatal NEET MDS notes worker error:",
    error
  );

  process.exit(1);
});
