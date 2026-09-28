"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// DATABASE CONFIGURATION
// jsonb_output → notes_json
// ─────────────────────────────────────────────

const TABLE =
  "neet_ss_pediatrics_pyt_source";

const INPUT_COL =
  "jsonb_output";

const OUTPUT_COL =
  "notes_json";

const LOCK_COL =
  "generation_lock";

const LOCK_AT_COL =
  "generation_locked_at";

// ─────────────────────────────────────────────
// ENVIRONMENT CONFIGURATION
// ─────────────────────────────────────────────

function parseIntegerEnv(
  name,
  fallback,
  min,
  max
) {
  const value = Number.parseInt(
    process.env[name] ||
      String(fallback),
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
  process.env.NEET_SS_PEDS_NOTES_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT =
  parseIntegerEnv(
    "NEET_SS_PEDS_NOTES_LIMIT",
    50,
    1,
    100
  );

const BATCH_SIZE =
  parseIntegerEnv(
    "NEET_SS_PEDS_NOTES_BATCH_SIZE",
    5,
    1,
    20
  );

const LOOP_SLEEP_MS =
  parseIntegerEnv(
    "NEET_SS_PEDS_NOTES_LOOP_SLEEP_MS",
    1000,
    250,
    60000
  );

const LOCK_TTL_MIN =
  parseIntegerEnv(
    "NEET_SS_PEDS_NOTES_LOCK_TTL_MIN",
    120,
    5,
    1440
  );

const API_RETRIES =
  parseIntegerEnv(
    "NEET_SS_PEDS_NOTES_API_RETRIES",
    2,
    0,
    5
  );

const WORKER_ID =
  process.env.NEET_SS_PEDS_NOTES_WORKER_ID ||
  `neet-ss-peds-notes-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = `
You are a **Senior NEET SS Pediatrics / American Board of Pediatrics / NBME / AMBOSS examiner** creating elite **Rapid Revision Notes** for last-mile superspecialty pediatric exam preparation.

## GOAL

Convert the supplied **PYT + clinical-decision flashcards** into an exceptionally compressed, clinically discriminating Rapid Revision note set.

The final notes should combine:

**First Aid compression + AMBOSS clinical connections + UWorld/NBME-style discrimination**

Each note must help the student solve an MCQ requiring **at least one useful clinical inference**, rather than merely recall an isolated textbook fact.

---

## INPUT

Topic / PYT:

{{TOPIC}}

Flashcards:

{{FLASHCARDS}}

---

# 1. USE THE FLASHCARDS AS DECISION-PATHWAY SIGNALS

Do NOT simply convert each flashcard into one note.

First analyze the entire flashcard set and determine:

- What clinical presentations are being tested?
- Which findings change diagnosis or management?
- Which competing pathways must be distinguished?
- Which physiological mechanisms explain those decisions?
- Which thresholds or qualifiers change the next step?
- Which complications create a new management pathway?
- Which tempting answers are examiner traps?
- Which pediatric-specific rules differ by age, weight, physiology, or developmental stage?

Then reconstruct the **minimum sufficient high-yield knowledge network** required to solve these flashcards and closely related NEET SS Pediatrics / NBME / ABP-style questions.

The notes must therefore be **broader than the literal answers**, but not become textbook notes.

---

# 2. PEDIATRIC CLINICAL DECISION HIERARCHY

For every PYT, deliberately search for useful notes across this hierarchy:

**Core PYT fact → clinical vignette clue → age-specific clue → mechanism → discriminator → investigation interpretation → next-best-step → treatment selection → escalation threshold → contraindication/adverse effect → exception → complication → examiner trap**

Do NOT mechanically create a note for every category.

Include a category only when it creates a **genuinely new examinable decision**.

---

# 3. PEDIATRIC-SPECIFIC DEPTH

Give special priority to relationships involving:

- **Age-dependent presentations**
- Neonate vs infant vs child vs adolescent differences
- **Weight-based treatment decisions**
- Developmental physiology
- Pediatric normal vs abnormal vital signs
- Congenital vs acquired disease
- Syndromic associations when decision-relevant
- Growth/development clues
- Immunization implications
- Pediatric fluid/electrolyte physiology
- Pediatric pharmacology differences
- Emergency stabilization priorities
- Hemodynamic phenotype
- Respiratory failure patterns
- Neurologic deterioration
- Neonatal physiology
- Pediatric endocrine/metabolic emergencies
- Immunodeficiency/infection patterns
- Hematology/oncology emergencies
- Renal replacement/escalation indications
- ICU escalation thresholds
- Treatment-response interpretation
- Treatment failure
- Rescue therapy
- Prevention of complications
- **Adult-management rules that should NOT be applied to children**

Do not include these merely because they are pediatric topics. Include them only when they improve an MCQ decision.

---

# 4. DECISION DENSITY > FACT DENSITY

Every note must do at least ONE of the following:

1. Answer the PYT directly
2. Identify a recognizable vignette pattern
3. Link a finding to its mechanism
4. Distinguish two plausible competing diagnoses
5. Distinguish two plausible management pathways
6. Interpret an investigation in clinical context
7. Change the next diagnostic step
8. Change the next treatment step
9. Define an escalation/rescue threshold
10. Identify a contraindication or important adverse effect
11. Identify an exception to the usual rule
12. Expose a likely examiner trap
13. Link a complication to the required response

If a note does none of these → **DELETE IT**.

---

# 5. ONE NOTE = ONE MCQ DECISION

Apply this rule aggressively:

> **Every additional note must create a new MCQ decision.**

Two medically different statements are still REDUNDANT if they allow the student to make essentially the same MCQ decision.

Keep the more clinically discriminating statement.

Do not produce separate synonymous notes merely to increase coverage.

---

# 6. CONTRASTIVE NOTES ARE HIGH PRIORITY

Prefer notes that explicitly distinguish competing pathways.

Examples:

"Cold shock + weak pulses → **epinephrine**"

"Warm shock + bounding pulses → **norepinephrine**"

"Shock + crackles/hepatomegaly → avoid **further bolus**"

"Shock + hyperdynamic LV → favor **vasodilatory physiology**"

"Shock + poor LV function → favor **myocardial dysfunction**"

"Persistent shock + fluid overload → start **vasoactive support**"

These are superior to several isolated notes separately defining shock findings.

Whenever two conditions, investigations, or treatments are commonly confused, compress them into a contrastive decision pair when possible.

---

# 7. BUILD 2-LEVEL CLINICAL LINKS

Do not stop at:

Finding → diagnosis.

Prefer:

**Finding → interpretation → decision**

or:

**Clinical context + discriminator → competing pathway → action**

or:

**Treatment + response/failure → escalation**

Examples:

"Fluid + crackles/hepatomegaly → **stop further boluses**"

"Cold shock after fluids → **epinephrine infusion**"

"Warm shock after fluids → **norepinephrine infusion**"

"Neonate + weak femorals + shock → **PGE₁ immediately**"

"Massive transfusion + QT↑ + Ca²⁺↓ → **IV calcium**"

"Effusion + diastolic collapse → **emergency pericardiocentesis**"

The note should contain enough context to trigger the correct pathway without becoming a miniature vignette.

---

# 8. TREATMENT-RESPONSE TRANSITIONS

Actively look for transitions where the same disease requires a different answer after an intervention.

Examples:

"Shock before fluids → **isotonic crystalloid**"

"Cold shock despite fluids → **epinephrine**"

"Warm shock despite fluids → **norepinephrine**"

"Fluid boluses + pulmonary overload → **stop fluids**"

"DKA + shock → cautious **10 mL/kg isotonic fluid**"

This is particularly important for NEET SS because the examiner may provide the diagnosis but test what should happen **next**.

---

# 9. CLINICAL QUALIFIERS MUST SURVIVE COMPRESSION

Never delete a qualifier that changes the answer.

For example:

BAD:
"Shock → epinephrine"

GOOD:
"Fluid-refractory **cold shock** → epinephrine"

BAD:
"Anaphylaxis → epinephrine"

GOOD:
"Anaphylactic shock → **IM epinephrine first**"

BAD:
"Neonatal shock → PGE₁"

GOOD:
"Neonate + weak femorals + shock → **PGE₁**"

BAD:
"Hypotension → pericardiocentesis"

GOOD:
"Effusion + diastolic collapse → **pericardiocentesis**"

Compression must remove words, **not clinical discrimination**.

---

# 10. EXAMINER TRAPS

Deliberately identify situations where a superficially reasonable answer becomes wrong because of one clue.

Examples:

"Anaphylaxis → antihistamine ≠ **first-line treatment**"

"Shock + pulmonary edema → more fluid = **wrong pathway**"

"DKA shock → avoid routine **20 mL/kg bolus**"

"Persistent tachycardia alone ≠ automatic **repeat fluid**"

"Low BP + unilateral absent sounds → **tension pneumothorax**"

"Trauma shock + ongoing bleeding → crystalloid ≠ **definitive resuscitation**"

Use traps selectively.

A trap deserves a note only if it could realistically change an MCQ answer.

---

# 11. MECHANISM NOTES

Include mechanism only when it helps infer an answer.

GOOD:

"Citrate transfusion → Ca²⁺ chelation → **hypocalcemia**"

"Hypothermia → enzyme dysfunction → **coagulopathy worsens**"

"LV dysfunction → fluid intolerance → **vasoactive support**"

BAD:

"Calcium is important for cardiac function."

The mechanism must help solve a vignette.

---

# 12. INVESTIGATION NOTES

Do not merely list investigations.

Show **why the result changes the pathway**.

Examples:

"Hyperdynamic LV + shock → **vasodilatory phenotype**"

"Reduced LV shortening → **myocardial dysfunction**"

"Serial lactate ↓ → improving **tissue perfusion**"

"RA/RV diastolic collapse → **tamponade physiology**"

"Glucose 38 mg/dL → immediate **IV dextrose**"

Prefer interpretation over test-name recall.

---

# 13. NUMBERS AND THRESHOLDS

Preserve numbers when they are genuinely decision-changing:

- Drug dose
- Fluid volume
- Weight-based dose
- Age cutoff
- Laboratory threshold
- Physiologic threshold
- Treatment/escalation threshold

Use Unicode where possible:

**10 mL/kg**

**20 mL/kg**

**Ca²⁺**

**PaCO₂**

**SpO₂**

**HCO₃⁻**

**Na⁺**

**K⁺**

Do NOT invent uncertain doses or thresholds.

If the supplied flashcards do not support an exact number and you are not highly confident that it represents a stable pediatric standard, omit the number rather than hallucinate precision.

---

# 14. NOTE LENGTH

Target:

**5–10 words per note**

Absolute preference:

**one testable idea per line**

Use **1–2 bold buzzwords** per note.

The bold words should represent the decision-driving clue, diagnosis, discriminator, investigation, or action.

Examples:

"Cold shock after fluids → **epinephrine infusion**"

"Warm shock after fluids → **norepinephrine infusion**"

"Shock + crackles → stop **further crystalloid**"

"Weak femorals + neonatal shock → **PGE₁**"

"Transfusion + Ca²⁺↓ → **calcium gluconate**"

"Tamponade physiology → emergency **pericardiocentesis**"

Avoid explanatory prose.

---

# 15. MARKDOWN + RNW COMPATIBILITY

The strings will render in a React Native Web frontend.

Use Markdown bold only:

**important term**

Use Unicode directly for:

- Superscripts
- Subscripts
- Arrows
- Mathematical symbols
- Greek letters
- Ions

Preferred forms:

→  
↑  
↓  
≥  
≤  
±  
≈  
≠  
α  
β  
γ  
Ca²⁺  
Na⁺  
K⁺  
H⁺  
HCO₃⁻  
PaCO₂  
PaO₂  
SpO₂  
FiO₂  
PGE₁  

Avoid LaTeX.

Do NOT use HTML.

Do NOT use Markdown tables.

---

# 16. SUBTOPIC ORGANIZATION

Organize notes into clinically meaningful subtopics.

Subtopics should represent **decision domains**, not arbitrary textbook headings.

For example, for pediatric shock:

- Initial Recognition & Stabilization
- Fluid Strategy
- Cold vs Warm Shock
- Cardiac Dysfunction
- Hemorrhagic Shock
- Anaphylactic Shock
- DKA & Metabolic Shock
- Neonatal Duct-Dependent Shock
- Endocrine Shock
- Perfusion Monitoring
- Obstructive Shock
- Fluid Overload
- Massive Transfusion
- Examiner Traps

Create only the subtopics genuinely needed for the supplied PYT.

---

# 17. TARGET NOTE COUNT

Do NOT aim for a fixed number merely to fill space.

For an average PYT, aim for approximately:

**25–35 exceptionally discriminating notes**

A narrow PYT may need fewer.

A broad PYT may require approximately 35–45.

Never increase note count merely to reach a target.

**25 exceptional notes are better than 50 repetitive notes.**

---

# 18. DESIRED CONTENT CHARACTER

As a rough conceptual target:

- ~20% core recall
- ~30% vignette recognition
- ~20% mechanism-linked inference
- ~20% differentiators / examiner traps
- ~10% next-step / management

Do NOT mechanically enforce percentages.

They describe the desired character of the final notes.

---

# 19. DEDUPLICATION PASS — MANDATORY

Before output, perform a silent final deduplication pass.

For EVERY note ask:

> "Does this note enable an additional MCQ decision that another note does not?"

If NO → delete it.

Then ask:

> "Can two notes be replaced by one stronger contrastive note?"

If YES → merge them.

Then ask:

> "Is this merely background knowledge without decision value?"

If YES → delete it.

Then ask:

> "Did compression remove a qualifier that changes the answer?"

If YES → restore the qualifier.

Do NOT increase content after deduplication merely to restore the original note count.

---

# 20. QUALITY STANDARD

The final notes should feel like:

**First Aid-style compression**
+
**AMBOSS-level clinical connections**
+
**UWorld/NBME-style competing-pathway discrimination**
+
**NEET SS Pediatrics next-step emphasis**

The student should be able to rapidly scan the notes immediately before an examination and recognize the hidden decision logic inside a clinical vignette.

The final product is NOT:

- textbook notes
- flashcard summaries
- explanatory paragraphs
- lists of disconnected facts
- definitions copied from source material

It is a **compressed clinical decision map**.

---

# 21. OUTPUT — STRICT JSON ONLY

Return valid JSON only.

No introduction.

No explanation.

No code fence.

No text before or after the JSON.

Use exactly this structure:

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

---

# FINAL INTERNAL TEST

Before returning the JSON, silently test every note against:

**Clinical clue → interpretation → competing pathway → decision**

Not every line must explicitly contain all four components, but every retained line must contribute something unique to that chain.

The governing principle is:

**Prefer decision density over fact density.**

Every additional note must create a **new MCQ decision**.

If another note already enables the same decision → **DELETE IT**.
`.trim();

if (!SYSTEM_PROMPT) {
  throw new Error(
    "SYSTEM_PROMPT cannot be empty"
  );
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

function requireString(
  value,
  label
) {
  const normalized =
    String(value ?? "").trim();

  if (!normalized) {
    throw new Error(
      `${label} must be a non-empty string`
    );
  }

  return normalized;
}

function normalizeForComparison(value) {
  return String(value)
    .replace(/\*\*/g, "")
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .toLowerCase();
}

function serializeJsonInput(value) {
  if (
    value === null ||
    value === undefined
  ) {
    throw new Error(
      "Flashcard input is missing"
    );
  }

  if (typeof value === "string") {
    const trimmed = value.trim();

    if (!trimmed) {
      throw new Error(
        "Flashcard input is empty"
      );
    }

    try {
      return JSON.stringify(
        JSON.parse(trimmed),
        null,
        2
      );
    } catch {
      return trimmed;
    }
  }

  return JSON.stringify(
    value,
    null,
    2
  );
}

// ─────────────────────────────────────────────
// BUILD INPUT
// Sends topic and jsonb_output flashcards.
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    `TOPIC / PYT: ${requireString(
      row.topic,
      "Topic"
    )}`,
    "",
    "FLASHCARDS:",
    serializeJsonInput(
      row[INPUT_COL]
    ),
    "",
    "Generate the database-ready notes JSON now."
  ].join("\n");
}

// ─────────────────────────────────────────────
// RESPONSE EXTRACTION
// ─────────────────────────────────────────────

function extractResponseText(response) {
  if (
    typeof response?.output_text ===
      "string" &&
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
        contentItem?.type ===
          "output_text" &&
        typeof contentItem.text ===
          "string"
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
// VALIDATION
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

  const rootKeys =
    Object.keys(parsed).sort();

  if (
    rootKeys.join(",") !==
    "subtopics,topic"
  ) {
    throw new Error(
      "Generated notes must contain exactly topic and subtopics"
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

  const seenSubtopics =
    new Set();

  const seenNotes =
    new Set();

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

        const groupKeys =
          Object.keys(group).sort();

        if (
          groupKeys.join(",") !==
          "notes,subtopic"
        ) {
          throw new Error(
            `Subtopic ${position} must contain exactly subtopic and notes`
          );
        }

        const subtopic =
          requireString(
            group.subtopic,
            `Subtopic ${position} name`
          );

        const subtopicKey =
          normalizeForComparison(
            subtopic
          );

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
                normalizeForComparison(
                  note
                );

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
// No max_output_tokens supplied.
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
          model: MODEL,

          instructions:
            SYSTEM_PROMPT,

          input:
            buildUserInput(row),

          text: {
            format: {
              type: "json_schema",
              name:
                "neet_ss_pediatrics_rapid_revision_notes",
              strict: true,
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
        /invalid JSON|one JSON object|exactly topic and subtopics|no subtopics|not an object|exactly subtopic and notes|contains no notes|duplicates another|non-empty string|Flashcard input/i.test(
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
        }/${API_RETRIES} after ${delay} ms: ${getErrorText(
          error
        )}`
      );

      await sleep(delay);
    }
  }

  throw (
    lastError ||
    new Error(
      "Notes generation failed"
    )
  );
}

// ─────────────────────────────────────────────
// RELEASE EXPIRED LOCKS
// Only pending notes rows are unlocked.
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
        [LOCK_COL]: false,
        [LOCK_AT_COL]: null
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
      `Failed to release expired Pediatrics notes locks: ${error.message}`
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
        [LOCK_COL]: true,
        [LOCK_AT_COL]: lockedAt
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
// Picks only:
// jsonb_output IS NOT NULL
// notes_json IS NULL
// generation_lock = false
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
        "serial_number"
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
      `Failed to find pending Pediatrics notes rows: ${error.message}`
    );
  }

  if (!availableRows?.length) {
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
// Prevents overwriting existing notes_json.
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
        row[LOCK_AT_COL]
      )
      .is(
        OUTPUT_COL,
        null
      )
      .select("id");

  if (error) {
    throw new Error(
      `Failed to save Pediatrics notes: ${error.message}`
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
        [LOCK_COL]: false,
        [LOCK_AT_COL]: null
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
        row[LOCK_AT_COL]
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
    `📝 Generating Pediatrics notes | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateNotes(row);

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | SUBTOPICS=${result.subtopicCount} | NOTES=${result.noteCount}`
    );

    return {
      creditExhausted: false
    };
  } catch (error) {
    await releaseRowLock(row);

    if (
      isCreditExhaustionError(
        error
      )
    ) {
      console.error(
        "🛑 OpenAI credits exhausted. Worker will stop safely."
      );

      return {
        creditExhausted: true
      };
    }

    console.error(
      `❌ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${getErrorText(
        error
      )}`
    );

    return {
      creditExhausted: false
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
  let creditExhausted = false;

  async function runner() {
    while (
      nextIndex < rows.length &&
      !creditExhausted
    ) {
      const currentIndex =
        nextIndex;

      nextIndex += 1;

      const result =
        await processRow(
          rows[currentIndex]
        );

      if (
        result.creditExhausted
      ) {
        creditExhausted = true;
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
        length: runnerCount
      },
      () => runner()
    )
  );

  if (creditExhausted) {
    await releaseClaimedRows(
      rows.slice(nextIndex)
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
    `🚀 NEET SS PEDIATRICS NOTES WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE}`
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
        `📥 Claimed ${rows.length} Pediatrics flashcard set(s)`
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
    "❌ Fatal Pediatrics notes worker error:",
    error
  );

  process.exit(1);
});
