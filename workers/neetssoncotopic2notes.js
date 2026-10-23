"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "neet_ss_oncology_pyt_source";
const OUTPUT_COL = "notes_json";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

const MIN_NOTES_PER_SUBTOPIC = 5;
const MAX_NOTES_PER_SUBTOPIC = 12;

function integerEnv(name, fallback, min, max) {
  const value = Number.parseInt(
    process.env[name] || String(fallback),
    10
  );

  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(
      `${name} must be an integer between ${min} and ${max}`
    );
  }

  return value;
}

const MODEL =
  process.env.NEETSS_ONCOLOGY_NOTES_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "NEETSS_ONCOLOGY_NOTES_LIMIT",
  50,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "NEETSS_ONCOLOGY_NOTES_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "NEETSS_ONCOLOGY_NOTES_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "NEETSS_ONCOLOGY_NOTES_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "NEETSS_ONCOLOGY_NOTES_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = integerEnv(
  "NEETSS_ONCOLOGY_NOTES_MAX_OUTPUT_TOKENS",
  16000,
  2000,
  50000
);

const WORKER_ID =
  process.env.NEETSS_ONCOLOGY_NOTES_WORKER_ID ||
  `neetss-oncology-notes-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/*
PASTE YOUR COMPLETE PROMPT INSIDE THE COMMENT BELOW.

Markdown, JSON examples, quotation marks and backticks are safe here.

IMPORTANT:
The prompt must not contain the comment-closing characters:
* followed immediately by /
*/
const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
ROLE
You are a senior NEET SS Medical Oncology and American Board of Internal Medicine (ABIM) Medical Oncology senior examiner, clinical trial editor, and molecular tumor board chair. Your goal is to transform the provided topic and subtopics into an elite, 10/10 quality clinical decision map for rapid revision.

OBJECTIVE
Produce an exceptionally dense, clinically discriminating decision network that maps every subtopic to specific MCQ decision axes. The content must reflect the clinical depth of DeVita, Hellman, and Rosenberg’s Cancer: Principles & Practice of Oncology, the MD Anderson Manual of Medical Oncology, and current practice-changing landmark trials (NCCN/ESMO/ASCO paradigms). 

INPUT
Topic/PYT: {{TOPIC}}
Required Curriculum Subtopics: {{SUBTOPICS}}
Flashcards JSON (If available, otherwise generate purely from subtopics): {{FLASHCARDS}}
Evidence Cutoff: Current (2026/2027)

1. RIGOROUS 10/10 QUALITY STANDARDS (ANTI-FLUFF PROTOCOLS)
- ZERO BENCH RESEARCH SUMMARY: Eliminate basic biology definitions, generic pathway descriptions, and experimental assays (e.g., do not explain what a kinase or western blot is). Every note must directly guide a clinical choice.
- ABSOLUTE PATHWAY PRECISION: Specify localized vs. metastatic, resectable vs. unresectable, curative vs. palliative, neoadjuvant vs. adjuvant vs. consolidation vs. maintenance, and first-line vs. post-progression options.
- MANDATORY TRIAL INTEGRATION: For every actionable pathway, you MUST explicitly name the landmark phase III trial (e.g., PAPILLON, FLAURA2, monarchE, DESTINY-Breast04, BEACON, KEYNOTE-177, NICHE-2, SOLO-1) that established the standard of care, along with the precise control arm comparator where applicable.
- EXACT MOLECULAR DISCRIMINATION: Identify specific variants (e.g., EGFR exon 19del vs. exon 20 insertion; KRAS G12C vs. G12D/V; PDGFRA D842V vs. KIT exon 9/11). Differentiate somatic-only from germline confirmation, VUS from pathogenic variants, and true driver progression from CHIP or plasma low-shedding false negatives.

2. SYSTEMATIC EXAMINER TRAINS & CONTRASTS
For every subtopic, you must generate contrastive notes that explicitly expose high-yield board traps:
- High PD-L1 Expression vs. Targetable Driver Mutations (Drivers take first-line priority; immunotherapy is deferred or contraindicated upfront).
- Oligoprogression vs. Widespread Systemic Progression (Local ablative therapy + continue TKI vs. mandatory systemic switch).
- Acquired Resistance Architectures (e.g., Osimertinib resistance via C797S mutation in cis vs. trans; MET or HER2 bypass amplifications requiring trial-directed multi-kinase targeting or re-biopsy to rule out small-cell transformation).
- Sporadic vs. Hereditary Deficiencies (e.g., MLH1/PMS2 loss with MLH1 promoter methylation and/or BRAF V600E vs. germline Lynch Syndrome cascade testing; isolated PMS2 or MSH6 loss anomalies).
- Toxicity-Driven Contraindications (e.g., Alpelisib severe hyperglycemia boundaries linked to baseline HbA1c ≥6.5% vs. Capivasertib broader tolerability).

3. NOTE ARCHITECTURE & DENSITY
- Structure: Every single note must adhere strictly to the three-link clinical chain: 
  **Precise Clinical Context + Decisive Biomarker/Clue** → Exact Interpretation/Trial Axiom → Defensible Next Action or Target Selection.
- Formatting: Start each bullet with **Bold Core Context**. Use a clear Unicode right arrow (→) to separate links. 
- Length: Target 12–25 highly condensed, punchy words per note. Avoid multi-sentence paragraphs, generic lists, or text padding.
- Coverage: Generate between 5 and 12 uniquely discriminating decision notes per subtopic. Ensure comprehensive coverage of all requested subtopics without dropping depth.

4. MANDATORY PRE-OUTPUT AUDIT
Before rendering the JSON, silently execute this verification checklist:
A. Are there any generic textbook facts? If yes, replace them with trial-specific clinical actions.
B. Is every drug regimen qualified by its exact setting, line of therapy, and biomarker eligibility?
C. Are VUS, somatic variant allele fractions (VAF), or unvalidated scores overinterpreted?
D. Does the note provide the explicit tool to differentiate between two highly plausible options in a brutal NEET SS / ABIM multiple-choice question?

OUTPUT FORMAT — STRICT
Return valid JSON only, UTF-8, with no conversational text, no explanations, no markdown fences, and no trailing commas. Follow this exact schema:

{
  "topic": "string",
  "subtopics": [
    {
      "subtopic": "string",
      "notes": [
        "**Decisive Context + Target Alteration** → Trial/Regimen Axiom → Precise Next Action",
        "**Contrast Context + Trapped Alteration** → Alternative Distinction → Distinct Next Action"
      ]
    }
  ]
}
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE NEET SS MEDICAL ONCOLOGY NOTES PROMPT HERE"
  )
) {
  throw new Error(
    "Paste the complete inline SYSTEM_PROMPT before starting the worker"
  );
}

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

function errorText(error) {
  return String(
    error?.message ||
      error?.error?.message ||
      error ||
      "Unknown error"
  );
}

function isCreditError(error) {
  return /no credits remaining|insufficient_quota|billing|credit balance|billing_hard_limit/i.test(
    errorText(error)
  );
}

function isRetryable(error) {
  if (isCreditError(error)) {
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
      errorText(error)
    )
  );
}

function requiredText(value, label, minimumLength = 1) {
  const text = String(value ?? "").trim();

  if (text.length < minimumLength) {
    throw new Error(`${label} must be a non-empty string`);
  }

  return text;
}

function normalizeText(value) {
  return String(value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function serializeValue(value, label) {
  if (value === null || value === undefined) {
    throw new Error(`${label} is missing`);
  }

  if (typeof value === "string") {
    const text = value.trim();

    if (!text) {
      throw new Error(`${label} is empty`);
    }

    try {
      return JSON.stringify(JSON.parse(text), null, 2);
    } catch {
      return text;
    }
  }

  return JSON.stringify(value, null, 2);
}

function serializeOptionalValue(value) {
  if (value === null || value === undefined) {
    return "Not supplied";
  }

  if (typeof value === "string") {
    const text = value.trim();

    if (!text) {
      return "Not supplied";
    }

    try {
      return JSON.stringify(JSON.parse(text), null, 2);
    } catch {
      return text;
    }
  }

  return JSON.stringify(value, null, 2);
}

function buildInput(row) {
  return [
    `TOPIC/PYT: ${requiredText(row.topic, "Topic")}`,
    "",
    "REQUIRED CURRICULUM SUBTOPICS:",
    serializeValue(row.subtopic, "Subtopic"),
    "",
    "FLASHCARDS JSON IF AVAILABLE:",
    serializeOptionalValue(row.jsonb_output),
    "",
    "EVIDENCE CUTOFF: Current (2026/2027)",
    "",
    "Create the complete NEET SS Medical Oncology clinical decision notes.",
    `Generate ${MIN_NOTES_PER_SUBTOPIC}–${MAX_NOTES_PER_SUBTOPIC} decision notes for every supplied subtopic.`,
    "Return only one valid JSON object matching the required schema.",
    "Do not use Markdown code fences.",
    "Do not add commentary before or after the JSON."
  ].join("\n");
}

function extractText(response) {
  if (
    typeof response?.output_text === "string" &&
    response.output_text.trim()
  ) {
    return response.output_text.trim();
  }

  const pieces = [];

  for (const item of response?.output || []) {
    for (const content of item?.content || []) {
      if (
        content?.type === "output_text" &&
        typeof content.text === "string"
      ) {
        pieces.push(content.text);
      }
    }
  }

  const text = pieces.join("\n").trim();

  if (!text) {
    throw new Error("OpenAI returned empty output");
  }

  return text;
}

function removeCodeFence(value) {
  let text = String(value ?? "").trim();

  const match = text.match(
    /^```(?:json)?\s*([\s\S]*?)\s*```$/i
  );

  if (match) {
    text = match[1].trim();
  }

  return text;
}

function parseJsonOutput(rawText) {
  const cleaned = removeCodeFence(rawText);

  try {
    return JSON.parse(cleaned);
  } catch {
    const firstBrace = cleaned.indexOf("{");
    const lastBrace = cleaned.lastIndexOf("}");

    if (
      firstBrace === -1 ||
      lastBrace === -1 ||
      lastBrace <= firstBrace
    ) {
      throw new Error(
        "Output does not contain a JSON object"
      );
    }

    const candidate = cleaned.slice(
      firstBrace,
      lastBrace + 1
    );

    try {
      return JSON.parse(candidate);
    } catch (error) {
      throw new Error(
        `Model returned invalid JSON: ${error.message}`
      );
    }
  }
}

function validateExactKeys(object, expectedKeys, label) {
  if (
    !object ||
    typeof object !== "object" ||
    Array.isArray(object)
  ) {
    throw new Error(`${label} must be an object`);
  }

  const actual = Object.keys(object).sort();
  const expected = [...expectedKeys].sort();

  if (
    JSON.stringify(actual) !==
    JSON.stringify(expected)
  ) {
    throw new Error(
      `${label} must contain exactly: ${expectedKeys.join(", ")}`
    );
  }
}

function parseExpectedSubtopics(value) {
  if (value === null || value === undefined) {
    return [];
  }

  let parsed = value;

  if (typeof value === "string") {
    const text = value.trim();

    if (!text) {
      return [];
    }

    try {
      parsed = JSON.parse(text);
    } catch {
      return [text];
    }
  }

  if (Array.isArray(parsed)) {
    return parsed
      .map((item) => {
        if (typeof item === "string") {
          return item.trim();
        }

        if (
          item &&
          typeof item === "object"
        ) {
          return String(
            item.subtopic ||
              item.title ||
              item.name ||
              ""
          ).trim();
        }

        return "";
      })
      .filter(Boolean);
  }

  if (
    parsed &&
    typeof parsed === "object"
  ) {
    if (Array.isArray(parsed.subtopics)) {
      return parseExpectedSubtopics(parsed.subtopics);
    }

    return Object.values(parsed)
      .flatMap((item) => parseExpectedSubtopics(item))
      .filter(Boolean);
  }

  return [];
}

function validateNote(note, label) {
  const text = requiredText(note, label, 10);

  if (!text.startsWith("**")) {
    throw new Error(
      `${label} must begin with bold Markdown context`
    );
  }

  if (!text.includes("→")) {
    throw new Error(
      `${label} must contain Unicode pathway arrows`
    );
  }

  const arrowCount =
    (text.match(/→/g) || []).length;

  if (arrowCount < 2) {
    throw new Error(
      `${label} must contain the complete three-link clinical chain`
    );
  }

  return text;
}

function validateNotesOutput(
  value,
  expectedTopic,
  sourceSubtopics
) {
  validateExactKeys(
    value,
    ["topic", "subtopics"],
    "Notes output"
  );

  const outputTopic = requiredText(
    value.topic,
    "Output topic"
  );

  if (
    normalizeText(outputTopic) !==
    normalizeText(expectedTopic)
  ) {
    throw new Error(
      `Output topic mismatch. Expected "${expectedTopic}", received "${outputTopic}"`
    );
  }

  if (
    !Array.isArray(value.subtopics) ||
    value.subtopics.length === 0
  ) {
    throw new Error(
      '"subtopics" must be a non-empty array'
    );
  }

  const outputSubtopicNames = new Set();
  const allNotes = new Set();

  value.subtopics.forEach(
    (subtopicObject, subtopicIndex) => {
      const number = subtopicIndex + 1;

      validateExactKeys(
        subtopicObject,
        ["subtopic", "notes"],
        `Subtopic ${number}`
      );

      const subtopicName = requiredText(
        subtopicObject.subtopic,
        `Subtopic ${number} name`
      );

      const normalizedSubtopic =
        normalizeText(subtopicName);

      if (
        outputSubtopicNames.has(
          normalizedSubtopic
        )
      ) {
        throw new Error(
          `Duplicate output subtopic: ${subtopicName}`
        );
      }

      outputSubtopicNames.add(
        normalizedSubtopic
      );

      if (
        !Array.isArray(subtopicObject.notes)
      ) {
        throw new Error(
          `Subtopic ${number} notes must be an array`
        );
      }

      if (
        subtopicObject.notes.length <
          MIN_NOTES_PER_SUBTOPIC ||
        subtopicObject.notes.length >
          MAX_NOTES_PER_SUBTOPIC
      ) {
        throw new Error(
          `Subtopic "${subtopicName}" contains ${subtopicObject.notes.length} notes; required range is ${MIN_NOTES_PER_SUBTOPIC}–${MAX_NOTES_PER_SUBTOPIC}`
        );
      }

      subtopicObject.notes =
        subtopicObject.notes.map(
          (note, noteIndex) => {
            const validated = validateNote(
              note,
              `Subtopic "${subtopicName}" note ${noteIndex + 1}`
            );

            const normalized =
              normalizeText(validated);

            if (allNotes.has(normalized)) {
              throw new Error(
                `Duplicate note detected in subtopic "${subtopicName}"`
              );
            }

            allNotes.add(normalized);

            return validated;
          }
        );

      subtopicObject.subtopic =
        subtopicName;
    }
  );

  if (sourceSubtopics.length > 1) {
    const missingSubtopics =
      sourceSubtopics.filter(
        (sourceName) =>
          !outputSubtopicNames.has(
            normalizeText(sourceName)
          )
      );

    if (missingSubtopics.length > 0) {
      throw new Error(
        `Output omitted required subtopics: ${missingSubtopics.join(" | ")}`
      );
    }
  }

  value.topic = expectedTopic;

  return value;
}

async function generateNotes(row) {
  let lastError;

  for (
    let attempt = 0;
    attempt <= API_RETRIES;
    attempt += 1
  ) {
    try {
      const response = await openai.responses.create({
        model: MODEL,
        instructions: SYSTEM_PROMPT,
        input: buildInput(row),
        max_output_tokens: MAX_OUTPUT_TOKENS
      });

      const rawText = extractText(response);
      const parsed = parseJsonOutput(rawText);

      const sourceSubtopics =
        parseExpectedSubtopics(row.subtopic);

      const validated =
        validateNotesOutput(
          parsed,
          requiredText(row.topic, "Topic"),
          sourceSubtopics
        );

      const totalNotes =
        validated.subtopics.reduce(
          (sum, item) =>
            sum + item.notes.length,
          0
        );

      return {
        json: validated,
        subtopics:
          validated.subtopics.length,
        notes: totalNotes,
        characters: rawText.length
      };
    } catch (error) {
      lastError = error;

      if (isCreditError(error)) {
        throw error;
      }

      const validationFailure =
        /empty output|JSON|Notes output|topic|subtopic|notes|three-link|pathway arrows|bold Markdown|duplicate|omitted|required range/i.test(
          errorText(error)
        );

      if (
        attempt === API_RETRIES ||
        (!isRetryable(error) &&
          !validationFailure)
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(Math.random() * 400);

      console.warn(
        `Retry ${attempt + 1}/${API_RETRIES} after ${delay} ms: ${errorText(error)}`
      );

      await sleep(delay);
    }
  }

  throw (
    lastError ||
    new Error(
      "NEET SS Medical Oncology notes generation failed"
    )
  );
}

async function verifySchema() {
  const { error } = await supabase
    .from(TABLE)
    .select(
      [
        "id",
        "topic",
        "subtopic",
        "jsonb_output",
        OUTPUT_COL,
        LOCK_COL,
        LOCK_AT_COL
      ].join(",")
    )
    .limit(1);

  if (error) {
    throw new Error(
      `Schema check failed: ${error.message}`
    );
  }
}

async function releaseExpiredLocks() {
  const cutoff = new Date(
    Date.now() -
      LOCK_TTL_MIN * 60 * 1000
  ).toISOString();

  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq(LOCK_COL, true)
    .not("topic", "is", null)
    .not("subtopic", "is", null)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

  if (error) {
    throw new Error(
      `Failed to release expired notes locks: ${error.message}`
    );
  }
}

async function lockRow(candidate) {
  const lockedAt =
    new Date().toISOString();

  const { data, error } =
    await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]: true,
        [LOCK_AT_COL]: lockedAt
      })
      .eq("id", candidate.id)
      .eq(LOCK_COL, false)
      .not("topic", "is", null)
      .not("subtopic", "is", null)
      .is(OUTPUT_COL, null)
      .select(
        [
          "id",
          "subject",
          "serial_number",
          "topic",
          "subtopic",
          "number_of_times_asked",
          "jsonb_output",
          LOCK_AT_COL
        ].join(",")
      )
      .maybeSingle();

  if (error) {
    throw new Error(
      `Failed to lock row ${candidate.id}: ${error.message}`
    );
  }

  return data || null;
}

async function claimRows(limit) {
  await releaseExpiredLocks();

  const { data, error } =
    await supabase
      .from(TABLE)
      .select("id,serial_number")
      .eq(LOCK_COL, false)
      .not("topic", "is", null)
      .not("subtopic", "is", null)
      .is(OUTPUT_COL, null)
      .order("serial_number", {
        ascending: true
      })
      .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending oncology notes rows: ${error.message}`
    );
  }

  if (!data?.length) {
    return [];
  }

  const results =
    await Promise.allSettled(
      data.map(lockRow)
    );

  const rows = [];

  for (const result of results) {
    if (
      result.status === "fulfilled" &&
      result.value
    ) {
      rows.push(result.value);
    }

    if (result.status === "rejected") {
      console.error(
        "Row-lock error:",
        errorText(result.reason)
      );
    }
  }

  return rows;
}

async function saveSuccess(
  row,
  notesJson
) {
  const { data, error } =
    await supabase
      .from(TABLE)
      .update({
        [OUTPUT_COL]: notesJson,
        [LOCK_COL]: false,
        [LOCK_AT_COL]: null
      })
      .eq("id", row.id)
      .eq(LOCK_COL, true)
      .eq(
        LOCK_AT_COL,
        row[LOCK_AT_COL]
      )
      .is(OUTPUT_COL, null)
      .select("id");

  if (error) {
    throw new Error(
      `Failed to save oncology notes: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or notes_json already exists"
    );
  }
}

async function releaseLock(row) {
  const { error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(
      LOCK_AT_COL,
      row[LOCK_AT_COL]
    )
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `Failed to release notes lock ${row.id}: ${error.message}`
    );
  }
}

async function processRow(row) {
  console.log(
    `Generating oncology notes | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateNotes(row);

    await saveSuccess(
      row,
      result.json
    );

    console.log(
      `Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | subtopics=${result.subtopics} | notes=${result.notes} | characters=${result.characters}`
    );

    return {
      creditExhausted: false
    };
  } catch (error) {
    await releaseLock(row);

    if (isCreditError(error)) {
      console.error(
        "OpenAI credits exhausted. Worker will stop safely."
      );

      return {
        creditExhausted: true
      };
    }

    console.error(
      `Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${errorText(error)}`
    );

    return {
      creditExhausted: false
    };
  }
}

async function processBatch(rows) {
  let next = 0;
  let creditExhausted = false;

  async function runner() {
    while (
      next < rows.length &&
      !creditExhausted
    ) {
      const index = next;
      next += 1;

      const result =
        await processRow(rows[index]);

      if (result.creditExhausted) {
        creditExhausted = true;
      }
    }
  }

  await Promise.all(
    Array.from(
      {
        length: Math.min(
          CONCURRENCY,
          rows.length
        )
      },
      runner
    )
  );

  if (creditExhausted) {
    await Promise.allSettled(
      rows
        .slice(next)
        .map(releaseLock)
    );
  }

  return {
    creditExhausted
  };
}

async function main() {
  await verifySchema();

  console.log(
    `NEET SS MEDICAL ONCOLOGY TOPIC -> NOTES WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=topic+subtopic+optional-jsonb_output | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY} | MaxOutputTokens=${MAX_OUTPUT_TOKENS}`
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
        `Claimed ${rows.length} pending oncology notes row(s)`
      );

      const result =
        await processBatch(rows);

      if (
        result.creditExhausted
      ) {
        process.exit(1);
      }
    } catch (error) {
      if (isCreditError(error)) {
        process.exit(1);
      }

      console.error(
        "Worker loop error:",
        errorText(error)
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
    "Fatal oncology notes worker error:",
    error
  );

  process.exit(1);
});
