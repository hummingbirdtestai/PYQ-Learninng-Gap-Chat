"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

/* -------------------------------------------------------------------------- */
/*                              WORKER CONFIG                                 */
/* -------------------------------------------------------------------------- */

const TABLE = "neetss_medicine_pyt_source";
const INPUT_COL = "flowchart";
const OUTPUT_COL = "infographics";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

function integerEnv(name, fallback, min, max) {
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
  process.env.NEETSS_MEDICINE_INFOGRAPHICS_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "NEETSS_MEDICINE_INFOGRAPHICS_LIMIT",
  20,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "NEETSS_MEDICINE_INFOGRAPHICS_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "NEETSS_MEDICINE_INFOGRAPHICS_LOOP_SLEEP_MS",
  3000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "NEETSS_MEDICINE_INFOGRAPHICS_LOCK_TTL_MIN",
  180,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "NEETSS_MEDICINE_INFOGRAPHICS_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.NEETSS_MEDICINE_INFOGRAPHICS_WORKER_ID ||
  `neetss-medicine-infographics-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/* -------------------------------------------------------------------------- */
/*                         PASTE SYSTEM PROMPT HERE                           */
/* -------------------------------------------------------------------------- */

/*
Paste your complete prompt between the comment markers below.

Important:
The pasted prompt must not contain an asterisk immediately
followed by a forward slash because that closes this comment.
*/

const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
this is the approach to a PYT in NEETSS Medicine Exam in algorithmic clinical pathway style  we are creating 3 Infographics which are A4 Sized Hand sCRIPTED REAL TIME COLOUR ILLUSTRATED PICNOMIC MIND MAP INFOGRAPHICS TO DO RAPID REVISION AND PRODUCE A 1 MINUTE reel per each infographic and 3 Minute Reel for each PYT out of 3 Infographics  So create the require text for each of these 3 Infographic Images from the notes on PYT given  give the Out put as JSON mentioning clearly as image 1 , image 2 , image 3  see to that most of High Yield rapid revision content of the nOTES IS DISTRIBUTED INTO THESE 3 iMAGES 
 GIVE THE OUTPUT JSON AS 
{
  "pyt": "string",
  "exam": "string",
  "deliverable": "string",
  "shared_visual_specifications": {
    "format": "string",
    "art_style": "string",
    "layout_rule": "string",
    "reel_duration_seconds": 60
  },
  "images": {
    "image_1": {
      "label": "string",
      "title": "string",
      "focus": "string",
      "reel_duration_seconds": 60,
      "illustrated_panels": [
        {
          "heading": "string",
          "illustration": "string",
          "text": [
            "string"
          ],
          "exam_trap": "string"
        }
      ],
      "bottom_memory_lock": "string",
      "narrative_flow": "string"
    },
    "image_2": {
      "label": "string",
      "title": "string",
      "focus": "string",
      "reel_duration_seconds": 60,
      "illustrated_panels": [
        {
          "heading": "string",
          "illustration": "string",
          "text": [
            "string"
          ],
          "exam_trap": "string"
        }
      ],
      "bottom_memory_lock": "string",
      "narrative_flow": "string"
    },
    "image_3": {
      "label": "string",
      "title": "string",
      "focus": "string",
      "reel_duration_seconds": 60,
      "illustrated_panels": [
        {
          "heading": "string",
          "illustration": "string",
          "text": [
            "string"
          ],
          "exam_trap": "string"
        }
      ],
      "bottom_memory_lock": "string",
      "narrative_flow": "string"
    }
  }
}
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source
    .slice(start, end)
    .trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE NEET SS MEDICINE"
  )
) {
  throw new Error(
    "Paste the complete SYSTEM_PROMPT before starting the worker"
  );
}

/* -------------------------------------------------------------------------- */
/*                              BASIC HELPERS                                 */
/* -------------------------------------------------------------------------- */

const sleep = (milliseconds) =>
  new Promise((resolve) =>
    setTimeout(resolve, milliseconds)
  );

function errorText(error) {
  return String(
    error?.message ||
      error?.error?.message ||
      error?.response?.data?.error?.message ||
      error ||
      "Unknown error"
  );
}

function errorStatus(error) {
  return Number(
    error?.status ||
      error?.statusCode ||
      error?.response?.status ||
      error?.error?.status ||
      0
  );
}

function isFatalOpenAiError(error) {
  const status = errorStatus(error);
  const message = errorText(error);

  return (
    status === 401 ||
    status === 403 ||
    /api key has been invalidated/i.test(message) ||
    /invalid api key/i.test(message) ||
    /incorrect api key/i.test(message) ||
    /authentication error/i.test(message) ||
    /unauthorized/i.test(message) ||
    /forbidden/i.test(message) ||
    /no credits remaining/i.test(message) ||
    /insufficient_quota/i.test(message) ||
    /billing_hard_limit/i.test(message) ||
    /credit balance/i.test(message)
  );
}

function isRetryable(error) {
  if (isFatalOpenAiError(error)) {
    return false;
  }

  const status = errorStatus(error);
  const message = errorText(error);

  return (
    status === 408 ||
    status === 409 ||
    status === 425 ||
    status === 429 ||
    status >= 500 ||
    /timeout/i.test(message) ||
    /temporar/i.test(message) ||
    /unavailable/i.test(message) ||
    /rate limit/i.test(message) ||
    /ECONNRESET/i.test(message) ||
    /ETIMEDOUT/i.test(message) ||
    /socket hang up/i.test(message) ||
    /connection reset/i.test(message)
  );
}

function isValidationError(error) {
  return /json|image_1|image_2|image_3|illustrated_panels|reel_duration_seconds|non-empty string|shared_visual_specifications|output does not contain/i.test(
    errorText(error)
  );
}

function requiredText(value, label) {
  const text = String(value ?? "").trim();

  if (!text) {
    throw new Error(
      `${label} must be a non-empty string`
    );
  }

  return text;
}

/* -------------------------------------------------------------------------- */
/*                           MODEL INPUT BUILDER                              */
/* -------------------------------------------------------------------------- */

function buildInput(row) {
  const topic = requiredText(
    row.topic,
    "Topic"
  );

  const subject = requiredText(
    row.subject,
    "Subject"
  );

  const flowchart = requiredText(
    row[INPUT_COL],
    INPUT_COL
  );

  return [
    `TOPIC: ${topic}`,
    `SUBJECT: ${subject}`,
    `SERIAL NUMBER: ${row.serial_number}`,
    `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`,
    "",
    "SOURCE CLINICAL PATHWAY FLOWCHART:",
    flowchart,
    "",
    "Convert the supplied clinical pathway into the content plan for exactly three A4 portrait illustrated infographics.",
    "Distribute all important high-yield clinical pathway content logically across image_1, image_2 and image_3.",
    "Each infographic must support approximately one minute of reel narration.",
    "Return only one valid JSON object.",
    "Use exactly the JSON structure specified in the system prompt.",
    "The images object must contain exactly image_1, image_2 and image_3.",
    "Do not use Markdown code fences.",
    "Do not include introductory text.",
    "Do not include concluding commentary."
  ].join("\n");
}

/* -------------------------------------------------------------------------- */
/*                           RESPONSE EXTRACTION                              */
/* -------------------------------------------------------------------------- */

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

  const text = pieces
    .join("\n")
    .trim();

  if (!text) {
    throw new Error(
      "OpenAI returned empty output"
    );
  }

  return text;
}

function removeCodeFence(value) {
  let text = String(value ?? "").trim();

  const fencedMatch = text.match(
    /^```(?:json)?\s*\n?([\s\S]*?)\n?```\s*$/i
  );

  if (fencedMatch) {
    text = fencedMatch[1].trim();
  }

  return text;
}

function extractJsonObject(value) {
  let text = removeCodeFence(value)
    .replace(/^\uFEFF/, "")
    .trim();

  try {
    return JSON.parse(text);
  } catch {
    const firstBrace =
      text.indexOf("{");

    const lastBrace =
      text.lastIndexOf("}");

    if (
      firstBrace === -1 ||
      lastBrace === -1 ||
      lastBrace <= firstBrace
    ) {
      throw new Error(
        "Generated output does not contain a JSON object"
      );
    }

    text = text.slice(
      firstBrace,
      lastBrace + 1
    );

    try {
      return JSON.parse(text);
    } catch (error) {
      throw new Error(
        `Generated output is not valid JSON: ${error.message}`
      );
    }
  }
}

/* -------------------------------------------------------------------------- */
/*                            JSON VALIDATION                                 */
/* -------------------------------------------------------------------------- */

function validateString(value, path) {
  if (
    typeof value !== "string" ||
    !value.trim()
  ) {
    throw new Error(
      `${path} must be a non-empty string`
    );
  }
}

function validateTextArray(value, path) {
  if (
    !Array.isArray(value) ||
    value.length === 0
  ) {
    throw new Error(
      `${path} must be a non-empty array`
    );
  }

  value.forEach((item, index) => {
    validateString(
      item,
      `${path}[${index}]`
    );
  });
}

function validatePanel(panel, path) {
  if (
    !panel ||
    typeof panel !== "object" ||
    Array.isArray(panel)
  ) {
    throw new Error(
      `${path} must be an object`
    );
  }

  validateString(
    panel.heading,
    `${path}.heading`
  );

  validateString(
    panel.illustration,
    `${path}.illustration`
  );

  validateTextArray(
    panel.text,
    `${path}.text`
  );

  validateString(
    panel.exam_trap,
    `${path}.exam_trap`
  );
}

function validateImage(image, key) {
  if (
    !image ||
    typeof image !== "object" ||
    Array.isArray(image)
  ) {
    throw new Error(
      `images.${key} must be an object`
    );
  }

  validateString(
    image.label,
    `images.${key}.label`
  );

  validateString(
    image.title,
    `images.${key}.title`
  );

  validateString(
    image.focus,
    `images.${key}.focus`
  );

  if (
    image.reel_duration_seconds !== 60
  ) {
    throw new Error(
      `images.${key}.reel_duration_seconds must equal 60`
    );
  }

  if (
    !Array.isArray(
      image.illustrated_panels
    ) ||
    image.illustrated_panels.length === 0
  ) {
    throw new Error(
      `images.${key}.illustrated_panels must be a non-empty array`
    );
  }

  image.illustrated_panels.forEach(
    (panel, index) => {
      validatePanel(
        panel,
        `images.${key}.illustrated_panels[${index}]`
      );
    }
  );

  validateString(
    image.bottom_memory_lock,
    `images.${key}.bottom_memory_lock`
  );

  validateString(
    image.narrative_flow,
    `images.${key}.narrative_flow`
  );
}

function validateInfographics(raw) {
  const output =
    extractJsonObject(raw);

  if (
    !output ||
    typeof output !== "object" ||
    Array.isArray(output)
  ) {
    throw new Error(
      "Generated infographics output must be a JSON object"
    );
  }

  validateString(
    output.pyt,
    "pyt"
  );

  validateString(
    output.exam,
    "exam"
  );

  validateString(
    output.deliverable,
    "deliverable"
  );

  if (
    !output.shared_visual_specifications ||
    typeof output.shared_visual_specifications !==
      "object" ||
    Array.isArray(
      output.shared_visual_specifications
    )
  ) {
    throw new Error(
      "shared_visual_specifications must be an object"
    );
  }

  const visual =
    output.shared_visual_specifications;

  validateString(
    visual.format,
    "shared_visual_specifications.format"
  );

  validateString(
    visual.art_style,
    "shared_visual_specifications.art_style"
  );

  validateString(
    visual.layout_rule,
    "shared_visual_specifications.layout_rule"
  );

  if (
    visual.reel_duration_seconds !== 60
  ) {
    throw new Error(
      "shared_visual_specifications.reel_duration_seconds must equal 60"
    );
  }

  if (
    !output.images ||
    typeof output.images !== "object" ||
    Array.isArray(output.images)
  ) {
    throw new Error(
      "images must be an object"
    );
  }

  const requiredImageKeys = [
    "image_1",
    "image_2",
    "image_3"
  ];

  const actualImageKeys =
    Object.keys(output.images).sort();

  if (
    JSON.stringify(actualImageKeys) !==
    JSON.stringify(requiredImageKeys)
  ) {
    throw new Error(
      "images must contain exactly image_1, image_2 and image_3"
    );
  }

  requiredImageKeys.forEach((key) => {
    validateImage(
      output.images[key],
      key
    );
  });

  const panelCount =
    requiredImageKeys.reduce(
      (total, key) =>
        total +
        output.images[key]
          .illustrated_panels.length,
      0
    );

  return {
    output,
    imageCount: 3,
    panelCount
  };
}

/* -------------------------------------------------------------------------- */
/*                          OPENAI GENERATION                                 */
/* -------------------------------------------------------------------------- */

async function generateInfographics(row) {
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
          instructions: SYSTEM_PROMPT,
          input: buildInput(row)
        });

      return validateInfographics(
        extractText(response)
      );
    } catch (error) {
      lastError = error;

      if (isFatalOpenAiError(error)) {
        throw error;
      }

      const canRetry =
        isRetryable(error) ||
        isValidationError(error);

      if (
        attempt === API_RETRIES ||
        !canRetry
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(
          Math.random() * 500
        );

      console.warn(
        `Retry ${attempt + 1}/${API_RETRIES} after ${delay} ms: ${errorText(error)}`
      );

      await sleep(delay);
    }
  }

  throw (
    lastError ||
    new Error(
      "Infographics generation failed"
    )
  );
}

/* -------------------------------------------------------------------------- */
/*                           LOCK MANAGEMENT                                  */
/* -------------------------------------------------------------------------- */

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
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

  if (error) {
    throw new Error(
      `Failed to release expired infographic locks: ${error.message}`
    );
  }
}

async function lockRow(candidate) {
  const lockedAt =
    new Date().toISOString();

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: true,
      [LOCK_AT_COL]: lockedAt
    })
    .eq("id", candidate.id)
    .eq(LOCK_COL, false)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .select(
      [
        "id",
        "subject",
        "serial_number",
        "topic",
        "number_of_times_asked",
        INPUT_COL,
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

  const { data, error } = await supabase
    .from(TABLE)
    .select("id,serial_number")
    .eq(LOCK_COL, false)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .order("serial_number", {
      ascending: true
    })
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending infographic rows: ${error.message}`
    );
  }

  if (!data?.length) {
    return [];
  }

  const results =
    await Promise.allSettled(
      data.map((candidate) =>
        lockRow(candidate)
      )
    );

  const claimedRows = [];

  for (const result of results) {
    if (
      result.status === "fulfilled" &&
      result.value
    ) {
      claimedRows.push(
        result.value
      );
    }

    if (
      result.status === "rejected"
    ) {
      console.error(
        "Row-lock error:",
        errorText(result.reason)
      );
    }
  }

  claimedRows.sort(
    (a, b) =>
      Number(a.serial_number) -
      Number(b.serial_number)
  );

  return claimedRows;
}

/* -------------------------------------------------------------------------- */
/*                        SAVE JSONB OUTPUT DIRECTLY                          */
/* -------------------------------------------------------------------------- */

async function saveSuccess(
  row,
  infographicJson
) {
  if (
    !infographicJson ||
    typeof infographicJson !== "object" ||
    Array.isArray(infographicJson)
  ) {
    throw new Error(
      "Infographics output must be a JSON object before saving"
    );
  }

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      /*
      IMPORTANT:
      infographics is a PostgreSQL jsonb column.

      Save the JavaScript object directly.
      Do not use JSON.stringify here.
      */
      [OUTPUT_COL]: infographicJson,
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
      `Failed to save infographics JSON: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or infographics already exist"
    );
  }
}

async function releaseLock(row) {
  if (
    !row?.id ||
    !row?.[LOCK_AT_COL]
  ) {
    return;
  }

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
      `Failed to release lock ${row.id}: ${error.message}`
    );
  }
}

/* -------------------------------------------------------------------------- */
/*                            PROCESS ONE ROW                                 */
/* -------------------------------------------------------------------------- */

async function processRow(row) {
  console.log(
    `Generating NEET SS Medicine infographics | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateInfographics(row);

    /*
    result.output is a JavaScript object.
    It is saved directly into the jsonb column.
    */
    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | images=${result.imageCount} | panels=${result.panelCount}`
    );

    return {
      fatalApiError: null
    };
  } catch (error) {
    await releaseLock(row);

    if (
      isFatalOpenAiError(error)
    ) {
      const fatalMessage =
        errorText(error);

      console.error(
        `FATAL OPENAI ERROR | ${row.subject} | ${row.serial_number} | ${row.topic}: ${fatalMessage}`
      );

      return {
        fatalApiError:
          fatalMessage
      };
    }

    console.error(
      `Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${errorText(error)}`
    );

    return {
      fatalApiError: null
    };
  }
}

/* -------------------------------------------------------------------------- */
/*                              PROCESS BATCH                                 */
/* -------------------------------------------------------------------------- */

async function processBatch(rows) {
  let nextIndex = 0;
  let fatalApiError = null;

  async function runner() {
    while (true) {
      if (fatalApiError) {
        return;
      }

      const index = nextIndex;

      if (index >= rows.length) {
        return;
      }

      nextIndex += 1;

      const result =
        await processRow(
          rows[index]
        );

      if (
        result.fatalApiError
      ) {
        fatalApiError =
          result.fatalApiError;

        return;
      }
    }
  }

  const runnerCount =
    Math.min(
      CONCURRENCY,
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

  if (fatalApiError) {
    const unstartedRows =
      rows.slice(nextIndex);

    await Promise.allSettled(
      unstartedRows.map((row) =>
        releaseLock(row)
      )
    );
  }

  return {
    fatalApiError
  };
}

/* -------------------------------------------------------------------------- */
/*                                 MAIN                                      */
/* -------------------------------------------------------------------------- */

async function main() {
  console.log(
    `NEET SS MEDICINE FLOWCHART -> INFOGRAPHICS WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | OutputType=JSONB | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY}`
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
        `Claimed ${rows.length} pending infographic row(s)`
      );

      const result =
        await processBatch(rows);

      if (
        result.fatalApiError
      ) {
        console.error(
          `Worker stopped because of fatal OpenAI authentication or billing error: ${result.fatalApiError}`
        );

        process.exitCode = 1;
        return;
      }
    } catch (error) {
      if (
        isFatalOpenAiError(error)
      ) {
        console.error(
          `Worker stopped because of fatal OpenAI authentication or billing error: ${errorText(error)}`
        );

        process.exitCode = 1;
        return;
      }

      console.error(
        "Worker loop error:",
        errorText(error)
      );

      await sleep(
        Math.max(
          LOOP_SLEEP_MS,
          3000
        )
      );
    }
  }
}

main().catch((error) => {
  console.error(
    "Fatal NEET SS Medicine infographics worker error:",
    error
  );

  process.exitCode = 1;
});
