"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "neetpg_pyt_source";
const INPUT_COL = "flowchart";
const OUTPUT_COL = "infographics";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

const REQUIRED_IMAGES = [
  "image_1",
  "image_2",
  "image_3"
];

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
  process.env.NEETPG_INFOGRAPHICS_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "NEETPG_INFOGRAPHICS_LIMIT",
  50,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "NEETPG_INFOGRAPHICS_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "NEETPG_INFOGRAPHICS_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "NEETPG_INFOGRAPHICS_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "NEETPG_INFOGRAPHICS_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = integerEnv(
  "NEETPG_INFOGRAPHICS_MAX_OUTPUT_TOKENS",
  16000,
  2000,
  50000
);

const WORKER_ID =
  process.env.NEETPG_INFOGRAPHICS_WORKER_ID ||
  `neetpg-infographics-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/*
PASTE YOUR COMPLETE PROMPT INSIDE THE COMMENT BELOW.

Markdown, JSON examples, quotation marks and backticks are safe here.

IMPORTANT:
The prompt must not contain the closing comment characters:
* followed immediately by /
*/
const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
this is the approach to a PYT in NEETPG Exam in algorithmic clinical pathway style  we are creating 3 Infographics which are A4 Sized Hand sCRIPTED REAL TIME COLOUR ILLUSTRATED PICNOMIC MIND MAP INFOGRAPHICS TO DO RAPID REVISION AND PRODUCE A 1 MINUTE reel per each infographic and 3 Minute Reel for each PYT out of 3 Infographics  So create the require text for each of these 3 Infographic Images from the notes on PYT given  give the Out put as JSON mentioning clearly as image 1 , image 2 , image 3  see to that most of High Yield rapid revision content of the nOTES IS DISTRIBUTED INTO THESE 3 iMAGES 
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

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE NEET PG INFOGRAPHICS PROMPT HERE"
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
    throw new Error(
      `${label} must be a non-empty string`
    );
  }

  return text;
}

function serializeFlowchart(value) {
  if (value === null || value === undefined) {
    throw new Error("flowchart is missing");
  }

  if (typeof value === "string") {
    const text = value.trim();

    if (!text) {
      throw new Error("flowchart is empty");
    }

    return text;
  }

  return JSON.stringify(value, null, 2);
}

function buildInput(row) {
  return [
    `PYT TOPIC: ${requiredText(row.topic, "Topic")}`,
    `SUBJECT: ${requiredText(row.subject, "Subject")}`,
    `SERIAL NUMBER: ${row.serial_number}`,
    `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`,
    "",
    "SOURCE CLINICAL FLOWCHART:",
    serializeFlowchart(row[INPUT_COL]),
    "",
    "Create the content plan for exactly three A4 portrait infographic images.",
    "Distribute the important source content logically across image_1, image_2 and image_3.",
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
      `${label} must contain exactly these keys: ${expectedKeys.join(", ")}`
    );
  }
}

function validateDuration(value, label) {
  if (value !== 60) {
    throw new Error(`${label} must be exactly 60`);
  }

  return value;
}

function validatePanel(panel, imageName, panelIndex) {
  const label =
    `${imageName} panel ${panelIndex + 1}`;

  validateExactKeys(
    panel,
    [
      "heading",
      "illustration",
      "text",
      "exam_trap"
    ],
    label
  );

  panel.heading = requiredText(
    panel.heading,
    `${label} heading`
  );

  panel.illustration = requiredText(
    panel.illustration,
    `${label} illustration`
  );

  if (
    !Array.isArray(panel.text) ||
    panel.text.length === 0
  ) {
    throw new Error(
      `${label} text must be a non-empty array`
    );
  }

  panel.text = panel.text.map(
    (item, textIndex) =>
      requiredText(
        item,
        `${label} text item ${textIndex + 1}`
      )
  );

  panel.exam_trap = requiredText(
    panel.exam_trap,
    `${label} exam_trap`
  );

  return panel;
}

function validateImage(image, imageName) {
  validateExactKeys(
    image,
    [
      "label",
      "title",
      "focus",
      "reel_duration_seconds",
      "illustrated_panels",
      "bottom_memory_lock",
      "narrative_flow"
    ],
    imageName
  );

  image.label = requiredText(
    image.label,
    `${imageName} label`
  );

  image.title = requiredText(
    image.title,
    `${imageName} title`
  );

  image.focus = requiredText(
    image.focus,
    `${imageName} focus`
  );

  validateDuration(
    image.reel_duration_seconds,
    `${imageName} reel_duration_seconds`
  );

  if (
    !Array.isArray(image.illustrated_panels) ||
    image.illustrated_panels.length === 0
  ) {
    throw new Error(
      `${imageName} illustrated_panels must be a non-empty array`
    );
  }

  image.illustrated_panels =
    image.illustrated_panels.map(
      (panel, index) =>
        validatePanel(panel, imageName, index)
    );

  image.bottom_memory_lock = requiredText(
    image.bottom_memory_lock,
    `${imageName} bottom_memory_lock`
  );

  image.narrative_flow = requiredText(
    image.narrative_flow,
    `${imageName} narrative_flow`
  );

  return image;
}

function validateInfographics(value) {
  validateExactKeys(
    value,
    [
      "pyt",
      "exam",
      "deliverable",
      "shared_visual_specifications",
      "images"
    ],
    "Infographics output"
  );

  value.pyt = requiredText(
    value.pyt,
    "pyt"
  );

  value.exam = requiredText(
    value.exam,
    "exam"
  );

  value.deliverable = requiredText(
    value.deliverable,
    "deliverable"
  );

  validateExactKeys(
    value.shared_visual_specifications,
    [
      "format",
      "art_style",
      "layout_rule",
      "reel_duration_seconds"
    ],
    "shared_visual_specifications"
  );

  const visual =
    value.shared_visual_specifications;

  visual.format = requiredText(
    visual.format,
    "shared_visual_specifications.format"
  );

  visual.art_style = requiredText(
    visual.art_style,
    "shared_visual_specifications.art_style"
  );

  visual.layout_rule = requiredText(
    visual.layout_rule,
    "shared_visual_specifications.layout_rule"
  );

  validateDuration(
    visual.reel_duration_seconds,
    "shared_visual_specifications.reel_duration_seconds"
  );

  validateExactKeys(
    value.images,
    REQUIRED_IMAGES,
    "images"
  );

  for (const imageName of REQUIRED_IMAGES) {
    value.images[imageName] = validateImage(
      value.images[imageName],
      imageName
    );
  }

  return value;
}

async function generateInfographics(row) {
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
      const validated = validateInfographics(parsed);

      return {
        json: validated,
        characters: rawText.length,
        panels:
          validated.images.image_1.illustrated_panels.length +
          validated.images.image_2.illustrated_panels.length +
          validated.images.image_3.illustrated_panels.length
      };
    } catch (error) {
      lastError = error;

      if (isCreditError(error)) {
        throw error;
      }

      const validationFailure =
        /empty output|JSON|Infographics|shared_visual|images|image_|panel|heading|illustration|exam_trap|memory_lock|narrative_flow|reel_duration|flowchart|exactly these keys|non-empty/i.test(
          errorText(error)
        );

      if (
        attempt === API_RETRIES ||
        (!isRetryable(error) && !validationFailure)
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
    new Error("NEET PG infographic generation failed")
  );
}

async function verifySchema() {
  const { error } = await supabase
    .from(TABLE)
    .select(
      [
        "id",
        INPUT_COL,
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
    Date.now() - LOCK_TTL_MIN * 60 * 1000
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
  const lockedAt = new Date().toISOString();

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
    .select("id,subject,serial_number,topic")
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

  const results = await Promise.allSettled(
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

async function saveSuccess(row, infographicsJson) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: infographicsJson,
      [LOCK_COL]: false,
      [LOCK_AT_COL]: null
    })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(LOCK_AT_COL, row[LOCK_AT_COL])
    .is(OUTPUT_COL, null)
    .select("id");

  if (error) {
    throw new Error(
      `Failed to save infographics: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or infographics already exist"
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
    .eq(LOCK_AT_COL, row[LOCK_AT_COL])
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `Failed to release lock ${row.id}: ${error.message}`
    );
  }
}

async function processRow(row) {
  console.log(
    `Generating NEET PG infographics | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result = await generateInfographics(row);

    await saveSuccess(row, result.json);

    console.log(
      `Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | images=3 | panels=${result.panels} | characters=${result.characters}`
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

      const result = await processRow(rows[index]);

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
      rows.slice(next).map(releaseLock)
    );
  }

  return {
    creditExhausted
  };
}

async function main() {
  await verifySchema();

  console.log(
    `NEET PG FLOWCHART -> INFOGRAPHICS WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY} | MaxOutputTokens=${MAX_OUTPUT_TOKENS}`
  );

  while (true) {
    try {
      const rows = await claimRows(PICKUP_LIMIT);

      if (!rows.length) {
        await sleep(LOOP_SLEEP_MS);
        continue;
      }

      console.log(
        `Claimed ${rows.length} pending infographic row(s)`
      );

      const result = await processBatch(rows);

      if (result.creditExhausted) {
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
        Math.max(LOOP_SLEEP_MS, 2000)
      );
    }
  }
}

main().catch((error) => {
  console.error(
    "Fatal NEET PG infographic worker error:",
    error
  );

  process.exit(1);
});
