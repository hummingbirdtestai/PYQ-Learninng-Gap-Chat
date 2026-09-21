"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "neetss_surgery_pyt_source";
const INPUT_COL = "jsonb_output";
const OUTPUT_COL = "notes_json";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

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
  process.env.NEETSS_SURGERY_NOTES_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "NEETSS_SURGERY_NOTES_LIMIT",
  50,
  1,
  100
);

const BATCH_SIZE = integerEnv(
  "NEETSS_SURGERY_NOTES_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "NEETSS_SURGERY_NOTES_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "NEETSS_SURGERY_NOTES_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "NEETSS_SURGERY_NOTES_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.NEETSS_SURGERY_NOTES_WORKER_ID ||
  `neetss-surgery-notes-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

const SYSTEM_PROMPT = `
This is a PYT , THERE IS FULL LIST OF cLINICAL vignette based and High Yield facts for NEETSS Surgery Prep , one need to have Notes as list of 5-8 Buzz words for Rapid revision Give that complete list as JSON with Mark down of highlighting 1-2 Words in Bold and Unicode for sUPER SCRIPTS , sUBSCRIPTS , SYMBOLS , MATH , GREEK LETTERS TO RENDER in RNW front end give as Sub topics deivided and under each Subtopic the LIST OF cLINICAL vignette based and High Yield facts for NEETPG Prep . Strict rule : 1) For every PYT deliberately extracT not just direct facts, but also diagnostic clues, differentiators, next-best-step, contraindications, adverse effects, mechanisms, exceptions and examiner traps similiar to NBME , USMLE DEEP CONTENT for 2 level MCQ analysis For the specific purpose of a “Rapid Revision Notes” tab, the high yield facts should not be too long overall. The individual notes should not be full explanatory sentences rather they should be 5–8-word visual recall triggers. The challenge is compression . Target roughly 5–10 words per note, with one testable idea per line. 1 note = 1 examinable fact → 5–10 words → 1–2 bold buzzwords THE FOLLOWING IS EXAMPLE Current: "Temporal-lobe epilepsy may cause wandering, but usually has impaired awareness/stereotyped episodes rather than organized fugue" Better: "TLE wandering → stereotyped + impaired awareness" Current: "Diagnosis requires exclusion of substance use, seizures, head injury, delirium and neurocognitive disorders" Better: "Fugue DDx → substances, seizures, head injury, delirium" Current: "Grandiosity continuously since early adulthood → narcissistic PD; grandiosity with ↓ sleep + pressured speech → mania" Better: "Chronic grandiosity → *NPD; episodic + ↓ sleep → *mania" never fall short of true AMBOSS-level depth —it is the clinical decision linkage. AMBOSS-quality material would more deliberately connect the finding to recognizable disease contexts and mechanism-based discrimination. For example: postherpetic neuralgia + clothing hurts → allodynia; diabetic neuropathy + bedsheet hurts → tactile allodynia; migraine + scalp/hair brushing hurts → central sensitization; CRPS + gentle touch produces severe pain → allodynia. Those are the kinds of clues that convert a memorized definition into a 2-level vignette answer. There should not be redundancy. Create exceptionally discriminating notes. Instead of repetitions aim for disease-linked clinical clues, mechanism traps, ,where they genuinely help differentiate an MCQ Adding more content would actually make the Rapid Revision tab worse. What is missing is a small number of more discriminating clinical relationships. For example, the highest-quality notes would deliberately distinguish: Early severe asthma: tachypnea → hyperventilation → PaCO₂ ↓ versus Deteriorating asthma: respiratory fatigue → PaCO₂ normalizes/↑ That is exactly the kind of physiological transition an examiner can hide inside a vignette. Similarly: Wheeze ↓ + dyspnea ↓ → **improvement** versus Wheeze ↓ + air entry ↓ → **impending failure** That distinction is more valuable than having three separate notes saying silent chest is severe. Another excellent 2-level linkage would be: Ventilated asthma + hypotension + high pressures → **dynamic hyperinflation** followed by the differentiator: Sudden hypotension + unilateral absent sounds → **pneumothorax** Now the student isn't merely recalling complications—they're discriminating between two causes of sudden deterioration. The standard I would use for RevisionCart For every PYT, your worker should aim for roughly this hierarchy: Direct PYT fact → vignette clue → mechanism → differentiator → next-best-step → exception/trap → complication/management linkage. But only include a category when it adds a genuinely new examinable decision. So instead of 60–80 repetitive facts, I would prefer approximately 30–45 exceptionally discriminating notes per PYT, depending on topic breadth. A good RevisionCart note should pass this test: Can this 5–10-word line help the student answer an MCQ that requires one extra inference beyond simple recall? For example: Silent chest → **minimal airflow**, not clinical improvement is good. Normal PaCO₂ + severe distress → **respiratory fatigue** is better. Wheeze ↓ + air entry ↓ → **impending failure** is excellent. Ventilated asthma + hypotension → **dynamic hyperinflation** is excellent. Hypotension + unilateral absent sounds → **pneumothorax** is excellent. Those last three are where your notes start approaching AMBOSS/UWorld-style clinical discrimination while retaining First Aid-like compression. So I would keep your current JSON architecture and note-length rule exactly as they are, but tighten the generation instruction to prioritize disease-linked clues and competing-diagnosis discrimination over repeated direct facts original hierarchy is correct: PYT fact → clinical clue → mechanism → discriminator → management decision → trap/exception → complication But not every PYT needs every category. For this particular PYT, I would rather have around 25–30 exceptional notes than the current ~50. The worker should ask of every generated line: “Does this note enable an additional MCQ decision that another note does not?” If no → delete it. That single rule would probably improve your generated notes more than adding additional medical content. The next improvement should be stronger deduplication and more contrastive 2-step decision links, not more facts. Every additional note must create a new MCQ decision. If another note already allows the same decision, delete it. Make notes more contrastive. A useful target mix would be roughly 20% core recall, 30% vignette recognition, 20% mechanism-linked inference, 20% differentiators/examiner traps, and 10% next-step/management. It doesn't need to be mechanically enforced, but it captures the desired character. Prefer decision density over fact density. Every note must either answer the PYT, identify a vignette presentation, explain a mechanism needed for inference, distinguish a plausible competing answer, change the next diagnostic/management decision, or expose an examiner trap. Delete isolated background facts that do none of these. Two medically different facts are still redundant if they lead to the same MCQ decision. Keep the more clinically discriminating one. DO not increase the amount of content , delete synonymous decision pathways and spend those tokens on exceptions, competing-test discrimination, and vignette-changing qualifiers. First Aid compression + AMBOSS clinical connections + UWorld/NBME-style discrimination → in 5–10-word recall triggers. That would be a genuinely strong format for NEETSS Surgery ABSITE Americal Board surgery exam last-mile revision. GIVE OUTPUT IN THIS JSON STRUCTURE { "topic": "string", "subtopics": [ { "subtopic": "string", "notes": [ "string", "string" ] } ] }
`.trim();

if (!SYSTEM_PROMPT) {
  throw new Error("SYSTEM_PROMPT cannot be empty");
}

const NOTES_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["topic", "subtopics"],
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
        required: ["subtopic", "notes"],
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

const sleep = (milliseconds) =>
  new Promise((resolve) =>
    setTimeout(resolve, milliseconds)
  );

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

function serializeInput(value) {
  if (typeof value === "string") {
    return value;
  }

  return JSON.stringify(value, null, 2);
}

function buildUserInput(row) {
  return [
    `SUBJECT: ${row.subject}`,
    `TOPIC SERIAL NUMBER: ${row.serial_number}`,
    `EXACT TOPIC/PYT: ${row.topic}`,
    `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`,
    "",
    "SUPPLIED NEET-SS SURGERY FLASHCARDS JSON:",
    serializeInput(row[INPUT_COL])
  ].join("\n");
}

function extractResponseText(response) {
  if (
    typeof response?.output_text === "string" &&
    response.output_text.trim()
  ) {
    return response.output_text.trim();
  }

  const collected = [];

  for (const outputItem of response?.output || []) {
    for (const contentItem of outputItem?.content || []) {
      if (
        contentItem?.type === "output_text" &&
        typeof contentItem.text === "string"
      ) {
        collected.push(contentItem.text);
      }
    }
  }

  const text = collected.join("\n").trim();

  if (!text) {
    throw new Error("OpenAI returned empty output");
  }

  return text;
}

function cleanJsonText(value) {
  return String(value)
    .trim()
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();
}

function countWords(value) {
  return String(value)
    .replace(/\*\*/g, "")
    .trim()
    .split(/\s+/u)
    .filter(Boolean)
    .length;
}

function validateAndNormalize(rawOutput, expectedTopic) {
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
      "Generated output must be one JSON object"
    );
  }

  if (
    !Array.isArray(parsed.subtopics) ||
    parsed.subtopics.length === 0
  ) {
    throw new Error(
      "Generated JSON contains no subtopics"
    );
  }

  const seenNotes = new Set();
  let noteCount = 0;

  const subtopics = parsed.subtopics.map(
    (group, groupIndex) => {
      if (
        !group ||
        typeof group !== "object" ||
        Array.isArray(group)
      ) {
        throw new Error(
          `Subtopic ${groupIndex + 1} is not an object`
        );
      }

      const subtopic = String(
        group.subtopic || ""
      ).trim();

      if (!subtopic) {
        throw new Error(
          `Subtopic ${groupIndex + 1} has no name`
        );
      }

      if (
        !Array.isArray(group.notes) ||
        group.notes.length === 0
      ) {
        throw new Error(
          `Subtopic '${subtopic}' contains no notes`
        );
      }

      const notes = group.notes.map(
        (item, noteIndex) => {
          const note = String(
            item || ""
          ).trim();

          if (!note) {
            throw new Error(
              `Empty note in '${subtopic}' at position ${noteIndex + 1}`
            );
          }

          const normalizedKey = note
            .replace(/\*\*/g, "")
            .replace(/\s+/g, " ")
            .toLowerCase();

          if (seenNotes.has(normalizedKey)) {
            throw new Error(
              `Duplicate note detected: ${note}`
            );
          }

          seenNotes.add(normalizedKey);
          noteCount += 1;

          return note;
        }
      );

      return {
        subtopic,
        notes
      };
    }
  );

  if (noteCount < 10) {
    throw new Error(
      `Generated only ${noteCount} notes; at least 10 required`
    );
  }

  /*
   * Do not reject medically useful output for a
   * minor word-count violation. Report a warning
   * instead of creating a repeated retry loop.
   */
  const offLengthCount = subtopics
    .flatMap((group) => group.notes)
    .filter((note) => {
      const words = countWords(note);

      return (
        words < 5 ||
        words > 10
      );
    })
    .length;

  if (offLengthCount > 0) {
    console.warn(
      `⚠️ ${offLengthCount}/${noteCount} notes fall outside the 5–10-word target`
    );
  }

  return {
    output: {
      /*
       * Preserve the exact database topic rather
       * than accepting a model-renamed topic.
       */
      topic: expectedTopic,
      subtopics
    },
    subtopicCount: subtopics.length,
    noteCount
  };
}

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

          instructions: SYSTEM_PROMPT,

          input: buildUserInput(row),

          text: {
            format: {
              type: "json_schema",
              name: "neetss_surgery_rapid_notes",
              strict: true,
              schema: NOTES_SCHEMA
            }
          }
        });

      return validateAndNormalize(
        extractResponseText(response),
        row.topic
      );
    } catch (error) {
      lastError = error;

      if (isCreditError(error)) {
        throw error;
      }

      if (
        attempt === API_RETRIES ||
        !isRetryable(error)
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(Math.random() * 300);

      console.warn(
        `⚠️ API retry ${attempt + 1}/${API_RETRIES} after ${delay} ms`
      );

      await sleep(delay);
    }
  }

  throw lastError;
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
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

  if (error) {
    throw new Error(
      `Failed to release expired locks: ${error.message}`
    );
  }
}

async function lockOneRow(row) {
  const lockedAt =
    new Date().toISOString();

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [LOCK_COL]: true,
      [LOCK_AT_COL]: lockedAt
    })
    .eq("id", row.id)
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
      `Failed to lock row ${row.id}: ${error.message}`
    );
  }

  return data || null;
}

async function claimRows(limit) {
  await releaseExpiredLocks();

  const { data: candidates, error } =
    await supabase
      .from(TABLE)
      .select(
        [
          "id",
          "subject",
          "serial_number",
          "topic",
          "number_of_times_asked"
        ].join(",")
      )
      .not(INPUT_COL, "is", null)
      .is(OUTPUT_COL, null)
      .eq(LOCK_COL, false)
      .order("serial_number", {
        ascending: true
      })
      .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending rows: ${error.message}`
    );
  }

  if (!candidates?.length) {
    return [];
  }

  const results =
    await Promise.allSettled(
      candidates.map((row) =>
        lockOneRow(row)
      )
    );

  const claimed = [];

  for (const result of results) {
    if (
      result.status === "fulfilled" &&
      result.value
    ) {
      claimed.push(result.value);
    } else if (
      result.status === "rejected"
    ) {
      console.error(
        "❌ Row-lock error:",
        errorText(result.reason)
      );
    }
  }

  return claimed;
}

async function saveSuccess(
  row,
  generatedOutput
) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: generatedOutput,
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
      `Failed to save notes: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the row lock changed or notes already exist"
    );
  }
}

async function releaseRowLock(row) {
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
      `❌ Failed to release lock ${row.id}: ${error.message}`
    );
  }
}

async function processRow(row) {
  console.log(
    `🧠 Generating notes | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateNotes(row);

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.serial_number} | Subtopics=${result.subtopicCount} | Notes=${result.noteCount}`
    );

    return {
      creditExhausted: false
    };
  } catch (error) {
    await releaseRowLock(row);

    if (isCreditError(error)) {
      console.error(
        "🛑 OpenAI credits exhausted. Worker will stop safely."
      );

      return {
        creditExhausted: true
      };
    }

    console.error(
      `❌ Failed | ${row.subject} | ${row.serial_number} | ${row.topic}: ${errorText(error)}`
    );

    return {
      creditExhausted: false
    };
  }
}

async function releaseClaimedRows(rows) {
  await Promise.allSettled(
    rows.map((row) =>
      releaseRowLock(row)
    )
  );
}

async function processWithConcurrency(rows) {
  let nextIndex = 0;
  let creditExhausted = false;

  async function runner() {
    while (
      nextIndex < rows.length &&
      !creditExhausted
    ) {
      const index = nextIndex;
      nextIndex += 1;

      const result =
        await processRow(
          rows[index]
        );

      if (result.creditExhausted) {
        creditExhausted = true;
      }
    }
  }

  await Promise.all(
    Array.from(
      {
        length: Math.min(
          BATCH_SIZE,
          rows.length
        )
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

async function main() {
  console.log(
    `🚀 NEET-SS SURGERY NOTES WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Pickup=${PICKUP_LIMIT} | Concurrent=${BATCH_SIZE}`
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
        `📥 Claimed ${rows.length} topic(s)`
      );

      const result =
        await processWithConcurrency(
          rows
        );

      if (result.creditExhausted) {
        console.error(
          "🛑 Worker stopped because API credits are unavailable."
        );

        process.exit(1);
      }
    } catch (error) {
      if (isCreditError(error)) {
        console.error(
          "🛑 Worker stopped: OpenAI credits exhausted."
        );

        process.exit(1);
      }

      console.error(
        "❌ Worker loop error:",
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
    "❌ Fatal NEET-SS Surgery notes worker error:",
    error
  );

  process.exit(1);
});
