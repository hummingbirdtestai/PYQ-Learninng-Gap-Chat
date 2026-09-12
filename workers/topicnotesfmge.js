require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────

const MODEL =
  process.env.TOPIC_NOTES_MODEL || "gpt-5-mini";

const LIMIT = parseInt(
  process.env.TOPIC_NOTES_LIMIT || "1",
  10
);

const BATCH_SIZE = parseInt(
  process.env.TOPIC_NOTES_CONCURRENCY || "1",
  10
);

const SLEEP_MS = parseInt(
  process.env.TOPIC_NOTES_IDLE_MS || "5000",
  10
);

const LOCK_TTL_MIN = parseInt(
  process.env.TOPIC_NOTES_LOCK_TTL_MIN || "30",
  10
);

const MAX_ATTEMPTS = parseInt(
  process.env.TOPIC_NOTES_MAX_ATTEMPTS || "3",
  10
);

const TABLE = "topic_notes_source";

const INPUT_COL = "combined_questions";
const OUTPUT_COL = "generated_notes";
const LOCK_COL = "notes_lock";
const LOCK_AT = "notes_locked_at";

console.log("🚀 TOPIC NOTES WORKER STARTED");
console.log(
  `⚙️ Model=${MODEL} | Pickup=${LIMIT} | Concurrent=${BATCH_SIZE}`
);

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// Paste your complete prompt between the backticks.
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`
This is a PYT , THERE IS FULL LIST OF cLINICAL vignette based and High Yield facts for NEETPG Prep , one need to have Notes as list of 5-8 Buzz words for Rapid revision
 Give that complete list as JSON with Mark down of highlighting 1-2 Words in Bold and Unicode for sUPER SCRIPTS , sUBSCRIPTS , SYMBOLS , MATH , GREEK LETTERS TO RENDER in RNW front end

give as Sub topics deivided and under each Subtopic the LIST OF cLINICAL vignette based and High Yield facts for NEETPG Prep

 GIVE OUTPUT IN THIS JSON STRUCTURE 

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

// ─────────────────────────────────────────────
// BUILD INPUT
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    `SUBJECT: ${row.subject}`,
    `EXACT TOPIC: ${row.topic}`,
    "",
    "SUPPLIED PYQs/PYTs/Q→A EXAMPLES:",
    row.combined_questions
  ].join("\n");
}

// ─────────────────────────────────────────────
// CALL OPENAI
// ─────────────────────────────────────────────

async function callOpenAI(row, attempt = 1) {
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
      response.choices?.[0]?.message?.content?.trim();

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
      const delay = 1000 * attempt;

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
// VALIDATE OUTPUT JSON
// ─────────────────────────────────────────────

function parseGeneratedOutput(rawOutput) {
  const cleaned = rawOutput
    .trim()
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
    .trim();

  let parsed;

  try {
    parsed = JSON.parse(cleaned);
  } catch (error) {
    throw new Error(
      `Invalid JSON: ${error.message}`
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
    typeof parsed.topic !== "string" ||
    !parsed.topic.trim()
  ) {
    throw new Error(
      "Generated JSON is missing topic"
    );
  }

  if (
    !Array.isArray(parsed.subtopics) ||
    parsed.subtopics.length === 0
  ) {
    throw new Error(
      "Generated JSON has no subtopics"
    );
  }

  let totalCards = 0;

  for (const group of parsed.subtopics) {
    if (
      !group ||
      typeof group.subtopic !== "string" ||
      !group.subtopic.trim()
    ) {
      throw new Error(
        "A subtopic name is missing"
      );
    }

    if (
      !Array.isArray(group.cards) ||
      group.cards.length === 0
    ) {
      throw new Error(
        `No cards found in subtopic: ${group.subtopic}`
      );
    }

    for (const card of group.cards) {
      if (
        !card ||
        typeof card.q !== "string" ||
        !card.q.trim() ||
        typeof card.a !== "string" ||
        !card.a.trim()
      ) {
        throw new Error(
          `Invalid Q→A card in: ${group.subtopic}`
        );
      }

      totalCards += 1;
    }
  }

  if (totalCards === 0) {
    throw new Error(
      "Generated output contains no cards"
    );
  }

  return {
    jsonText: JSON.stringify(
      parsed,
      null,
      2
    ),
    totalCards
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
      [LOCK_AT]: null,
      generation_status: "pending"
    })
    .eq(LOCK_COL, true)
    .eq("generation_status", "processing")
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
// No Supabase RPC required.
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
      course_id,
      subject_id,
      pyt_id,
      subject,
      topic,
      combined_questions,
      generation_attempts
    `)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .eq(LOCK_COL, false)
    .eq("generation_status", "pending")
    .eq("active", true)
    .lt(
      "generation_attempts",
      MAX_ATTEMPTS
    )
    .order(
      "created_at",
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

  const claimedRows = [];

  /*
   * Lock each row conditionally.
   * If another worker gets the row first,
   * the update returns no rows.
   */
  for (const row of availableRows) {
    const lockedAt =
      new Date().toISOString();

    const nextAttempt =
      Number(
        row.generation_attempts || 0
      ) + 1;

    const {
      data: lockedRows,
      error: lockError
    } = await supabase
      .from(TABLE)
      .update({
        [LOCK_COL]: true,
        [LOCK_AT]: lockedAt,
        generation_status: "processing",
        generation_attempts: nextAttempt,
        generation_error: null
      })
      .eq("id", row.id)
      .eq(LOCK_COL, false)
      .eq(
        "generation_status",
        "pending"
      )
      .is(OUTPUT_COL, null)
      .eq("active", true)
      .select(`
        id,
        course_id,
        subject_id,
        pyt_id,
        subject,
        topic,
        combined_questions,
        generation_attempts
      `);

    if (lockError) {
      console.error(
        `❌ Failed to lock ${row.id}:`,
        lockError.message
      );

      continue;
    }

    if (lockedRows?.length) {
      claimedRows.push(
        lockedRows[0]
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
  generatedNotes
) {
  const completedAt =
    new Date().toISOString();

  const {
    data: savedRows,
    error
  } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: generatedNotes,
      generation_status: "completed",
      generation_error: null,
      completed_at: completedAt,
      [LOCK_COL]: false,
      [LOCK_AT]: null
    })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(
      "generation_status",
      "processing"
    )
    .is(OUTPUT_COL, null)
    .select("id");

  if (error) {
    throw new Error(
      `Failed to save notes: ${error.message}`
    );
  }

  if (!savedRows?.length) {
    throw new Error(
      "Notes were not saved because the lock changed"
    );
  }
}

// ─────────────────────────────────────────────
// SAVE FAILURE
// ─────────────────────────────────────────────

async function saveFailure(
  row,
  processingError
) {
  const permanentFailure =
    Number(row.generation_attempts) >=
    MAX_ATTEMPTS;

  const errorMessage = String(
    processingError?.message ||
      processingError
  ).slice(0, 4000);

  const { error } = await supabase
    .from(TABLE)
    .update({
      generation_status:
        permanentFailure
          ? "failed"
          : "pending",

      generation_error: errorMessage,
      [LOCK_COL]: false,
      [LOCK_AT]: null
    })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(
      "generation_status",
      "processing"
    )
    .is(OUTPUT_COL, null);

  if (error) {
    console.error(
      `❌ Failed to record error for ${row.id}:`,
      error.message
    );
  }
}

// ─────────────────────────────────────────────
// PROCESS ONE TOPIC
// ─────────────────────────────────────────────

async function processRow(row) {
  try {
    console.log(
      `🧠 Generating | ${row.subject} | ${row.topic}`
    );

    const rawOutput =
      await callOpenAI(row);

    const {
      jsonText,
      totalCards
    } = parseGeneratedOutput(
      rawOutput
    );

    await saveSuccess(
      row,
      jsonText
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.topic} | ${totalCards} cards`
    );
  } catch (error) {
    console.error(
      `❌ Failed | ${row.subject} | ${row.topic}:`,
      error?.message || error
    );

    await saveFailure(
      row,
      error
    );
  }
}

// ─────────────────────────────────────────────
// PROCESS BATCHES
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
    "🧠 TOPIC NOTES WORKER RUNNING"
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
