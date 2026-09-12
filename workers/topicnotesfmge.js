require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// SETTINGS
// ─────────────────────────────────────────────

const MODEL =
  process.env.TOPIC_NOTES_MODEL || "gpt-5-mini";

const CLAIM_LIMIT = parseIntegerEnv(
  "TOPIC_NOTES_LIMIT",
  5,
  1,
  50
);

const CONCURRENCY = parseIntegerEnv(
  "TOPIC_NOTES_CONCURRENCY",
  2,
  1,
  10
);

const IDLE_MS = parseIntegerEnv(
  "TOPIC_NOTES_IDLE_MS",
  5000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "TOPIC_NOTES_LOCK_TTL_MIN",
  30,
  5,
  240
);

const MAX_ATTEMPTS = parseIntegerEnv(
  "TOPIC_NOTES_MAX_ATTEMPTS",
  3,
  1,
  10
);

const API_RETRIES = parseIntegerEnv(
  "TOPIC_NOTES_API_RETRIES",
  2,
  0,
  5
);

const TABLE = "topic_notes_source";

const WORKER_ID =
  process.env.WORKER_ID ||
  `topic-notes-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// Paste your complete prompt between the backticks.
// Do not add the Subject, Topic or questions here.
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = String.raw`
You are an expert medical board-review editor for NEET-PG, FMGE, INI-CET and USMLE Step 1/2 CK.

Benchmark:

NEET-PG/FMGE yield + First Aid breadth + UWorld reasoning + NBME discrimination + AMBOSS depth only when exam-relevant.

INPUT: ONE exact Topic + PYQs/PYTs/Q→A examples.

TASK: Generate a complete but aggressively high-yield, high-discrimination, non-redundant Rapid Revision Q→A bank for that exact topic.

This is for rapid exam revision, not textbook study.

PYQs = LOCKED KNOWLEDGE ANCHORS

First silently extract every distinct tested knowledge unit from the supplied PYQs.

Every distinct PYQ-tested fact MUST survive in the final bank.

You may:

rewrite weak PYQs into stronger board-style retrieval

combine duplicate or closely related PYQs

integrate mechanism/discrimination

compress several PYQs into fewer stronger cards

You may NOT:

delete a distinct tested fact because it seems elementary

replace it with a harder adjacent fact

assume a broad card implicitly covers it

sacrifice tested knowledge during deduplication

Preserve PYQ knowledge, NOT one card per PYQ.

A PYQ concept counts as preserved only when the final card explicitly requires recall of that tested knowledge.

Therefore:

100% PYQ coverage ≠ 100% PYQs as separate cards.

Compress related PYQs into the smallest number of cards that preserves every independently testable fact.

A PYQ guarantees coverage, not its own card.

PYQs are anchors, not the syllabus.

After locking them, independently add the core board-testable knowledge needed to solve realistic questions from the exact topic.

If a PYQ touches a neighboring topic, preserve that tested fact but do not expand the neighboring topic unless naturally within scope.

SILENT CORE EXAM MAP

Before writing, silently map:

core architecture → natural subtopics → classic presentations → mechanisms → discriminators → investigations → algorithms → treatments → complications → classifications/scores/cutoffs → exceptions

Priority:

locked PYQs + core concepts + high-frequency associations + decisive discriminators + decision-changing branches > secondary facts > advanced details

Never expand peripheral material while a core concept remains absent.

Advanced knowledge may supplement but NEVER replace a simpler, higher-yield core fact.

STRICT BOARD-VALUE GATE

Every NON-PYQ card must plausibly improve performance on a difficult but standard board MCQ.

Normally include only facts satisfying ≥2:

frequently/repeatedly tested

classic stem clue/buzzword

commonly confused or forgotten

decisive differential discriminator

changes diagnosis/investigation/management

mechanism required to predict a finding

classic toxicity/complication/exception

important image/pathology/lab association

board-relevant numerical criterion/cutoff

necessary for a major algorithm

If not → DELETE.

Do not include a card merely because the fact is medically correct.

Completeness = exam completeness, not textbook completeness.

NEET-PG/FMGE PRIORITY FILTER

Use UWorld/NBME/AMBOSS to improve reasoning and discrimination, NOT to inflate coverage.

For every optional expansion ask:

“Will remembering this materially improve NEET-PG/FMGE/INI-CET board-question solving?”

Exclude unless clearly exam-relevant:

specialist guideline minutiae

rare associations

obscure second-line therapies

exhaustive complications

tertiary-care nuances

decorative mechanisms

generic clinical common sense

low-frequency post-management details

textbook completeness facts

When two facts compete for revision space, retain the one with greater exam strike value.

Do not chase AMBOSS-level obscurity at the expense of First Aid/NEET-PG/FMGE-level completeness.

BUZZWORD/STEM-FIRST RETRIEVAL

Prefer:

high-information clue(s) → one best answer

Ideal patterns:

presentation + discriminator → diagnosis

image/pathology clue → diagnosis

mechanism → predicted finding

test/result + context → interpretation/next step

drug + toxicity → antidote/action

organism + discriminator → organism

lesion + deficit → localization

two competing entities + decisive clue → correct entity

severity/stability + finding → management

Avoid weak prompts such as:

“What is X?”

“What does X do?”

“Which classification is used?”

“What happens after X?”

unless the fact is a locked PYQ or intrinsically board-critical.

Never use Yes/No, True/False or similarly weak retrieval cards.

If the answer is obvious from the topic title, wording or common sense, upgrade or delete the card.

DISCRIMINATION > FLAT RECALL

For every card silently ask:

“What would a prepared student confuse this with?”

Prioritize distinctions between:

diagnosis A vs B

organism A vs look-alike

drug A vs similar drug

lesion/localization alternatives

screening vs diagnostic test

next-best vs confirmatory/gold-standard test

initial vs definitive treatment

stable vs unstable patient

indication vs contraindication

expected vs pathological finding

stage/grade/class consequences

common exception/reversal

Direct recall remains appropriate for board-critical:

anatomy, genes, enzymes, markers, antibodies, organisms, toxins, antidotes, derivatives, drugs, equations, numerical criteria and classic associations.

ALGORITHMS MUST BE RECONSTRUCTABLE

For diagnostic/management topics, include enough information to reconstruct the major exam-relevant decision pathway:

clinical state → probability/severity → first action → test → result → interpretation → next action → definitive treatment → major failure/complication branch

Include only branches that can realistically change an MCQ answer.

For emergencies prioritize:

stability → immediate life-saving action → diagnosis → definitive management

For scores/classifications/equations:

Do not merely ask their names.

Include the decision-relevant components/cutoffs AND consequence when board-relevant.

Do not exhaustively reproduce specialist scoring systems unless their individual components are themselves commonly tested.

MECHANISM → PREDICTION

Mechanisms must explain or predict an examinable finding.

Prefer integrated reasoning chains:

gene defect → missing protein/enzyme → phenotype

drug/receptor target → physiological effect → clinical effect/toxicity

immune mechanism → tissue injury → morphology/disease

anatomic lesion → affected structure → deficit

enzyme defect → metabolite change → clinical/lab finding

physicochemical property → pharmacological behavior → clinical consequence

Bundle facts belonging to one reasoning chain.

Do NOT manufacture depth by splitting one mechanism into multiple trivia cards.

Keep facts separate only when each independently determines a realistic MCQ answer.

PYQ → FUTURE QUESTION RULE

The bank must accomplish BOTH:

A. Guarantee recall of every distinct PYQ-tested fact

AND

B. Prepare for likely new questions from the same topic.

For each PYQ cluster silently ask:

“What is the most likely examiner variation, reversal, discriminator, mechanism or next-step question?”

Add such variations only when they pass the Board-Value Gate.

Do NOT mechanically generate:

reverse questions

harder versions

mechanism cards

complication cards

for every PYQ.

Expansion must earn its revision value.

FACTUAL-INTEGRITY GATE

Before accepting every card verify:

association ≠ causation

suggestive ≠ pathognomonic

preferred ≠ mandatory

not recommended ≠ contraindicated

diagnostic ≠ confirmatory

screening ≠ diagnostic

initial ≠ definitive

first-line ≠ only therapy

risk ≠ inevitable outcome

historical teaching ≠ current standard

If an action depends on a condition, include that condition in the question.

Use current accepted board-level science and stable criteria.

Avoid unstable specialist-guideline minutiae.

If a supplied PYQ tests obsolete or disputed teaching, preserve the tested knowledge but clearly frame it as:

historical/older/source-keyed teaching

when necessary.

Never convert an obsolete PYQ answer into an unqualified current medical rule.

SEMANTIC DEDUPLICATION — AGGRESSIVE

Deduplicate by knowledge required, not wording.

Remove:

direct/reverse duplicates

recall-vs-vignette duplicates

repeated clue→answer pathways

same fact repeated across subtopics

trivial fragmentation

multiple cards teaching one reasoning chain

If knowing Card A automatically answers Card B without additional board-relevant knowledge, keep the stronger card.

Examples of preferred compression:

Na⁺-channel blockade + ↓ Na⁺ influx → one mechanism card

vasoconstriction → ↓ systemic absorption → prolonged action → one reasoning card

several drugs repeatedly tested as ester/amide → compact classification retrieval rather than one unnecessary card per drug

But never compress so aggressively that an independently testable PYQ fact becomes only implicit.

Final compression test:

“Can two or more cards become one without losing an independently retrievable exam answer?”

If YES → combine them.

PYQ protection prevents knowledge loss; it does not justify redundancy.

CARD-BUDGET RULE

Every card consumes revision time.

Before adding any card ask:

“Would I want a NEET-PG/FMGE student to spend 10 seconds revising this instead of another missing fact?”

If NO → exclude it.

Before adding an advanced fact ask:

“Is there a more exam-important missing fact from this exact topic?”

If YES → add the stronger fact instead.

A fact being medically important does NOT automatically justify a card.

A fact being PYQ-derived guarantees coverage, not necessarily an independent card.

There is no fixed card count.

Do not inflate small topics.

Do not under-cover large topics.

Fewer exceptional cards > many mixed-quality cards.

QUESTION DESIGN

Questions should usually be 8–16 words.

Use compressed board-stem/buzzword style.

Markdown-bold exactly 1–2 decisive clues.

Each question should test one independently retrievable exam unit.

Use mini-vignettes only when inference improves discrimination.

Remove unnecessary demographics, history and filler.

The ideal question resembles the decisive 1–2 lines extracted from a difficult MCQ stem.

Never reveal or nearly reconstruct the answer in the question.

ANSWER DESIGN

Answers should usually be 1–5 words.

Use the most precise:

diagnosis / drug / test / organism / mechanism / lesion / structure / marker / criterion / action / complication

No explanation in answers.

Avoid vague wording.

Use Unicode directly:

α β γ ↑ ↓ → ≤ ≥ Na⁺ K⁺ Ca²⁺ HCO₃⁻ CO₂.

FINAL FOUR-PASS AUDIT

Perform silently before output.

PASS 1 — PYQ LOCK

Confirm:

100% of distinct PYQ-tested knowledge survives

exact duplicate PYQs are compressed

no PYQ fact became merely implicit

no obsolete PYQ is presented as an unqualified current rule

PASS 2 — CORE COVERAGE

Ask:

“Could a difficult but standard board MCQ require a CORE fact from this exact topic that is absent?”

If YES → add it.

Ensure relevant:

classic presentations/buzzwords

discriminators

mechanisms

images/pathology/labs

investigations

algorithms

treatments

complications/toxicities

classifications/scores/cutoffs

important exceptions

are covered.

Do not add a category merely because it appears in this checklist; include it only when naturally relevant to the topic.

PASS 3 — BOARD QUALITY

For each NON-PYQ card ask:

“Does this help the student recognize, distinguish, predict, interpret or decide?”

If it is merely easy factual recall without strong board value → upgrade or delete.

Then ask:

“Did UWorld/AMBOSS-style expansion dilute NEET-PG/FMGE revision value?”

If YES → remove the excess.

PASS 4 — MAXIMUM COMPRESSION

For every card ask:

“If this disappeared, would realistic exam-solving ability decrease?”

If NO → DELETE.

Then ask:

“Can two cards be collapsed without losing an independently testable answer?”

If YES → COLLAPSE.

Finally verify:

Did any advanced fact replace a simpler but more important core fact?
→ Restore the core fact.

Is any major discriminator or decision branch still absent?
→ Add it.

Is any knowledge repeated in different wording?
→ Keep the stronger retrieval.

Is any card present mainly because it is medically interesting?
→ Delete it.

The final bank must feel:

PYQ-secure + NEET-PG/FMGE-optimized + First Aid-complete + UWorld-reasoned + NBME-discriminating + aggressively compressed.

The goal is NOT the largest possible bank.

The goal is:

the smallest bank that preserves all PYQ-tested knowledge and the core high-yield knowledge needed to solve difficult standard questions from the exact topic.

Every card must earn its revision time.

OUTPUT

Return ONLY valid JSON inside ONE code block:

{
"topic": "Exact topic title",
"subtopics": [
{
"subtopic": "Specific examinable subgroup",
"cards": [
{
"q": "Compressed question with 1–2 decisive clues?",
"a": "Precise buzzword answer"
}
]
}
]
}

No introduction, commentary, scoring, citations or explanations outside JSON.
`.trim();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT === "PASTE YOUR COMPLETE SYSTEM PROMPT HERE"
) {
  throw new Error(
    "Paste the complete system prompt into SYSTEM_PROMPT"
  );
}

// ─────────────────────────────────────────────
// GENERAL HELPERS
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
      `${name} must be an integer from ${min} to ${max}`
    );
  }

  return value;
}

const sleep = (milliseconds) =>
  new Promise((resolve) =>
    setTimeout(resolve, milliseconds)
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
// BUILD USER INPUT
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
// OPENAI CALL
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
        await openai.chat.completions.create({
          model: MODEL,

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

      const rawOutput =
        response.choices?.[0]?.message?.content?.trim();

      if (!rawOutput) {
        throw new Error(
          "OpenAI returned empty content"
        );
      }

      return validateAndNormalizeOutput(rawOutput);
    } catch (error) {
      lastError = error;

      const retriesExhausted =
        attempt === API_RETRIES;

      if (
        !isRetryable(error) ||
        retriesExhausted
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(Math.random() * 250);

      console.warn(
        `⚠️ API retry ${attempt + 1}/${API_RETRIES} after ${delay} ms`
      );

      await sleep(delay);
    }
  }

  throw lastError;
}

// ─────────────────────────────────────────────
// VALIDATE GENERATED JSON
// ─────────────────────────────────────────────

function validateAndNormalizeOutput(rawOutput) {
  const cleaned = rawOutput
    .replace(/^\s*```(?:json)?\s*/i, "")
    .replace(/\s*```\s*$/i, "")
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
      "Generated JSON must contain a non-empty subtopics array"
    );
  }

  let totalCards = 0;

  for (const group of parsed.subtopics) {
    if (
      !group ||
      typeof group !== "object" ||
      typeof group.subtopic !== "string" ||
      !group.subtopic.trim()
    ) {
      throw new Error(
        "Every subtopic requires a non-empty name"
      );
    }

    if (
      !Array.isArray(group.cards) ||
      group.cards.length === 0
    ) {
      throw new Error(
        `Subtopic "${group.subtopic}" has no cards`
      );
    }

    for (const card of group.cards) {
      if (
        !card ||
        typeof card !== "object" ||
        typeof card.q !== "string" ||
        !card.q.trim() ||
        typeof card.a !== "string" ||
        !card.a.trim()
      ) {
        throw new Error(
          `Invalid Q→A card in "${group.subtopic}"`
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

  /*
   * generated_notes is a PostgreSQL TEXT column.
   * Store formatted, valid JSON text.
   */
  return JSON.stringify(parsed, null, 2);
}

// ─────────────────────────────────────────────
// CLAIM JOBS ATOMICALLY
// Requires claim_topic_notes_jobs() RPC.
// ─────────────────────────────────────────────

async function claimJobs() {
  const { data, error } = await supabase.rpc(
    "claim_topic_notes_jobs",
    {
      p_worker_id: WORKER_ID,
      p_limit: CLAIM_LIMIT,
      p_lock_ttl_minutes: LOCK_TTL_MIN,
      p_max_attempts: MAX_ATTEMPTS
    }
  );

  if (error) {
    throw new Error(
      `Failed to claim rows: ${error.message}`
    );
  }

  return data || [];
}

// ─────────────────────────────────────────────
// SAVE SUCCESS
// Only the worker owning the lock can save.
// ─────────────────────────────────────────────

async function saveSuccess(row, generatedNotes) {
  const completedAt = new Date().toISOString();

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      generated_notes: generatedNotes,
      generation_status: "completed",
      generation_error: null,
      completed_at: completedAt,
      locked_at: null,
      locked_by: null
    })
    .eq("id", row.id)
    .eq("generation_status", "processing")
    .eq("locked_by", WORKER_ID)
    .is("generated_notes", null)
    .select("id");

  if (error) {
    throw new Error(
      `Failed to save generated notes: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because this worker no longer owns the lock"
    );
  }
}

// ─────────────────────────────────────────────
// SAVE FAILURE
// Retry until MAX_ATTEMPTS is reached.
// ─────────────────────────────────────────────

async function saveFailure(row, processingError) {
  const permanentFailure =
    row.generation_attempts >= MAX_ATTEMPTS;

  const errorMessage = String(
    processingError?.message || processingError
  ).slice(0, 4000);

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      generation_status: permanentFailure
        ? "failed"
        : "pending",

      generation_error: errorMessage,
      locked_at: null,
      locked_by: null
    })
    .eq("id", row.id)
    .eq("generation_status", "processing")
    .eq("locked_by", WORKER_ID)
    .select("id");

  if (error) {
    console.error(
      `❌ Could not record failure for ${row.id}:`,
      error.message
    );

    return;
  }

  if (!data?.length) {
    console.warn(
      `⚠️ Failure not saved: worker no longer owns ${row.id}`
    );
  }
}

// ─────────────────────────────────────────────
// PROCESS ONE TOPIC
// ─────────────────────────────────────────────

async function processJob(row) {
  try {
    const generatedNotes =
      await generateNotes(row);

    await saveSuccess(
      row,
      generatedNotes
    );

    console.log(
      `✅ Completed | ${row.subject} | ${row.topic}`
    );
  } catch (error) {
    console.error(
      `❌ Failed | ${row.subject} | ${row.topic} |`,
      error?.message || error
    );

    await saveFailure(row, error);
  }
}

// ─────────────────────────────────────────────
// CONTROL CONCURRENCY
// ─────────────────────────────────────────────

async function processWithConcurrency(rows) {
  let nextIndex = 0;

  async function runner() {
    while (nextIndex < rows.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      await processJob(
        rows[currentIndex]
      );
    }
  }

  const runnerCount = Math.min(
    CONCURRENCY,
    rows.length
  );

  await Promise.all(
    Array.from(
      { length: runnerCount },
      () => runner()
    )
  );
}

// ─────────────────────────────────────────────
// MAIN LOOP
// ─────────────────────────────────────────────

async function main() {
  console.log(
    `🚀 Topic Notes worker started: ${WORKER_ID}`
  );

  console.log(
    `⚙️ Model=${MODEL} | Claim=${CLAIM_LIMIT} | Concurrent=${CONCURRENCY}`
  );

  while (true) {
    try {
      const rows = await claimJobs();

      if (!rows.length) {
        await sleep(IDLE_MS);
        continue;
      }

      console.log(
        `📥 Claimed ${rows.length} topic(s)`
      );

      await processWithConcurrency(rows);
    } catch (error) {
      console.error(
        "❌ Worker loop error:",
        error?.message || error
      );

      await sleep(
        Math.max(IDLE_MS, 2000)
      );
    }
  }
}

main().catch((error) => {
  console.error(
    "❌ Fatal worker error:",
    error
  );

  process.exit(1);
});
