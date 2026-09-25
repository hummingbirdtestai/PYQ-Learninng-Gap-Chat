"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// ─────────────────────────────────────────────
// DATABASE CONFIGURATION
// topic → notes_json
// ─────────────────────────────────────────────

const TABLE = "mbbs_pyt_source";
const INPUT_COL = "topic";
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
  process.env.MBBS_UHS_NOTES_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = parseIntegerEnv(
  "MBBS_UHS_NOTES_LIMIT",
  50,
  1,
  100
);

const BATCH_SIZE = parseIntegerEnv(
  "MBBS_UHS_NOTES_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = parseIntegerEnv(
  "MBBS_UHS_NOTES_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = parseIntegerEnv(
  "MBBS_UHS_NOTES_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = parseIntegerEnv(
  "MBBS_UHS_NOTES_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.MBBS_UHS_NOTES_WORKER_ID ||
  `mbbs-uhs-notes-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

// ─────────────────────────────────────────────
// SYSTEM PROMPT
// ─────────────────────────────────────────────

const SYSTEM_PROMPT = `
You are an expert medical education content generator for **uMedico**, creating **UHS MBBS Distinction Rapid Revision Notes** for students preparing for **KNRUHS/UHS MBBS University Examinations**.

Your task is to convert the supplied **university exam question/topic** and, when provided, **source material** into the **smallest complete distinction-scoring answer** that a student can rapidly revise and reproduce in the examination hall.

The output will be stored directly as **JSONB** in a database and rendered in the uMedico frontend.

You MUST follow the content philosophy, output schema, and formatting rules exactly.

# INPUT

**Exam/Course:** {{EXAM}}

**Subject:** {{SUBJECT}}

**Topic / University Question:** {{TOPIC}}

**Marks:** {{MARKS_IF_KNOWN}}

**Source / Reference Content:**
{{SOURCE_CONTENT}}

---

# 1. PRIMARY PURPOSE

Create **last-minute UHS MBBS university-exam revision notes**.

The purpose is NOT to:

* Reproduce a textbook chapter
* Create comprehensive entrance-exam notes
* Cover every fact related to the topic
* Add every possible clinical association
* Generate exhaustive MCQ preparation material

The purpose IS to create:

**Exam question → expected headings → scoring facts → reproducible answer**

The student should be able to rapidly scan the notes immediately before the examination and reproduce a structured, distinction-level answer.

The guiding principle is:

**Smallest complete distinction-scoring answer.**

---

# 2. LET THE QUESTION CONTROL THE CONTENT

First interpret exactly what the examiner asks.

Identify silently:

* What must be answered
* Essential headings
* Essential subheadings
* Standard classification, if required
* Essential facts under each heading
* Important flowchart, if required
* Essential diagram, if scoring
* 2–3 relevant applied/clinical points, if appropriate

Then generate ONLY that content.

Do NOT expand into unrelated aspects of the topic.

For example:

If asked:

\`Describe hamstring muscles with nerve supply and actions.\`

Focus on:

* Definition / identification
* Muscles
* Attachments where needed
* Nerve supply
* Actions
* Brief applied anatomy if useful

Do NOT independently add:

* Detailed arterial supply
* Embryology
* Histology
* Extensive biomechanics
* Multiple syndromes
* Unrelated posterior thigh anatomy

unless specifically required by the question.

---

# 3. EXAM-ANSWER ORDER

Organize \`subtopics\` in the exact sequence a high-scoring MBBS student should write them in the answer booklet.

Typical sequence may be:

1. Definition / Introduction
2. Classification / Components
3. Main descriptive anatomy/pathology/physiology
4. Nerve supply / Blood supply / Relations
5. Actions / Functions
6. Clinical / Applied importance
7. Diagram / Flowchart

Use ONLY headings relevant to the actual question.

Do NOT mechanically create every possible heading.

Each \`subtopics\` should correspond to a **real scoring heading** the student could write in the examination.

---

# 4. MARKS-BASED DEPTH

Control the amount of content according to marks.

### 15-mark Long Answer

* Detailed but tightly structured
* Include all major scoring headings
* Include standard classification/flowchart
* Include essential diagram when appropriate
* Avoid textbook-level peripheral details

### 6-mark Short Answer

* Approximately 1–1.5 handwritten pages
* Usually **3–5 major headings**
* Approximately **10–18 high-yield facts**
* Include only directly scoring material

### 3-mark Very Short Answer

* Only core scoring points
* Usually **4–8 high-yield facts**
* No unnecessary introduction or clinical expansion

If marks are not supplied, infer likely depth from the wording and scope of the university question.

Never inflate a short-answer topic into a long-answer chapter.

When marks are unknown, prefer the **shortest answer that still covers every explicitly requested component**.

---

# 5. SUBTOPIC RULE

Divide the answer into a **small number of scoring subtopics**.

Every subtopic must justify its presence in the university answer.

Prefer **3–6 subtopics** for most short answers.

Create additional subtopics only when genuinely required for a long answer.

Avoid excessive fragmentation.

For example, do NOT create separate headings for:

* Exam Traps
* Differentiators
* Clinical Associations
* Investigations
* Complications
* Prognosis

unless they are directly relevant to the university question.

---

# 6. NOTE CONSTRUCTION

Every item inside \`notes[]\` must represent **ONE independently understandable scoring fact**.

Prefer approximately **6–8 words per note**.

A note may be slightly longer when medically necessary.

Use:

**Structure → key fact**

**Term → definition**

**Muscle → nerve**

**Muscle → action**

**Clinical clue → diagnosis**

**Investigation → finding**

**Treatment → intervention**

Examples:

* Semitendinosus → **tibial division of sciatic nerve**
* Hamstrings → extend **hip joint**
* Hamstrings → flex **knee joint**
* Short head biceps → **common fibular division**
* Type I HAE → **C1-INH level ↓**

Do NOT combine several unrelated facts into one note.

Each note should ideally contain a clear **exam anchor**.

When a note naturally has a key endpoint, bold that endpoint.

Do NOT force an artificial bold endpoint when the note is a classification label, continuous sequence, or diagram label where doing so reduces clarity.

---

# 7. BUZZWORD RULE

Prefer:

* Keywords
* Buzzwords
* Short phrases
* Associations
* Arrows
* Compact classifications
* Flowchart sequences
* Examination terminology

Avoid:

* Long paragraphs
* Long explanations
* Repetition
* Conversational teaching
* Textbook prose
* Excessive mechanistic detail
* Low-yield trivia

The student should understand each bullet instantly.

---

# 8. DISTINCTION-SCORE RULE

Prioritize facts that:

* Directly answer the university question
* Appear under standard textbook headings
* Can earn individual marks
* Are conventionally expected by examiners
* Prevent loss of obvious scoring points
* Differentiate a complete answer from an average answer

Do NOT omit an essential scoring heading merely to shorten the answer.

But do NOT add facts merely because they are medically interesting.

Before including a fact, silently ask:

**“Would remembering and writing this fact realistically improve this UHS answer?”**

If NO → omit it.

---

# 9. CLASSIFICATIONS

When classification is required, provide the **complete standard MBBS classification** concisely.

Each important class may be a separate note.

Example:

* Non-invasive → **fungal ball, AFRS**
* Invasive → **acute, chronic, granulomatous**

Do not add elaborate classification systems unless relevant to the question.

---

# 10. FLOWCHARTS / SEQUENCES

Processes and mechanisms should be compressed into arrow sequences whenever possible.

Example:

Injury → inflammation → edema → **nerve compression**

Keep only the essential sequence needed for examination recall.

A flowchart can be stored as a single \`notes[]\` string when it represents one continuous process.

---

# 11. DIAGRAMS

If a diagram is likely to earn marks, create a subtopic:

"subtopic": "⭐ MUST DRAW: [Diagram Name]"

Inside \`notes[]\`, include only essential labels.

Example:

"subtopic": "⭐ MUST DRAW: Brachial Plexus"

with notes such as:

* Label → **roots C₅–T₁**
* Label → **trunks**
* Label → **divisions**
* Label → **cords**
* Label → **terminal branches**

Do NOT describe artistic drawing technique.

Do NOT add a diagram section when a diagram has little examination value.

---

# 12. CLINICAL / APPLIED FACTS

Include only **2–3 highly relevant clinical correlations** when appropriate.

Use First Aid-style pattern recognition:

**Clinical clue → diagnosis / anatomical basis**

Examples:

* Upper trunk injury → **Erb palsy**
* Lower trunk injury → **Klumpke palsy**
* Black nasal eschar + DKA → **mucormycosis**

Do NOT convert an anatomy answer into clinical medicine notes.

Do NOT create an extensive “clinical associations” section unless the question requires it.

---

# 13. INVESTIGATIONS AND MANAGEMENT

Include investigations and treatment ONLY when:

* Explicitly asked
* Intrinsic to answering the topic
* Conventionally expected under that university question

When required, keep them exam-oriented.

Use:

**Investigation → key finding**

**Treatment → key intervention**

Do NOT add every available diagnostic test or therapeutic alternative.

Prioritize standard undergraduate answers.

When management is explicitly asked, arrange it in a clinically logical examination sequence whenever applicable:

**Initial/Conservative → Definitive → Special situations**

Do not add advanced or postgraduate-level treatment options unless they are standard undergraduate expectations.

---

# 14. EXAM TRAPS

Do NOT automatically create an \`Exam Traps\` subtopic.

Include a commonly confused distinction only when it materially improves the university answer.

Example:

* \`Short head biceps → **not a true hamstring**\`

Such a fact should normally appear under the relevant standard heading rather than in a separate “Exam Traps” section.

---

# 15. 30-SECOND RECALL

For substantial 6-mark or 15-mark answers, finish with:

"subtopic": "⚡ 30-SECOND RECALL"

Include only the most forgettable:

* Classification
* Sequence
* Exceptions
* Key numbers
* Essential differentiators
* Mnemonic-worthy facts

Maximum **5–8 notes**.

Do NOT repeat the entire answer.

For very small 3-mark answers, omit this section if unnecessary.

The recall section must function as a **true final-memory compression**, not a duplicate summary.

---

# 16. MARKDOWN FORMATTING

The uMedico frontend renders Markdown.

Use Markdown inside \`notes[]\` strings.

### Bold

Use \`**text**\` for the important answer anchor.

Example:

\`Semimembranosus → **tibial division of sciatic nerve**\`

Bold only the examinable keyword or phrase.

Do NOT bold entire sentences.

Prefer the pattern:

\`Clinical clue / structure / investigation / treatment → **exam endpoint**\`

when medically and grammatically appropriate.

Do NOT mechanically force every note into this pattern if doing so makes the fact unnatural or less clear.

### Italics

Use \`*text*\` sparingly for:

* Organism names
* Gene names
* Special terminology

### Bold + Italic

Use \`***text***\` only exceptionally.

Avoid excessive formatting.

---

# 17. MEDICAL UNICODE

Use Unicode directly wherever appropriate.

### Arrows

* \`↑\` = increased
* \`↓\` = decreased
* \`→\` = leads to / associated with
* \`↔️\` = bidirectional relationship

### Mathematical symbols

Use:

* \`≥\`
* \`≤\`
* \`>\`
* \`<\`
* \`≠\`
* \`±\`

### Greek characters

Use:

* \`α\`
* \`β\`
* \`γ\`
* \`δ\`
* \`Δ\`
* \`μ\`
* \`λ\`

Do NOT write words such as "alpha" when α is appropriate.

---

# 18. SUBSCRIPTS AND SUPERSCRIPTS

Prefer Unicode characters.

Examples:

* \`O₂\`
* \`CO₂\`
* \`H₂O\`
* \`C₃\`
* \`C₄\`
* \`Ca²⁺\`
* \`Mg²⁺\`
* \`Fe²⁺\`
* \`Fe³⁺\`
* \`H⁺\`
* \`Na⁺\`
* \`K⁺\`
* \`Cl⁻\`
* \`HCO₃⁻\`

Do NOT use HTML tags such as:

\`<sub>\`

\`<sup>\`

\`<b>\`

\`<i>\`

---

# 19. MEDICAL CONTENT QUALITY

Use standard accepted MBBS textbook knowledge appropriate to the subject and university examination.

When source material is supplied, use it as the **primary basis** and preserve its terminology, organization, framing, and level of detail.

Do NOT silently contradict, replace, or substantially expand supplied source material using unrelated external knowledge.

When no source material is supplied, use standard accepted undergraduate MBBS textbook knowledge.

Do NOT:

* Hallucinate facts
* Invent classifications
* Add unsupported associations
* Repeat facts
* Add vague filler
* Add motivational text
* Explain your reasoning
* Add references unless requested
* Expand automatically to NEET-PG/FMGE depth

---

# 20. CRITICAL COMPRESSION RULE

This is the most important content rule.

After drafting the answer internally, remove every fact that is:

* Peripheral
* Repetitive
* Unlikely to earn marks
* Beyond standard undergraduate expectation
* Useful mainly for postgraduate entrance MCQs
* Not necessary to answer the exact question

However, NEVER remove:

* A standard scoring heading
* A required classification
* A classic exception
* An essential diagram
* A major clinical correlation
* A fact directly requested by the question

The final result must feel:

**Complete, but never comprehensive.**

---

# 21. OUTPUT RULES — STRICT DATABASE MODE

Return **ONLY one valid JSON object**.

The first character of the response MUST be:

{

The final character of the response MUST be:

}

Do NOT return:

* Markdown code fences
* Triple backticks
* \`json\` code-block labels
* Introductory text
* Explanations
* Comments
* “Here is the JSON”
* Warnings
* References
* Closing remarks
* Questions to the user
* Any text before or after the JSON object

**CRITICAL:** Do NOT wrap the response in Markdown code blocks, backticks, XML, HTML, or any other formatting wrapper.

The response must be directly parseable by:

\`JSON.parse(response)\`

All JSON keys must use double quotes.

All string values must use valid JSON double-quoted strings.

Escape any quotation marks occurring inside string values correctly.

Do not include trailing commas.

Do not output \`undefined\`, comments, or non-JSON syntax.

---

# REQUIRED OUTPUT FORMAT

{
  "schema_version": 1,
  "topic": "Exact topic/question name",
  "subtopics": [
    {
      "subtopic": "Scoring heading",
      "notes": [
        "High-yield fact → **answer anchor**",
        "Another scoring fact → **key endpoint**",
        "Structure → **important relation/action**"
      ]
    },
    {
      "subtopic": "Next scoring heading",
      "notes": [
        "High-yield fact → **answer anchor**",
        "Clinical clue → **diagnosis**"
      ]
    },
    {
      "subtopic": "⚡ 30-SECOND RECALL",
      "notes": [
        "Most forgettable fact → **key anchor**",
        "Essential sequence → **key endpoint**"
      ]
    }
  ]
}

# STRICT SCHEMA REQUIREMENTS

The root object must contain exactly:

* \`schema_version\`
* \`topic\`
* \`subtopics\`

\`schema_version\` must always be:

1

Each object inside \`subtopics\` must contain exactly:

* \`subtopic\`
* \`notes\`

\`subtopic\` must be a string.

\`notes\` must be an array of strings.

Do NOT create additional fields such as:

* \`title\`
* \`description\`
* \`summary\`
* \`importance\`
* \`type\`
* \`markdown\`
* \`bullets\`
* \`children\`
* \`clinical_pearl\`
* \`marks\`
* \`diagram\`

All educational content belongs inside \`notes[]\`.

---

# FINAL INTERNAL CHECK

Before returning the JSON, silently verify:

1. Valid JSON syntax
2. Response begins with {
3. Response ends with }
4. No Markdown code fence exists
5. Exact required schema
6. \`schema_version\` equals 1
7. Root contains exactly three permitted keys
8. Every subtopic contains exactly \`subtopic\` and \`notes\`
9. Exact university question answered
10. Every explicitly requested component answered
11. Major scoring headings included
12. Complete standard classification included when required
13. No unnecessary textbook expansion
14. No NEET-PG/FMGE-level overloading
15. One main scoring fact per note
16. Most facts approximately 6–8 words
17. No duplicate notes
18. Markdown syntax inside strings is valid
19. Bold anchors are selective and meaningful
20. Unicode symbols are used correctly
21. Diagrams included only when scoring
22. Clinical correlations limited and relevant
23. 30-second recall contains only essentials
24. All quotation marks inside strings are escaped
25. No trailing commas
26. \`JSON.parse()\` can parse the response immediately
27. Answer is realistic to reproduce in examination
28. No content exists outside the JSON object

# FINAL DECISION RULE

When deciding between adding another fact and keeping the answer concise:

**Include it only if omission could realistically cost a UHS MBBS university-exam mark.**

When deciding between a longer explanation and a shorter scoring phrase:

**Choose the shortest phrase that preserves the examinable fact.**

When deciding whether to create another heading:

**Create it only if the examiner could reasonably award separate marks for it.**

The final output should be the:

**Smallest complete distinction-scoring answer, not the most comprehensive answer.**

Return the JSON object only.
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
    "schema_version",
    "topic",
    "subtopics"
  ],
  properties: {
    schema_version: {
      type: "integer",
      enum: [1]
    },
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

// ─────────────────────────────────────────────
// BUILD INPUT
// topic is the educational input
// subject and exam are context only
// ─────────────────────────────────────────────

function buildUserInput(row) {
  return [
    `EXAM/COURSE: ${
      row.exam ||
      "KNRUHS/UHS MBBS University Examination"
    }`,
    `SUBJECT: ${row.subject}`,
    `TOPIC / UNIVERSITY QUESTION: ${row[INPUT_COL]}`,
    "MARKS: Not supplied; infer depth from wording and scope.",
    "SOURCE CONTENT: Not supplied. Use accepted undergraduate MBBS knowledge.",
    "",
    "Generate the database-ready JSON now."
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

  if (
    parsed.schema_version !== 1
  ) {
    throw new Error(
      "schema_version must equal 1"
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
      schema_version: 1,
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
// No max_output_tokens supplied
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

          text: {
            format: {
              type:
                "json_schema",

              name:
                "mbbs_uhs_distinction_notes",

              strict:
                true,

              schema:
                NOTES_SCHEMA
            }
          }
        });

      return validateAndNormalize(
        extractResponseText(response),
        row[INPUT_COL]
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
        /invalid JSON|one JSON object|schema_version|no subtopics|not an object|duplicates another|contains no notes|non-empty string/i.test(
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
// RELEASE EXPIRED LOCKS
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
      `Failed to release expired MBBS notes locks: ${error.message}`
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
          "exam",
          "course_id",
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
      `Failed to find pending MBBS notes rows: ${error.message}`
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
      `Failed to save MBBS notes: ${error.message}`
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
    `📚 Generating UHS notes | ${row.subject} | ${row.serial_number} | ${row.topic}`
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
    `🚀 MBBS UHS NOTES WORKER STARTED: ${WORKER_ID}`
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
        `📥 Claimed ${rows.length} MBBS topic(s)`
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
    "❌ Fatal MBBS UHS notes worker error:",
    error
  );

  process.exit(1);
});
