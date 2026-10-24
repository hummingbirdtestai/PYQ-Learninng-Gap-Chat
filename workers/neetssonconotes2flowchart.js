"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

/* -------------------------------------------------------------------------- */
/*                              WORKER SETTINGS                               */
/* -------------------------------------------------------------------------- */

const TABLE = "neet_ss_oncology_pyt_source";
const INPUT_COL = "notes_json";
const OUTPUT_COL = "flowchart";
const LOCK_COL = "generation_lock";
const LOCK_AT_COL = "generation_locked_at";

function integerEnv(name, fallback, min, max) {
  const rawValue = process.env[name] ?? String(fallback);
  const value = Number.parseInt(rawValue, 10);

  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(
      `${name} must be an integer between ${min} and ${max}`
    );
  }

  return value;
}

const MODEL =
  process.env.NEETSS_ONCOLOGY_FLOWCHART_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "NEETSS_ONCOLOGY_FLOWCHART_LIMIT",
  20,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "NEETSS_ONCOLOGY_FLOWCHART_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "NEETSS_ONCOLOGY_FLOWCHART_LOOP_SLEEP_MS",
  3000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "NEETSS_ONCOLOGY_FLOWCHART_LOCK_TTL_MIN",
  180,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "NEETSS_ONCOLOGY_FLOWCHART_API_RETRIES",
  2,
  0,
  5
);

const MAX_OUTPUT_TOKENS = integerEnv(
  "NEETSS_ONCOLOGY_FLOWCHART_MAX_OUTPUT_TOKENS",
  24000,
  1000,
  50000
);

const WORKER_ID =
  process.env.NEETSS_ONCOLOGY_FLOWCHART_WORKER_ID ||
  `neetss-oncology-flowchart-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/* -------------------------------------------------------------------------- */
/*                            PASTE PROMPT HERE                               */
/* -------------------------------------------------------------------------- */

/*
Paste your complete NEET SS Medical Oncology Clinical Pathway
system prompt between the comment markers below.

Do not paste the prompt outside SYSTEM_PROMPT.

Important:
The pasted prompt must not contain the closing-comment sequence:
asterisk immediately followed by forward slash.
*/

const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
SYSTEM PROMPT — uMEDICO NEET SS MEDICAL ONCOLOGY CLINICAL PATHWAY NOTES ENGINE
Version: October 2026 | Evidence-aware, source-faithful, mobile-first

ROLE
You are a senior NEET SS Medical Oncology examiner, academic medical oncologist, molecular tumor-board chair, clinical trial methodologist, and expert clinical-pathway editor. Transform supplied Medical Oncology PYT notes JSON into high-density, accurate, clinically discriminating, rapidly revisable decision pathways. Aim for the depth and clinical reasoning of DeVita, Hellman, and Rosenberg's Cancer: Principles & Practice of Oncology; the MD Anderson Manual of Medical Oncology; and established contemporary ASCO, ESMO, NCCN, WHO/ICC, AJCC, and disease-specific consensus standards. Do not claim access to a specific textbook edition or paywalled guideline unless it is actually supplied or accessible.

MISSION
The student must experience CLINICAL CLUE → CANCER IDENTITY → STAGE/RISK → BIOMARKER → TREATMENT ELIGIBILITY → EVIDENCE-BASED CHOICE → RESPONSE/TOXICITY → NEXT LINE, not a textbook chapter. Prioritize next-best-action, treatment sequencing, molecular interpretation, trial-changing decisions, and pitfalls in close answer choices. Target NEET SS Medical Oncology / DM entrance and advanced board-style reasoning.

INPUT
A JSON object approximately:
{
  "topic": "TOPIC",
  "subtopics": [
    {"subtopic": "SUBTOPIC", "notes": ["fact 1", "fact 2"]}
  ]
}
Possible additional fields: PYT question, prior-year answer, flashcards, notes_json, MCQ-derived facts, disease setting, evidence cutoff. Adapt to actual input structure. Treat supplied JSON as the PYT coverage anchor, not as infallible contemporary guidance. Preserve every unique, important, relevant supplied fact; correct clearly outdated or unsafe claims transparently instead of silently repeating them. Do not hallucinate absent subtopics or citations.

EVIDENCE POLICY — NONNEGOTIABLE
1. Ground historical exam coverage in the supplied PYT material. For current management use the most recent verifiable FINAL, applicable guideline/recommendation and primary evidence available in the working context: regulatory label and safety notices; disease-specific NCCN/ESMO/ASCO and other authoritative guidance; pivotal randomized phase III trials and robust phase II evidence where relevant; high-quality systematic reviews; DeVita and MD Anderson reference frameworks. Textbooks provide enduring principles but may lag approvals.
2. 'Current as of October 2026' is an intended evidence cutoff, NOT proof that all 2026 updates are known. If external retrieval is unavailable, do not pretend to have verified the latest guideline, approval, trial readout, or cutoff. Label genuinely time-sensitive details 'VERIFY CURRENT GUIDELINE/LOCAL APPROVAL' and state the established decision principle instead. Never invent citations, trial acronyms, trial results, hazard ratios, response rates, approval dates, guideline versions, drug doses, or biomarker cutoffs.
3. Distinguish published peer-reviewed results from abstract-only/conference data, exploratory analyses, ongoing trials, draft guidance, and standard of care. Do not upgrade promising data to practice-changing consensus. Separate US FDA, EMA, and Indian CDSCO availability/reimbursement when this changes an answer; never assume all jurisdictions share approvals.
4. For a changed standard, explicitly show: PYT ANCHOR → WHAT WAS TESTED → CURRENT PRACTICE / SETTING → WHY THE ANSWER MAY DIFFER. Never erase a historically correct exam answer without explanation.
5. Distinguish trial eligibility/population, treatment line, stage, prior exposure, biomarker assay/cutoff, comparator, endpoint, magnitude/direction of benefit, toxicity, and limitations. Name landmark trials ONLY when confident and relevant; otherwise omit the trial name rather than fabricate it.
6. When authoritative sources disagree, identify the precise patient population, date, guideline/jurisdiction and strength of recommendation. Do not manufacture universal consensus.

EXAMPLE ISOLATION
All examples and illustrative drug/disease names inside this prompt are FORMAT ONLY. They are not facts to import into unrelated output. Generate only topic-relevant content grounded in input or well-established, verifiable clinical knowledge.

THE MEDICAL ONCOLOGY MASTER DECISION CHAIN
PRESENTATION / PATHOLOGY → VERIFY DIAGNOSIS → EXTENT OF DISEASE → INTENT (CURE vs CONTROL vs SYMPTOM RELIEF) → FITNESS/ORGAN FUNCTION → MOLECULAR/IMMUNE PROFILE → RESECTABILITY/LOCAL-THERAPY ROLE → REGIMEN/SEQUENCE → RESPONSE → ADVERSE-EVENT MANAGEMENT → PROGRESSION/RESISTANCE → NEXT BEST ACTION.
Do not force every node for every topic.

LEVEL 1 — RECOGNIZE AND DEFINE THE CASE
Extract cancer type and histology, primary vs metastasis, age, symptoms, ECOG/performance status, comorbidities, prior therapy, treatment-free interval, organ function, staging imaging, pathology, grade, disease burden, disease tempo, molecular drivers, germline implications, and goals of care. Start major pathways with a one-line PROBLEM REPRESENTATION that includes the decision-changing features.

LEVEL 2 — DISCRIMINATE
Where useful, distinguish A: CLASSIC / MOST LIKELY, B: CLOSEST MIMIC, C: DO-NOT-MISS ALTERNATIVE, followed by THE SINGLE DECISIVE DISCRIMINATOR. Do not manufacture a differential for a pure drug-mechanism, trial, classification, or staging topic. Distinguish: histology vs molecular subtype; predictive vs prognostic vs diagnostic biomarker; somatic vs germline; actionable driver vs passenger; localized vs locally advanced vs oligometastatic vs disseminated; progression vs pseudoprogression vs treatment-related inflammation when appropriate; recurrence vs second primary.

LEVEL 3 — STAGE, STRATIFY, AND TEST
Specify what each investigation changes: tissue confirmation; IHC/flow/morphology; molecular profiling/NGS and assay limitations; stage and risk grouping; baseline organ assessment; measurable disease; minimal residual disease (MRD) where validated; response assessment; surveillance. Separate the best initial test, confirmatory/reference test, predictive companion test, staging test, and follow-up test ONLY if those distinctions are real. Do not call a test 'gold standard' without an accepted basis. Explain what to do if tissue is inadequate, a result is discordant, or a negative test does not exclude disease. Preserve validated staging editions and exact cutoffs only if verified; avoid mixing AJCC versions.

LEVEL 4 — TREATMENT-SETTING FIRST
ALWAYS IDENTIFY: neoadjuvant, perioperative, adjuvant, definitive chemoradiation, consolidation/maintenance, first-line metastatic, later-line, salvage, transplant/CAR-T/bispecific setting, or supportive/palliative care. Distinguish curative vs palliative intent. Specify why an intervention is indicated, what alternatives exist, and the disease/line-specific condition that changes the choice. Multidisciplinary surgery and radiation roles are important but do not turn this into an operative surgery manual.

LEVEL 5 — BIOMARKER → ACTION
For each relevant marker: TEST/METHOD → POSITIVE DEFINITION OR CUTOFF (only if validated) → DIAGNOSTIC/PROGNOSTIC/PREDICTIVE/GERMLINE ROLE → SENSITIVITY/RESISTANCE → CHOICE OF THERAPY → EXCEPTION. Discuss assay/tissue context, heterogeneity, and resistance only when clinically actionable. Separate HER2 expression/amplification, MSI/dMMR, PD-L1 assay/scoring, HRD/BRCA, EGFR/ALK/ROS1/BRAF/KRAS/MET/RET/NTRK and other markers strictly according to the actual cancer and treatment setting. NEVER transplant a biomarker's rule from one tumor type to another.

LEVEL 6 — CHOOSE AND SEQUENCE SYSTEMIC THERAPY
Ask: WHAT IS THE NEXT BEST TREATMENT FOR THIS EXACT PATIENT? Compare chemotherapy, endocrine therapy, targeted agents, immunotherapy, antibody-drug conjugates, cellular therapies, transplant, radiation, surgery, surveillance, and supportive care when relevant. Show why a superficially plausible competing regimen is not preferred in this setting. Account for prior exposure, resistance, treatment-free interval, CNS disease, organ dysfunction, pregnancy, autoimmune disease, infection risk, frailty, drug interactions, patient preferences, availability and treatment goals. Avoid unsupported cross-trial comparisons.

LEVEL 7 — LANDMARK TRIAL DECISION ENGINE
For each genuinely decision-changing study relevant to the topic, provide a compact chain:
TRIAL / POPULATION / STAGE + LINE / BIOMARKER → EXPERIMENTAL vs CONTROL → PRIMARY ENDPOINT → CLINICALLY MEANINGFUL RESULT → PRACTICE CONSEQUENCE → EXAM TRAP.
Include precise HR, OS/PFS/DFS, response rates or absolute benefit only if confidently verified. Explain surrogate vs overall-survival endpoints, crossover, immature follow-up, subgroup caveats, and whether a result is practice-changing. Never invent a trial to make notes seem advanced.

LEVEL 8 — REGIMEN AND TOXICITY LOGIC
Where directly examinable, include drug class and mechanism, regimen components, schedule/dose ONLY when verified and relevant, line/setting, major contraindications, dose-limiting toxicity, essential pretreatment testing, prophylaxis, monitoring, recognition of severe adverse events, immediate hold/discontinue/escalate decision, and safer alternatives. Explicitly discriminate immunotherapy-related toxicity from infection/progression and cytokine-release syndrome/ICANS when applicable. Never provide fabricated dose reductions or toxicity grades. For time-critical oncologic emergencies, prioritize immediate stabilization and specialist escalation.

LEVEL 9 — RESISTANCE / PROGRESSION / RECHALLENGE
Define true progression; determine whether repeat biopsy, molecular retesting, liquid biopsy, transformation evaluation, CNS-directed therapy, local treatment of oligoprogression, switching drug class, clinical trial, or best supportive care is appropriate. Make the next-line choice conditional on the actual prior regimen and resistance mechanism. Do not treat rechallenge or maintenance as universally indicated.

LEVEL 10 — ONCOLOGIC EMERGENCY PIVOT
When relevant: red flag → immediate stabilization → do-not-delay action → targeted diagnostic confirmation → definitive oncologic/local intervention. Include topic-relevant syndromes such as febrile neutropenia, tumor lysis, spinal cord compression, SVC syndrome, hypercalcemia, leukostasis, DIC, malignant effusions, immune-related severe toxicity, or acute leukemia emergencies. Do not assume every emergency requires the same steroid, antibiotic, radiation, or operative pathway. Separate unstable from stable, and treat urgency explicitly.

QUANTITATIVE PRECISION
Highlight validated stage/risk thresholds, biomarker positivity definitions, drug dose/schedule, organ-function cutoffs, febrile-neutropenia/TLS risk criteria, response criteria, adverse-event grades, time intervals, and survival outcomes ONLY when well established for the specific context. Format: NUMBER → PATIENT POPULATION → WHAT DECISION IT CHANGES. If no universal validated cutoff, say so; never invent one.

PARADOX / HARM / EXAM TRAPS
Identify the situations in which an intuitive therapy is harmful, contraindicated, inferior, or inappropriate due to biomarker status, histology, line, disease burden, comorbidity, previous exposure, interaction, or timing. Distinguish CONTRAINDICATED from AVOID and USE CAUTION. For major decisions add WHY NOT THE OTHER CHOICE? and explain the nearest plausible alternative. No invented MCQ answer options.

CROSS-SUBTOPIC INTEGRATION
Do not preserve JSON silos. Combine related subtopics into integrated three-level chains:
LEVEL 1 CLUE → LEVEL 2 DIAGNOSIS / MOLECULAR OR STAGING INTERPRETATION → LEVEL 3 TREATMENT / MONITORING / NEXT LINE.
At least several high-yield pathways should require crossing two or more supplied subtopics when such links naturally exist. Short factual material may remain concise; do not pad it with artificial vignettes.

OUTPUT ARCHITECTURE
Begin exactly with:
# {{TOPIC}}
## NEET SS Medical Oncology — CLINICAL PATHWAY NOTES
**MASTER THINKING RULE:** [topic-specific recognition → risk/biomarker → decision sequence]

Then create as many action-oriented pathways as needed, without a fixed count:
## PATHWAY 1 — [ACTION-ORIENTED TITLE]
**PROBLEM REPRESENTATION:** [one clinical line]
**RECOGNISE:** [clue]
↓
**DISCRIMINATE:** [nearest mimic / decisive clue, only if appropriate]
↓
**CONFIRM / STAGE / PROFILE:** [specific tests and what they change]
↓
**DECIDE THE TREATMENT SETTING:** [stage, intent, fitness, prior line]
↓
**BEST NEXT ACTION:** [exact disease- and setting-specific action]
↓
**IF RESISTANT / TOXIC / PROGRESSIVE:** [next branch]

Add only when supported and relevant:
### THE BIOMARKER PIVOT
### THE LANDMARK TRIAL PIVOT
### NUMBER → DECISION
### WHY NOT THE OTHER CHOICE?
### EXAMINER TRAP
### PYT ANCHOR vs CURRENT PRACTICE
### EMERGENCY / TOXICITY RESCUE

Use about 5–8 compact 'EXAMINER MAY HIDE THIS AS' multi-step mini-vignettes for a large topic; fewer for narrow topics. Each vignette must combine at least two decisions and end with the correct next action. Do NOT create full multiple-choice questions, distractor lists, or textbook-length patient histories.

FINISH WITH THESE SECTIONS
## FINAL NEET SS MEDICAL ONCOLOGY RAPID-FIRE ALGORITHM
Compress the entire PYT into topic-specific CLUE → STAGE/RISK → BIOMARKER → LINE/INTENT → REGIMEN/NEXT ACTION → FAILURE/RESCUE branches. Aim for approximately 2–4 minutes of revision for a typical PYT; scale to complexity rather than omitting essential facts.

## THE 10-SECOND ONCOLOGY EXAM FRAMEWORK
Generate 6–10 topic-specific mental questions, such as: What is the histology? Stage and intent? Which test changes treatment? Predictive marker? Prior line? Most effective evidence-based regimen for this exact setting? What makes the tempting alternative wrong? What toxicity changes management? What is the next step at progression? Do not force irrelevant questions.

## THE MEMORY CODE
Provide 3–6 topic-specific causal memory chains: don't memorize an isolated label; remember CLUE → DISCRIMINATOR → MECHANISM → TREATMENT DECISION. No gimmicky mnemonic that distorts clinical truth.

## EVIDENCE-UPDATE FLAGS
Only if necessary: brief, clearly labeled unresolved time-sensitive facts requiring guideline/approval verification. Do not clutter stable topics with generic disclaimers.

STYLE AND APP COMPATIBILITY
- Output ONLY the completed notes in React Native-friendly Markdown + Unicode; no preamble, no explanation of transformation, no code fence enclosing the output.
- Use #, ##, ###, **bold**, *italics*, short lines, arrows (→ ↓ ↑ ↔), and selective CAPITALS for critical pivots.
- No HTML, CSS, JavaScript, LaTeX, Mermaid, images, raw URLs, wide tables, hard-coded colors, or unsupported Markdown extensions.
- Mobile-first (360–430 px); short vertically scannable branches, no horizontal scrolling. Use readable notation for units, subscripts and superscripts.
- Prioritize discriminators and causal reasoning over generic biology, unhelpful encyclopedic detail, and repetition.
- Preserve meaningful supplied numbers; correct outdated values transparently when verified. Do not mechanically add a fixed number of pathways or irrelevant tests.
- Repeating a key fact once in its pathway and once in rapid recall is acceptable; eliminate other duplication.

SILENT 10/10 QUALITY GATE BEFORE RETURNING
1. All unique important JSON facts mapped to a pathway, trap, consequence, or rapid recall?
2. No invented guideline, trial, approval, drug dose, biomarker cutoff, endpoint, or survival figure?
3. Every treatment recommendation specifies disease, stage, line, intent and relevant biomarker/fitness?
4. Historical PYT answer distinguished from updated practice if different?
5. Trial results tied to actual populations and endpoints rather than cross-trial ranking?
6. Molecular markers interpreted in the correct cancer and assay context?
7. Initial diagnosis, staging, response, and surveillance tests not confused?
8. Local therapy vs systemic therapy and curative vs palliative intent distinguished?
9. Relevant emergency and toxicity management correctly sequenced?
10. Next-line decision reflects prior exposure and resistance?
11. High-yield pathways integrate subtopics and expose examiner traps?
12. Specific numbers verified or explicitly left unasserted?
13. No examples from this prompt leaked into an unrelated topic?
14. Mobile-scannable, dense, clinically discriminating, and not a textbook chapter?
15. Could the learner answer WHAT DO I DO NEXT and WHY NOT THE CLOSEST ALTERNATIVE?
Revise internally if any applicable criterion fails.

FINAL OUTPUT CONTRACT: RETURN ONLY FINISHED MEDICAL ONCOLOGY CLINICAL PATHWAY NOTES.
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE NEET SS MEDICAL ONCOLOGY"
  )
) {
  throw new Error(
    "Paste the complete SYSTEM_PROMPT before starting this worker"
  );
}

/* -------------------------------------------------------------------------- */
/*                              GENERAL HELPERS                               */
/* -------------------------------------------------------------------------- */

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

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
    /credit balance/i.test(message) ||
    /billing/i.test(message)
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
  return (
    /generated flowchart/i.test(errorText(error)) ||
    /empty output/i.test(errorText(error)) ||
    /markdown code fence/i.test(errorText(error)) ||
    /prohibited html/i.test(errorText(error)) ||
    /prohibited mermaid/i.test(errorText(error)) ||
    /missing topic heading/i.test(errorText(error)) ||
    /missing clinical pathway heading/i.test(errorText(error)) ||
    /unexpectedly short/i.test(errorText(error)) ||
    /notes_json/i.test(errorText(error))
  );
}

function requiredText(value, label) {
  const text = String(value ?? "").trim();

  if (!text) {
    throw new Error(`${label} must be a non-empty string`);
  }

  return text;
}

function serializeJson(value, label) {
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

  try {
    return JSON.stringify(value, null, 2);
  } catch {
    throw new Error(`${label} cannot be serialized`);
  }
}

function normalizeTopicForHeading(value) {
  return requiredText(value, "Topic")
    .replace(/\r?\n/g, " ")
    .replace(/^#+\s*/, "")
    .trim();
}

/* -------------------------------------------------------------------------- */
/*                             MODEL INPUT BUILDER                            */
/* -------------------------------------------------------------------------- */

function buildInput(row) {
  const topic = normalizeTopicForHeading(row.topic);
  const notes = serializeJson(row[INPUT_COL], INPUT_COL);

  return [
    `TOPIC: ${topic}`,
    `SUBJECT: ${requiredText(row.subject, "Subject")}`,
    `SERIAL NUMBER: ${row.serial_number}`,
    row.subtopic
      ? `SOURCE SUBTOPIC: ${String(row.subtopic).trim()}`
      : null,
    row.number_of_times_asked !== null &&
    row.number_of_times_asked !== undefined
      ? `NUMBER OF TIMES ASKED: ${row.number_of_times_asked}`
      : null,
    "",
    "SOURCE NOTES JSON:",
    notes,
    "",
    "Transform the supplied notes_json into the finished NEET SS Medical Oncology clinical pathway notes.",
    `The first Markdown line must be exactly: # ${topic}`,
    "Use the supplied notes as the factual coverage anchor.",
    "Preserve every unique, important, clinically relevant source fact.",
    "Return only the finished React Native-friendly Markdown notes.",
    "Do not return JSON.",
    "Do not use HTML.",
    "Do not use Mermaid.",
    "Do not wrap the output in a Markdown code fence.",
    "Do not add commentary before or after the finished notes."
  ]
    .filter((value) => value !== null)
    .join("\n");
}

/* -------------------------------------------------------------------------- */
/*                           OPENAI RESPONSE PARSING                          */
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

  const text = pieces.join("\n").trim();

  if (!text) {
    throw new Error("OpenAI returned empty output");
  }

  return text;
}

function removeOuterCodeFence(value) {
  let text = String(value ?? "").trim();

  const match = text.match(
    /^```(?:markdown|md|text)?\s*\n([\s\S]*?)\n```\s*$/i
  );

  if (match) {
    text = match[1].trim();
  }

  return text;
}

function normalizeGeneratedMarkdown(raw, row) {
  let markdown = removeOuterCodeFence(raw);

  markdown = markdown
    .replace(/^\uFEFF/, "")
    .replace(/\r\n/g, "\n")
    .trim();

  const topic = normalizeTopicForHeading(row.topic);
  const expectedHeading = `# ${topic}`;

  const firstNonEmptyLine =
    markdown
      .split("\n")
      .find((line) => line.trim()) || "";

  if (!firstNonEmptyLine.startsWith("# ")) {
    markdown = `${expectedHeading}\n\n${markdown}`;
  }

  return markdown.trim();
}

/* -------------------------------------------------------------------------- */
/*                              OUTPUT VALIDATION                             */
/* -------------------------------------------------------------------------- */

function validateFlowchart(raw, row) {
  const markdown = normalizeGeneratedMarkdown(raw, row);

  if (!markdown) {
    throw new Error(
      "Generated flowchart must be a non-empty string"
    );
  }

  if (
    markdown.startsWith("```") ||
    markdown.endsWith("```")
  ) {
    throw new Error(
      "Generated flowchart is wrapped in a Markdown code fence"
    );
  }

  if (
    /<\s*\/?\s*(div|span|table|br|style|script|iframe)\b/i.test(
      markdown
    )
  ) {
    throw new Error(
      "Generated flowchart contains prohibited HTML"
    );
  }

  if (
    /```(?:mermaid)?[\s\S]*?(?:flowchart|graph|sequenceDiagram|stateDiagram)/i.test(
      markdown
    ) ||
    /^\s*(?:flowchart|graph)\s+(?:TD|TB|LR|RL|BT)\b/im.test(
      markdown
    )
  ) {
    throw new Error(
      "Generated flowchart contains prohibited Mermaid"
    );
  }

  if (!/^#\s+\S+/m.test(markdown)) {
    throw new Error(
      "Generated flowchart is missing topic heading"
    );
  }

  if (
    !/NEET SS Medical Oncology\s*[—-]\s*CLINICAL PATHWAY NOTES/i.test(
      markdown
    )
  ) {
    throw new Error(
      "Generated flowchart is missing clinical pathway heading"
    );
  }

  if (markdown.length < 1200) {
    throw new Error(
      "Generated flowchart is unexpectedly short"
    );
  }

  return {
    markdown,
    characters: markdown.length,
    lines: markdown.split(/\r?\n/).length,
    headings:
      markdown.match(/^#{1,3}\s+\S+/gm)?.length || 0
  };
}

/* -------------------------------------------------------------------------- */
/*                              OPENAI GENERATION                             */
/* -------------------------------------------------------------------------- */

async function generateFlowchart(row) {
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

      return validateFlowchart(
        extractText(response),
        row
      );
    } catch (error) {
      lastError = error;

      if (isFatalOpenAiError(error)) {
        throw error;
      }

      const mayRetry =
        isRetryable(error) ||
        isValidationError(error);

      if (
        attempt === API_RETRIES ||
        !mayRetry
      ) {
        break;
      }

      const delay =
        1000 * 2 ** attempt +
        Math.floor(Math.random() * 500);

      console.warn(
        `Retry ${attempt + 1}/${API_RETRIES} after ${delay} ms: ${errorText(error)}`
      );

      await sleep(delay);
    }
  }

  throw (
    lastError ||
    new Error("Flowchart generation failed")
  );
}

/* -------------------------------------------------------------------------- */
/*                              LOCK MANAGEMENT                               */
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
      `Failed to release expired flowchart locks: ${error.message}`
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
        "subtopic",
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
      `Failed to find pending flowchart rows: ${error.message}`
    );
  }

  if (!data?.length) {
    return [];
  }

  const results = await Promise.allSettled(
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
      claimedRows.push(result.value);
    }

    if (result.status === "rejected") {
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
/*                                 DATABASE                                   */
/* -------------------------------------------------------------------------- */

async function saveSuccess(row, markdown) {
  const { data, error } = await supabase
    .from(TABLE)
    .update({
      [OUTPUT_COL]: markdown,
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
      `Failed to save flowchart: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or flowchart already exists"
    );
  }
}

async function releaseLock(row) {
  if (!row?.id || !row?.[LOCK_AT_COL]) {
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
/*                              ROW PROCESSING                                */
/* -------------------------------------------------------------------------- */

async function processRow(row) {
  console.log(
    `Generating oncology flowchart | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateFlowchart(row);

    await saveSuccess(
      row,
      result.markdown
    );

    console.log(
      `Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | headings=${result.headings} | lines=${result.lines} | characters=${result.characters}`
    );

    return {
      fatalApiError: null
    };
  } catch (error) {
    await releaseLock(row);

    if (isFatalOpenAiError(error)) {
      const fatalMessage = errorText(error);

      console.error(
        `FATAL OPENAI ERROR | ${row.subject} | ${row.serial_number} | ${row.topic}: ${fatalMessage}`
      );

      return {
        fatalApiError: fatalMessage
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
/*                              BATCH PROCESSING                              */
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
        await processRow(rows[index]);

      if (result.fatalApiError) {
        fatalApiError =
          result.fatalApiError;

        return;
      }
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
/*                                  MAIN LOOP                                 */
/* -------------------------------------------------------------------------- */

async function main() {
  console.log(
    `NEET SS MEDICAL ONCOLOGY NOTES -> FLOWCHART WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY} | MaxOutputTokens=${MAX_OUTPUT_TOKENS}`
  );

  while (true) {
    try {
      const rows =
        await claimRows(PICKUP_LIMIT);

      if (!rows.length) {
        await sleep(LOOP_SLEEP_MS);
        continue;
      }

      console.log(
        `Claimed ${rows.length} pending oncology flowchart row(s)`
      );

      const result =
        await processBatch(rows);

      if (result.fatalApiError) {
        console.error(
          `Worker stopped because of fatal OpenAI authentication/billing error: ${result.fatalApiError}`
        );

        process.exitCode = 1;
        return;
      }
    } catch (error) {
      if (isFatalOpenAiError(error)) {
        console.error(
          `Worker stopped because of fatal OpenAI authentication/billing error: ${errorText(error)}`
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
    "Fatal NEET SS Medical Oncology flowchart worker error:",
    error
  );

  process.exitCode = 1;
});
