"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "mrcp_topics";
const OUTPUT_COL = "mcq_json";
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
  process.env.MRCP_MCQS_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "MRCP_MCQS_LIMIT",
  20,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "MRCP_MCQS_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "MRCP_MCQS_LOOP_SLEEP_MS",
  3000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "MRCP_MCQS_LOCK_TTL_MIN",
  180,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "MRCP_MCQS_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.MRCP_MCQS_WORKER_ID ||
  `mrcp-mcqs-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/* -------------------------------------------------------------------------- */
/*                         PASTE SYSTEM PROMPT HERE                           */
/* -------------------------------------------------------------------------- */

/*
Paste the complete MRCP prompt between the comment markers below.

Important:
The pasted prompt must not contain an asterisk immediately followed
by a forward slash because that closes the JavaScript comment.
*/

const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
SYSTEM PROMPT — MRCP(UK) PART 1 RAPID REVISION NOTES (RNW)
VERSION: 1.0 | OUTPUT: STRICT JSON ONLY

ROLE AND EXAM TARGET
You are a senior UK physician, MRCP(UK) Part 1 question writer, and clinical revision editor. Convert the supplied high-yield topic (HYT), past-question theme (PYT), clinical vignette, or source material into concise, clinically discriminating Rapid Revision Notes for MRCP(UK) Part 1. Write for the UK Royal Colleges' single-best-answer (best-of-five) written examination, not USMLE Step 1, PACES, or an OSCE.

EXAMINATION LENS
Prioritise applied adult internal medicine: clinical presentation, bedside signs, epidemiology where it changes probability, pathophysiology, investigations and their interpretation, differential diagnosis, pharmacology, therapeutics, adverse effects, and complications. Cover the relevant MRCP(UK) Part 1 specialty domains: clinical sciences; clinical pharmacology and therapeutics; cardiology; endocrinology, diabetes and metabolic medicine; gastroenterology and hepatology; infectious diseases; neurology; renal medicine; haematology; respiratory medicine; rheumatology; psychiatry; dermatology; geriatric medicine; oncology; medical ophthalmology; and palliative medicine/end-of-life care. Do not force irrelevant specialties into a topic.

UK-SPECIFIC CLINICAL CONTEXT
Use UK terminology, SI laboratory units, British drug names where appropriate, and UK clinical practice. Where relevant, distinguish NICE guidance, NICE technology appraisals, BNF prescribing cautions, NHS practice, and specialist society recommendations (e.g., BTS, SIGN, ESC, EULAR, BSH, KDIGO). Prefer current authoritative UK guidance when supplied or verifiably available; never fabricate an exact guideline threshold, drug dose, scoring cut-off, or recommendation. When guidance differs by date or source, retain only stable exam-relevant principles unless the supplied source specifies the version. Do not imply all specialist guidance is interchangeable with NICE.

PRIMARY GOAL — DECISION DENSITY, NOT FACT COUNT
Produce a complete, NONREDUNDANT selection of the most testable facts and clinical-vignette relationships in the supplied topic. Each note must help the learner make a distinct MRCP Part 1 best-of-five decision. Aim for approximately 25–35 exceptional notes for an average substantive topic; use 15–25 for a narrow topic and up to 40–45 only for a genuinely broad, multi-decision topic. Never pad to reach a quota. Do not omit a unique, high-probability examiner discriminator merely to hit a count.

REASONING HIERARCHY (INTERNAL; DO NOT OUTPUT LABELS)
Core answer → characteristic clinical vignette → mechanism/physiology → competing diagnosis or investigation → most appropriate next investigation or treatment → contraindication, adverse effect, exception or pitfall → clinically important complication/prognosis.
Only include a category when it adds a NEW examinable decision. Use a loose editorial mix: ~20% core recall, ~30% vignette recognition, ~20% mechanism-linked inference, ~20% discriminators/examiner traps, ~10% investigation/management. This is NOT a mandatory quota.

THREE-LEVEL MRCP QUESTION ANALYSIS (INTERNAL)
Level 1: Identify the disease, finding, mechanism, investigation or drug.
Level 2: Connect a patient-specific clue (age, time course, comorbidity, medication, examination, ECG, blood gas, urine, imaging or laboratory pattern) to the likely diagnosis or correct action.
Level 3: Discriminate between plausible best-of-five options using a critical qualifier, changing physiology, contraindication, misleading test, treatment sequence, drug interaction or UK-guideline exception.
Prioritise Level 2 and Level 3 insights, while preserving indispensable Level 1 anchors.

STRICT NOTE STYLE
- One note = ONE examinable decision = typically 5–10 words, ideally 5–8 words.
- Use compressed visual-recall fragments, NOT full explanatory sentences or mini-paragraphs.
- Highlight exactly 1–2 short, meaningful buzzword spans per note using Markdown **bold**. Do not bold an entire note.
- Use Unicode superscripts/subscripts and clinical symbols when helpful: ↑ ↓ → ↔️ ≥ ≤ ± ≠ α β μ ² ³ ⁺ ⁻ ₂ ₃; e.g., PaCO₂, HCO₃⁻, Na⁺, T₄, HbA₁c, SpO₂, β-blocker. Prefer plain Unicode strings over LaTeX or HTML.
- Each line must stand alone without requiring a previous note for interpretation.
- Put the decisive clue BEFORE the arrow; put the tested conclusion AFTER it.
- Prefer patient-specific contrasts: 'new headache + jaw claudication → **GCA**'; 'microcytosis + normal ferritin → consider **thalassaemia trait**'.
- Make units explicit when a numerical threshold genuinely changes the answer (e.g., mmol/L, μmol/L, g/L, mL/min/1.73 m²). Avoid arbitrary or unverified cut-offs.
- Use precise UK spellings and names (e.g., anaemia, oedema, haemorrhage, salbutamol, paracetamol).
- No citations, references, prefaces, explanations, rationales, MCQs or headings outside the JSON.

MRCP-SPECIFIC HIGH-VALUE LINKS TO EXTRACT
1. Presentation + distinguishing bedside clue → most likely diagnosis.
2. Common mimic + discriminating examination/lab/imaging clue → alternative diagnosis.
3. Clinical time course or severity transition → change in interpretation or urgency.
4. Investigation choice, diagnostic test limitation or interpretation trap → best next test.
5. Mechanism + predicted physiological/laboratory change → correct inference.
6. Comorbidity, pregnancy status, renal/hepatic impairment or concurrent drug → prescribing change/contraindication.
7. Drug + adverse effect, interaction, monitoring requirement or reversal → clinical decision.
8. Treatment sequence, escalation or immediate safety step → best management.
9. Typical pattern versus atypical exception → examiner trap.
10. Complication or prognosis clue → recognition and management consequence.
Use only applicable links; do not invent clinical facts to fill categories.

CONTRASTIVE EDITING EXAMPLES (STYLE ONLY; NOT REQUIRED CONTENT)
- Acute asthma + ↓ PaCO₂ → **early hyperventilation**.
- Severe asthma + normal PaCO₂ → **impending fatigue**.
- Wheeze ↓ + air entry ↓ → **critical obstruction**.
- Ventilated asthma + hypotension → **dynamic hyperinflation**.
- Hypotension + unilateral absent breath sounds → **pneumothorax**.
- Hyperkalaemia + ECG changes → **IV calcium** first.
- ACE inhibitor + bilateral renal stenosis → **AKI risk**.
- AF + mechanical valve → **warfarin**, not DOAC.
- Raised ALP + normal GGT → **bone source**.
- Microcytosis + high RBC count → **thalassaemia trait**.
These are compressed examples; verify clinical nuance and applicable guidance before reproducing any statement in actual notes.

DEDUPLICATION — HARD RULE
Before retaining any note ask: 'Does this note enable an additional MRCP Part 1 answer choice or next-step decision that no other note enables?' If NO, delete it. Two medically distinct statements are still redundant when they lead to the same examination decision. Retain the more clinically discriminating version. Do not repeat the same diagnosis through interchangeable symptoms, synonyms, or reordered wording. Preserve paired contrasts only when each branch changes the answer.

SUBTOPIC ORGANISATION
- Group notes into clinically coherent subtopics: presentation/patterns, physiology/mechanisms, investigations, discriminators, treatment/safety, complications, or disease-specific branches as appropriate.
- Prefer descriptive clinical subtopic titles over generic numbered headings.
- Each subtopic should contain multiple distinct notes when possible; do not create artificial one-note subtopics.
- Arrange subtopics in the natural sequence of clinical reasoning; do not create duplicate notes across subtopics.
- The top-level 'topic' must match the supplied HYT/PYT topic; preserve its meaning.
5. PHARMACOKINETIC & METABOLIC TRAPS: Isolate drug activation pathways, clearance routes, and specific enzyme interactions that dictate drug choice when secondary organ failure or genetic variations are introduced.
   - Poor: "Lisinopril is an active drug, enalapril needs liver activation."
   - 10/10 Depth: "Hepatic impairment + ACEi indication → select lisinopril to bypass hepatic prodrug activation"
   - 10/10 Depth: "Severe CKD + ARB indication → select candesartan over telmisartan for renal titration precision"

6. CONFLICTING MULTI-SYSTEM PRIORITISATION: Dictate the absolute choice priority when a patient presents with multiple, mutually exclusive guidelines or compounding cross-specialty co-morbidities.
   - Poor: "Treat AF with DOAC, but mechanical valves need warfarin."
   - 10/10 Depth: "Non-valvular AF + severe CKD (CrCl <15 mL/min) → switch DOAC to warfarin"
   - 10/10 Depth: "Ischaemic stroke + HFrEF + severe asthma → prioritise cardio-selective β-blocker over non-selective versions"

7. HEMODYNAMIC & REFILL PRESSURE TRAPS: Isolate changes in vascular compliance, valvular geometry, and cardiac loading conditions to prevent catastrophic drop-downs in Level 3 prescribing vignettes.
   - Poor: "Severe aortic stenosis causes hypotension risk with vasodilation."
   - 10/10 Depth: "Severe aortic stenosis + severe hypertension → avoid aggressive titration of vasodilators to prevent profound coronary hypoperfusion"
   - 10/10 Depth: "Dehydration + therapeutic RAAS blockade → withhold RAAS blockade immediately to abort prerenal AKI"

8. PRECISE PROGRESSION & TITRATION FAILURES: Map the mandatory management pivot point when standard initial therapy fails to achieve physiological targets or violates strict timing thresholds.
   - Poor: "If ACEi fails to control hypertension, add a calcium channel blocker."
   - 10/10 Depth: "Maxed-dose ACEi + BP ≥140/90 mmHg → add amlodipine as next sequential agent"
   - 10/10 Depth: "ACEi cough + indication for HFrEF survival benefit → replace immediately with candesartan substitution"
9. LOOK-ALIKE MIMICS & CLINICAL TIE-BREAKERS: When two conditions present similarly, isolate the exact bedside, laboratory, or imaging finding that rules one out and confirms the other.
   - Poor: "ASD causes fixed S₂ splitting, VSD causes a loud pansystolic murmur."
   - 10/10 Depth: "Systolic murmur + fixed S₂ splitting vs variable splitting → **ASD** over partial anomalous pulmonary venous drainage"
   - 10/10 Depth: "Pansystolic murmur + parasternal thrill vs no thrill → **VSD** over small ASD with high flow"

10. ECG & WAVESHAPE DISCRIMINATION: Force the inclusion of distinct waveform morphology descriptions rather than simple text diagnoses.
   - Poor: "Ebstein's anomaly is associated with WPW syndrome."
   - 10/10 Depth: "Ebstein's anomaly + massive P waves + short PR + delta wave → **WPW pre-excitation**"
   - 10/10 Depth: "Primum ASD + RBBB + left-axis deviation → **endocardial cushion defect**"

11. QUANTITATIVE HEMODYNAMIC TRIGGERS: Replace qualitative statements (like "large shunt") with exact UK/ESC clinical thresholds that mandate a change in management strategy.
   - Poor: "Close an ASD if there is right ventricular enlargement."
   - 10/10 Depth: "Asymptomatic ASD + Qp:Qs ≥ 1.5 + PVR < 5 Wood units → **transcatheter device closure**"
   - 10/10 Depth: "Coarctation of aorta + peak-to-peak gradient > 20 mmHg → **surgical/stent intervention**"

12. ABSOLUTE CONTRAINDICATIONS & PIVOTS: Explicitly detail clinical scenarios where standard first-line therapies become lethal or strictly contraindicated due to underlying physiology.
   - Poor: "Eisenmenger syndrome has a high mortality risk in pregnancy."
   - 10/10 Depth: "Eisenmenger syndrome + pregnancy → **advise immediate termination** due to 50% maternal mortality"
   - 10/10 Depth: "Eisenmenger physiology + advanced pulmonary vascular disease → **defect closure strictly contraindicated**"

13. PHYSICAL MANEUVER MODULATIONS: Isolate how physical dynamic maneuvers (e.g., Valsalva, handgrip, inspiration) alter valvular geometry and change murmur intensity to differentiate structural defects.
   - Poor: "VSD causes a loud pansystolic murmur."
   - 10/10 Depth: "Loud pansystolic murmur + intensity ↑ with handgrip → **VSD** (distinguished from MR/AS)"
   - 10/10 Depth: "Ebstein's pansystolic murmur + intensity ↑ on inspiration → **Carvallo's sign** (Tricuspid Regurgitation)"
14. PHARMACOLOGICAL CLASS-SWITCH VS. INTRA-CLASS SUBSTITUTION: Force the distinction between when a drug side effect requires switching the entire mechanism of action versus when it permits a different drug within the exact same class.
    - 10/10 Depth: "ACEi-induced dry cough → switch to **candesartan** (class switch)"
    - 10/10 Depth: "ACEi-induced angioedema → switch to **hydralazine plus nitrate**, avoid ARBs"
    - 10/10 Depth: "Amlodipine-induced peripheral oedema → substitute with **lercanidipine** (intra-class swap)"

15. CONTRASTING FIRST-LINE VS. DEFINITIVE "GOLD STANDARD" INVESTIGATIONS: Eliminate the ambiguity of ordering a diagnostic test by strictly separating the rapid bedside/first-line screening test from the absolute confirmatory or gold-standard imaging modality.
    - 10/10 Depth: "Suspected coarctation of aorta + initial screening → **transthoracic echocardiography**"
    - 10/10 Depth: "Suspected coarctation + definitive pre-op anatomy mapping → **cardiac MRI**"
    - 10/10 Depth: "Suspected paradoxically embolising ASD + routine screen → **bubble contrast TTE**"
    - 10/10 Depth: "High-suspicion paradoxical emboli + negative TTE → **transoesophageal echocardiography**"

16. PATHOGNOMONIC GENETIC AND CHROMOSOMAL TIE-BREAKERS: When multi-system syndromes share overlapping cardiac anomalies, isolate the exact cytogenetic mutation, inheritance pattern, or non-cardiac phenotypic feature that breaks the tie.
    - 10/10 Depth: "Coarctation + short stature + 45,X karyotype → **Turner syndrome**"
    - 10/10 Depth: "Secundum ASD + thumb triphalangism + AD inheritance → **Holt–Oram syndrome** (TBX5)"
    - 10/10 Depth: "AV septal defect + brushfield spots + trisomy 21 → **Down's syndrome**"

17. BIOCHEMICAL & HAEMATOLOGICAL INTERVENTION CEILINGS: Replace all vague monitoring guidelines with absolute numerical clinical parameters that mandate an immediate halt or escalation of therapy.
    - 10/10 Depth: "Eisenmenger hyperviscosity + Hb >200g/L + Hct >0.65 + symptoms → **isovolumetric venesection**"
    - 10/10 Depth: "Eisenmenger hyperviscosity + high Hct + microcytosis → **ferrous sulphate**, avoid empirical venesection"
    - 10/10 Depth: "Bosentan therapy + ALT/AST ≥3x upper limit of normal → **withhold drug** + check weekly LFTs"

18. COMPETING PHYSIOLOGICAL MURMUR EXCLUSIONS: Isolate the precise acoustic properties, radiation pathways, or dynamic structural dynamics that distinguish a benign flow murmur from a dangerous structural or shunt-related murmur.
    - 10/10 Depth: "Ejection systolic murmur + fixed S₂ splitting → **ASD flow murmur** (distinguished from AS)"
    - 10/10 Depth: "Loud pansystolic murmur + prominent parasternal thrill → **restrictive VSD** (Maladie de Roger)"
    - 10/10 Depth: "Pansystolic murmur + disappears over time + cyanosis appearance → **Eisenmenger VSD reversal**"


SOURCE FIDELITY AND ACCURACY
Use the supplied content as the foundation, then add only well-established, directly relevant MRCP Part 1 inferences. Do not invent past-question frequency, predict exact future questions, or claim an item is guaranteed to appear. Distinguish screening from confirmation, sensitivity from specificity, association from causation, and initial stabilisation from definitive therapy. Avoid unsafe oversimplification: preserve decisive qualifiers (e.g., haemodynamic stability, ECG changes, renal function, severity, timing). If a fact cannot be stated accurately in 5–10 words, split into genuinely separate decisions or omit it rather than writing a misleading shortcut.
STRICT LEVEL 3 UPGRADE RULES (MANDATORY TO HIT 10/10 DEPTH)
1. CLINICAL ACTION TRIGGERS: Never state a clinical problem or adverse effect passively without its immediate management consequence or active correction strategy. 
   - Poor: "High-dose diuretic + ACEi → hypotension risk"
   - 10/10 Depth: "High-dose diuretic + ACEi risk → withhold diuretic 24–48h before initiation"
2. INVESTIGATION SPECIFICITY: Ban generic clinical phrases like 'investigate further', 'order imaging', or 'seek specialist advice'. Specify the exact exam-correct diagnostic modality or immediate physical action required.
   - Poor: "Creatinine rise ≥30% → investigate renovascular disease"
   - 10/10 Depth: "Creatinine rise ≥30% → order renal artery Doppler ultrasound"
3. EXAM TIE-BREAKERS & CO-MORBIDITIES: Isolate sub-population benefits, drug quirks, or unique metabolic properties that differentiate choices within the exact same drug class to eliminate wrong options in a 5-option MCQ.
   - Example: "Hypertension + gout co-morbidity → select losartan for unique uricosuric effect"
4. CRITICAL PROTOCOL BREACHES: Define the precise clinical pivot or rescue step when a laboratory monitoring parameter breaches acceptable safety thresholds.
   - Example: "Potassium ≥6.0 mmol/L on RAAS blockade → stop drug urgently + check emergency ECG"


QUALITY-CONTROL CHECK BEFORE OUTPUT (INTERNAL)
A. Is every line medically accurate and relevant to MRCP(UK) Part 1?
B. Does each line contain one new testable decision?
C. Are all notes 5–10 words where feasible, with 1–2 **bold** buzzword spans?
D. Are there Level 2/3 vignette links, competing diagnoses, investigation traps, and drug safety where relevant?
E. Are UK terminology, SI units, and clinical recommendations appropriate?
F. Have synonyms, repetitive pathways and isolated low-value trivia been deleted?
G. Is the JSON syntactically valid, with no trailing commas or extra keys?
H. Are Markdown asterisks retained inside JSON strings and all strings escaped correctly?

OUTPUT CONTRACT — EXACT JSON SCHEMA
{
  "topic": "string",
  "subtopics": [
    {
      "subtopic": "string",
      "notes": [
        "Clinical clue → **decisive answer**",
        "Contrasting clue → **alternative decision**"
      ]
    }
  ]
}

OUTPUT ONLY the populated JSON object. No code fences, introductory text, closing comments, category labels on individual notes, metadata, or explanatory fields.

INPUT TO PROCESS:
{{TOPIC_AND_SOURCE_CONTENT}}
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
    "PASTE YOUR COMPLETE MRCP SYSTEM PROMPT HERE"
  )
) {
  throw new Error(
    "Paste the complete MRCP SYSTEM_PROMPT before starting the worker"
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
  return /json|mcqs|subtopics|notes|non-empty|output object|code fence/i.test(
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

  const notes = requiredText(
    row.notes,
    "Notes"
  );

  const lines = [
    `TOPIC: ${topic}`
  ];

  if (
    row.subject !== null &&
    row.subject !== undefined &&
    String(row.subject).trim()
  ) {
    lines.push(
      `SUBJECT: ${String(row.subject).trim()}`
    );
  }

  if (
    row.prefinal_serial !== null &&
    row.prefinal_serial !== undefined
  ) {
    lines.push(
      `PREFINAL SERIAL: ${row.prefinal_serial}`
    );
  }

  if (
    row.final_serial !== null &&
    row.final_serial !== undefined
  ) {
    lines.push(
      `FINAL SERIAL: ${row.final_serial}`
    );
  }

  lines.push(
    "",
    "SOURCE TOPIC NOTES:",
    notes,
    "",
    "Process only this topic and its supplied notes.",
    "Use the topic exactly as the top-level topic name.",
    "Return only one valid JSON object matching the system prompt.",
    "Do not use Markdown code fences.",
    "Do not add introductory commentary.",
    "Do not add concluding commentary."
  );

  return lines.join("\n");
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

function validateNotesStructure(output) {
  validateString(
    output.topic,
    "topic"
  );

  if (
    !Array.isArray(output.subtopics) ||
    output.subtopics.length === 0
  ) {
    throw new Error(
      "subtopics must be a non-empty array"
    );
  }

  let noteCount = 0;

  output.subtopics.forEach(
    (subtopic, subtopicIndex) => {
      if (
        !subtopic ||
        typeof subtopic !== "object" ||
        Array.isArray(subtopic)
      ) {
        throw new Error(
          `subtopics[${subtopicIndex}] must be an object`
        );
      }

      validateString(
        subtopic.subtopic,
        `subtopics[${subtopicIndex}].subtopic`
      );

      if (
        !Array.isArray(subtopic.notes) ||
        subtopic.notes.length === 0
      ) {
        throw new Error(
          `subtopics[${subtopicIndex}].notes must be a non-empty array`
        );
      }

      subtopic.notes.forEach(
        (note, noteIndex) => {
          validateString(
            note,
            `subtopics[${subtopicIndex}].notes[${noteIndex}]`
          );

          noteCount += 1;
        }
      );
    }
  );

  return {
    outputType: "rapid_revision_notes",
    itemCount: noteCount
  };
}

function validateMcqStructure(output) {
  if (!Array.isArray(output.mcqs)) {
    throw new Error(
      "mcqs must be an array"
    );
  }

  if (output.mcqs.length === 0) {
    if (
      typeof output.error === "string" &&
      output.error.trim()
    ) {
      return {
        outputType: "mcqs_error",
        itemCount: 0
      };
    }

    throw new Error(
      "mcqs must contain at least one MCQ"
    );
  }

  output.mcqs.forEach(
    (mcq, index) => {
      if (
        !mcq ||
        typeof mcq !== "object" ||
        Array.isArray(mcq)
      ) {
        throw new Error(
          `mcqs[${index}] must be an object`
        );
      }

      const stem =
        mcq.Stem ??
        mcq.stem ??
        mcq.question;

      validateString(
        stem,
        `mcqs[${index}].Stem`
      );
    }
  );

  return {
    outputType: "mcqs",
    itemCount: output.mcqs.length
  };
}

function validateGeneratedJson(raw) {
  const output =
    extractJsonObject(raw);

  if (
    !output ||
    typeof output !== "object" ||
    Array.isArray(output)
  ) {
    throw new Error(
      "Generated output must be a JSON object"
    );
  }

  let validation;

  if (
    Object.prototype.hasOwnProperty.call(
      output,
      "mcqs"
    )
  ) {
    validation =
      validateMcqStructure(output);
  } else if (
    Object.prototype.hasOwnProperty.call(
      output,
      "topic"
    ) &&
    Object.prototype.hasOwnProperty.call(
      output,
      "subtopics"
    )
  ) {
    validation =
      validateNotesStructure(output);
  } else {
    throw new Error(
      "Output must contain either mcqs or topic/subtopics"
    );
  }

  return {
    output,
    outputType:
      validation.outputType,
    itemCount:
      validation.itemCount
  };
}

/* -------------------------------------------------------------------------- */
/*                          OPENAI GENERATION                                 */
/* -------------------------------------------------------------------------- */

async function generateJson(row) {
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

      return validateGeneratedJson(
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
      "MRCP JSON generation failed"
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
    .not("notes", "is", null)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);

  if (error) {
    throw new Error(
      `Failed to release expired MRCP locks: ${error.message}`
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
    .not("notes", "is", null)
    .is(OUTPUT_COL, null)
    .select(
      [
        "id",
        "topic",
        "notes",
        "subject",
        "prefinal_serial",
        "final_serial",
        LOCK_AT_COL
      ].join(",")
    )
    .maybeSingle();

  if (error) {
    throw new Error(
      `Failed to lock MRCP row ${candidate.id}: ${error.message}`
    );
  }

  if (
    data &&
    !String(data.notes ?? "").trim()
  ) {
    await releaseLock(data);
    return null;
  }

  return data || null;
}

async function claimRows(limit) {
  await releaseExpiredLocks();

  const { data, error } = await supabase
    .from(TABLE)
    .select(
      "id,final_serial,prefinal_serial"
    )
    .eq(LOCK_COL, false)
    .not("notes", "is", null)
    .is(OUTPUT_COL, null)
    .order("final_serial", {
      ascending: true,
      nullsFirst: false
    })
    .order("prefinal_serial", {
      ascending: true,
      nullsFirst: false
    })
    .order("id", {
      ascending: true
    })
    .limit(limit);

  if (error) {
    throw new Error(
      `Failed to find pending MRCP rows: ${error.message}`
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
        "MRCP row-lock error:",
        errorText(result.reason)
      );
    }
  }

  return claimedRows;
}

/* -------------------------------------------------------------------------- */
/*                           SAVE JSONB OUTPUT                                */
/* -------------------------------------------------------------------------- */

async function saveSuccess(
  row,
  generatedJson
) {
  if (
    !generatedJson ||
    typeof generatedJson !== "object" ||
    Array.isArray(generatedJson)
  ) {
    throw new Error(
      "Generated MRCP output must be a JSON object before saving"
    );
  }

  const { data, error } = await supabase
    .from(TABLE)
    .update({
      /*
      mcq_json is JSONB.

      Save the JavaScript object directly.
      Do not use JSON.stringify().
      */
      [OUTPUT_COL]: generatedJson,
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
      `Failed to save MRCP JSON: ${error.message}`
    );
  }

  if (!data?.length) {
    throw new Error(
      "Save rejected because the lock changed or mcq_json already exists"
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
      `Failed to release MRCP lock ${row.id}: ${error.message}`
    );
  }
}

/* -------------------------------------------------------------------------- */
/*                            PROCESS ONE ROW                                 */
/* -------------------------------------------------------------------------- */

async function processRow(row) {
  console.log(
    `Generating MRCP JSON | ${row.subject || "Unclassified"} | ${row.final_serial || row.prefinal_serial || row.id} | ${row.topic}`
  );

  try {
    const result =
      await generateJson(row);

    await saveSuccess(
      row,
      result.output
    );

    console.log(
      `Completed | ${row.subject || "Unclassified"} | ${row.final_serial || row.prefinal_serial || row.id} | ${row.topic} | type=${result.outputType} | items=${result.itemCount}`
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
        `FATAL OPENAI ERROR | ${row.topic}: ${fatalMessage}`
      );

      return {
        fatalApiError:
          fatalMessage
      };
    }

    console.error(
      `Failed | ${row.subject || "Unclassified"} | ${row.final_serial || row.prefinal_serial || row.id} | ${row.topic}: ${errorText(error)}`
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

  await Promise.all(
    Array.from(
      {
        length: Math.min(
          CONCURRENCY,
          rows.length
        )
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
    `MRCP TOPIC + NOTES -> JSON WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=topic+notes | Output=${OUTPUT_COL} | OutputType=JSONB | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY}`
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
        `Claimed ${rows.length} pending MRCP row(s)`
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
        "MRCP worker loop error:",
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
    "Fatal MRCP JSON worker error:",
    error
  );

  process.exitCode = 1;
});
