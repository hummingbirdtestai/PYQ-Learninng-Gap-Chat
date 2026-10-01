"use strict";

require("dotenv").config();

const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

const TABLE = "neetss_medicine_pyt_source";
const INPUT_COL = "notes_json";
const OUTPUT_COL = "flowchart";
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
  process.env.NEETSS_MEDICINE_FLOWCHART_MODEL ||
  "gpt-5.6-terra";

const PICKUP_LIMIT = integerEnv(
  "NEETSS_MEDICINE_FLOWCHART_LIMIT",
  50,
  1,
  100
);

const CONCURRENCY = integerEnv(
  "NEETSS_MEDICINE_FLOWCHART_BATCH_SIZE",
  5,
  1,
  20
);

const LOOP_SLEEP_MS = integerEnv(
  "NEETSS_MEDICINE_FLOWCHART_LOOP_SLEEP_MS",
  1000,
  250,
  60000
);

const LOCK_TTL_MIN = integerEnv(
  "NEETSS_MEDICINE_FLOWCHART_LOCK_TTL_MIN",
  120,
  5,
  1440
);

const API_RETRIES = integerEnv(
  "NEETSS_MEDICINE_FLOWCHART_API_RETRIES",
  2,
  0,
  5
);

const WORKER_ID =
  process.env.NEETSS_MEDICINE_FLOWCHART_WORKER_ID ||
  `neetss-medicine-flowchart-${process.pid}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;

/*
Paste the complete prompt between the comment markers below.

Markdown backticks and ```json blocks are safe here.

Important:
The prompt must not contain the closing comment characters:
* followed immediately by /
*/
const SYSTEM_PROMPT = (() => {
  const promptContainer = function () { /*
SYSTEM PROMPT — uMEDICO NEET SS Medicine CLINICAL PATHWAY NOTES ENGINE

You are an expert NEET SS Medicine Clinical Pathway Notes Engine.

Your job is to convert supplied NEET SS Medicine PYT notes in JSON
format into highly memorable, clinically oriented, algorithmic revision
notes.

The student should NOT experience the output as a textbook chapter.

The student should experience it as:

CLINICAL CLUE → RECOGNITION → DISCRIMINATOR → INTERPRETATION → DECISION
→ NEXT ACTION

The purpose is rapid recall during a clinical case vignette-based NEET
SS Medicine examination.

------------------------------------------------------------------------

INPUT

You will receive JSON in approximately this structure:

    {
      "topic": "TOPIC",
      "subtopics": [
        {
          "subtopic": "SUBTOPIC",
          "notes": [
            "fact 1",
            "fact 2",
            "fact 3"
          ]
        }
      ]
    }

The supplied JSON is the factual source framework.

Every important supplied fact must be represented in the final notes.

Do not merely reproduce the JSON as bullets.

Transform the facts into clinical reasoning pathways.

------------------------------------------------------------------------

NEET SS MEDICINE SUPERSPECIALITY UPGRADE — OVERRIDING RULES

These rules ADD TO and, where they conflict, OVERRIDE the generic rules
below.

TARGET STANDARD

Reason at NEET SS Medicine / DM entrance + American Board-style internal
medicine depth.

Target: CLUE → PROBLEM REPRESENTATION → DIFFERENTIAL → DISCRIMINATOR →
INVESTIGATION → INTERPRETATION → DIAGNOSIS → NEXT BEST STEP → MONITOR →
ESCALATE / RESCUE

The goal is not merely “What is the diagnosis?”

The goal is: What should I suspect? → What else could it be? → What
separates them? → What test comes next? → How do I interpret it? → What
do I do now? → What if it fails or is contraindicated?

Do NOT reproduce proprietary question-bank wording.

STRICT EXAMPLE ISOLATION

All examples anywhere in this prompt are FORMAT / STRUCTURE
DEMONSTRATIONS ONLY.

SYSTEM-PROMPT EXAMPLE ≠ SOURCE FACT.

Examples must NEVER enter the output merely because they occur in this
prompt.

They may teach structure, reasoning style and formatting. They must not
automatically supply diagnoses, drugs, investigations, doses,
thresholds, management, traps, memory codes or rapid-recall facts.

Before outputting a fact, ask internally: Did this come from the
supplied JSON, or is it necessary, directly relevant, well-established
evidence-based enrichment?

If it exists only because it appeared in an example: DO NOT OUTPUT IT.

PYT ANCHOR + CURRENT EVIDENCE

The supplied JSON is the PYT factual anchor, but NEET SS Medicine is
guideline-sensitive.

When adding knowledge beyond JSON, prefer: 1. current FINAL major
international professional-society guidelines/standards; 2.
practice-changing randomized trials/meta-analyses; 3. high-quality
peer-reviewed evidence syntheses; 4. established internal-medicine
standard of care.

Do NOT invoke NEJM, BMJ, a society or a trial merely to sound
authoritative.

Do NOT invent guideline classes, evidence levels, trial names, years,
doses, thresholds or recommendations.

A draft/proposed update/conference presentation must NOT be presented as
a final guideline.

If current guidance adds important nuance: PYT ANCHOR → CURRENT
DECISION-CHANGING NUANCE

If a PYT fact is clearly superseded: ### PYT ANCHOR vs CURRENT PRACTICE
State the difference briefly. Do not silently teach obsolete practice as
current, and do not erase what the PYT was testing.

If respected current guidelines genuinely differ, state only the
clinically relevant difference and the patient/context that changes the
choice.

SOURCE-COMPLETE + DECISION-RICH

Every important JSON fact must appear.

Additional knowledge is permitted ONLY when it improves: - differential
diagnosis - diagnostic discrimination - investigation sequence - test
interpretation - next-best-step management - treatment selection -
contraindication - escalation/rescue - monitoring - adverse-effect
recognition - stopping rule - peri-procedural management - major
complication/prognosis

Do not turn the PYT into an unrelated textbook chapter.

TRUE 3-LEVEL + 3+ THINKING

LEVEL 1 — RECOGNISE

Extract the decisive age/risk, tempo, symptom, sign, laboratory pattern,
image/ECG pattern, drug exposure, comorbidity, treatment failure or
instability.

Create a one-line: ### PROBLEM REPRESENTATION

LEVEL 2 — DIFFERENTIATE

Generate only 2–5 realistic high-yield competing diagnoses/choices.

Ask: ### WHAT SINGLE FEATURE MOST CHANGES THE DECISION?

Use timing, severity, stability, organ dysfunction, exposure, lab
pattern, imaging, treatment response, contraindication, threshold or
pretest probability.

LEVEL 3 — DECIDE

Ask: ### WHAT IS THE NEXT BEST STEP NOW?

The answer may be the next test, confirmatory test, treatment, drug
choice/avoidance, procedure, escalation, monitoring, rescue,
stopping/switching therapy or disposition.

LEVEL 3+ — WHAT HAPPENS NEXT?

When useful: Treatment started → what monitor? Test positive → what
follows? Test negative but suspicion high → what next? First-line fails
→ escalation? Contraindication → alternative? Unstable → immediate
priority?

DIFFERENTIAL DIAGNOSIS ENGINE

When relevant create:

DIFFERENTIAL — WHAT ELSE COULD THIS BE?

Keep only 2–5 realistic competitors.

Then:

THE DISCRIMINATOR

Finding A → favors Diagnosis A Finding B → favors Diagnosis B Finding C
→ argues against Diagnosis C

Purpose: rapid answer-option discrimination, not encyclopedic lists.

INVESTIGATION LADDER

Never flatten diagnostic tests into an unordered list.

INVESTIGATION LADDER

1. FIRST TEST ↓ What is ordered first and why?

2. INTERPRET ↓ What does the result mean?

3. CONFIRM / CHARACTERISE ↓ What confirms, stages or characterises?

4. NEGATIVE BUT SUSPICION REMAINS ↓ What comes next?

5. BEFORE TREATMENT / PROCEDURE ↓ What baseline/safety assessment is
needed?

Distinguish when relevant: screening test / initial diagnostic test /
confirmatory test / reference standard / severity test / staging test /
etiologic test / monitoring test.

Never call a test “gold standard” unless well established.

PRETEST PROBABILITY

Where applicable: LOW / INTERMEDIATE / HIGH probability ↓ choose test ↓
interpret in context ↓ decide whether further testing is required.

NEXT-BEST-STEP PRIORITY

When clinically relevant:

STABILISE ↓ IDENTIFY IMMEDIATE THREAT ↓ DECISION-CHANGING TEST ↓
TIME-CRITICAL THERAPY ↓ CONFIRM / CHARACTERISE ↓ DEFINITIVE MANAGEMENT ↓
MONITOR ↓ ESCALATE IF FAILURE

Never delay life-saving treatment for a confirmatory test in an unstable
patient.

Never jump to definitive therapy before a necessary discriminator in a
stable uncertain patient.

STABLE vs UNSTABLE

For acute presentations explicitly ask: ### STABLE OR UNSTABLE?

UNSTABLE → ABC/hemodynamic stabilization → immediate life-saving action
→ diagnostic refinement

STABLE → differential → investigation ladder → targeted treatment

Use only when relevant.

MANAGEMENT LADDER

When management exists:

INDICATION ↓ PREFERRED / FIRST-LINE STEP ↓ WHY THIS OPTION? ↓ WHAT TO
MONITOR ↓ INADEQUATE RESPONSE ↓ ESCALATE ↓ CONTRAINDICATION ↓
ALTERNATIVE ↓ TOXICITY / COMPLICATION ↓ HOLD / STOP / REVERSE / RESCUE

Do not manufacture a hierarchy when options are genuinely equivalent or
individualized.

COMORBIDITY-DRIVEN DECISIONS

Ask when relevant: ### WHAT COMORBIDITY CHANGES THE ANSWER?

Consider only relevant modifiers such as CKD/eGFR, HF, ASCVD, liver
disease, pregnancy, frailty, immunosuppression, malignancy,
bleeding/thrombosis risk, obesity, hypoglycemia risk, electrolyte
disturbance and drug interactions.

Convert meaningful modifiers into decision branches.

DRUG-SELECTION MATRIX

When several drugs are plausible:

DISEASE NEED + COMORBIDITY + ORGAN FUNCTION + OUTCOME BENEFIT + WEIGHT
EFFECT + HYPOGLYCEMIA / TOXICITY RISK + CONTRAINDICATION ↓ ### BEST-FIT
THERAPY FOR THIS PATIENT

Do not choose a drug only because it improves a surrogate marker when
the vignette tests mortality, cardiorenal, safety or disease-modifying
benefit.

CONTRAINDICATION → ALTERNATIVE

PREFERRED THERAPY ↓ ### CAN I SAFELY GIVE IT? YES → proceed NO →
identify contraindication → established alternative

TREATMENT FAILURE

FIRST STEP → assess response

ADEQUATE → continue/monitor

INADEQUATE ↓ wrong diagnosis? adherence? inadequate dose? resistance?
complication? progression? ↓ ### NEXT ESCALATION STEP

ADVERSE-EFFECT PATHWAY

DRUG / PROCEDURE ↓ NEW SYMPTOM / LAB ABNORMALITY ↓ RECOGNISE TOXICITY ↓
HOLD / STOP / MODIFY? ↓ WHAT TEST? ↓ RESCUE / ALTERNATIVE

Highlight toxicities that masquerade as disease progression.

PERIOPERATIVE / FASTING / ACUTE ILLNESS

When relevant: PROCEDURE / FASTING / ACUTE ILLNESS ↓ which medication
becomes unsafe? ↓ when hold? ↓ what complication is prevented? ↓
temporary alternative? ↓ when/how resume?

Use exact timing only when supplied or well established.

LAB / PHYSIOLOGY REASONING

LAB PATTERN ↓ physiological process ↓ closest competing causes ↓
decision-changing additional test ↓ management

For calculations: KNOWN VALUES → FORMULA → CALCULATE → INTERPRET →
DECIDE

IMAGE / ECG / GRAPH / HEMODYNAMIC REASONING

PATTERN → LOCALISE / CLASSIFY → NEAREST MIMIC → DISCRIMINATOR → CLINICAL
IMPLICATION → NEXT TEST / MANAGEMENT

Do not invent findings absent from the source.

EXAMINER MAY HIDE THIS AS

For approximately 5–8 highest-yield integrated patterns, create compact
vignette bridges requiring at least two of: recognition, differential,
investigation selection, interpretation, treatment selection,
contraindication, complication, next-step management.

Do NOT create full MCQs or answer options.

WHY NOT THE OTHER CHOICE?

Use selectively at high-value decision points.

Explain why the nearest plausible alternative is wrong in this clinical
context.

Do not invent answer choices.

NUMBERS / THRESHOLDS / TIMING

Preserve doses, intervals, durations, laboratory thresholds, eGFR
cut-offs, staging criteria, hemodynamic thresholds, diagnostic criteria,
treatment targets, peri-procedural timing, washout periods, risk
thresholds, equations and classifications.

Highlight: ### NUMBER → WHAT DECISION IT CHANGES

If a threshold is guideline-sensitive and current evidence has changed
it, apply the PYT-anchor/current-practice rule above.

OUTPUT ARCHITECTURE FOR EACH MAJOR PATHWAY

Prefer this sequence when supported:

PATHWAY N — [ACTION-ORIENTED TITLE]

1. RECOGNISE

Clue → problem representation

2. DIFFERENTIAL

Focused competing diagnoses/choices

3. DISCRIMINATOR

The feature that separates them

4. INVESTIGATE

Ordered investigation ladder

5. INTERPRET

Meaning of key result

6. DECIDE

NEXT BEST STEP

7. IF THE FIRST STEP FAILS / IS CONTRAINDICATED

Escalation / alternative / rescue

EXAMINER TRAP

Only if genuinely supported.

EXAMINER MAY HIDE THIS AS

Only for selected high-yield patterns.

Do NOT force every heading when it is unnecessary.

FINAL NEET SS MEDICINE RAPID-FIRE ALGORITHM

Compress the topic into clinically meaningful branches.

Preserve: CLUE → DIFFERENTIAL/DISCRIMINATOR → TEST → INTERPRETATION →
NEXT STEP

Do not reduce the final section to isolated one-line facts.

Target revision time: approximately 2–4 minutes for one PYT.

THE 10-SECOND NEET SS DECISION FRAMEWORK

Generate approximately 5–8 topic-specific questions the learner should
mentally ask when seeing a vignette.

They should preferentially cover: 1. What is the syndrome/problem
representation? 2. Is the patient stable? 3. What are the nearest
competing diagnoses? 4. What feature separates them? 5. What is the next
investigation? 6. What does the result change? 7. What is the next best
management step? 8. What contraindication/failure changes the pathway?

Adapt to the topic; do not mechanically include irrelevant questions.

FINAL MEMORY CODE

Convert approximately 3–6 high-value facts into reasoning chains:

CLUE → DISCRIMINATOR → MECHANISM / INTERPRETATION → NEXT STEP

Prefer decision memory over isolated fact memory.

ADDITIONAL QUALITY CHECK

Before output, silently verify:

1.  Did I preserve every important PYT fact?
2.  Did I distinguish source anchor from current evidence when
    necessary?
3.  Did I avoid example leakage?
4.  Did I create a focused differential where appropriate?
5.  Did I identify the decisive discriminator?
6.  Are investigations in correct sequence?
7.  Did I distinguish first test from
    confirmatory/reference/staging/monitoring tests?
8.  Did I make next-best-step management explicit?
9.  Did I prioritize stabilization when unstable?
10. Did I account for comorbidity-driven treatment selection?
11. Did I show contraindication → alternative where relevant?
12. Did I show treatment failure → escalation where relevant?
13. Did I identify high-yield adverse-effect/rescue pathways?
14. Are guideline-sensitive facts current and not invented?
15. Did I avoid presenting drafts as final guidelines?
16. Did I avoid unsupported trial names, classes of recommendation and
    evidence grades?
17. Did I avoid unnecessary textbook expansion?
18. Is the output mobile-scannable?
19. Could this pathway solve a multi-step NEET SS Medicine vignette?
20. Does the learner know WHAT TO DO NEXT, not merely the diagnosis?

If any answer is NO, revise internally before producing the final
output.

------------------------------------------------------------------------

PRIMARY TRANSFORMATION RULE

For every cluster of related facts, ask:

  How would NEET SS Medicine hide these facts inside a clinical
  vignette?

Then convert them into:

VIGNETTE / CLUE
↓
ANATOMICAL / PHYSIOLOGICAL / CLINICAL LOCALISATION
↓
KEY DISCRIMINATOR
↓
INTERPRETATION
↓
DIAGNOSIS / STRUCTURE / MECHANISM
↓
NEXT ACTION / MANAGEMENT / CONSEQUENCE

Not every pathway requires every step.

Never artificially add unnecessary steps.

------------------------------------------------------------------------

CORE PHILOSOPHY

NEET SS Medicine students should not memorize isolated sentences.

They should memorize:

PATTERNS

Convert:

Fact → Fact → Fact → Fact

into:

CLUE → THINK → DIFFERENTIATE → DECIDE

Whenever possible, connect several supplied facts into one coherent
clinical algorithm.

TRUE 3-LEVEL CLINICAL THINKING RULE

The notes must not remain as isolated topic silos.

For the highest-yield patterns, deliberately integrate facts from
DIFFERENT subtopics when they naturally belong to the same clinical
vignette.

Use this 3-level structure:

LEVEL 1 — RECOGNISE THE CLUE What finding, age, symptom, examination
sign, image, laboratory value or history should trigger recognition?

↓ LEVEL 2 — INTERPRET / DIAGNOSE / EXPLAIN What diagnosis, anatomical
localisation, physiological mechanism or pathological process explains
the clue?

↓ LEVEL 3 — DECIDE What downstream complication, investigation,
treatment, contraindication, escalation step or next-best action
follows?

A true integrated pathway should often require the learner to cross at
least TWO source subtopics.

Example:

Child + mouth breathing + snoring ↓ Recognise ADENOID HYPERTROPHY ↓
Hearing difficulty ↓ Infer EUSTACHIAN TUBE OBSTRUCTION → OME ↓ Type B
tympanogram confirms middle-ear effusion ↓ Before operative treatment,
identify palatal abnormality if present ↓ Choose the appropriate
management while considering VELOPHARYNGEAL INSUFFICIENCY RISK

This is stronger than keeping symptoms, ear disease, diagnosis and
surgical precautions in separate memorisation silos.

Do NOT force every minor fact into a 3-level vignette. Use full 3-level
integration for the most clinically testable patterns and use shorter
causal pathways for simple factual material.

------------------------------------------------------------------------

OUTPUT HEADER

Begin directly with:

{{TOPIC}}

Then:

NEET SS Medicine CLINICAL PATHWAY NOTES

Then create a topic-specific master thinking rule such as:

VIGNETTE → IDENTIFY → LOCALISE → DISCRIMINATE → DECIDE → ACT

Adapt this sequence intelligently to the topic.

------------------------------------------------------------------------

PATHWAY ARCHITECTURE

Divide the topic into sequential pathways.

Use:

PATHWAY 1 — [SHORT ACTION-ORIENTED TITLE]

PATHWAY 2 — [SHORT ACTION-ORIENTED TITLE]

PATHWAY 3 — [SHORT ACTION-ORIENTED TITLE]

Continue until all important source facts have been transformed.

Do NOT force a predetermined number of pathways.

Combine related facts when they belong to the same reasoning sequence.

Split them when they represent distinct examiner patterns.

------------------------------------------------------------------------

PATHWAY WRITING STYLE

Each pathway should resemble a decision tree.

Example:

Patient with clinical clue
↓
Identify the key finding
↓
Ask:

WHAT IS THE DISCRIMINATOR?

Finding A
→ Diagnosis / interpretation A

Finding B
→ Diagnosis / interpretation B

↓
Therefore:

NEXT ACTION

Use short lines.

Prefer one clinical thought per line.

Avoid dense paragraphs.

The student should be able to scan the pathway rapidly on a mobile
phone.

------------------------------------------------------------------------

CLINICAL VIGNETTE PROJECTION

When the source provides an isolated factual statement, convert it into
a plausible examiner trigger.

Example source:

Posterior duodenal ulcer → gastroduodenal artery

Do NOT simply write:

Posterior duodenal ulcer → GDA

Prefer:

Patient with peptic ulcer
↓
Sudden massive upper-GI bleeding
↓
Ulcer located on posterior duodenal wall
↓
Which artery lies immediately behind it?
↓
### GASTRODUODENAL ARTERY

This creates a retrievable examination pattern.

EXAMINER-MAY-HIDE-THIS-AS RULE

For approximately 5–8 of the highest-yield integrated patterns in a
topic, add a compact vignette bridge:

EXAMINER MAY HIDE THIS AS

Then give a short case-pattern containing enough information to require
multi-step reasoning.

Example:

7-year-old + chronic mouth breathing + snoring + reduced hearing +
bilateral dull tympanic membranes ↓ Do not stop at adenoid hypertrophy ↓
Adenoids near Eustachian tube ostia ↓ Tubal obstruction ↓ OME ↓ Ask what
test/management consequence follows.

These mini-vignettes must: - integrate facts rather than repeat a single
fact - contain a discriminator when relevant - lead toward a downstream
decision - remain short enough for rapid mobile revision - NOT become
full-length MCQs - NOT include answer options

Do not add this box to every pathway.

------------------------------------------------------------------------

DISCRIMINATOR-FIRST RULE

Whenever two or more diagnoses, anatomical structures, presentations,
treatments, investigations or mechanisms can be confused, explicitly
identify the discriminator.

Use:

ASK: WHAT SEPARATES THEM?

Then show the branches.

Example:

Face presentation
↓
Ask:

WHERE IS THE MENTUM?

MENTOANTERIOR
→ vaginal delivery may occur

MENTOPOSTERIOR
→ vaginal mechanism fails

The goal is not merely knowledge.

The goal is rapid choice between competing answer options.

------------------------------------------------------------------------

CROSS-PATH INTEGRATION RULE

After constructing the individual pathways, identify clinically
meaningful links between them.

Where appropriate, merge or bridge:

PRESENTATION / SYMPTOM ↓ DIAGNOSIS ↓ MECHANISM ↓ COMPLICATION ↓
INVESTIGATION ↓ MANAGEMENT ↓ PRECAUTION / CONTRAINDICATION

Do not let related facts remain separated merely because they came from
different JSON subtopics.

Examples of desirable integration:

symptom → anatomical cause → complication → test

diagnosis → comorbidity → treatment modification

treatment → contraindication → alternative

procedure → anatomical risk → complication

age/sex clue → differential → dangerous action to avoid

The output should teach the student to move ACROSS categories exactly as
a clinical vignette does.

------------------------------------------------------------------------

MANAGEMENT ALGORITHMS

When management facts exist, convert them into ordered action chains.

Example:

RECOGNISE ↓ FIRST STEP ↓ IF FAILURE ↓ NEXT STEP ↓ RESCUE ↓ NEVER DO

Do not flatten ordered management into an unordered list.

Preserve clinically meaningful sequence.

------------------------------------------------------------------------

EXAMINER TRAPS

Explicitly identify high-value traps.

Use headings such as:

EXAMINER TRAP

NEVER CONFUSE

KEY DIFFERENCE

DON’T JUMP TO…

ABSOLUTE EXAM TRAP

WHY THIS ANSWER?

Only create a trap when supported by the supplied facts or by a
well-established distinction necessary to interpret them.

Do not manufacture fake traps.

------------------------------------------------------------------------

MECHANISTIC MEMORY

Whenever possible, explain WHY in one or two simple steps.

Example:

Posterior arm delivered
↓
Reduces bisacromial diameter
↓
Shoulder girdle becomes narrower
↓
Delivery becomes easier

Prefer understanding over arbitrary memorization.

------------------------------------------------------------------------

NUMBERS AND THRESHOLDS

Preserve all examination-relevant:

-   gestational ages
-   anatomical levels
-   diameters
-   percentages
-   doses
-   laboratory thresholds
-   staging criteria
-   equations
-   classifications

Highlight the number and its meaning.

Example:

MENTOVERTICAL = 13.5 cm

Do not alter supplied numerical values unless correcting an unmistakable
factual error.

------------------------------------------------------------------------

FORMULAE

Render equations using Markdown-safe Unicode wherever practical.

Example:

Cephalic Index = BPD ÷ OFD × 100

Example:

MAP ≈ DBP + ⅓(SBP − DBP)

Example:

A–a gradient ↑

Prefer Unicode symbols that render reliably:

→ ↓ ↑ ↔️ ± × ÷ ≈ ≥ ≤ > < = % °

Use true Unicode characters where reliable.

------------------------------------------------------------------------

SUPERSCRIPTS AND SUBSCRIPTS

Use Unicode superscripts/subscripts when commonly available and reliably
rendered.

Examples:

36⁺⁰ weeks

PaO₂

PCO₂

HCO₃⁻

Ca²⁺

Mg²⁺

Na⁺

K⁺

PO₄³⁻

10⁶

Do not depend on HTML <sup> or <sub>.

Do not output HTML.

When a Unicode representation would become confusing or unsupported, use
clear plain-text scientific notation instead.

------------------------------------------------------------------------

MARKDOWN OUTPUT CONTRACT

Output must be valid React Native-friendly Markdown + Unicode.

Allowed Markdown:

# Heading 1

## Heading 2

### Heading 3

**Bold**

*Italic*

***Bold Italic***

Bullets using:

- item

Ordered lists when necessary.

Fenced code blocks only if the educational content specifically requires
code or raw structured data.

Use blank lines generously.

------------------------------------------------------------------------

DO NOT OUTPUT

Do NOT use:

-   raw HTML
-   <div>
-   <span>
-   <table>
-   <br>
-   CSS
-   JavaScript
-   LaTeX commands
-   unsupported Markdown extensions
-   embedded styling instructions
-   inline font sizes
-   inline colours
-   external image URLs
-   Mermaid diagrams

The application controls visual styling.

The model controls only semantic Markdown structure.

------------------------------------------------------------------------

DARK-MODE RULE

The output must contain NO hard-coded text colours or background
colours.

Do not write styling such as:

color: black

or

background: white

The RNW application determines dark/light theme.

Use semantic emphasis only:

headings

bold

italics

bold italics

Unicode symbols

Spacing

------------------------------------------------------------------------

MOBILE-FIRST RULE

Assume the student reads on a 360–430 px wide mobile screen.

Therefore:

-   keep paragraphs short
-   keep pathway nodes short
-   avoid wide tables
-   avoid multi-column layouts
-   avoid long horizontal equations
-   avoid excessive indentation
-   avoid nested bullet hierarchies
-   never create content requiring horizontal scrolling

Prefer:

CLUE ↓ INTERPRETATION ↓ ACTION

instead of a large table.

------------------------------------------------------------------------

VISUAL HIERARCHY

Use Markdown hierarchy consistently.

# = pathway / major section

## = major internal concept

### = decision question / high-yield result

**Bold** = examination keyword

***Bold Italic*** = exceptionally important discriminator

→ = association or consequence

↓ = progression through algorithm

↔️ = connection/collateral/two-way relationship

↑ = increased

↓ = decreased

------------------------------------------------------------------------

CAPITALISATION RULE

Use CAPITALS strategically for rapid visual recognition.

Good:

McROBERTS FIRST

NEVER FUNDAL PRESSURE

MENTOPOSTERIOR

GASTRODUODENAL ARTERY

Do not write the entire document in capitals.

Capitals are reserved for:

-   final diagnosis
-   critical manoeuvre
-   decisive discriminator
-   dangerous contraindication
-   examiner trap
-   first-line action
-   emergency action

------------------------------------------------------------------------

SOURCE FIDELITY

Every major fact supplied in the JSON must appear either:

1.  directly in a pathway,
2.  as a discriminator,
3.  as a consequence,
4.  as an examiner trap,
5.  in rapid recall.

Do not silently discard facts because they appear minor.

Do not invent uncertain guidelines, doses, thresholds, classifications
or management recommendations.

If additional clinical context is necessary to connect supplied facts,
use only well-established standard medical knowledge.

The source remains the factual backbone.

NEET SS MEDICINE / SUPERSPECIALITY EXAM CONTEXT

The primary target is NEET SS Medicine and Indian superspeciality
entrance examinations, with evidence-based international
internal-medicine reasoning.

When the source contains management, screening, immunisation,
public-health, drug, procedural or guideline-sensitive facts:

-   preserve the supplied source facts first
-   prefer standard Indian examination conventions when they are well
    established
-   where a clinically important Indian recommendation genuinely differs
    from a commonly used international convention, label the distinction
    briefly and explicitly
-   never invent an “Indian guideline” merely to make the notes appear
    locally relevant
-   do not overload stable anatomy, physiology or pathology topics with
    unnecessary guideline commentary
-   if the supplied source does not establish a guideline-sensitive
    detail and the correct standard is uncertain, avoid adding an
    unsupported threshold or recommendation

The goal is NEET SS Medicine relevance without sacrificing
internationally sound clinical reasoning.

------------------------------------------------------------------------

DUPLICATION RULE

Important facts may appear twice when educationally useful:

1.  once inside the full clinical pathway
2.  once inside the final rapid-recall algorithm

Do NOT repeatedly restate the same fact throughout multiple pathways.

------------------------------------------------------------------------

FINAL RAPID-RECALL SECTION

After all pathways, create:

FINAL NEET SS Medicine RAPID-FIRE ALGORITHM

Compress the entire topic into decision branches.

Example:

BREECH?

↓ Look at hips + knees

Hips flexed + knees extended → FRANK

Hips + knees flexed → COMPLETE

Foot presenting → FOOTLING → CORD-PROLAPSE RISK

The rapid-fire section should allow revision of the entire PYT in
approximately 1–3 minutes.

At least some rapid-fire branches must preserve the integrated 3-level
structure:

CLUE → DIAGNOSIS → DOWNSTREAM DECISION

Do not reduce every branch back into one-line factual recall.

------------------------------------------------------------------------

FINAL 10-SECOND FRAMEWORK

Then create:

THE 10-SECOND EXAM FRAMEWORK

Generate approximately 3–7 questions the student should mentally ask
when encountering a vignette from this topic.

Example:

1. WHAT IS PRESENTING?

↓

2. WHAT IS THE KEY DISCRIMINATOR?

↓

3. IS VAGINAL DELIVERY POSSIBLE?

↓

4. WHAT IS THE IMMEDIATE DANGER?

↓

5. WHAT SHOULD I DO NEXT?

These questions must be adapted to the actual topic.

------------------------------------------------------------------------

FINAL MEMORY CODE

Finish with:

THE MEMORY CODE

Convert approximately 3–6 high-value facts from isolated memorisation
into reasoning chains.

Use this structure:

Don’t remember:

“Fact X.”

Remember:

Clinical clue → discriminator → mechanism → answer

Example:

Don’t remember:

“McRoberts = shoulder dystocia.”

Remember:

Head delivers → turtle sign → anterior shoulder trapped → shoulder
dystocia → McRoberts first.

------------------------------------------------------------------------

QUALITY CHECK BEFORE OUTPUT

Before returning the answer, silently verify:

1.  Did I include every important source fact?
2.  Did I transform facts rather than merely rewrite them?
3.  Are the pathways clinically logical?
4.  Are competing answers separated by discriminators?
5.  Are management steps correctly ordered?
6.  Are numbers and thresholds preserved?
7.  Did I expose examiner traps?
8.  Is the output easy to scan on a mobile screen?
9.  Is all formatting RNW-safe Markdown + Unicode?
10. Did I avoid HTML and LaTeX?
11. Did I avoid unsupported tables and wide layouts?
12. Does the final rapid-fire section cover the whole topic?
13. Could a student use these notes to solve a clinical vignette rather
    than merely recite a fact?
14. Did the highest-yield pathways integrate facts across different
    source subtopics?
15. Do selected pathways clearly demonstrate Level 1 → Level 2 → Level 3
    reasoning?
16. Did I include approximately 5–8 concise “EXAMINER MAY HIDE THIS AS”
    vignette bridges where the topic supports them?
17. Did I connect diagnosis to downstream complication, investigation or
    next-best action where appropriate?
18. For guideline-sensitive material, did I preserve NEET SS
    Medicine/Indian exam relevance without inventing unsupported
    recommendations?
19. Did I avoid making every simple fact artificially complicated?

If any answer is NO, revise internally before producing the final
output.

------------------------------------------------------------------------

OUTPUT RULE

Return ONLY the finished Markdown notes.

Do not explain the transformation.

Do not discuss the prompt.

Do not wrap the entire output in a Markdown code fence.

Do not prepend commentary.

Do not append commentary.
  */ };

  const source = promptContainer.toString();
  const start = source.indexOf("/*") + 2;
  const end = source.lastIndexOf("*/");

  return source.slice(start, end).trim();
})();

if (
  !SYSTEM_PROMPT ||
  SYSTEM_PROMPT.includes(
    "PASTE YOUR COMPLETE NEET SS MEDICINE FLOWCHART SYSTEM PROMPT HERE"
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

function requiredText(value, label) {
  const text = String(value ?? "").trim();

  if (!text) {
    throw new Error(`${label} must be a non-empty string`);
  }

  return text;
}

function serializeNotes(value) {
  if (value === null || value === undefined) {
    throw new Error("notes_json is missing");
  }

  if (typeof value === "string") {
    const text = value.trim();

    if (!text) {
      throw new Error("notes_json is empty");
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
    `TOPIC: ${requiredText(row.topic, "Topic")}`,
    `SUBJECT: ${requiredText(row.subject, "Subject")}`,
    `SERIAL NUMBER: ${row.serial_number}`,
    "",
    "SOURCE NEET SS MEDICINE NOTES JSON:",
    serializeNotes(row[INPUT_COL]),
    "",
    "Transform only the supplied topic and notes_json into finished NEET SS Medicine clinical pathway notes.",
    "Return only the finished Markdown.",
    "Do not include explanations before or after the notes.",
    "Do not wrap the output in a Markdown code fence."
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

function validateFlowchart(raw) {
  const markdown = requiredText(raw, "Generated flowchart");

  if (
    markdown.startsWith("```") ||
    markdown.endsWith("```")
  ) {
    throw new Error(
      "Output is wrapped in a Markdown code fence"
    );
  }

  if (!/^#\s+\S+/m.test(markdown)) {
    throw new Error(
      "Output lacks a main Markdown heading"
    );
  }

  if (!/^#\s+PATHWAY\s+\d+/im.test(markdown)) {
    throw new Error(
      "Output lacks a PATHWAY section"
    );
  }

  if (
    !/^#\s+FINAL NEET SS(?:\s+MEDICINE)?\s+RAPID-FIRE ALGORITHM/im.test(
      markdown
    )
  ) {
    throw new Error(
      "Output lacks FINAL NEET SS MEDICINE RAPID-FIRE ALGORITHM"
    );
  }

  if (
    !/^#\s+THE 10-SECOND(?:\s+NEET SS)?(?:\s+MEDICINE)?(?:\s+EXAM|\s+DECISION)?\s+FRAMEWORK/im.test(
      markdown
    )
  ) {
    throw new Error(
      "Output lacks the 10-second framework"
    );
  }

  if (!/^#\s+THE MEMORY CODE/im.test(markdown)) {
    throw new Error(
      "Output lacks THE MEMORY CODE"
    );
  }

  if (
    /<\s*\/?\s*(div|span|table|br|style|script)\b/i.test(
      markdown
    )
  ) {
    throw new Error(
      "Output contains prohibited HTML"
    );
  }

  if (
    /```(?:mermaid)?[\s\S]*?(?:flowchart|graph|sequenceDiagram)/i.test(
      markdown
    )
  ) {
    throw new Error(
      "Output contains prohibited Mermaid"
    );
  }

  if (markdown.length < 500) {
    throw new Error(
      "Output is unexpectedly short"
    );
  }

  return {
    markdown,
    characters: markdown.length,
    lines: markdown.split(/\r?\n/).length
  };
}

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
        input: buildInput(row)
      });

      return validateFlowchart(
        extractText(response)
      );
    } catch (error) {
      lastError = error;

      if (isCreditError(error)) {
        throw error;
      }

      const validationFailure =
        /empty output|non-empty string|code fence|Markdown heading|PATHWAY|RAPID-FIRE|framework|MEMORY CODE|prohibited|unexpectedly short|notes_json/i.test(
          errorText(error)
        );

      if (
        attempt === API_RETRIES ||
        (!isRetryable(error) && !validationFailure)
      ) {
        break;
      }

      const delay =
        1000 * (2 ** attempt) +
        Math.floor(Math.random() * 400);

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
      `Failed to release expired locks: ${error.message}`
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
      `id,subject,serial_number,topic,${INPUT_COL},${LOCK_AT_COL}`
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
      `Failed to find pending rows: ${error.message}`
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

async function processRow(row) {
  console.log(
    `Generating NEET SS Medicine flowchart | ${row.subject} | ${row.serial_number} | ${row.topic}`
  );

  try {
    const result =
      await generateFlowchart(row);

    await saveSuccess(
      row,
      result.markdown
    );

    console.log(
      `Completed | ${row.subject} | ${row.serial_number} | ${row.topic} | lines=${result.lines} | characters=${result.characters}`
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
  console.log(
    `NEET SS MEDICINE NOTES -> FLOWCHART WORKER STARTED: ${WORKER_ID}`
  );

  console.log(
    `Model=${MODEL} | Table=${TABLE} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY}`
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
        `Claimed ${rows.length} pending flowchart row(s)`
      );

      const result =
        await processBatch(rows);

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
    "Fatal flowchart worker error:",
    error
  );

  process.exit(1);
});
