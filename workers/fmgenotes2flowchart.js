"use strict";

require("dotenv").config();

const fs = require("node:fs");
const path = require("node:path");
const { supabase } = require("../config/supabaseClient");
const openai = require("../config/openaiClient");

// Database: generated_notes -> flowcharts
const TABLE = "topic_notes_source";
const INPUT_COL = "generated_notes";
const OUTPUT_COL = "flowcharts";
const LOCK_COL = "notes_lock";
const LOCK_AT_COL = "notes_locked_at";

function integerEnv(name, fallback, min, max) {
  const value = Number.parseInt(process.env[name] || String(fallback), 10);
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }
  return value;
}

const MODEL = process.env.TOPIC_FLOWCHART_MODEL || "gpt-5.6-terra";
const PICKUP_LIMIT = integerEnv("TOPIC_FLOWCHART_LIMIT", 50, 1, 100);
const CONCURRENCY = integerEnv("TOPIC_FLOWCHART_BATCH_SIZE", 5, 1, 20);
const LOOP_SLEEP_MS = integerEnv("TOPIC_FLOWCHART_LOOP_SLEEP_MS", 1000, 250, 60000);
const LOCK_TTL_MIN = integerEnv("TOPIC_FLOWCHART_LOCK_TTL_MIN", 120, 5, 1440);
const API_RETRIES = integerEnv("TOPIC_FLOWCHART_API_RETRIES", 2, 0, 5);
const WORKER_ID = process.env.TOPIC_FLOWCHART_WORKER_ID ||
  `topic-flowchart-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;

// Keeping the prompt in a separate UTF-8 file makes all Markdown backticks safe.
const PROMPT_FILE = process.env.TOPIC_FLOWCHART_PROMPT_FILE ||
  path.join(__dirname, "fmge-flowchart-prompt.md");

let SYSTEM_PROMPT;
try {
  SYSTEM_PROMPT = fs.readFileSync(PROMPT_FILE, "utf8").trim();
} catch (error) {
  throw new Error(`Cannot read flowchart prompt at ${PROMPT_FILE}: ${error.message}`);
}
if (!SYSTEM_PROMPT) throw new Error("Flowchart system prompt is empty");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function errorText(error) {
  return String(error?.message || error?.error?.message || error || "Unknown error");
}

function isCreditError(error) {
  return /no credits remaining|insufficient_quota|billing|credit balance|billing_hard_limit/i.test(
    errorText(error)
  );
}

function isRetryable(error) {
  if (isCreditError(error)) return false;
  const status = Number(error?.status || error?.statusCode || error?.response?.status);
  return status === 408 || status === 409 || status === 429 || status >= 500 ||
    /timeout|temporar|unavailable|rate limit|ECONNRESET|ETIMEDOUT|socket hang up/i.test(
      errorText(error)
    );
}

function requiredText(value, label) {
  const text = String(value ?? "").trim();
  if (!text) throw new Error(`${label} must be a non-empty string`);
  return text;
}

function buildInput(row) {
  return [
    `TOPIC: ${requiredText(row.topic, "Topic")}`,
    `SUBJECT: ${requiredText(row.subject, "Subject")}`,
    "",
    "SOURCE RAPID-REVISION NOTES:",
    requiredText(row[INPUT_COL], "Generated notes"),
    "",
    "Transform only this supplied topic and these supplied notes into the finished FMGE clinical pathway notes.",
    "Return only the finished Markdown. Do not include a surrounding code fence."
  ].join("\n");
}

function extractText(response) {
  if (typeof response?.output_text === "string" && response.output_text.trim()) {
    return response.output_text.trim();
  }
  const pieces = [];
  for (const item of response?.output || []) {
    for (const content of item?.content || []) {
      if (content?.type === "output_text" && typeof content.text === "string") {
        pieces.push(content.text);
      }
    }
  }
  const text = pieces.join("\n").trim();
  if (!text) throw new Error("OpenAI returned empty output");
  return text;
}

function validateFlowchart(raw) {
  const markdown = requiredText(raw, "Generated flowchart");
  if (markdown.startsWith("```") || markdown.endsWith("```")) {
    throw new Error("Output is wrapped in a Markdown code fence");
  }
  if (!/^#\s+\S+/m.test(markdown)) {
    throw new Error("Output lacks a main Markdown heading");
  }
  if (!/^#\s+PATHWAY\s+\d+/im.test(markdown)) {
    throw new Error("Output lacks a PATHWAY section");
  }
  if (!/^#\s+FINAL FMGE RAPID-FIRE ALGORITHM/im.test(markdown)) {
    throw new Error("Output lacks FINAL FMGE RAPID-FIRE ALGORITHM");
  }
  if (!/^#\s+THE 10-SECOND EXAM FRAMEWORK/im.test(markdown)) {
    throw new Error("Output lacks THE 10-SECOND EXAM FRAMEWORK");
  }
  if (!/^#\s+THE MEMORY CODE/im.test(markdown)) {
    throw new Error("Output lacks THE MEMORY CODE");
  }
  if (/<\s*\/?\s*(div|span|table|br|style|script)\b/i.test(markdown)) {
    throw new Error("Output contains prohibited HTML");
  }
  if (/```(?:mermaid)?[\s\S]*?(?:flowchart|graph|sequenceDiagram)/i.test(markdown)) {
    throw new Error("Output contains prohibited Mermaid");
  }
  if (markdown.length < 500) throw new Error("Output is unexpectedly short");
  return {
    markdown,
    characters: markdown.length,
    lines: markdown.split(/\r?\n/).length
  };
}

async function generate(row) {
  let lastError;
  for (let attempt = 0; attempt <= API_RETRIES; attempt += 1) {
    try {
      const response = await openai.responses.create({
        model: MODEL,
        instructions: SYSTEM_PROMPT,
        input: buildInput(row)
      });
      return validateFlowchart(extractText(response));
    } catch (error) {
      lastError = error;
      if (isCreditError(error)) throw error;
      const validationFailure = /empty output|non-empty string|code fence|Markdown heading|PATHWAY|RAPID-FIRE|10-SECOND|MEMORY CODE|prohibited|unexpectedly short|Generated notes/i.test(
        errorText(error)
      );
      if (attempt === API_RETRIES || (!isRetryable(error) && !validationFailure)) break;
      const delay = 1000 * (2 ** attempt) + Math.floor(Math.random() * 400);
      console.warn(`Retry ${attempt + 1}/${API_RETRIES} after ${delay} ms: ${errorText(error)}`);
      await sleep(delay);
    }
  }
  throw lastError || new Error("Flowchart generation failed");
}

async function releaseExpiredLocks() {
  const cutoff = new Date(Date.now() - LOCK_TTL_MIN * 60 * 1000).toISOString();
  const { error } = await supabase
    .from(TABLE)
    .update({ [LOCK_COL]: false, [LOCK_AT_COL]: null })
    .eq(LOCK_COL, true)
    .eq("active", true)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .lt(LOCK_AT_COL, cutoff);
  if (error) throw new Error(`Failed to release expired flowchart locks: ${error.message}`);
}

async function lockRow(candidate) {
  const lockedAt = new Date().toISOString();
  const { data, error } = await supabase
    .from(TABLE)
    .update({ [LOCK_COL]: true, [LOCK_AT_COL]: lockedAt })
    .eq("id", candidate.id)
    .eq("active", true)
    .eq(LOCK_COL, false)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .select(`id,subject,topic,course_id,subject_id,pyt_id,${INPUT_COL},${LOCK_AT_COL}`)
    .maybeSingle();
  if (error) throw new Error(`Failed to lock row ${candidate.id}: ${error.message}`);
  return data || null;
}

async function claimRows(limit) {
  await releaseExpiredLocks();
  const { data, error } = await supabase
    .from(TABLE)
    .select("id,created_at")
    .eq("active", true)
    .eq(LOCK_COL, false)
    .not(INPUT_COL, "is", null)
    .is(OUTPUT_COL, null)
    .order("created_at", { ascending: true })
    .limit(limit);
  if (error) throw new Error(`Failed to find pending flowchart rows: ${error.message}`);
  if (!data?.length) return [];

  const results = await Promise.allSettled(data.map(lockRow));
  const rows = [];
  for (const result of results) {
    if (result.status === "fulfilled" && result.value) rows.push(result.value);
    if (result.status === "rejected") console.error("Row-lock error:", errorText(result.reason));
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
    .eq(LOCK_AT_COL, row[LOCK_AT_COL])
    .is(OUTPUT_COL, null)
    .select("id");
  if (error) throw new Error(`Failed to save flowchart: ${error.message}`);
  if (!data?.length) throw new Error("Save rejected: lock changed or flowchart already exists");
}

async function releaseLock(row) {
  const { error } = await supabase
    .from(TABLE)
    .update({ [LOCK_COL]: false, [LOCK_AT_COL]: null })
    .eq("id", row.id)
    .eq(LOCK_COL, true)
    .eq(LOCK_AT_COL, row[LOCK_AT_COL])
    .is(OUTPUT_COL, null);
  if (error) console.error(`Failed to release lock ${row.id}: ${error.message}`);
}

async function processRow(row) {
  console.log(`Generating flowchart | ${row.subject} | ${row.topic}`);
  try {
    const result = await generate(row);
    await saveSuccess(row, result.markdown);
    console.log(`Completed | ${row.subject} | ${row.topic} | lines=${result.lines} | characters=${result.characters}`);
    return { creditExhausted: false };
  } catch (error) {
    await releaseLock(row);
    if (isCreditError(error)) {
      console.error("OpenAI credits exhausted. Worker will stop safely.");
      return { creditExhausted: true };
    }
    console.error(`Failed | ${row.subject} | ${row.topic}: ${errorText(error)}`);
    return { creditExhausted: false };
  }
}

async function processBatch(rows) {
  let next = 0;
  let creditExhausted = false;
  async function runner() {
    while (next < rows.length && !creditExhausted) {
      const index = next;
      next += 1;
      const result = await processRow(rows[index]);
      if (result.creditExhausted) creditExhausted = true;
    }
  }
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, rows.length) }, runner));
  if (creditExhausted) {
    await Promise.allSettled(rows.slice(next).map(releaseLock));
  }
  return { creditExhausted };
}

async function main() {
  console.log(`TOPIC NOTES -> FLOWCHARTS WORKER STARTED: ${WORKER_ID}`);
  console.log(`Model=${MODEL} | Input=${INPUT_COL} | Output=${OUTPUT_COL} | Pickup=${PICKUP_LIMIT} | Concurrent=${CONCURRENCY}`);
  console.log(`Prompt=${PROMPT_FILE}`);

  while (true) {
    try {
      const rows = await claimRows(PICKUP_LIMIT);
      if (!rows.length) {
        await sleep(LOOP_SLEEP_MS);
        continue;
      }
      console.log(`Claimed ${rows.length} pending flowchart row(s)`);
      const result = await processBatch(rows);
      if (result.creditExhausted) process.exit(1);
    } catch (error) {
      if (isCreditError(error)) process.exit(1);
      console.error("Worker loop error:", errorText(error));
      await sleep(Math.max(LOOP_SLEEP_MS, 2000));
    }
  }
}

main().catch((error) => {
  console.error("Fatal flowchart worker error:", error);
  process.exit(1);
});
