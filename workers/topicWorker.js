import OpenAI from "openai";
import pkg from "pg";
const { Client } = pkg;

const client = new Client({ connectionString: process.env.SUPABASE_DB_URL });
await client.connect();

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const MODEL = process.env.CLASSIFY_MODEL || "gpt-5-mini";
const LIMIT = parseInt(process.env.MCQ_LIMIT || "180", 10);
const BLOCK_SIZE = parseInt(process.env.MCQ_BLOCK_SIZE || "60", 10);
const SLEEP_MS = parseInt(process.env.MCQ_LOOP_SLEEP_MS || "800", 10);
const LOCK_TTL_MIN = parseInt(process.env.MCQ_LOCK_TTL_MIN || "15", 10);

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

async function fetchBatch() {
  const { rows } = await client.query(`
    with cte as (
      select id, mcq
      from public.mcq_bank
      where topic is null
      order by id
      limit $1
      for update skip locked
    )
    update public.mcq_bank as m
    set locked_at = now()
    from cte
    where m.id = cte.id
    returning m.id, m.mcq;
  `, [LIMIT]);
  return rows;
}

async function classifyAndUpdate(batch) {
  const inputs = batch.map(r => r.mcq);
  const response = await openai.chat.completions.create({
    model: MODEL,
    messages: [
      { role: "user", content: `
You are an expert NEETPG & USMLE medical teacher with 20+ years of experience.
I will give you a list of raw MCQ texts (from the mcq_bank table, column mcq).

Your task:
- For EACH input row, identify the **single most relevant high-yield topic** (from standard NEETPG subjects).
- The topic name must be **exactly 1–3 words long**, and must match the **canonical textbook-style heading** used in NEETPG/PG preparation.
- Be consistent: e.g. always use **Myocardial Infarction** (never "MI" or "Heart Attack").
- If drug-related → exact drug name/class (e.g., "Metformin").
- Output a valid JSON array. Each element must be: { "mcq_text": "<exact MCQ text>", "topic": "<Canonical Topic>" }
- Do not output anything except the JSON array.

Here are the inputs:
${JSON.stringify(inputs, null, 2)}
      `}
    ],
    response_format: { type: "json_object" }
  });

  const parsed = JSON.parse(response.choices[0].message.content);
  for (const row of parsed) {
    await client.query(
      `update public.mcq_bank set topic = $1, locked_at = null where id = (
         select id from public.mcq_bank 
         where mcq = $2 
         order by created_at asc 
         limit 1
       )`,
      [row.topic, row.mcq_text]
    );
  }
}

async function loop() {
  while (true) {
    const batch = await fetchBatch();
    if (batch.length === 0) {
      console.log("No more rows, sleeping...");
      await sleep(SLEEP_MS * 10);
      continue;
    }

    // break into BLOCK_SIZE chunks
    for (let i = 0; i < batch.length; i += BLOCK_SIZE) {
      const slice = batch.slice(i, i + BLOCK_SIZE);
      try {
        await classifyAndUpdate(slice);
        console.log(`✅ Classified ${slice.length} MCQs`);
      } catch (err) {
        console.error("❌ Error", err.message);
      }
      await sleep(SLEEP_MS);
    }
  }
}

loop();
