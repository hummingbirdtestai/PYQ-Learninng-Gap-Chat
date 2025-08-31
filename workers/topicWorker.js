import OpenAI from "openai";
import { createClient } from "@supabase/supabase-js";

// 🔹 Supabase client (service role key for backend workers)
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const MODEL = process.env.CLASSIFY_MODEL || "gpt-5-mini";
const LIMIT = parseInt(process.env.TOPIC_LIMIT || "180", 10);
const BLOCK_SIZE = parseInt(process.env.TOPIC_BLOCK_SIZE || "60", 10);
const SLEEP_MS = parseInt(process.env.TOPIC_LOOP_SLEEP_MS || "800", 10);
const LOCK_TTL_MIN = parseInt(process.env.TOPIC_LOCK_TTL_MIN || "15", 10);

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

// 🔹 Fetch batch of MCQs that need topics
async function fetchBatch() {
  const { data, error } = await supabase
    .from("mcq_bank")
    .select("id, mcq")
    .is("topic", null)
    .or(
      `topic_lock.is.null,topic_locked_at.lt.${new Date(
        Date.now() - LOCK_TTL_MIN * 60 * 1000
      ).toISOString()}`
    )
    .order("id", { ascending: true })
    .limit(LIMIT);

  if (error) throw error;

  // Lock them so no other worker takes the same rows
  if (data.length > 0) {
    const ids = data.map(r => r.id);
    await supabase
      .from("mcq_bank")
      .update({
        topic_lock: "locked",
        topic_locked_at: new Date().toISOString(),
      })
      .in("id", ids);
  }

  return data;
}

// 🔹 Send MCQs to GPT and update topics back in DB
async function classifyAndUpdate(batch) {
  const inputs = batch.map(r => r.mcq);

  const response = await openai.chat.completions.create({
    model: MODEL,
    messages: [
      {
        role: "user",
        content: `
You are an expert NEETPG & USMLE medical teacher with 20+ years of experience.  
I will give you a list of raw MCQ texts (from the mcq_bank table, column mcq).

Your task:
- For EACH input row, identify the **single most relevant high-yield topic**.  
- The topic must be **exactly 1–3 words**, like textbook headings (e.g., "Myocardial Infarction", "Elbow Dislocation").  
- Be consistent: always use the same canonical phrase (never synonyms).  
- If drug-related → exact drug/class (e.g., "Metformin").  
- Output one topic **per line**, in the same order as inputs.  
- Do not output numbers, JSON, extra text, or explanations — only plain topic names, one per line.
  
Here are the inputs:
${inputs.join("\n")}
        `,
      },
    ],
  });

  const raw = response.choices[0].message.content.trim();
  const topics = raw.split("\n").map(t => t.trim());

  // 🔹 Update using IDs
  for (let i = 0; i < batch.length; i++) {
    const id = batch[i].id;
    const topic = topics[i] || null;

    if (!topic) {
      console.warn(`⚠️ No topic returned for MCQ ID: ${id}`);
      continue;
    }

    const { error } = await supabase
      .from("mcq_bank")
      .update({ topic, topic_lock: null, topic_locked_at: null })
      .eq("id", id);

    if (error) {
      console.error(`❌ Failed to update topic for ID: ${id}`, error);
    }
  }
}

// 🔹 Loop forever
async function loop() {
  while (true) {
    const batch = await fetchBatch();
    if (batch.length === 0) {
      console.log("No more rows, sleeping...");
      await sleep(SLEEP_MS * 10);
      continue;
    }

    console.log(`🔍 Picked up ${batch.length} MCQs`);

    // break into chunks for GPT
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
