// usage-to-supabase.js
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// --- ENV Vars ---
const API_KEY = process.env.OPENAI_API_KEY; // org key
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// --- Config ---
const START_DATE = "2025-09-01";
const END_DATE   = "2025-09-10";

const PRICING = {
  "gpt-4o-mini": { prompt: 0.00015, completion: 0.0006 },
  "gpt-4o":      { prompt: 0.0025, completion: 0.01 },
  "gpt-4-turbo": { prompt: 0.01,   completion: 0.03 },
  "gpt-3.5-turbo": { prompt: 0.0005, completion: 0.0015 },
};

// --- Init Supabase ---
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

async function fetchUsage() {
  const url = `https://api.openai.com/v1/organization/usage?start_date=${START_DATE}&end_date=${END_DATE}`;
  const res = await fetch(url, {
    headers: {
      "Authorization": `Bearer ${API_KEY}`,
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    console.error("❌ Error fetching usage:", res.status, await res.text());
    process.exit(1);
  }
  return res.json();
}

function calculateCost(record) {
  const { model, prompt_tokens, completion_tokens } = record;
  const pricing = PRICING[model];
  if (!pricing) return 0;
  return (
    (prompt_tokens / 1000) * pricing.prompt +
    (completion_tokens / 1000) * pricing.completion
  );
}

async function storeInSupabase(records) {
  const rows = records.map((r) => ({
    api_key_id: r.api_key_id,
    model: r.model,
    prompt_tokens: r.prompt_tokens,
    completion_tokens: r.completion_tokens,
    total_tokens: r.total_tokens,
    cost: calculateCost(r),
    start_date: START_DATE,
    end_date: END_DATE,
  }));

  const { error } = await supabase.from("openai_usage").insert(rows);
  if (error) {
    console.error("❌ Supabase insert failed:", error);
  } else {
    console.log("✅ Inserted usage records into Supabase:", rows.length);
  }
}

async function main() {
  const data = await fetchUsage();
  await storeInSupabase(data.data);
}

main().catch(console.error);
