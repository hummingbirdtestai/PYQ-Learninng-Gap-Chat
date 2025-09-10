import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// --- ENV Vars ---
const API_KEY = process.env.OPENAI_API_KEY; // org-level key
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// --- Init Supabase ---
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// --- Pricing Table (adjust if you use more models) ---
const PRICING = {
  "gpt-4o-mini": { prompt: 0.00015, completion: 0.0006 },
  "gpt-4o": { prompt: 0.0025, completion: 0.01 },
  "gpt-4-turbo": { prompt: 0.01, completion: 0.03 },
  "gpt-3.5-turbo": { prompt: 0.0005, completion: 0.0015 },
};

function calculateCost(record) {
  const { model, prompt_tokens, completion_tokens } = record;
  const pricing = PRICING[model];
  if (!pricing) return 0;
  return (
    (prompt_tokens / 1000) * pricing.prompt +
    (completion_tokens / 1000) * pricing.completion
  );
}

export async function refreshUsage({ startDate, endDate }) {
  const url = `https://api.openai.com/v1/organization/usage?start_date=${startDate}&end_date=${endDate}`;

  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    throw new Error(
      `OpenAI Usage API failed: ${res.status} ${await res.text()}`
    );
  }

  const data = await res.json();

  // Prepare rows for Supabase
  const rows = data.data.map((r) => ({
    api_key_id: r.api_key_id,
    model: r.model,
    prompt_tokens: r.prompt_tokens,
    completion_tokens: r.completion_tokens,
    total_tokens: r.total_tokens,
    cost: calculateCost(r),
    start_date: startDate,
    end_date: endDate,
  }));

  const { error } = await supabase.from("openai_usage").insert(rows);
  if (error) throw error;

  return rows;
}

export async function getLatestCost(apiKeyId) {
  const { data, error } = await supabase
    .from("openai_usage")
    .select("cost")
    .eq("api_key_id", apiKeyId)
    .order("created_at", { ascending: false })
    .limit(1);

  if (error) throw error;
  return data.length ? data[0].cost : 0;
}
