// workers/myWorker.js

// Import dependencies
const { createClient } = require("@supabase/supabase-js");
const OpenAI = require("openai");

// --- Supabase client ---
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// --- Pricing map ---
const PRICING = {
  "gpt-5-mini": { prompt: 0.00015, completion: 0.0006 }, // adjust if OpenAI changes
  "gpt-4o": { prompt: 0.0025, completion: 0.01 },
  "gpt-4o-mini": { prompt: 0.00015, completion: 0.0006 },
};

// --- Cost calculator ---
function calculateCost(model, prompt_tokens, completion_tokens) {
  const pricing = PRICING[model];
  if (!pricing) return 0;
  return (
    (prompt_tokens / 1000) * pricing.prompt +
    (completion_tokens / 1000) * pricing.completion
  );
}

// --- Worker function ---
async function runWorker() {
  try {
    const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

    // Example API call
    const response = await client.chat.completions.create({
      model: "gpt-5-mini",
      messages: [
        { role: "system", content: "You are a helpful tutor." },
        { role: "user", content: "Explain photosynthesis in 2 lines." },
      ],
    });

    // Extract usage from response
    const usage = response.usage; // { prompt_tokens, completion_tokens, total_tokens }
    const model = response.model;

    if (!usage) {
      console.error("❌ No usage data returned from OpenAI");
      return;
    }

    // Calculate cost
    const cost = calculateCost(model, usage.prompt_tokens, usage.completion_tokens);
    console.log(`💰 Cost for this job: $${cost.toFixed(4)}`);

    // Insert into Supabase
    const { error } = await supabase.from("openai_usage").insert([
      {
        api_key_id: process.env.OPENAI_API_KEY, // later map to alias via api_keys
        model,
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        total_tokens: usage.total_tokens,
        cost,
        start_date: new Date().toISOString().slice(0, 10),
        end_date: new Date().toISOString().slice(0, 10),
      },
    ]);

    if (error) {
      console.error("❌ Error inserting into Supabase:", error.message);
    } else {
      console.log("✅ Usage inserted into Supabase");
    }
  } catch (err) {
    console.error("❌ Worker failed:", err.message);
  }
}

// Run worker
runWorker();
