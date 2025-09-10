// workers/usageAggregator.js
const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

async function runAggregator() {
  const { data, error } = await supabase
    .from("openai_usage")
    .select("api_key_id, model, cost, created_at");

  if (error) {
    console.error("❌ Error fetching usage:", error.message);
    return;
  }

  // Group by api_key_id + day
  const dailyTotals = {};
  for (const row of data) {
    const day = row.created_at.slice(0, 10);
    const key = `${row.api_key_id}_${day}`;
    if (!dailyTotals[key]) {
      dailyTotals[key] = {
        api_key_id: row.api_key_id,
        day,
        total_cost: 0,
      };
    }
    dailyTotals[key].total_cost += parseFloat(row.cost);
  }

  console.log("📊 Daily totals:", Object.values(dailyTotals));
}

runAggregator();
