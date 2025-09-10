import { refreshUsage, getLatestCost } from "../services/openaiUsageService.js";

async function runWorker() {
  // Refresh usage dynamically for today
  const today = new Date().toISOString().slice(0, 10);
  await refreshUsage({ startDate: today, endDate: today });

  // Check my budget for this API key
  const apiKeyId = "sk-xxxx"; // <-- you’ll map your worker’s key → id
  const cost = await getLatestCost(apiKeyId);

  console.log(`💰 Worker cost so far today: $${cost}`);

  if (cost > 5.0) {
    console.log("⚠️ Budget exceeded, downgrading model...");
    // switch to cheaper model or trim prompt
  }

  // … run your OpenAI job here …
}

runWorker().catch(console.error);
