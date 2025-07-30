const { processNextInQueue } = require('./mcq.worker');

// Utility delay
const delay = (ms) => new Promise((res) => setTimeout(res, ms));

async function startWorkerLoop(workerId) {
  console.log(`🧠 Worker ${workerId} started`);

  let emptyCount = 0;
  const maxEmptyRetries = 10;

  while (emptyCount < maxEmptyRetries) {
    try {
      const processed = await processNextInQueue(workerId);

      if (!processed) {
        emptyCount++;
        console.log(`📭 Worker ${workerId}: No MCQ in queue (${emptyCount}/${maxEmptyRetries}).`);
        await delay(3000);
      } else {
        emptyCount = 0;
        await delay(500);
      }
    } catch (err) {
      console.error(`❌ Worker ${workerId} crashed:`, err.message);
      break;
    }
  }

  console.log(`🛑 Worker ${workerId} exiting after ${emptyCount} empty retries.`);
}

async function startWorkers(parallelCount = 8) {
  console.log(`🚀 Launching ${parallelCount} GPT workers...`);

  const workers = Array.from({ length: parallelCount }).map((_, i) =>
    startWorkerLoop(i + 1)
  );

  await Promise.all(workers);
  console.log(`✅ All ${parallelCount} workers completed.`);
}

module.exports = { startWorkers };
