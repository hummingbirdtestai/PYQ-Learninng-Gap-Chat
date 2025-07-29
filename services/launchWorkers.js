const { processNextInQueue } = require('./gptWorker');

// Utility delay
const delay = (ms) => new Promise((res) => setTimeout(res, ms));

async function startWorkerLoop(workerId) {
  console.log(`🧠 Worker ${workerId} started`);

  let emptyCount = 0;
  const maxEmptyRetries = 10; // Stop after 10 empty polls (30s total if 3s delay)

  while (emptyCount < maxEmptyRetries) {
    try {
      const processed = await processNextInQueue(workerId);

      if (!processed) {
        emptyCount++;
        console.log(`📭 Worker ${workerId}: No MCQ in queue (${emptyCount}/${maxEmptyRetries}).`);
        await delay(3000);
      } else {
        emptyCount = 0; // Reset if work was done
        await delay(500); // Throttle
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

  await Promise.all(workers); // Wait for all to finish
  console.log(`✅ All ${parallelCount} workers completed.`);
}

// ✅ Auto-start if executed directly
if (require.main === module) {
  startWorkers(8);
}

module.exports = { startWorkers };
