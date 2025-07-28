const { processNextInQueue } = require('./gptWorker');

async function startWorkers(parallelCount = 5) {
  console.log(`🚀 Launching ${parallelCount} GPT workers...`);

  const workers = Array.from({ length: parallelCount }).map((_, i) =>
    (async function loop() {
      const workerId = i + 1;
      console.log(`🧠 Worker ${workerId} started`);

      while (true) {
        try {
          await processNextInQueue(workerId); // 👈 Pass workerId for individual logging
        } catch (err) {
          console.error(`❌ Worker ${workerId} crashed:`, err.message);
        }

        await new Promise(res => setTimeout(res, 1000)); // 🔁 1s delay before retry
      }
    })()
  );

  await Promise.all(workers); // Keep all workers alive
}

// ✅ Auto-start if executed directly
if (require.main === module) {
  startWorkers(5); // Default: 5 workers (can be changed)
}

module.exports = { startWorkers };
