const { processNextInQueue } = require('./gptWorker');

async function startWorkers(parallelCount = 5) {
  console.log(`🚀 Launching ${parallelCount} GPT workers...`);

  const workers = Array.from({ length: parallelCount }).map((_, i) =>
    (async function loop() {
      console.log(`🧠 Worker ${i + 1} started`);

      while (true) {
        try {
          await processNextInQueue();
        } catch (err) {
          console.error(`❌ Worker ${i + 1} crashed:`, err.message);
        }

        await new Promise(res => setTimeout(res, 1000)); // Sleep 1s before next task
      }
    })()
  );

  await Promise.all(workers);
}

module.exports = { startWorkers };
