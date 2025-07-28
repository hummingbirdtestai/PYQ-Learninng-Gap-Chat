const { runWorker } = require('./services/gptWorker');

for (let i = 0; i < 5; i++) {
  runWorker(i + 1); // Engage 5 parallel workers
}
