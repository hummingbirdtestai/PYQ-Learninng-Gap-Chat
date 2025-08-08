require('dotenv').config();
const ctrl = require('../controllers/classifier.controller');

// Minimal wrapper that invokes the run with env tunables
(async () => {
  // Fake req/res to reuse the controller logic
  const req = { body: {
    batchSize: parseInt(process.env.CLASSIFY_BATCH_SIZE || '80', 10),
    concurrency: parseInt(process.env.CLASSIFY_CONCURRENCY || '6', 10),
    pageSize: parseInt(process.env.CLASSIFY_PAGE_SIZE || '2000', 10),
  }};
  const res = {
    json: (x) => (console.log('✅ Done:', x), process.exit(0)),
    status: (c) => ({ json: (x) => (console.error('❌', c, x), process.exit(1)) })
  };

  await ctrl.classifySubjectsRun(req, res);
})();
