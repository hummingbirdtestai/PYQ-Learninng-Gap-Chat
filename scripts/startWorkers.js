const { startWorkers } = require('../services/launchWorkers');

const count = parseInt(process.argv[2]) || 5;

startWorkers(count);
