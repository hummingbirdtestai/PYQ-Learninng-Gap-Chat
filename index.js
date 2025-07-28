// index.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const swaggerUi = require('swagger-ui-express');
const YAML = require('yamljs');

const app = express();

// ✅ Load Swagger Docs
const swaggerDocument = YAML.load('./docs/swagger.yaml');
const importRoutes = require('./routes/import.route');

// ✅ Middleware: CORS
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// ✅ Middleware: JSON Parser
app.use(express.json());

// ✅ Swagger UI Route
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument));

// ✅ API Route Registrations
app.use('/auth', require('./routes/auth.routes'));             // Twilio OTP
app.use('/users', require('./routes/user.routes'));            // User Profile + Activation
app.use('/colleges', require('./routes/college.routes'));      // Medical Colleges
app.use('/exams', require('./routes/exam.routes'));            // Exams + Subjects
app.use('/api', importRoutes);

// ✅ Start Express Server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
