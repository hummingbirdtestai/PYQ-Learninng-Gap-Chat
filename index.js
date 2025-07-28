// index.js

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const swaggerUi = require('swagger-ui-express');
const YAML = require('yamljs');

const app = express();

// ✅ Load Swagger Docs
const swaggerDocument = YAML.load('./docs/swagger.yaml');

// ✅ Import Custom Routes
const importRoutes = require('./routes/import.routes');
const authRoutes = require('./routes/auth.routes');
const userRoutes = require('./routes/user.routes');
const collegeRoutes = require('./routes/college.routes');
const examRoutes = require('./routes/exam.routes');
const generationRoutes = require('./routes/generation.routes');

// ✅ Middleware: CORS
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// ✅ Middleware: JSON Body Parser
app.use(express.json());

// ✅ Swagger Documentation Route
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument));

// ✅ Register API Routes
app.use('/auth', authRoutes);             // Twilio OTP Auth
app.use('/users', userRoutes);            // User Registration + Activation
app.use('/colleges', collegeRoutes);      // Medical Colleges List
app.use('/exams', examRoutes);            // Exams and Subjects
app.use('/api', importRoutes);            // Import MCQs from Google Sheets
app.use('/api/generation', generationRoutes);

// ✅ Start Express Server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
