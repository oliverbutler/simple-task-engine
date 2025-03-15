const express = require("express");
const app = express();
const port = process.env.PORT || 3000;

// Configuration from environment variables
const taskSuccessRate = parseFloat(process.env.TASK_SUCCESS_RATE || "0.9");
const minProcessingTime = parseInt(process.env.MIN_PROCESSING_TIME_MS || "100");
const maxProcessingTime = parseInt(
  process.env.MAX_PROCESSING_TIME_MS || "2000",
);

app.use(express.json());

// Health check endpoint
app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

// Task processing endpoint
app.post("/task/:type", (req, res) => {
  const taskId = req.header("X-Task-ID") || "unknown";
  const taskType = req.params.type;
  const payload = req.body;

  console.log(`Processing task ${taskId} of type ${taskType}`);

  // Simulate processing time
  const processingTime = Math.floor(
    Math.random() * (maxProcessingTime - minProcessingTime) + minProcessingTime,
  );

  setTimeout(() => {
    // Randomly succeed or fail based on configured success rate
    if (Math.random() < taskSuccessRate) {
      console.log(
        `Task ${taskId} completed successfully after ${processingTime}ms`,
      );
      res.status(200).json({
        success: true,
        task_id: taskId,
        type: taskType,
        processing_time_ms: processingTime,
      });
    } else {
      console.log(`Task ${taskId} failed after ${processingTime}ms`);
      res.status(500).json({
        success: false,
        task_id: taskId,
        type: taskType,
        error: "Simulated task failure",
        processing_time_ms: processingTime,
      });
    }
  }, processingTime);
});

app.listen(port, () => {
  console.log(`Mock API server listening at http://localhost:${port}`);
});
