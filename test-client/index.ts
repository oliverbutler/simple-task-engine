import type { InferInsertModel } from "drizzle-orm";
import {
  mysqlTable,
  varchar,
  json,
  datetime,
  int,
  mysqlEnum,
  text,
} from "drizzle-orm/mysql-core";
import { drizzle } from "drizzle-orm/mysql2";
import mysql from "mysql2/promise";
import { v4 as uuidv4 } from "uuid";

/**
 * Generates a random delay between 20ms and 10 seconds with median around 100ms
 * Uses a left-skewed distribution (inverse of exponential) to create values that
 * lean toward the lower end of the range
 * @returns Delay in milliseconds
 */
function generateRandomDelay(): number {
  // For a left-skewed distribution, we can use 1 - exponential distribution
  // First, create a value between 0 and 1 that's heavily weighted toward 1
  const lambda = Math.log(2) / 0.3; // Parameter to control the skew
  const expRandom = Math.exp(-Math.random() * lambda);

  // Now map this to our range (20ms to 10000ms)
  // This creates a left-skewed distribution with values mostly near the minimum
  const minDelay = 20;
  const maxDelay = 10000;
  const range = maxDelay - minDelay;

  // Calculate the delay with the desired median of 100ms
  // We use a power function to further shape the distribution
  const medianPoint = (100 - minDelay) / range;
  const power = Math.log(medianPoint) / Math.log(0.5);
  const scaledRandom = Math.pow(expRandom, power);

  const delay = minDelay + scaledRandom * range;

  return Math.round(delay);
}

// Create the connection
const connection = mysql.createPool({
  host: "localhost",
  user: "taskuser",
  password: "taskpassword",
  database: "taskdb",
});

const db = drizzle(connection);

// Define the task table schema based on task.sql
const taskTable = mysqlTable("task_pool", {
  id: varchar("id", { length: 255 }).primaryKey(),
  idempotencyKey: varchar("idempotency_key", { length: 255 }),
  type: varchar("type", { length: 255 }).notNull(),
  priority: mysqlEnum("priority", ["low", "medium", "high", "critical"])
    .default("medium")
    .notNull(),
  payload: json("payload").notNull(),
  status: mysqlEnum("status", [
    "pending",
    "processing",
    "completed",
    "failed",
    "cancelled",
    "scheduled",
  ])
    .default("pending")
    .notNull(),
  lockedUntil: datetime("locked_until", { fsp: 6 }),
  retryCount: int("retry_count").default(0).notNull(),
  maxRetryCount: int("max_retry_count").default(5).notNull(),
  lastError: text("last_error"),
  processAfter: datetime("process_after", { fsp: 6 }).notNull(),
  correlationId: varchar("correlation_id", { length: 255 }),
  createdAt: datetime("created_at", { fsp: 6 }).default(new Date()).notNull(),
  updatedAt: datetime("updated_at", { fsp: 6 }).default(new Date()).notNull(),
});

Bun.serve({
  port: 3000,
  routes: {
    "/task/:name": {
      POST: async (req) => {
        const body = await req.json();

        // Add a random processing delay
        const delay = generateRandomDelay();
        console.log(
          `Processing task ${req.params.name} with delay of ${delay}ms`,
        );

        // Simulate processing time
        await new Promise((resolve) => setTimeout(resolve, delay));

        console.log(`Task ${req.params.name} processed after ${delay}ms`);
        return Response.json({ ok: true, processingTime: delay });
      },
    },
    "/task-seed": {
      POST: async (req) => {
        const body = await req.json();
        const count = body.count || 10; // Default to 10 tasks if not specified
        const batchSize = 100; // Insert in batches to avoid overwhelming the DB
        const taskTypes = [
          "email",
          "notification",
          "report",
          "sync",
          "cleanup",
        ];
        const priorities = ["low", "medium", "high", "critical"] as const;

        console.log(`Seeding ${count} tasks...`);

        const startTime = Date.now();
        let tasksCreated = 0;

        try {
          // Process in batches
          for (let i = 0; i < count; i += batchSize) {
            const batchCount = Math.min(batchSize, count - i);
            const tasks = [];

            for (let j = 0; j < batchCount; j++) {
              const taskType =
                taskTypes[Math.floor(Math.random() * taskTypes.length)];
              const priority =
                priorities[Math.floor(Math.random() * priorities.length)];

              tasks.push({
                id: uuidv4(),
                idempotencyKey: `seed-${uuidv4()}`,
                type: taskType,
                priority: priority,
                payload: JSON.stringify({
                  data: `Sample data for ${taskType} task #${i + j + 1}`,
                  seedBatch: Math.floor((i + j) / batchSize),
                }),
                status: "pending",
                processAfter: new Date(Date.now() + generateRandomDelay()),
                correlationId: `seed-batch-${Math.floor((i + j) / batchSize)}`,
              } satisfies InferInsertModel<typeof taskTable>);
            }

            // Insert the batch
            await db.insert(taskTable).values(tasks);
            tasksCreated += tasks.length;
            console.log(
              `Inserted batch of ${tasks.length} tasks. Progress: ${tasksCreated}/${count}`,
            );
          }

          const duration = (Date.now() - startTime) / 1000;
          return Response.json({
            ok: true,
            tasksCreated,
            duration: `${duration.toFixed(2)}s`,
            tasksPerSecond: (tasksCreated / duration).toFixed(2),
          });
        } catch (error) {
          console.error("Error seeding tasks:", error);
          return Response.json(
            {
              ok: false,
              error: error instanceof Error ? error.message : "Unknown",
            },
            { status: 500 },
          );
        }
      },
    },
  },
});
