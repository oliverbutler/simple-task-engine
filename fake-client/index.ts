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
import { z } from "zod";

/**
 * Generates a random processing delay between 20ms and 5 seconds
 * with median around 100ms (using exponential distribution)
 * @returns Delay time in milliseconds
 */
function generateRandomDelay(): number {
  // Use exponential distribution to get median around 100ms
  // but allow for values up to 5000ms (5 seconds)
  const lambda = Math.log(2) / 100; // Set median to 100ms
  const randomValue = -Math.log(Math.random()) / lambda;

  // Clamp between 20ms and 5000ms
  return Math.min(Math.max(Math.round(randomValue), 20), 5000);
}

/**
 * Creates a promise that rejects after the specified timeout
 * @param timeoutMs - Timeout in milliseconds
 * @returns A promise that rejects after the timeout
 */
function createTimeout(timeoutMs: number): Promise<never> {
  return new Promise((_, reject) => {
    setTimeout(
      () => reject(new Error(`Operation timed out after ${timeoutMs}ms`)),
      timeoutMs,
    );
  });
}

/**
 * Simulates processing work with a random delay
 * @returns Processing time in milliseconds
 */
async function simulateProcessing(): Promise<number> {
  const delay = generateRandomDelay();
  const startTime = Date.now();

  await new Promise((resolve) => setTimeout(resolve, delay));

  return Date.now() - startTime;
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

// Define Zod schemas for task payloads
const SendEmailTaskSchema = z.object({
  email: z.string().email(),
});

Bun.serve({
  port: 3000,
  routes: {
    "/task/:name": {
      POST: async (req) => {
        const body = await req.json();
        const taskType = req.params.name;
        const TIMEOUT_MS = 10000; // 10 seconds timeout

        // Create response headers with initial CPU metrics
        const headers = new Headers({
          "Content-Type": "application/json",
        });

        try {
          // Parse the payload using Zod
          const payload = SendEmailTaskSchema.parse(body.payload);
          let result: any = { ok: true };

          // Process with timeout
          const processingPromise = async () => {
            // Simulate email sending with a delay
            const processingTime = await simulateProcessing();

            result = {
              ok: true,
              email: payload.email,
              processingTime,
            };

            console.log(
              `Simulated sending email to: ${payload.email} in ${processingTime}ms`,
            );

            return result;
          };

          // Race between processing and timeout
          const processedResult = await Promise.race([
            processingPromise(),
            createTimeout(TIMEOUT_MS),
          ]);

          console.log(`Task ${taskType} processed`);

          return Response.json(processedResult, { headers });
        } catch (error) {
          console.error("Error processing task:", error);

          // Check if it's a timeout error
          const isTimeout =
            error instanceof Error && error.message.includes("timed out");

          return Response.json(
            {
              code: isTimeout ? "TimeoutError" : "ValidationError",
              message: error instanceof Error ? error.message : "Unknown error",
            },
            { status: isTimeout ? 408 : 400, headers },
          );
        }
      },
    },

    "/health": {
      GET: async () => {
        return Response.json({ ok: true });
      },
    },

    "/task-seed": {
      POST: async (req) => {
        const body = await req.json();
        const count = body.count || 10; // Default to 10 tasks if not specified
        const batchSize = 100; // Insert in batches to avoid overwhelming the DB

        console.log(`Seeding ${count} tasks...`);

        const startTime = Date.now();
        let tasksCreated = 0;

        try {
          // Process in batches
          for (let i = 0; i < count; i += batchSize) {
            const batchCount = Math.min(batchSize, count - i);
            const tasks = [];

            for (let j = 0; j < batchCount; j++) {
              tasks.push({
                id: uuidv4(),
                idempotencyKey: `seed-${uuidv4()}`,
                type: "SendEmail",
                priority: "medium",
                payload: {
                  email: uuidv4() + "@example.com",
                },
                status: "pending",
                processAfter: new Date(),
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
