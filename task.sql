CREATE TABLE `task_pool` (
  -- Core identification
  `id` VARCHAR(255) PRIMARY KEY,
  `idempotency_key` VARCHAR(255) NULL,
  -- Task classification
  `type` VARCHAR(255) NOT NULL,
  `priority` ENUM ('low', 'medium', 'high', 'critical') NOT NULL DEFAULT 'medium',
  -- Task content
  `payload` JSON NOT NULL,
  -- Status management
  `status` ENUM (
    'pending',
    'processing',
    'completed',
    'failed',
    'cancelled',
    'scheduled'
  ) NOT NULL DEFAULT 'pending',
  `locked_until` DATETIME (6) NULL,
  -- Retry management
  `retry_count` INT UNSIGNED NOT NULL DEFAULT 0,
  `max_retry_count` INT UNSIGNED NOT NULL DEFAULT 5,
  `last_error` TEXT NULL,
  -- Scheduling
  `process_after` DATETIME (6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  -- Workflow management
  `correlation_id` VARCHAR(255) NULL,
  -- Audit
  `created_at` DATETIME (6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME (6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  -- Constraints
  UNIQUE KEY `uk_idempotency_key` (`idempotency_key`),
  -- Indexes
  INDEX `idx_status_process_after_priority` (`status`, `process_after`, `priority`),
  INDEX `idx_correlation_id` (`correlation_id`),
  INDEX `idx_type` (`type`),
  INDEX `idx_locked_until` (`locked_until`)
);
