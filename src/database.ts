/* src/database.ts */

import { Database } from "bun:sqlite";
import { join } from "path";
import { existsSync, mkdirSync } from "fs";
import { log, err } from "./utils/logger.js";

// Ensure the data directory exists
const DATA_DIR = join(import.meta.dir, "..", "data");
if (!existsSync(DATA_DIR)) {
  mkdirSync(DATA_DIR, { recursive: true });
}

// Initialize database connection
const db = new Database(join(DATA_DIR, "invalid_records.sqlite"), {
  create: true,
});

// Initialize the database schema
export function initializeDatabase(): void {
  try {
    log(
      `Initializing SQLite database at ${join(DATA_DIR, "invalid_records.sqlite")}...`,
    );

    // Create table for invalid records if it doesn't exist
    db.run(`
      CREATE TABLE IF NOT EXISTS invalid_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL,
        monitor_name TEXT,
        error_message TEXT NOT NULL,
        first_seen DATETIME NOT NULL,
        last_seen DATETIME NOT NULL,
        count INTEGER DEFAULT 1
      )
    `);

    // Create index for faster lookups
    db.run(`
      CREATE INDEX IF NOT EXISTS idx_monitor_type
      ON invalid_records(monitor_name, type)
    `);

    log("Database initialized successfully", {
      database_event: "init",
    });
  } catch (error) {
    err("Failed to initialize database", {
      database_error: error,
    });
    throw error;
  }
}

// Store an invalid record
export function storeInvalidRecord(
  type: string,
  errorMessage: string,
  monitorName?: string,
): void {
  const timestamp = new Date().toISOString();

  try {
    // Check if this error already exists for this monitor and type
    const existingRecord = (db
      .query(
        `
      SELECT id, count FROM invalid_records
      WHERE type = ?
        AND monitor_name = ?
        AND error_message = ?
    `,
      )
      .get(type, monitorName || null, errorMessage) as {
      id?: number;
      count?: number;
    }) || { id: undefined, count: undefined };

    if (existingRecord) {
      // Update existing record
      db.run(
        `
        UPDATE invalid_records
        SET count = $count, last_seen = $timestamp
        WHERE id = $id
      `,
        [(existingRecord.count || 0) + 1, timestamp, existingRecord.id || 0],
      );
    } else {
      // Insert new record
      db.run(
        `
        INSERT INTO invalid_records (
          type, monitor_name, error_message, first_seen, last_seen
        ) VALUES (
          $type, $monitorName, $errorMessage, $timestamp, $timestamp
        )
      `,
        [type, monitorName || null, errorMessage, timestamp],
      );
    }
  } catch (error) {
    err("Error storing invalid record", {
      database_error: error,
    });
    if (error instanceof Error) {
      err("Error details", {
        database_error_details: {
          message: error.message,
          stack: error.stack,
          name: error.name,
        },
      });
    }
  }
}

// Get all invalid records
export function getAllInvalidRecords() {
  return db
    .query(
      `
    SELECT
      id, type, monitor_name, error_message,
      first_seen, last_seen, count
    FROM invalid_records
    ORDER BY last_seen DESC
  `,
    )
    .all();
}

// Get invalid records by type
export function getInvalidRecordsByType(type: string) {
  return db
    .query(
      `
    SELECT
      id, type, monitor_name, error_message,
      first_seen, last_seen, count
    FROM invalid_records
    WHERE type = ?
    ORDER BY last_seen DESC
  `,
    )
    .all(type);
}

// Get invalid records by monitor name
export function getInvalidRecordsByMonitor(monitorName: string) {
  return db
    .query(
      `
    SELECT
      id, type, monitor_name, error_message,
      first_seen, last_seen, count
    FROM invalid_records
    WHERE monitor_name = ?
    ORDER BY last_seen DESC
  `,
    )
    .all(monitorName);
}

// Get summary of invalid records grouped by type
export function getInvalidRecordsSummary() {
  return db
    .query(
      `
    SELECT
      type,
      COUNT(DISTINCT monitor_name) as monitor_count,
      SUM(count) as error_count,
      MAX(last_seen) as latest_error
    FROM invalid_records
    GROUP BY type
    ORDER BY latest_error DESC
  `,
    )
    .all();
}

// Delete a record by ID
export function deleteInvalidRecord(id: number): boolean {
  try {
    const result = db.run(`DELETE FROM invalid_records WHERE id = ?`, [id]);
    return result.changes > 0;
  } catch (error) {
    err(`Error deleting invalid record with ID ${id}`, {
      database_error: error,
    });
    return false;
  }
}

// Close database connection
export function closeDatabase() {
  db.close();
}
