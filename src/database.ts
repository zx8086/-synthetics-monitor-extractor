/* src/database.ts */

import { Database } from "bun:sqlite";
import { join } from "path";
import { existsSync, mkdirSync } from "fs";

// Ensure the data directory exists
const DATA_DIR = join(import.meta.dir, "..", "data");
if (!existsSync(DATA_DIR)) {
  mkdirSync(DATA_DIR, { recursive: true });
}

// Initialize database connection
const db = new Database(join(DATA_DIR, "invalid_records.sqlite"), { create: true });

// Initialize the database schema
export function initializeDatabase(): void {
  console.log(`Initializing SQLite database at ${join(DATA_DIR, "invalid_records.sqlite")}...`);
  
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
  
  console.log("Database initialized successfully");
}

// Store an invalid record
export function storeInvalidRecord(
  type: string, 
  errorMessage: string, 
  monitorName?: string
): void {
  const timestamp = new Date().toISOString();
  
  try {
    // Check if this error already exists for this monitor and type
    const existingRecord = db.query(`
      SELECT id, count FROM invalid_records 
      WHERE type = $type 
        AND monitor_name = $monitorName 
        AND error_message = $errorMessage
    `).get({
      $type: type,
      $monitorName: monitorName || null,
      $errorMessage: errorMessage
    });
    
    if (existingRecord) {
      // Update existing record
      db.run(`
        UPDATE invalid_records 
        SET count = $count, last_seen = $timestamp 
        WHERE id = $id
      `, {
        $count: (existingRecord.count as number) + 1,
        $timestamp: timestamp,
        $id: existingRecord.id
      });
    } else {
      // Insert new record
      db.run(`
        INSERT INTO invalid_records (
          type, monitor_name, error_message, first_seen, last_seen
        ) VALUES (
          $type, $monitorName, $errorMessage, $timestamp, $timestamp
        )
      `, {
        $type: type,
        $monitorName: monitorName || null,
        $errorMessage: errorMessage,
        $timestamp: timestamp
      });
    }
  } catch (error) {
    console.error("Error storing invalid record:", error);
    if (error instanceof Error) {
      console.error("Error details:", {
        message: error.message,
        stack: error.stack,
        name: error.name
      });
    }
  }
}

// Get all invalid records
export function getAllInvalidRecords() {
  return db.query(`
    SELECT 
      id, type, monitor_name, error_message, 
      first_seen, last_seen, count
    FROM invalid_records
    ORDER BY last_seen DESC
  `).all();
}

// Get invalid records by type
export function getInvalidRecordsByType(type: string) {
  return db.query(`
    SELECT 
      id, type, monitor_name, error_message, 
      first_seen, last_seen, count
    FROM invalid_records
    WHERE type = $type
    ORDER BY last_seen DESC
  `).all({
    $type: type
  });
}

// Get invalid records by monitor name
export function getInvalidRecordsByMonitor(monitorName: string) {
  return db.query(`
    SELECT 
      id, type, monitor_name, error_message, 
      first_seen, last_seen, count
    FROM invalid_records
    WHERE monitor_name = $monitorName
    ORDER BY last_seen DESC
  `).all({
    $monitorName: monitorName
  });
}

// Get summary of invalid records grouped by type
export function getInvalidRecordsSummary() {
  return db.query(`
    SELECT 
      type, 
      COUNT(DISTINCT monitor_name) as monitor_count,
      SUM(count) as error_count,
      MAX(last_seen) as latest_error
    FROM invalid_records
    GROUP BY type
    ORDER BY latest_error DESC
  `).all();
}

// Close database connection
export function closeDatabase() {
  db.close();
}
