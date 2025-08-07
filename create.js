// =============================================================
// MongoDB Setup Script: Load Profiles Database
// Purpose: Create collections, indexes, and sample data
// Usage: mongosh <connection-string> --file setup_load_profiles_db.js
// =============================================================

// Switch to the target database
db = db.getSiblingDB('load_profiles_db');

print("✅ Using database: load_profiles_db");

// =============================================================
// 1. Create Collections
// =============================================================

// Time-series collection for measurements
print("⚙️ Creating time-series collection: measurements");
db.createCollection("measurements", {
  timeseries: {
    timeField: "timestamp",
    metaField: "metadata",
    granularity: "minutes"
  }
});

// Regular collections
print("⚙️ Creating collection: customers");
db.createCollection("customers");

print("⚙️ Creating collection: meters");
db.createCollection("meters");

print("⚙️ Creating collection: processed_files");
db.createCollection("processed_files");

// =============================================================
// 2. Create Indexes
// =============================================================

print("🔍 Creating indexes...");

// Customers
db.customers.createIndex({ "_id": 1 });
db.customers.createIndex({ "email": 1 });
db.customers.createIndex({ "updatedAt": 1 });

// Meters
db.meters.createIndex({ "_id": 1 });
db.meters.createIndex({ "customerRef": 1 });

// Measurements
db.measurements.createIndex({ "metadata.serial": 1 });
db.measurements.createIndex({ "metadata.serial": 1, "timestamp": -1 });

// Processed Files
db.processed_files.createIndex({ "fileName": 1 }, { unique: true });

print("✅ All indexes created");