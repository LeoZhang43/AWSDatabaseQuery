# Transit Database Project

## Schema Decisions: Natural vs. Surrogate Keys
I used **surrogate keys** (e.g., `line_id`, `stop_id`, `trip_id`) as primary keys instead of natural keys such as `line_name` or `stop_name`.

- Surrogate keys are **simple, numeric, and stable** — they don’t change if a stop or line name changes.  
- Natural keys like stop names can change (e.g., a renamed station), which could break relationships in other tables.  
- Surrogate keys improve **join performance** and keep foreign key relationships consistent.

---

## Constraints
Several constraints were added to maintain **data integrity** and **consistency** across the database.

### **CHECK Constraints**
- `CHECK (latitude BETWEEN -90 AND 90)` and `CHECK (longitude BETWEEN -180 AND 180)`  
  → Prevent invalid geographic coordinates.  
- `CHECK (sequence_number > 0)` and `CHECK (time_offset_minutes >= 0)`  
  → Ensure positive and logical route sequencing.

### **UNIQUE Constraints**
- `UNIQUE (line_id, stop_id)`  
  → Prevents duplicate stop entries for the same line.  
- `UNIQUE (line_id, sequence_number)`  
  → Ensures unique order of stops along a route.  
- `UNIQUE (line_name)` and `UNIQUE (stop_name)`  
  → Prevent duplicate line or stop definitions.

---

## Complex Query
**Q5 – Routes serving both "Wilshire / Veteran" and "Le Conte / Broxton"** was the most challenging.

- It required **joining multiple tables** (`lines`, `line_stops`, `stops`).  
- The query had to correctly **group** and **filter** lines that contained **both** stops, not just one.  
- It combined conditional logic with aggregation using `HAVING COUNT(DISTINCT stop_name) = 2`.

This query was conceptually tricky and required careful debugging of join conditions and filters.

---

## Foreign Keys
Foreign key relationships enforce data consistency and prevent invalid entries.

### Examples
- `trips(line_id)` → `lines(line_id)`  
  → Prevents inserting a trip that references a non-existent line.  
- `stop_events(trip_id)` → `trips(trip_id)`  
  → Prevents adding stop events for trips that don’t exist.  
- `stop_events(stop_id)` → `stops(stop_id)`  
  → Ensures stop events always reference valid stops.

Without these constraints, **orphaned data** could appear and break route and trip integrity.

---

## Why Relational: SQL for Transit Data
The transit domain is highly relational — it involves **lines**, **stops**, **trips**, and **events** that are naturally connected.

SQL is ideal for this kind of data because:
- It handles **structured relationships** and **referential integrity** efficiently.  
- Queries like “top 10 busiest stops” or “average ridership by line” are simple using SQL joins and aggregations.  
- **Normalization** reduces redundancy, ensuring consistent and maintainable data.  
- **Constraints** and **foreign keys** automatically enforce real-world rules.

SQL’s strong relational model, data integrity controls, and expressive query language make it perfect for managing and analyzing public transit systems.

---
