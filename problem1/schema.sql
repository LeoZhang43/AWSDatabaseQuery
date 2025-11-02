-- Drop tables if they exist
DROP TABLE IF EXISTS stop_events;
DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS line_stops;
DROP TABLE IF EXISTS stops;
DROP TABLE IF EXISTS lines;

-- ============================================
-- Table: lines
-- ============================================
CREATE TABLE lines (
    line_id SERIAL PRIMARY KEY,
    line_name VARCHAR(50) NOT NULL UNIQUE,
    vehicle_type VARCHAR(10) NOT NULL
        CHECK (vehicle_type IN ('rail', 'bus', 'tram', 'ferry'))
);

-- ============================================
-- Table: stops
-- ============================================
CREATE TABLE stops (
    stop_id SERIAL PRIMARY KEY,
    stop_name VARCHAR(100) NOT NULL UNIQUE,
    latitude DECIMAL(9,6) NOT NULL
        CHECK (latitude BETWEEN -90 AND 90),
    longitude DECIMAL(9,6) NOT NULL
        CHECK (longitude BETWEEN -180 AND 180)
);

-- ============================================
-- Table: line_stops
-- ============================================
CREATE TABLE line_stops (
    line_stop_id SERIAL PRIMARY KEY,
    line_id INT NOT NULL REFERENCES lines(line_id) ON DELETE CASCADE,
    stop_id INT NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE,
    sequence_number INT NOT NULL CHECK (sequence_number > 0),
    time_offset_minutes INT NOT NULL CHECK (time_offset_minutes >= 0),
    UNIQUE (line_id, stop_id),
    UNIQUE (line_id, sequence_number)
);

-- ============================================
-- Table: trips
-- ============================================
CREATE TABLE trips (
    trip_id VARCHAR(20) PRIMARY KEY,         -- Trip IDs like T0001
    line_id INT NOT NULL REFERENCES lines(line_id) ON DELETE CASCADE,
    departure_time TIMESTAMP NOT NULL,       -- Full date + time
    vehicle_id VARCHAR(20) NOT NULL,
    UNIQUE (line_id, departure_time, vehicle_id)
);

-- ============================================
-- Table: stop_events
-- ============================================
CREATE TABLE stop_events (
    stop_event_id SERIAL PRIMARY KEY,
    trip_id VARCHAR(20) NOT NULL REFERENCES trips(trip_id) ON DELETE CASCADE,
    stop_id INT NOT NULL REFERENCES stops(stop_id) ON DELETE CASCADE,
    scheduled_time TIMESTAMP NOT NULL,
    actual_time TIMESTAMP,
    passengers_on INT DEFAULT 0 CHECK (passengers_on >= 0),
    passengers_off INT DEFAULT 0 CHECK (passengers_off >= 0),
    UNIQUE (trip_id, stop_id)
);
