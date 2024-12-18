CREATE TABLE TaskStatus (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL UNIQUE,            -- Celery Task ID
    user_id INTEGER NOT NULL,                -- ID of the user associated with the task
    task_type TEXT NOT NULL,                 -- Type of the task (e.g., 'recharging')
    status TEXT DEFAULT 'pending',           -- Status of the task (pending, in_progress, completed, failed)
    result TEXT,                             -- Stores task result or error message
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,  -- Task creation timestamp
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,  -- Task update timestamp (requires trigger for updates)
    FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE -- Foreign key constraint
);


CREATE TABLE invoice_record (
    id SERIAL PRIMARY KEY,
    invoice_type VARCHAR(50) NOT NULL,
    week_number INTEGER NOT NULL,
    year INTEGER NOT NULL,
    store_number VARCHAR(20) NOT NULL,
    store_name VARCHAR(100) NOT NULL,
    tenant_name VARCHAR(100) NOT NULL
);


ALTER TABLE TaskStatus RENAME TO task_status;


DROP TABLE IF EXISTS TaskStatus;



ALTER TABLE task_status
ADD COLUMN progress INT DEFAULT 0;


DELETE FROM invoice_record;

-- Drop the old table if it exists
DROP TABLE IF EXISTS supplier_invoice_record;

-- Create the new table with updated constraints
CREATE TABLE supplier_invoice_record (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    store_name VARCHAR(255),
    invoice_type VARCHAR(100),
    invoice_number VARCHAR(100) NOT NULL,
    invoice_id VARCHAR(100),
    errors TEXT,
    run_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    triggered_by VARCHAR(50),
    date_of_invoice DATE NOT NULL
);

CREATE TABLE task_schedules (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(100) UNIQUE NOT NULL,
    interval_minutes INT NOT NULL,
    last_run TIMESTAMP DEFAULT NULL,
    next_run TIMESTAMP DEFAULT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

INSERT INTO task_schedules (task_name, interval_minutes, last_run, next_run, is_active)
VALUES
    ('process_cocacola_task', 1440, NULL, '2024-12-19 11:30:00', TRUE), -- Runs daily at 11:30
    ('process_textman_task', 1440, NULL, '2024-12-19 11:30:00', TRUE), -- Runs daily at 11:30
    ('process_eden_farm_task', 1440, NULL, '2024-12-19 11:30:00', TRUE); -- Runs daily at 11:30



-- Drop the old table if it exists
DROP TABLE IF EXISTS task_schedules;

-- Create the new table
CREATE TABLE task_schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name TEXT UNIQUE NOT NULL,
    interval_minutes INTEGER NOT NULL,
    last_run DATETIME DEFAULT NULL,
    next_run DATETIME DEFAULT NULL,
    is_active BOOLEAN DEFAULT 1
);

-- Insert initial data
INSERT INTO task_schedules (task_name, interval_minutes, last_run, next_run, is_active)
VALUES
    ('process_cocacola_task', 1440, NULL, '2024-12-19 11:30:00', 1),
    ('process_textman_task', 1440, NULL, '2024-12-19 11:30:00', 1),
    ('process_eden_farm_task', 1440, NULL, '2024-12-19 11:30:00', 1);

-- Verify data
SELECT * FROM task_schedules;


-- Update interval_minutes from 1440 to 1
UPDATE task_schedules
SET interval_minutes = 1
WHERE interval_minutes = 1440;

-- Verify the changes
SELECT * FROM task_schedules;


-- Drop the old table if it exists
DROP TABLE IF EXISTS task_schedules;


-- Drop the table if it already exists
DROP TABLE IF EXISTS task_schedules;

-- Create the new task_schedules table
CREATE TABLE task_schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_name TEXT UNIQUE NOT NULL,
    schedule_type TEXT NOT NULL DEFAULT 'interval', -- 'interval' or 'crontab'
    interval_minutes INTEGER, -- For interval scheduling
    specific_time TIME, -- For crontab-like scheduling
    last_run DATETIME DEFAULT NULL, -- Last run time
    next_run DATETIME DEFAULT NULL, -- Next run time
    is_active BOOLEAN DEFAULT 1, -- Enable/disable tasks
    arguments TEXT -- Store arguments as a string
);


-- Insert tasks into the task_schedules table

-- Task that repeats every 1 minute
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_cocacola_task', 'interval', 1, NULL, '[1]', 1);

-- Task that runs at 23:42
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_textman_task', 'crontab', NULL, '23:32:00', '[1]', 1);


-- Task that runs at 23:42
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_eden_farm_task', 'crontab', NULL, '23:32:00', '[1]', 1);



-- Verify the data
SELECT * FROM task_schedules;


-- Task that repeats every 1 minute
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_cocacola_task', 'interval', 1, NULL, '[1]', TRUE);

-- Task that runs at 23:42
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_textman_task', 'crontab', NULL, '23:42:00', '[2]', TRUE);

-- Task that repeats every 1 minute
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_eden_farm_task', 'interval', 1, NULL, '[3]', TRUE);


-- Task that runs at 23:42
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_textman_task', 'crontab', NULL, '23:00:00', '[1]', TRUE);

-- Task that runs at 23:42
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_cocacola_task', 'crontab', NULL, '23:00:00', '[1]', TRUE);

-- Task that runs at 23:42
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_eden_farm_task', 'crontab', NULL, '23:00:00', '[1]', TRUE);

-- Task that runs at 23:42
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_textman_task', 'crontab', NULL, '23:00:00', '[2]', TRUE);

-- Task that runs at 23:42
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_cocacola_task', 'crontab', NULL, '23:00:00', '[2]', TRUE);

-- Task that runs at 23:42
INSERT INTO task_schedules (task_name, schedule_type, interval_minutes, specific_time, arguments, is_active)
VALUES
    ('process_eden_farm_task', 'crontab', NULL, '23:00:00', '[2]', TRUE);