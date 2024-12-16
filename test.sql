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

ALTER TABLE TaskStatus RENAME TO task_status;


