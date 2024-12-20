

-- Verify the data


--DELETE FROM inventory_record;

--CREATE TABLE inventory_record (
--    id INTEGER PRIMARY KEY AUTOINCREMENT,
--    user_id INTEGER NOT NULL,
--   month INTEGER NOT NULL,
--    year INTEGER NOT NULL,
--    store_name VARCHAR(255) NOT NULL,
--   amount FLOAT NOT NULL,
--    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
--    FOREIGN KEY (user_id) REFERENCES user (id)
--);


UPDATE inventory_record
SET processed = TRUE
WHERE user_id = 1
  AND month = 12;




