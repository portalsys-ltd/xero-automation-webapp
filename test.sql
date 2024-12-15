DELETE FROM log_entries
WHERE log_type = 'dom_purchase_invoice';

DELETE FROM tracking_categories
WHERE tracking_category_option = 'NOTTINGHAM' AND user_id = 2; 


DELETE FROM tracking_categories
WHERE user_id = 2;

ALTER TABLE user ADD COLUMN token_expires_at FLOAT;
