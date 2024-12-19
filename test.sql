

-- Verify the data
SELECT * FROM task_schedules;


DELETE FROM supplier_invoice_record;


UPDATE xero_tenant
SET tenant_name = 'J & R Corporation Limited'
WHERE tenant_name = 'J&R Corporation Limited';


SELECT *
FROM supplier_invoice_record
WHERE invoice_number = 'HD 658020194';
