-- Expected to fail: full_name already exists. Run this file separately.
ALTER TABLE ice.learning_evolution.customers
RENAME COLUMN region TO full_name;
