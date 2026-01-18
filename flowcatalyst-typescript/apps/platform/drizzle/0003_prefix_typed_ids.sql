-- 0003: Add 3-character prefixes to all typed IDs
-- Format: {prefix}_{tsid} (e.g., "clt_0HZXEQ5Y8JY5Z")
-- Total length: 17 characters (3 prefix + 1 underscore + 13 TSID)
--
-- This follows the Stripe pattern where typed IDs are stored WITH the prefix
-- in the database, eliminating serialization/deserialization overhead.

-- =============================================================================
-- Part 1: Expand VARCHAR columns from 13 to 17 characters
-- =============================================================================

-- Principals table
ALTER TABLE principals ALTER COLUMN id TYPE VARCHAR(17);
ALTER TABLE principals ALTER COLUMN client_id TYPE VARCHAR(17);
ALTER TABLE principals ALTER COLUMN application_id TYPE VARCHAR(17);

-- Clients table
ALTER TABLE clients ALTER COLUMN id TYPE VARCHAR(17);

-- =============================================================================
-- Part 2: Add prefixes to existing IDs
-- =============================================================================

-- Clients (clt_)
UPDATE clients SET id = 'clt_' || id WHERE id NOT LIKE '%_%';

-- Principals (prn_)
UPDATE principals SET id = 'prn_' || id WHERE id NOT LIKE '%_%';

-- Update client_id foreign keys (clt_)
UPDATE principals SET client_id = 'clt_' || client_id
WHERE client_id IS NOT NULL AND client_id NOT LIKE '%_%';

-- Update application_id foreign keys (app_)
UPDATE principals SET application_id = 'app_' || application_id
WHERE application_id IS NOT NULL AND application_id NOT LIKE '%_%';
