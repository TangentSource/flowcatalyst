-- FlowCatalyst JOOQ Schema
-- This is a consolidated schema for JOOQ code generation.
-- Represents the final state after all Flyway migrations (V1-V7).
-- PostgreSQL-specific features (JSONB indexes) are omitted for H2 compatibility.

-- =============================================================================
-- Core Tables
-- =============================================================================

-- Clients (organizations/tenants)
CREATE TABLE clients (
    id VARCHAR(17) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    identifier VARCHAR(100) UNIQUE NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
    status_reason VARCHAR(255),
    status_changed_at TIMESTAMP WITH TIME ZONE,
    notes TEXT,  -- JSONB stored as TEXT
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Principals (Users & Service Accounts)
CREATE TABLE principals (
    id VARCHAR(17) PRIMARY KEY,
    type VARCHAR(20) NOT NULL,
    scope VARCHAR(20),
    client_id VARCHAR(17),
    application_id VARCHAR(17),
    name VARCHAR(255) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    email VARCHAR(255),
    email_domain VARCHAR(100),
    idp_type VARCHAR(50),
    external_idp_id VARCHAR(255),
    password_hash VARCHAR(255),
    last_login_at TIMESTAMP WITH TIME ZONE,
    service_account TEXT,  -- JSONB stored as TEXT
    roles TEXT DEFAULT '[]',  -- JSONB stored as TEXT
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Applications
CREATE TABLE applications (
    id VARCHAR(17) PRIMARY KEY,
    code VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    type VARCHAR(50) NOT NULL DEFAULT 'APPLICATION',
    default_base_url VARCHAR(500),
    service_account_id VARCHAR(17),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    icon_url VARCHAR(500),
    website VARCHAR(500),
    logo TEXT,
    logo_mime_type VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Service Accounts (standalone table)
CREATE TABLE service_accounts (
    id VARCHAR(17) PRIMARY KEY,
    code VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    client_ids ARRAY,  -- TEXT[] stored as ARRAY
    application_id VARCHAR(17),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    wh_auth_type VARCHAR(50) DEFAULT 'BEARER_TOKEN',
    wh_auth_token_ref VARCHAR(500),
    wh_signing_secret_ref VARCHAR(500),
    wh_signing_algorithm VARCHAR(50) DEFAULT 'HMAC_SHA256',
    wh_credentials_created_at TIMESTAMP WITH TIME ZONE,
    wh_credentials_regenerated_at TIMESTAMP WITH TIME ZONE,
    roles TEXT DEFAULT '[]',  -- JSONB stored as TEXT
    last_used_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Authorization Tables
-- =============================================================================

-- Auth Roles
CREATE TABLE auth_roles (
    id VARCHAR(17) PRIMARY KEY,
    application_id VARCHAR(17),
    application_code VARCHAR(100),
    name VARCHAR(100) NOT NULL,
    display_name VARCHAR(255),
    description TEXT,
    permissions ARRAY,  -- TEXT[] stored as ARRAY
    source VARCHAR(20) NOT NULL DEFAULT 'DATABASE',
    client_managed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(application_code, name)
);

-- Auth Permissions
CREATE TABLE auth_permissions (
    id VARCHAR(17) PRIMARY KEY,
    application_id VARCHAR(17),
    name VARCHAR(100) NOT NULL,
    display_name VARCHAR(255),
    description TEXT,
    source VARCHAR(20) NOT NULL DEFAULT 'SDK',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Event/Messaging Tables
-- =============================================================================

-- Event Types
CREATE TABLE event_types (
    id VARCHAR(17) PRIMARY KEY,
    code VARCHAR(200) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    spec_versions TEXT DEFAULT '[]',  -- JSONB stored as TEXT
    status VARCHAR(20) NOT NULL DEFAULT 'CURRENT',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Events
CREATE TABLE events (
    id VARCHAR(17) PRIMARY KEY,
    type VARCHAR(200) NOT NULL,
    source VARCHAR(500) NOT NULL,
    subject VARCHAR(500),
    time TIMESTAMP WITH TIME ZONE NOT NULL,
    spec_version VARCHAR(50),
    data TEXT,  -- Changed from JSONB to TEXT
    correlation_id VARCHAR(100),
    causation_id VARCHAR(100),
    deduplication_id VARCHAR(100),
    message_group VARCHAR(200),
    client_id VARCHAR(17),
    context_data TEXT DEFAULT '[]',  -- JSONB stored as TEXT
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Events Read (read-optimized projection)
CREATE TABLE events_read (
    id VARCHAR(17) PRIMARY KEY,
    event_id VARCHAR(17) NOT NULL,
    spec_version VARCHAR(20),
    type VARCHAR(200) NOT NULL,
    application VARCHAR(100),
    subdomain VARCHAR(100),
    aggregate VARCHAR(100),
    source VARCHAR(500) NOT NULL,
    subject VARCHAR(500),
    time TIMESTAMP WITH TIME ZONE NOT NULL,
    data TEXT,
    message_group VARCHAR(200),
    correlation_id VARCHAR(100),
    causation_id VARCHAR(100),
    deduplication_id VARCHAR(100),
    context_data TEXT DEFAULT '[]',  -- JSONB stored as TEXT
    client_id VARCHAR(17),
    projected_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Subscriptions
CREATE TABLE subscriptions (
    id VARCHAR(17) PRIMARY KEY,
    code VARCHAR(100) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    client_id VARCHAR(17),
    client_identifier VARCHAR(100),
    event_types TEXT DEFAULT '[]',  -- JSONB stored as TEXT
    target VARCHAR(500) NOT NULL,
    queue VARCHAR(200),
    custom_config TEXT DEFAULT '[]',  -- JSONB stored as TEXT
    source VARCHAR(20) NOT NULL DEFAULT 'API',
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    max_age_seconds INT NOT NULL DEFAULT 86400,
    dispatch_pool_id VARCHAR(17),
    dispatch_pool_code VARCHAR(100),
    delay_seconds INT NOT NULL DEFAULT 0,
    sequence INT NOT NULL DEFAULT 99,
    mode VARCHAR(30) NOT NULL DEFAULT 'IMMEDIATE',
    timeout_seconds INT NOT NULL DEFAULT 30,
    max_retries INT NOT NULL DEFAULT 3,
    service_account_id VARCHAR(17),
    data_only BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(client_id, code)
);

-- Dispatch Pools
CREATE TABLE dispatch_pools (
    id VARCHAR(17) PRIMARY KEY,
    code VARCHAR(100) NOT NULL,
    name VARCHAR(255),
    description TEXT,
    client_identifier VARCHAR(100),
    rate_limit INT NOT NULL,
    concurrency INT NOT NULL DEFAULT 1,
    client_id VARCHAR(17),
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(client_id, code)
);

-- Dispatch Jobs
CREATE TABLE dispatch_jobs (
    id VARCHAR(17) PRIMARY KEY,
    external_id VARCHAR(100),
    source VARCHAR(500),
    kind VARCHAR(20) NOT NULL DEFAULT 'EVENT',
    code VARCHAR(200) NOT NULL,
    subject VARCHAR(500),
    event_id VARCHAR(17),
    correlation_id VARCHAR(100),
    metadata TEXT DEFAULT '[]',  -- JSONB stored as TEXT
    target_url VARCHAR(500) NOT NULL,
    protocol VARCHAR(30) NOT NULL DEFAULT 'HTTP_WEBHOOK',
    headers TEXT DEFAULT '{}',  -- JSONB stored as TEXT
    payload TEXT,
    payload_content_type VARCHAR(100) DEFAULT 'application/json',
    data_only BOOLEAN NOT NULL DEFAULT TRUE,
    service_account_id VARCHAR(17),
    client_id VARCHAR(17),
    subscription_id VARCHAR(17),
    mode VARCHAR(30) NOT NULL DEFAULT 'IMMEDIATE',
    dispatch_pool_id VARCHAR(17),
    message_group VARCHAR(200),
    sequence INT NOT NULL DEFAULT 99,
    timeout_seconds INT NOT NULL DEFAULT 30,
    schema_id VARCHAR(17),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    max_retries INT NOT NULL DEFAULT 3,
    retry_strategy VARCHAR(50) DEFAULT 'exponential',
    scheduled_for TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE,
    attempt_count INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_millis BIGINT,
    last_error TEXT,
    idempotency_key VARCHAR(100),
    attempts TEXT DEFAULT '[]',  -- JSONB stored as TEXT
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Dispatch Jobs Read (read-optimized projection)
CREATE TABLE dispatch_jobs_read (
    id VARCHAR(17) PRIMARY KEY,
    external_id VARCHAR(100),
    source VARCHAR(500),
    kind VARCHAR(20) NOT NULL,
    code VARCHAR(200) NOT NULL,
    subject VARCHAR(500),
    event_id VARCHAR(17),
    correlation_id VARCHAR(100),
    target_url VARCHAR(500) NOT NULL,
    protocol VARCHAR(30) NOT NULL,
    service_account_id VARCHAR(17),
    client_id VARCHAR(17),
    subscription_id VARCHAR(17),
    mode VARCHAR(30) NOT NULL,
    dispatch_pool_id VARCHAR(17),
    message_group VARCHAR(200),
    status VARCHAR(20) NOT NULL,
    max_retries INT NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_millis BIGINT,
    last_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    application VARCHAR(100),
    subdomain VARCHAR(100),
    aggregate VARCHAR(100),
    sequence INT DEFAULT 99,
    timeout_seconds INT DEFAULT 30,
    retry_strategy VARCHAR(50),
    scheduled_for TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE,
    idempotency_key VARCHAR(100),
    is_completed BOOLEAN,
    is_terminal BOOLEAN,
    projected_at TIMESTAMP WITH TIME ZONE
);

-- Schemas
CREATE TABLE schemas (
    id VARCHAR(17) PRIMARY KEY,
    schema_type VARCHAR(20) NOT NULL,
    content TEXT NOT NULL,
    event_type_id VARCHAR(17),
    version VARCHAR(50),
    mime_type VARCHAR(100),
    name VARCHAR(255),
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Authentication Tables
-- =============================================================================

-- OAuth Clients
CREATE TABLE oauth_clients (
    id VARCHAR(17) PRIMARY KEY,
    client_id VARCHAR(100) UNIQUE NOT NULL,
    client_name VARCHAR(255) NOT NULL,
    client_type VARCHAR(20) NOT NULL DEFAULT 'PUBLIC',
    client_secret_ref VARCHAR(500),
    redirect_uris ARRAY,  -- TEXT[] stored as ARRAY
    allowed_origins ARRAY,  -- TEXT[] stored as ARRAY
    grant_types ARRAY,  -- TEXT[] stored as ARRAY
    default_scopes VARCHAR(500),
    pkce_required BOOLEAN NOT NULL DEFAULT TRUE,
    application_ids ARRAY,  -- TEXT[] stored as ARRAY
    service_account_principal_id VARCHAR(17),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Authorization Codes (short-lived)
CREATE TABLE authorization_codes (
    id VARCHAR(17) PRIMARY KEY,
    code VARCHAR(100) UNIQUE NOT NULL,
    client_id VARCHAR(100) NOT NULL,
    principal_id VARCHAR(17) NOT NULL,
    redirect_uri VARCHAR(500) NOT NULL,
    scope VARCHAR(500),
    code_challenge VARCHAR(200),
    code_challenge_method VARCHAR(20),
    nonce VARCHAR(100),
    state VARCHAR(100),
    context_client_id VARCHAR(17),
    used BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Refresh Tokens
CREATE TABLE refresh_tokens (
    token_hash VARCHAR(64) PRIMARY KEY,
    principal_id VARCHAR(17) NOT NULL,
    client_id VARCHAR(100) NOT NULL,
    context_client_id VARCHAR(17),
    scope VARCHAR(500),
    token_family VARCHAR(100),
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    revoked_at TIMESTAMP WITH TIME ZONE,
    replaced_by VARCHAR(64),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- OIDC Login State
CREATE TABLE oidc_login_states (
    state VARCHAR(100) PRIMARY KEY,
    email_domain VARCHAR(255) NOT NULL,
    auth_config_id VARCHAR(17),
    nonce VARCHAR(100),
    code_verifier VARCHAR(200),
    return_url VARCHAR(500),
    oauth_client_id VARCHAR(100),
    oauth_redirect_uri VARCHAR(500),
    oauth_scope VARCHAR(500),
    oauth_state VARCHAR(100),
    oauth_nonce VARCHAR(100),
    oauth_code_challenge VARCHAR(200),
    oauth_code_challenge_method VARCHAR(20),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- IDP Role Mappings
CREATE TABLE idp_role_mappings (
    id VARCHAR(17) PRIMARY KEY,
    idp_role_name VARCHAR(200) UNIQUE NOT NULL,
    internal_role_name VARCHAR(200) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Configuration Tables
-- =============================================================================

-- Client Auth Config
CREATE TABLE client_auth_configs (
    id VARCHAR(17) PRIMARY KEY,
    email_domain VARCHAR(255) UNIQUE NOT NULL,
    config_type VARCHAR(20) NOT NULL DEFAULT 'CLIENT',
    client_id VARCHAR(17),
    primary_client_id VARCHAR(17),
    additional_client_ids ARRAY,  -- TEXT[] stored as ARRAY
    granted_client_ids ARRAY,  -- TEXT[] stored as ARRAY
    auth_provider VARCHAR(20) NOT NULL DEFAULT 'INTERNAL',
    oidc_issuer_url VARCHAR(500),
    oidc_client_id VARCHAR(200),
    oidc_client_secret_ref VARCHAR(500),
    oidc_multi_tenant BOOLEAN DEFAULT FALSE,
    oidc_issuer_pattern VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Application Client Config
CREATE TABLE application_client_configs (
    id VARCHAR(17) PRIMARY KEY,
    application_id VARCHAR(17) NOT NULL,
    client_id VARCHAR(17) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    base_url_override VARCHAR(500),
    website_override VARCHAR(500),
    config_json TEXT,  -- JSONB stored as TEXT
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(application_id, client_id)
);

-- Client Access Grants
CREATE TABLE client_access_grants (
    id VARCHAR(17) PRIMARY KEY,
    principal_id VARCHAR(17) NOT NULL,
    client_id VARCHAR(17) NOT NULL,
    granted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE,
    UNIQUE(principal_id, client_id)
);

-- Anchor Domains
CREATE TABLE anchor_domains (
    id VARCHAR(17) PRIMARY KEY,
    domain VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CORS Allowed Origins
CREATE TABLE cors_allowed_origins (
    id VARCHAR(17) PRIMARY KEY,
    origin VARCHAR(500) UNIQUE NOT NULL,
    description VARCHAR(255),
    created_by VARCHAR(17),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Cache Entries
CREATE TABLE cache_entries (
    cache_name VARCHAR(100) NOT NULL,
    cache_key VARCHAR(500) NOT NULL,
    cache_value TEXT NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cache_name, cache_key)
);

-- =============================================================================
-- Audit Tables
-- =============================================================================

-- Audit Logs
CREATE TABLE audit_logs (
    id VARCHAR(17) PRIMARY KEY,
    entity_type VARCHAR(100) NOT NULL,
    entity_id VARCHAR(17) NOT NULL,
    operation VARCHAR(100) NOT NULL,
    operation_json TEXT,  -- JSONB stored as TEXT
    principal_id VARCHAR(17),
    performed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Normalized Junction Tables (V8-V10)
-- =============================================================================

-- Principal Roles (normalized from principals.roles JSONB)
CREATE TABLE principal_roles (
    principal_id VARCHAR(17) NOT NULL,
    role_name VARCHAR(100) NOT NULL,
    assignment_source VARCHAR(50),
    assigned_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (principal_id, role_name)
);

-- Service Account Roles (normalized from service_accounts.roles JSONB)
CREATE TABLE service_account_roles (
    service_account_id VARCHAR(17) NOT NULL,
    role_name VARCHAR(100) NOT NULL,
    assigned_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (service_account_id, role_name)
);

-- Subscription Event Types (normalized from subscriptions.event_types JSONB)
CREATE TABLE subscription_event_types (
    subscription_id VARCHAR(17) NOT NULL,
    event_type_code VARCHAR(200) NOT NULL,
    PRIMARY KEY (subscription_id, event_type_code)
);

-- Dispatch Job Attempts (normalized from dispatch_jobs.attempts JSONB)
CREATE TABLE dispatch_job_attempts (
    id VARCHAR(17) PRIMARY KEY,
    dispatch_job_id VARCHAR(17) NOT NULL,
    attempt_number INT NOT NULL,
    status VARCHAR(20) NOT NULL,
    status_code INT,
    error_message TEXT,
    duration_millis BIGINT,
    attempted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE(dispatch_job_id, attempt_number)
);
