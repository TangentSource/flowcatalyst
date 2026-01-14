package tech.flowcatalyst.platform.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.bson.Document;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jdbi.v3.core.Jdbi;
import org.jboss.logging.Logger;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.List;

/**
 * Migration utility to copy data from MongoDB to PostgreSQL.
 *
 * <p>This class provides methods to migrate each MongoDB collection to
 * its corresponding PostgreSQL table. Designed for small datasets
 * (up to 100 records per collection) during development/testing phase.</p>
 *
 * <p>Usage: Call {@link #migrateAll()} to migrate all collections,
 * or individual migrate methods for specific collections.</p>
 */
@ApplicationScoped
public class MongoToPostgresMigration {

    private static final Logger LOG = Logger.getLogger(MongoToPostgresMigration.class);

    @Inject
    MongoClient mongoClient;

    @Inject
    Jdbi jdbi;

    @ConfigProperty(name = "quarkus.mongodb.database")
    String mongoDatabase;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    /**
     * Migrate all collections from MongoDB to PostgreSQL.
     */
    public void migrateAll() {
        LOG.info("Starting MongoDB to PostgreSQL migration...");

        // Core entities (order matters due to foreign keys)
        migrateClients();
        migratePrincipals();
        migrateApplications();
        migrateServiceAccounts();

        // Authorization
        migrateAuthRoles();
        migrateAuthPermissions();

        // Configuration
        migrateAnchorDomains();
        migrateClientAuthConfigs();
        migrateClientAccessGrants();
        migrateApplicationClientConfigs();
        migrateCorsAllowedOrigins();

        // Authentication
        migrateOAuthClients();
        migrateIdpRoleMappings();
        migrateOidcLoginStates();
        migrateAuthorizationCodes();
        migrateRefreshTokens();

        // Messaging (if enabled)
        migrateEventTypes();
        migrateEvents();
        migrateSchemas();
        migrateSubscriptions();
        migrateDispatchPools();
        migrateDispatchJobs();

        // Audit
        migrateAuditLogs();

        LOG.info("Migration completed successfully!");
    }

    public void migrateClients() {
        LOG.info("Migrating clients...");
        MongoCollection<Document> collection = getCollection("clients");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO clients (id, name, identifier, status, status_reason,
                        status_changed_at, notes, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("name"),
                    doc.getString("identifier"),
                    doc.getString("status"),
                    doc.getString("statusReason"),
                    toTimestamp(doc.get("statusChangedAt")),
                    toJson(doc.getList("notes", Document.class)),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate client %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d clients", collection.countDocuments());
    }

    public void migratePrincipals() {
        LOG.info("Migrating principals...");
        MongoCollection<Document> collection = getCollection("principals");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO principals (id, type, scope, client_id, application_id, name, active,
                        user_identity, service_account, roles, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("type"),
                    doc.getString("scope"),
                    doc.getString("clientId"),
                    doc.getString("applicationId"),
                    doc.getString("name"),
                    doc.getBoolean("active", true),
                    toJson(doc.get("userIdentity")),
                    toJson(doc.get("serviceAccount")),
                    toJson(doc.getList("roles", Document.class)),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate principal %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d principals", collection.countDocuments());
    }

    public void migrateApplications() {
        LOG.info("Migrating applications...");
        MongoCollection<Document> collection = getCollection("applications");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO applications (id, code, name, description, type, default_base_url,
                        service_account_id, active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("code"),
                    doc.getString("name"),
                    doc.getString("description"),
                    doc.getString("type"),
                    doc.getString("defaultBaseUrl"),
                    doc.getString("serviceAccountId"),
                    doc.getBoolean("active", true),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate application %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d applications", collection.countDocuments());
    }

    public void migrateServiceAccounts() {
        LOG.info("Migrating service_accounts...");
        MongoCollection<Document> collection = getCollection("service_accounts");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO service_accounts (id, code, name, description, client_ids,
                        application_id, active, webhook_credentials, roles, last_used_at,
                        created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("code"),
                    doc.getString("name"),
                    doc.getString("description"),
                    toStringArray(doc.getList("clientIds", String.class)),
                    doc.getString("applicationId"),
                    doc.getBoolean("active", true),
                    toJson(doc.get("webhookCredentials")),
                    toJson(doc.getList("roles", Document.class)),
                    toTimestamp(doc.get("lastUsedAt")),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate service_account %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d service_accounts", collection.countDocuments());
    }

    public void migrateAuthRoles() {
        LOG.info("Migrating auth_roles (from roles)...");
        MongoCollection<Document> collection = getCollection("roles");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO auth_roles (id, application_id, application_code, name, display_name,
                        permissions, source, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("applicationId"),
                    doc.getString("applicationCode"),
                    doc.getString("name"),
                    doc.getString("displayName"),
                    toStringArray(doc.getList("permissions", String.class)),
                    doc.getString("source"),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate auth_role %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d auth_roles (from roles collection)", collection.countDocuments());
    }

    public void migrateAuthPermissions() {
        LOG.info("Migrating auth_permissions (from permissions)...");
        MongoCollection<Document> collection = getCollection("permissions");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO auth_permissions (id, application_id, name, display_name, source,
                        created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("applicationId"),
                    doc.getString("name"),
                    doc.getString("displayName"),
                    doc.getString("source"),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate auth_permission %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d auth_permissions (from permissions collection)", collection.countDocuments());
    }

    public void migrateAnchorDomains() {
        LOG.info("Migrating anchor_domains...");
        MongoCollection<Document> collection = getCollection("anchor_domains");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO anchor_domains (id, domain, created_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("domain"),
                    toTimestamp(doc.get("createdAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate anchor_domain %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d anchor_domains", collection.countDocuments());
    }

    public void migrateClientAuthConfigs() {
        LOG.info("Migrating client_auth_configs (from auth_configs)...");
        MongoCollection<Document> collection = getCollection("auth_configs");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO client_auth_configs (id, email_domain, primary_client_id,
                        additional_client_ids, granted_client_ids, auth_provider, oidc_issuer_url,
                        oidc_client_id, oidc_client_secret_ref, oidc_multi_tenant, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("emailDomain"),
                    doc.getString("primaryClientId"),
                    toStringArray(doc.getList("additionalClientIds", String.class)),
                    toStringArray(doc.getList("grantedClientIds", String.class)),
                    doc.getString("authProvider"),
                    doc.getString("oidcIssuerUrl"),
                    doc.getString("oidcClientId"),
                    doc.getString("oidcClientSecretRef"),
                    doc.getBoolean("oidcMultiTenant", false),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate client_auth_config %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d client_auth_configs (from auth_configs collection)", collection.countDocuments());
    }

    public void migrateClientAccessGrants() {
        LOG.info("Migrating client_access_grants...");
        MongoCollection<Document> collection = getCollection("client_access_grants");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO client_access_grants (id, principal_id, client_id, granted_at, expires_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("principalId"),
                    doc.getString("clientId"),
                    toTimestamp(doc.get("grantedAt")),
                    toTimestamp(doc.get("expiresAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate client_access_grant %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d client_access_grants", collection.countDocuments());
    }

    public void migrateApplicationClientConfigs() {
        LOG.info("Migrating application_client_configs...");
        MongoCollection<Document> collection = getCollection("application_client_configs");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO application_client_configs (id, application_id, client_id, enabled,
                        base_url_override, config_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("applicationId"),
                    doc.getString("clientId"),
                    doc.getBoolean("enabled", true),
                    doc.getString("baseUrlOverride"),
                    toJson(doc.get("configJson")),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate application_client_config %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d application_client_configs", collection.countDocuments());
    }

    public void migrateCorsAllowedOrigins() {
        LOG.info("Migrating cors_allowed_origins...");
        MongoCollection<Document> collection = getCollection("cors_allowed_origins");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO cors_allowed_origins (id, origin, created_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("origin"),
                    toTimestamp(doc.get("createdAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate cors_allowed_origin %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d cors_allowed_origins", collection.countDocuments());
    }

    public void migrateOAuthClients() {
        LOG.info("Migrating oauth_clients...");
        MongoCollection<Document> collection = getCollection("oauth_clients");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO oauth_clients (id, client_id, client_name, client_type, redirect_uris,
                        allowed_origins, grant_types, application_ids, client_secret_ref,
                        created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("clientId"),
                    doc.getString("clientName"),
                    doc.getString("clientType"),
                    toStringArray(doc.getList("redirectUris", String.class)),
                    toStringArray(doc.getList("allowedOrigins", String.class)),
                    toStringArray(doc.getList("grantTypes", String.class)),
                    toStringArray(doc.getList("applicationIds", String.class)),
                    doc.getString("clientSecretRef"),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate oauth_client %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d oauth_clients", collection.countDocuments());
    }

    public void migrateIdpRoleMappings() {
        LOG.info("Migrating idp_role_mappings...");
        MongoCollection<Document> collection = getCollection("idp_role_mappings");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO idp_role_mappings (id, idp_role_name, internal_role_name, created_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("idpRoleName"),
                    doc.getString("internalRoleName"),
                    toTimestamp(doc.get("createdAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate idp_role_mapping %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d idp_role_mappings", collection.countDocuments());
    }

    public void migrateOidcLoginStates() {
        LOG.info("Migrating oidc_login_states...");
        MongoCollection<Document> collection = getCollection("oidc_login_states");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO oidc_login_states (state, email_domain, nonce, code_verifier, return_url,
                        oauth_client_id, oauth_redirect_uri, oauth_scope, oauth_state, oauth_nonce,
                        oauth_code_challenge, oauth_code_challenge_method, expires_at, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (state) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("emailDomain"),
                    doc.getString("nonce"),
                    doc.getString("codeVerifier"),
                    doc.getString("returnUrl"),
                    doc.getString("oauthClientId"),
                    doc.getString("oauthRedirectUri"),
                    doc.getString("oauthScope"),
                    doc.getString("oauthState"),
                    doc.getString("oauthNonce"),
                    doc.getString("oauthCodeChallenge"),
                    doc.getString("oauthCodeChallengeMethod"),
                    toTimestamp(doc.get("expiresAt")),
                    toTimestamp(doc.get("createdAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate oidc_login_state %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d oidc_login_states", collection.countDocuments());
    }

    public void migrateAuthorizationCodes() {
        LOG.info("Migrating authorization_codes...");
        MongoCollection<Document> collection = getCollection("authorization_codes");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO authorization_codes (id, code, client_id, principal_id, redirect_uri,
                        scope, code_challenge, code_challenge_method, nonce, used, expires_at, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("code"),
                    doc.getString("clientId"),
                    doc.getString("principalId"),
                    doc.getString("redirectUri"),
                    doc.getString("scope"),
                    doc.getString("codeChallenge"),
                    doc.getString("codeChallengeMethod"),
                    doc.getString("nonce"),
                    doc.getBoolean("used", false),
                    toTimestamp(doc.get("expiresAt")),
                    toTimestamp(doc.get("createdAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate authorization_code %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d authorization_codes", collection.countDocuments());
    }

    public void migrateRefreshTokens() {
        LOG.info("Migrating refresh_tokens...");
        MongoCollection<Document> collection = getCollection("refresh_tokens");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO refresh_tokens (token_hash, principal_id, client_id, token_family,
                        revoked, expires_at, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (token_hash) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("principalId"),
                    doc.getString("clientId"),
                    doc.getString("tokenFamily"),
                    doc.getBoolean("revoked", false),
                    toTimestamp(doc.get("expiresAt")),
                    toTimestamp(doc.get("createdAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate refresh_token %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d refresh_tokens", collection.countDocuments());
    }

    public void migrateEventTypes() {
        LOG.info("Migrating event_types...");
        MongoCollection<Document> collection = getCollection("event_types");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO event_types (id, code, name, description, spec_versions, status,
                        created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("code"),
                    doc.getString("name"),
                    doc.getString("description"),
                    toJson(doc.getList("specVersions", Document.class)),
                    doc.getString("status"),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate event_type %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d event_types", collection.countDocuments());
    }

    public void migrateEvents() {
        LOG.info("Migrating events...");
        MongoCollection<Document> collection = getCollection("events");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO events (id, type, source, subject, time, data, correlation_id,
                        causation_id, deduplication_id, message_group, client_id, context_data, created_at)
                    VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?::jsonb, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("type"),
                    doc.getString("source"),
                    doc.getString("subject"),
                    toTimestamp(doc.get("time")),
                    toJson(doc.get("data")),
                    doc.getString("correlationId"),
                    doc.getString("causationId"),
                    doc.getString("deduplicationId"),
                    doc.getString("messageGroup"),
                    doc.getString("clientId"),
                    toJson(doc.getList("contextData", Document.class)),
                    toTimestamp(doc.get("createdAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate event %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d events", collection.countDocuments());
    }

    public void migrateSchemas() {
        LOG.info("Migrating schemas...");
        MongoCollection<Document> collection = getCollection("schemas");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO schemas (id, schema_type, content, event_type_id, version, mime_type,
                        created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("schemaType"),
                    doc.getString("content"),
                    doc.getString("eventTypeId"),
                    doc.getString("version"),
                    doc.getString("mimeType"),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate schema %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d schemas", collection.countDocuments());
    }

    public void migrateSubscriptions() {
        LOG.info("Migrating subscriptions...");
        MongoCollection<Document> collection = getCollection("subscriptions");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO subscriptions (id, code, name, description, client_id, client_identifier,
                        event_types, target, queue, custom_config, source, status, max_age_seconds,
                        dispatch_pool_id, dispatch_pool_code, delay_seconds, sequence, mode,
                        timeout_seconds, max_retries, service_account_id, data_only, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("code"),
                    doc.getString("name"),
                    doc.getString("description"),
                    doc.getString("clientId"),
                    doc.getString("clientIdentifier"),
                    toJson(doc.getList("eventTypes", Document.class)),
                    doc.getString("target"),
                    doc.getString("queue"),
                    toJson(doc.getList("customConfig", Document.class)),
                    doc.getString("source"),
                    doc.getString("status"),
                    doc.getInteger("maxAgeSeconds", 86400),
                    doc.getString("dispatchPoolId"),
                    doc.getString("dispatchPoolCode"),
                    doc.getInteger("delaySeconds", 0),
                    doc.getInteger("sequence", 99),
                    doc.getString("mode"),
                    doc.getInteger("timeoutSeconds", 30),
                    doc.getInteger("maxRetries", 3),
                    doc.getString("serviceAccountId"),
                    doc.getBoolean("dataOnly", true),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate subscription %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d subscriptions", collection.countDocuments());
    }

    public void migrateDispatchPools() {
        LOG.info("Migrating dispatch_pools...");
        MongoCollection<Document> collection = getCollection("dispatch_pools");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO dispatch_pools (id, code, rate_limit, concurrency, client_id, status,
                        created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("code"),
                    doc.getInteger("rateLimit", 0),
                    doc.getInteger("concurrency", 1),
                    doc.getString("clientId"),
                    doc.getString("status"),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate dispatch_pool %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d dispatch_pools", collection.countDocuments());
    }

    public void migrateDispatchJobs() {
        LOG.info("Migrating dispatch_jobs...");
        MongoCollection<Document> collection = getCollection("dispatch_jobs");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO dispatch_jobs (id, external_id, source, kind, code, subject, event_id,
                        correlation_id, metadata, target_url, protocol, headers, payload, payload_content_type,
                        data_only, service_account_id, client_id, subscription_id, mode, dispatch_pool_id,
                        message_group, sequence, timeout_seconds, schema_id, status, max_retries,
                        retry_strategy, scheduled_for, expires_at, attempt_count, last_attempt_at,
                        completed_at, duration_millis, last_error, idempotency_key, attempts,
                        created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("externalId"),
                    doc.getString("source"),
                    doc.getString("kind"),
                    doc.getString("code"),
                    doc.getString("subject"),
                    doc.getString("eventId"),
                    doc.getString("correlationId"),
                    toJson(doc.getList("metadata", Document.class)),
                    doc.getString("targetUrl"),
                    doc.getString("protocol"),
                    toJson(doc.get("headers")),
                    doc.getString("payload"),
                    doc.getString("payloadContentType"),
                    doc.getBoolean("dataOnly", true),
                    doc.getString("serviceAccountId"),
                    doc.getString("clientId"),
                    doc.getString("subscriptionId"),
                    doc.getString("mode"),
                    doc.getString("dispatchPoolId"),
                    doc.getString("messageGroup"),
                    doc.getInteger("sequence", 99),
                    doc.getInteger("timeoutSeconds", 30),
                    doc.getString("schemaId"),
                    doc.getString("status"),
                    doc.getInteger("maxRetries", 3),
                    doc.getString("retryStrategy"),
                    toTimestamp(doc.get("scheduledFor")),
                    toTimestamp(doc.get("expiresAt")),
                    doc.getInteger("attemptCount", 0),
                    toTimestamp(doc.get("lastAttemptAt")),
                    toTimestamp(doc.get("completedAt")),
                    doc.getLong("durationMillis"),
                    doc.getString("lastError"),
                    doc.getString("idempotencyKey"),
                    toJson(doc.getList("attempts", Document.class)),
                    toTimestamp(doc.get("createdAt")),
                    toTimestamp(doc.get("updatedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate dispatch_job %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d dispatch_jobs", collection.countDocuments());
    }

    public void migrateAuditLogs() {
        LOG.info("Migrating audit_logs...");
        MongoCollection<Document> collection = getCollection("audit_logs");

        collection.find().forEach(doc -> {
            try {
                jdbi.useHandle(handle -> handle.execute("""
                    INSERT INTO audit_logs (id, entity_type, entity_id, operation, operation_json,
                        principal_id, performed_at)
                    VALUES (?, ?, ?, ?, ?::jsonb, ?, ?)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    doc.getString("_id"),
                    doc.getString("entityType"),
                    doc.getString("entityId"),
                    doc.getString("operation"),
                    toJson(doc.get("operationJson")),
                    doc.getString("principalId"),
                    toTimestamp(doc.get("performedAt"))
                ));
            } catch (Exception e) {
                LOG.warnf("Failed to migrate audit_log %s: %s", doc.getString("_id"), e.getMessage());
            }
        });
        LOG.infof("Migrated %d audit_logs", collection.countDocuments());
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    private MongoCollection<Document> getCollection(String name) {
        MongoDatabase db = mongoClient.getDatabase(mongoDatabase);
        return db.getCollection(name);
    }

    private Timestamp toTimestamp(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Date date) {
            return Timestamp.from(date.toInstant());
        }
        if (obj instanceof Instant instant) {
            return Timestamp.from(instant);
        }
        return null;
    }

    private String toJson(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            if (obj instanceof List<?> list) {
                return list.isEmpty() ? "[]" : objectMapper.writeValueAsString(list);
            }
            if (obj instanceof Document doc) {
                return objectMapper.writeValueAsString(doc);
            }
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            LOG.warnf("Failed to serialize to JSON: %s", e.getMessage());
            return null;
        }
    }

    private String[] toStringArray(List<String> list) {
        if (list == null || list.isEmpty()) {
            return new String[0];
        }
        return list.toArray(new String[0]);
    }
}
