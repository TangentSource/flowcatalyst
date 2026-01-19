package tech.flowcatalyst.platform.common.jooq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.dispatchpool.DispatchPool;
import tech.flowcatalyst.dispatchpool.DispatchPoolStatus;
import tech.flowcatalyst.dispatch.DispatchMode;
import tech.flowcatalyst.dispatchjob.model.SignatureAlgorithm;
import tech.flowcatalyst.eventtype.EventType;
import tech.flowcatalyst.eventtype.EventTypeStatus;
import tech.flowcatalyst.eventtype.SchemaType;
import tech.flowcatalyst.eventtype.SpecVersion;
import tech.flowcatalyst.platform.application.Application;
import tech.flowcatalyst.platform.application.ApplicationClientConfig;
import tech.flowcatalyst.platform.authentication.oauth.OAuthClient;
import tech.flowcatalyst.platform.authorization.AuthPermission;
import tech.flowcatalyst.platform.authorization.AuthRole;
import tech.flowcatalyst.platform.client.Client;
import tech.flowcatalyst.platform.client.ClientStatus;
import tech.flowcatalyst.platform.principal.*;
import tech.flowcatalyst.schema.Schema;
import tech.flowcatalyst.serviceaccount.entity.ServiceAccount;
import tech.flowcatalyst.serviceaccount.entity.WebhookAuthType;
import tech.flowcatalyst.serviceaccount.entity.WebhookCredentials;
import tech.flowcatalyst.subscription.*;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static tech.flowcatalyst.platform.jooq.generated.tables.Applications.APPLICATIONS;
import static tech.flowcatalyst.platform.jooq.generated.tables.ApplicationClientConfigs.APPLICATION_CLIENT_CONFIGS;
import static tech.flowcatalyst.platform.jooq.generated.tables.AuthPermissions.AUTH_PERMISSIONS;
import static tech.flowcatalyst.platform.jooq.generated.tables.AuthRoles.AUTH_ROLES;
import static tech.flowcatalyst.platform.jooq.generated.tables.Clients.CLIENTS;
import static tech.flowcatalyst.platform.jooq.generated.tables.DispatchPools.DISPATCH_POOLS;
import static tech.flowcatalyst.platform.jooq.generated.tables.EventTypes.EVENT_TYPES;
import static tech.flowcatalyst.platform.jooq.generated.tables.OauthClients.OAUTH_CLIENTS;
import static tech.flowcatalyst.platform.jooq.generated.tables.Principals.PRINCIPALS;
import static tech.flowcatalyst.platform.jooq.generated.tables.Schemas.SCHEMAS;
import static tech.flowcatalyst.platform.jooq.generated.tables.ServiceAccounts.SERVICE_ACCOUNTS;
import static tech.flowcatalyst.platform.jooq.generated.tables.Subscriptions.SUBSCRIPTIONS;

/**
 * JOOQ-based registry for persisting aggregates within a transaction.
 *
 * <p>This replaces the JDBI-based AggregateRegistry with JOOQ DSL operations.
 * All operations use the provided DSLContext to participate in the caller's transaction.
 */
@ApplicationScoped
public class JooqAggregateRegistry {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Persist an aggregate (insert or update) within the given DSLContext's transaction.
     *
     * @param dsl The JOOQ DSLContext (within a transaction)
     * @param aggregate The entity to persist
     */
    public void persist(DSLContext dsl, Object aggregate) {
        if (aggregate == null) {
            throw new IllegalArgumentException("Aggregate cannot be null");
        }

        // Set timestamps for mutable classes
        setTimestamps(aggregate);

        Class<?> clazz = aggregate.getClass();

        if (clazz == Application.class) {
            persistApplication(dsl, (Application) aggregate);
        } else if (clazz == ApplicationClientConfig.class) {
            persistApplicationClientConfig(dsl, (ApplicationClientConfig) aggregate);
        } else if (clazz == EventType.class) {
            persistEventType(dsl, (EventType) aggregate);
        } else if (clazz == Subscription.class) {
            persistSubscription(dsl, (Subscription) aggregate);
        } else if (clazz == Schema.class) {
            persistSchema(dsl, (Schema) aggregate);
        } else if (clazz == Client.class) {
            persistClient(dsl, (Client) aggregate);
        } else if (clazz == Principal.class) {
            persistPrincipal(dsl, (Principal) aggregate);
        } else if (clazz == OAuthClient.class) {
            persistOAuthClient(dsl, (OAuthClient) aggregate);
        } else if (clazz == ServiceAccount.class) {
            persistServiceAccount(dsl, (ServiceAccount) aggregate);
        } else if (clazz == AuthRole.class) {
            persistAuthRole(dsl, (AuthRole) aggregate);
        } else if (clazz == AuthPermission.class) {
            persistAuthPermission(dsl, (AuthPermission) aggregate);
        } else if (clazz == DispatchPool.class) {
            persistDispatchPool(dsl, (DispatchPool) aggregate);
        } else {
            throw new IllegalArgumentException("Unknown aggregate type: " + clazz.getName() +
                ". Register it in JooqAggregateRegistry.");
        }
    }

    /**
     * Delete an aggregate within the given DSLContext's transaction.
     *
     * @param dsl The JOOQ DSLContext (within a transaction)
     * @param aggregate The entity to delete
     */
    public void delete(DSLContext dsl, Object aggregate) {
        if (aggregate == null) {
            throw new IllegalArgumentException("Aggregate cannot be null");
        }

        Class<?> clazz = aggregate.getClass();
        String id = extractId(aggregate);

        if (clazz == Application.class) {
            dsl.deleteFrom(APPLICATIONS).where(APPLICATIONS.ID.eq(id)).execute();
        } else if (clazz == ApplicationClientConfig.class) {
            dsl.deleteFrom(APPLICATION_CLIENT_CONFIGS).where(APPLICATION_CLIENT_CONFIGS.ID.eq(id)).execute();
        } else if (clazz == EventType.class) {
            dsl.deleteFrom(EVENT_TYPES).where(EVENT_TYPES.ID.eq(id)).execute();
        } else if (clazz == Subscription.class) {
            dsl.deleteFrom(SUBSCRIPTIONS).where(SUBSCRIPTIONS.ID.eq(id)).execute();
        } else if (clazz == Schema.class) {
            dsl.deleteFrom(SCHEMAS).where(SCHEMAS.ID.eq(id)).execute();
        } else if (clazz == Client.class) {
            dsl.deleteFrom(CLIENTS).where(CLIENTS.ID.eq(id)).execute();
        } else if (clazz == Principal.class) {
            dsl.deleteFrom(PRINCIPALS).where(PRINCIPALS.ID.eq(id)).execute();
        } else if (clazz == OAuthClient.class) {
            dsl.deleteFrom(OAUTH_CLIENTS).where(OAUTH_CLIENTS.ID.eq(id)).execute();
        } else if (clazz == ServiceAccount.class) {
            dsl.deleteFrom(SERVICE_ACCOUNTS).where(SERVICE_ACCOUNTS.ID.eq(id)).execute();
        } else if (clazz == AuthRole.class) {
            dsl.deleteFrom(AUTH_ROLES).where(AUTH_ROLES.ID.eq(id)).execute();
        } else if (clazz == AuthPermission.class) {
            dsl.deleteFrom(AUTH_PERMISSIONS).where(AUTH_PERMISSIONS.ID.eq(id)).execute();
        } else if (clazz == DispatchPool.class) {
            dsl.deleteFrom(DISPATCH_POOLS).where(DISPATCH_POOLS.ID.eq(id)).execute();
        } else {
            throw new IllegalArgumentException("Unknown aggregate type for delete: " + clazz.getName());
        }
    }

    // ========================================================================
    // Entity-Specific Persist Methods
    // ========================================================================

    private void persistApplication(DSLContext dsl, Application app) {
        boolean exists = dsl.fetchExists(dsl.selectFrom(APPLICATIONS).where(APPLICATIONS.ID.eq(app.id)));
        if (exists) {
            dsl.update(APPLICATIONS)
                .set(APPLICATIONS.CODE, app.code)
                .set(APPLICATIONS.NAME, app.name)
                .set(APPLICATIONS.DESCRIPTION, app.description)
                .set(APPLICATIONS.TYPE, app.type != null ? app.type.name() : Application.ApplicationType.APPLICATION.name())
                .set(APPLICATIONS.DEFAULT_BASE_URL, app.defaultBaseUrl)
                .set(APPLICATIONS.SERVICE_ACCOUNT_ID, app.serviceAccountId)
                .set(APPLICATIONS.ACTIVE, app.active)
                .set(APPLICATIONS.ICON_URL, app.iconUrl)
                .set(APPLICATIONS.WEBSITE, app.website)
                .set(APPLICATIONS.LOGO, app.logo)
                .set(APPLICATIONS.LOGO_MIME_TYPE, app.logoMimeType)
                .set(APPLICATIONS.UPDATED_AT, toOffsetDateTime(app.updatedAt))
                .where(APPLICATIONS.ID.eq(app.id))
                .execute();
        } else {
            dsl.insertInto(APPLICATIONS)
                .set(APPLICATIONS.ID, app.id)
                .set(APPLICATIONS.CODE, app.code)
                .set(APPLICATIONS.NAME, app.name)
                .set(APPLICATIONS.DESCRIPTION, app.description)
                .set(APPLICATIONS.TYPE, app.type != null ? app.type.name() : Application.ApplicationType.APPLICATION.name())
                .set(APPLICATIONS.DEFAULT_BASE_URL, app.defaultBaseUrl)
                .set(APPLICATIONS.SERVICE_ACCOUNT_ID, app.serviceAccountId)
                .set(APPLICATIONS.ACTIVE, app.active)
                .set(APPLICATIONS.ICON_URL, app.iconUrl)
                .set(APPLICATIONS.WEBSITE, app.website)
                .set(APPLICATIONS.LOGO, app.logo)
                .set(APPLICATIONS.LOGO_MIME_TYPE, app.logoMimeType)
                .set(APPLICATIONS.CREATED_AT, toOffsetDateTime(app.createdAt))
                .set(APPLICATIONS.UPDATED_AT, toOffsetDateTime(app.updatedAt))
                .execute();
        }
    }

    private void persistApplicationClientConfig(DSLContext dsl, ApplicationClientConfig config) {
        String configJson = toJson(config.configJson);
        boolean exists = dsl.fetchExists(dsl.selectFrom(APPLICATION_CLIENT_CONFIGS).where(APPLICATION_CLIENT_CONFIGS.ID.eq(config.id)));
        if (exists) {
            dsl.update(APPLICATION_CLIENT_CONFIGS)
                .set(APPLICATION_CLIENT_CONFIGS.APPLICATION_ID, config.applicationId)
                .set(APPLICATION_CLIENT_CONFIGS.CLIENT_ID, config.clientId)
                .set(APPLICATION_CLIENT_CONFIGS.ENABLED, config.enabled)
                .set(APPLICATION_CLIENT_CONFIGS.BASE_URL_OVERRIDE, config.baseUrlOverride)
                .set(APPLICATION_CLIENT_CONFIGS.WEBSITE_OVERRIDE, config.websiteOverride)
                .set(APPLICATION_CLIENT_CONFIGS.CONFIG_JSON, configJson)
                .set(APPLICATION_CLIENT_CONFIGS.UPDATED_AT, toOffsetDateTime(config.updatedAt))
                .where(APPLICATION_CLIENT_CONFIGS.ID.eq(config.id))
                .execute();
        } else {
            dsl.insertInto(APPLICATION_CLIENT_CONFIGS)
                .set(APPLICATION_CLIENT_CONFIGS.ID, config.id)
                .set(APPLICATION_CLIENT_CONFIGS.APPLICATION_ID, config.applicationId)
                .set(APPLICATION_CLIENT_CONFIGS.CLIENT_ID, config.clientId)
                .set(APPLICATION_CLIENT_CONFIGS.ENABLED, config.enabled)
                .set(APPLICATION_CLIENT_CONFIGS.BASE_URL_OVERRIDE, config.baseUrlOverride)
                .set(APPLICATION_CLIENT_CONFIGS.WEBSITE_OVERRIDE, config.websiteOverride)
                .set(APPLICATION_CLIENT_CONFIGS.CONFIG_JSON, configJson)
                .set(APPLICATION_CLIENT_CONFIGS.CREATED_AT, toOffsetDateTime(config.createdAt))
                .set(APPLICATION_CLIENT_CONFIGS.UPDATED_AT, toOffsetDateTime(config.updatedAt))
                .execute();
        }
    }

    private void persistEventType(DSLContext dsl, EventType eventType) {
        String specVersionsJson = toJson(eventType.specVersions());
        boolean exists = dsl.fetchExists(dsl.selectFrom(EVENT_TYPES).where(EVENT_TYPES.ID.eq(eventType.id())));
        if (exists) {
            dsl.update(EVENT_TYPES)
                .set(EVENT_TYPES.CODE, eventType.code())
                .set(EVENT_TYPES.NAME, eventType.name())
                .set(EVENT_TYPES.DESCRIPTION, eventType.description())
                .set(EVENT_TYPES.SPEC_VERSIONS, specVersionsJson)
                .set(EVENT_TYPES.STATUS, eventType.status() != null ? eventType.status().name() : EventTypeStatus.CURRENT.name())
                .set(EVENT_TYPES.UPDATED_AT, toOffsetDateTime(eventType.updatedAt()))
                .where(EVENT_TYPES.ID.eq(eventType.id()))
                .execute();
        } else {
            dsl.insertInto(EVENT_TYPES)
                .set(EVENT_TYPES.ID, eventType.id())
                .set(EVENT_TYPES.CODE, eventType.code())
                .set(EVENT_TYPES.NAME, eventType.name())
                .set(EVENT_TYPES.DESCRIPTION, eventType.description())
                .set(EVENT_TYPES.SPEC_VERSIONS, specVersionsJson)
                .set(EVENT_TYPES.STATUS, eventType.status() != null ? eventType.status().name() : EventTypeStatus.CURRENT.name())
                .set(EVENT_TYPES.CREATED_AT, toOffsetDateTime(eventType.createdAt()))
                .set(EVENT_TYPES.UPDATED_AT, toOffsetDateTime(eventType.updatedAt()))
                .execute();
        }
    }

    private void persistSubscription(DSLContext dsl, Subscription sub) {
        String eventTypesJson = toJson(sub.eventTypes());
        String customConfigJson = toJson(sub.customConfig());
        boolean exists = dsl.fetchExists(dsl.selectFrom(SUBSCRIPTIONS).where(SUBSCRIPTIONS.ID.eq(sub.id())));
        if (exists) {
            dsl.update(SUBSCRIPTIONS)
                .set(SUBSCRIPTIONS.CODE, sub.code())
                .set(SUBSCRIPTIONS.NAME, sub.name())
                .set(SUBSCRIPTIONS.DESCRIPTION, sub.description())
                .set(SUBSCRIPTIONS.CLIENT_ID, sub.clientId())
                .set(SUBSCRIPTIONS.CLIENT_IDENTIFIER, sub.clientIdentifier())
                .set(SUBSCRIPTIONS.EVENT_TYPES, eventTypesJson)
                .set(SUBSCRIPTIONS.TARGET, sub.target())
                .set(SUBSCRIPTIONS.QUEUE, sub.queue())
                .set(SUBSCRIPTIONS.CUSTOM_CONFIG, customConfigJson)
                .set(SUBSCRIPTIONS.SOURCE, sub.source() != null ? sub.source().name() : SubscriptionSource.API.name())
                .set(SUBSCRIPTIONS.STATUS, sub.status() != null ? sub.status().name() : SubscriptionStatus.ACTIVE.name())
                .set(SUBSCRIPTIONS.MAX_AGE_SECONDS, sub.maxAgeSeconds())
                .set(SUBSCRIPTIONS.DISPATCH_POOL_ID, sub.dispatchPoolId())
                .set(SUBSCRIPTIONS.DISPATCH_POOL_CODE, sub.dispatchPoolCode())
                .set(SUBSCRIPTIONS.DELAY_SECONDS, sub.delaySeconds())
                .set(SUBSCRIPTIONS.SEQUENCE, sub.sequence())
                .set(SUBSCRIPTIONS.MODE, sub.mode() != null ? sub.mode().name() : DispatchMode.IMMEDIATE.name())
                .set(SUBSCRIPTIONS.TIMEOUT_SECONDS, sub.timeoutSeconds())
                .set(SUBSCRIPTIONS.MAX_RETRIES, sub.maxRetries())
                .set(SUBSCRIPTIONS.SERVICE_ACCOUNT_ID, sub.serviceAccountId())
                .set(SUBSCRIPTIONS.DATA_ONLY, sub.dataOnly())
                .set(SUBSCRIPTIONS.UPDATED_AT, toOffsetDateTime(sub.updatedAt()))
                .where(SUBSCRIPTIONS.ID.eq(sub.id()))
                .execute();
        } else {
            dsl.insertInto(SUBSCRIPTIONS)
                .set(SUBSCRIPTIONS.ID, sub.id())
                .set(SUBSCRIPTIONS.CODE, sub.code())
                .set(SUBSCRIPTIONS.NAME, sub.name())
                .set(SUBSCRIPTIONS.DESCRIPTION, sub.description())
                .set(SUBSCRIPTIONS.CLIENT_ID, sub.clientId())
                .set(SUBSCRIPTIONS.CLIENT_IDENTIFIER, sub.clientIdentifier())
                .set(SUBSCRIPTIONS.EVENT_TYPES, eventTypesJson)
                .set(SUBSCRIPTIONS.TARGET, sub.target())
                .set(SUBSCRIPTIONS.QUEUE, sub.queue())
                .set(SUBSCRIPTIONS.CUSTOM_CONFIG, customConfigJson)
                .set(SUBSCRIPTIONS.SOURCE, sub.source() != null ? sub.source().name() : SubscriptionSource.API.name())
                .set(SUBSCRIPTIONS.STATUS, sub.status() != null ? sub.status().name() : SubscriptionStatus.ACTIVE.name())
                .set(SUBSCRIPTIONS.MAX_AGE_SECONDS, sub.maxAgeSeconds())
                .set(SUBSCRIPTIONS.DISPATCH_POOL_ID, sub.dispatchPoolId())
                .set(SUBSCRIPTIONS.DISPATCH_POOL_CODE, sub.dispatchPoolCode())
                .set(SUBSCRIPTIONS.DELAY_SECONDS, sub.delaySeconds())
                .set(SUBSCRIPTIONS.SEQUENCE, sub.sequence())
                .set(SUBSCRIPTIONS.MODE, sub.mode() != null ? sub.mode().name() : DispatchMode.IMMEDIATE.name())
                .set(SUBSCRIPTIONS.TIMEOUT_SECONDS, sub.timeoutSeconds())
                .set(SUBSCRIPTIONS.MAX_RETRIES, sub.maxRetries())
                .set(SUBSCRIPTIONS.SERVICE_ACCOUNT_ID, sub.serviceAccountId())
                .set(SUBSCRIPTIONS.DATA_ONLY, sub.dataOnly())
                .set(SUBSCRIPTIONS.CREATED_AT, toOffsetDateTime(sub.createdAt()))
                .set(SUBSCRIPTIONS.UPDATED_AT, toOffsetDateTime(sub.updatedAt()))
                .execute();
        }
    }

    private void persistSchema(DSLContext dsl, Schema schema) {
        boolean exists = dsl.fetchExists(dsl.selectFrom(SCHEMAS).where(SCHEMAS.ID.eq(schema.id())));
        if (exists) {
            dsl.update(SCHEMAS)
                .set(SCHEMAS.NAME, schema.name())
                .set(SCHEMAS.DESCRIPTION, schema.description())
                .set(SCHEMAS.MIME_TYPE, schema.mimeType())
                .set(SCHEMAS.SCHEMA_TYPE, schema.schemaType() != null ? schema.schemaType().name() : null)
                .set(SCHEMAS.CONTENT, schema.content())
                .set(SCHEMAS.EVENT_TYPE_ID, schema.eventTypeId())
                .set(SCHEMAS.VERSION, schema.version())
                .set(SCHEMAS.UPDATED_AT, toOffsetDateTime(schema.updatedAt()))
                .where(SCHEMAS.ID.eq(schema.id()))
                .execute();
        } else {
            dsl.insertInto(SCHEMAS)
                .set(SCHEMAS.ID, schema.id())
                .set(SCHEMAS.NAME, schema.name())
                .set(SCHEMAS.DESCRIPTION, schema.description())
                .set(SCHEMAS.MIME_TYPE, schema.mimeType())
                .set(SCHEMAS.SCHEMA_TYPE, schema.schemaType() != null ? schema.schemaType().name() : null)
                .set(SCHEMAS.CONTENT, schema.content())
                .set(SCHEMAS.EVENT_TYPE_ID, schema.eventTypeId())
                .set(SCHEMAS.VERSION, schema.version())
                .set(SCHEMAS.CREATED_AT, toOffsetDateTime(schema.createdAt()))
                .set(SCHEMAS.UPDATED_AT, toOffsetDateTime(schema.updatedAt()))
                .execute();
        }
    }

    private void persistClient(DSLContext dsl, Client client) {
        String notesJson = toJson(client.notes);
        boolean exists = dsl.fetchExists(dsl.selectFrom(CLIENTS).where(CLIENTS.ID.eq(client.id)));
        if (exists) {
            dsl.update(CLIENTS)
                .set(CLIENTS.NAME, client.name)
                .set(CLIENTS.IDENTIFIER, client.identifier)
                .set(CLIENTS.STATUS, client.status != null ? client.status.name() : ClientStatus.ACTIVE.name())
                .set(CLIENTS.STATUS_REASON, client.statusReason)
                .set(CLIENTS.STATUS_CHANGED_AT, toOffsetDateTime(client.statusChangedAt))
                .set(CLIENTS.NOTES, notesJson)
                .set(CLIENTS.UPDATED_AT, toOffsetDateTime(client.updatedAt))
                .where(CLIENTS.ID.eq(client.id))
                .execute();
        } else {
            dsl.insertInto(CLIENTS)
                .set(CLIENTS.ID, client.id)
                .set(CLIENTS.NAME, client.name)
                .set(CLIENTS.IDENTIFIER, client.identifier)
                .set(CLIENTS.STATUS, client.status != null ? client.status.name() : ClientStatus.ACTIVE.name())
                .set(CLIENTS.STATUS_REASON, client.statusReason)
                .set(CLIENTS.STATUS_CHANGED_AT, toOffsetDateTime(client.statusChangedAt))
                .set(CLIENTS.NOTES, notesJson)
                .set(CLIENTS.CREATED_AT, toOffsetDateTime(client.createdAt))
                .set(CLIENTS.UPDATED_AT, toOffsetDateTime(client.updatedAt))
                .execute();
        }
    }

    private void persistPrincipal(DSLContext dsl, Principal principal) {
        String serviceAccountJson = toJson(principal.serviceAccount);
        String rolesJson = toJson(principal.roles != null ? principal.roles : new ArrayList<>());

        var ui = principal.userIdentity;

        boolean exists = dsl.fetchExists(dsl.selectFrom(PRINCIPALS).where(PRINCIPALS.ID.eq(principal.id)));
        if (exists) {
            dsl.update(PRINCIPALS)
                .set(PRINCIPALS.TYPE, principal.type != null ? principal.type.name() : PrincipalType.USER.name())
                .set(PRINCIPALS.SCOPE, principal.scope != null ? principal.scope.name() : null)
                .set(PRINCIPALS.CLIENT_ID, principal.clientId)
                .set(PRINCIPALS.APPLICATION_ID, principal.applicationId)
                .set(PRINCIPALS.NAME, principal.name)
                .set(PRINCIPALS.ACTIVE, principal.active)
                .set(PRINCIPALS.EMAIL, ui != null ? ui.email : null)
                .set(PRINCIPALS.EMAIL_DOMAIN, ui != null ? ui.emailDomain : null)
                .set(PRINCIPALS.IDP_TYPE, ui != null && ui.idpType != null ? ui.idpType.name() : null)
                .set(PRINCIPALS.EXTERNAL_IDP_ID, ui != null ? ui.externalIdpId : null)
                .set(PRINCIPALS.PASSWORD_HASH, ui != null ? ui.passwordHash : null)
                .set(PRINCIPALS.LAST_LOGIN_AT, ui != null ? toOffsetDateTime(ui.lastLoginAt) : null)
                .set(PRINCIPALS.SERVICE_ACCOUNT, serviceAccountJson)
                .set(PRINCIPALS.ROLES, rolesJson)
                .set(PRINCIPALS.UPDATED_AT, toOffsetDateTime(principal.updatedAt))
                .where(PRINCIPALS.ID.eq(principal.id))
                .execute();
        } else {
            dsl.insertInto(PRINCIPALS)
                .set(PRINCIPALS.ID, principal.id)
                .set(PRINCIPALS.TYPE, principal.type != null ? principal.type.name() : PrincipalType.USER.name())
                .set(PRINCIPALS.SCOPE, principal.scope != null ? principal.scope.name() : null)
                .set(PRINCIPALS.CLIENT_ID, principal.clientId)
                .set(PRINCIPALS.APPLICATION_ID, principal.applicationId)
                .set(PRINCIPALS.NAME, principal.name)
                .set(PRINCIPALS.ACTIVE, principal.active)
                .set(PRINCIPALS.EMAIL, ui != null ? ui.email : null)
                .set(PRINCIPALS.EMAIL_DOMAIN, ui != null ? ui.emailDomain : null)
                .set(PRINCIPALS.IDP_TYPE, ui != null && ui.idpType != null ? ui.idpType.name() : null)
                .set(PRINCIPALS.EXTERNAL_IDP_ID, ui != null ? ui.externalIdpId : null)
                .set(PRINCIPALS.PASSWORD_HASH, ui != null ? ui.passwordHash : null)
                .set(PRINCIPALS.LAST_LOGIN_AT, ui != null ? toOffsetDateTime(ui.lastLoginAt) : null)
                .set(PRINCIPALS.SERVICE_ACCOUNT, serviceAccountJson)
                .set(PRINCIPALS.ROLES, rolesJson)
                .set(PRINCIPALS.CREATED_AT, toOffsetDateTime(principal.createdAt))
                .set(PRINCIPALS.UPDATED_AT, toOffsetDateTime(principal.updatedAt))
                .execute();
        }
    }

    private void persistOAuthClient(DSLContext dsl, OAuthClient client) {
        String[] redirectUris = client.redirectUris != null ? client.redirectUris.toArray(new String[0]) : new String[0];
        String[] allowedOrigins = client.allowedOrigins != null ? client.allowedOrigins.toArray(new String[0]) : new String[0];
        String[] grantTypes = client.grantTypes != null ? client.grantTypes.toArray(new String[0]) : new String[0];
        String[] applicationIds = client.applicationIds != null ? client.applicationIds.toArray(new String[0]) : new String[0];

        boolean exists = dsl.fetchExists(dsl.selectFrom(OAUTH_CLIENTS).where(OAUTH_CLIENTS.ID.eq(client.id)));
        if (exists) {
            dsl.update(OAUTH_CLIENTS)
                .set(OAUTH_CLIENTS.CLIENT_ID, client.clientId)
                .set(OAUTH_CLIENTS.CLIENT_NAME, client.clientName)
                .set(OAUTH_CLIENTS.CLIENT_TYPE, client.clientType != null ? client.clientType.name() : OAuthClient.ClientType.PUBLIC.name())
                .set(OAUTH_CLIENTS.CLIENT_SECRET_REF, client.clientSecretRef)
                .set(OAUTH_CLIENTS.REDIRECT_URIS, redirectUris)
                .set(OAUTH_CLIENTS.ALLOWED_ORIGINS, allowedOrigins)
                .set(OAUTH_CLIENTS.GRANT_TYPES, grantTypes)
                .set(OAUTH_CLIENTS.DEFAULT_SCOPES, client.defaultScopes)
                .set(OAUTH_CLIENTS.PKCE_REQUIRED, client.pkceRequired)
                .set(OAUTH_CLIENTS.APPLICATION_IDS, applicationIds)
                .set(OAUTH_CLIENTS.SERVICE_ACCOUNT_PRINCIPAL_ID, client.serviceAccountPrincipalId)
                .set(OAUTH_CLIENTS.ACTIVE, client.active)
                .set(OAUTH_CLIENTS.UPDATED_AT, toOffsetDateTime(client.updatedAt))
                .where(OAUTH_CLIENTS.ID.eq(client.id))
                .execute();
        } else {
            dsl.insertInto(OAUTH_CLIENTS)
                .set(OAUTH_CLIENTS.ID, client.id)
                .set(OAUTH_CLIENTS.CLIENT_ID, client.clientId)
                .set(OAUTH_CLIENTS.CLIENT_NAME, client.clientName)
                .set(OAUTH_CLIENTS.CLIENT_TYPE, client.clientType != null ? client.clientType.name() : OAuthClient.ClientType.PUBLIC.name())
                .set(OAUTH_CLIENTS.CLIENT_SECRET_REF, client.clientSecretRef)
                .set(OAUTH_CLIENTS.REDIRECT_URIS, redirectUris)
                .set(OAUTH_CLIENTS.ALLOWED_ORIGINS, allowedOrigins)
                .set(OAUTH_CLIENTS.GRANT_TYPES, grantTypes)
                .set(OAUTH_CLIENTS.DEFAULT_SCOPES, client.defaultScopes)
                .set(OAUTH_CLIENTS.PKCE_REQUIRED, client.pkceRequired)
                .set(OAUTH_CLIENTS.APPLICATION_IDS, applicationIds)
                .set(OAUTH_CLIENTS.SERVICE_ACCOUNT_PRINCIPAL_ID, client.serviceAccountPrincipalId)
                .set(OAUTH_CLIENTS.ACTIVE, client.active)
                .set(OAUTH_CLIENTS.CREATED_AT, toOffsetDateTime(client.createdAt))
                .set(OAUTH_CLIENTS.UPDATED_AT, toOffsetDateTime(client.updatedAt))
                .execute();
        }
    }

    private void persistServiceAccount(DSLContext dsl, ServiceAccount sa) {
        String[] clientIdsArray = sa.clientIds != null ? sa.clientIds.toArray(new String[0]) : new String[0];
        String rolesJson = toJson(sa.roles != null ? sa.roles : new ArrayList<>());

        var wc = sa.webhookCredentials;

        boolean exists = dsl.fetchExists(dsl.selectFrom(SERVICE_ACCOUNTS).where(SERVICE_ACCOUNTS.ID.eq(sa.id)));
        if (exists) {
            dsl.update(SERVICE_ACCOUNTS)
                .set(SERVICE_ACCOUNTS.CODE, sa.code)
                .set(SERVICE_ACCOUNTS.NAME, sa.name)
                .set(SERVICE_ACCOUNTS.DESCRIPTION, sa.description)
                .set(SERVICE_ACCOUNTS.CLIENT_IDS, clientIdsArray)
                .set(SERVICE_ACCOUNTS.APPLICATION_ID, sa.applicationId)
                .set(SERVICE_ACCOUNTS.ACTIVE, sa.active)
                .set(SERVICE_ACCOUNTS.WH_AUTH_TYPE, wc != null && wc.authType != null ? wc.authType.name() : null)
                .set(SERVICE_ACCOUNTS.WH_AUTH_TOKEN_REF, wc != null ? wc.authTokenRef : null)
                .set(SERVICE_ACCOUNTS.WH_SIGNING_SECRET_REF, wc != null ? wc.signingSecretRef : null)
                .set(SERVICE_ACCOUNTS.WH_SIGNING_ALGORITHM, wc != null && wc.signingAlgorithm != null ? wc.signingAlgorithm.name() : null)
                .set(SERVICE_ACCOUNTS.WH_CREDENTIALS_CREATED_AT, wc != null ? toOffsetDateTime(wc.createdAt) : null)
                .set(SERVICE_ACCOUNTS.WH_CREDENTIALS_REGENERATED_AT, wc != null ? toOffsetDateTime(wc.regeneratedAt) : null)
                .set(SERVICE_ACCOUNTS.ROLES, rolesJson)
                .set(SERVICE_ACCOUNTS.LAST_USED_AT, toOffsetDateTime(sa.lastUsedAt))
                .set(SERVICE_ACCOUNTS.UPDATED_AT, toOffsetDateTime(sa.updatedAt))
                .where(SERVICE_ACCOUNTS.ID.eq(sa.id))
                .execute();
        } else {
            dsl.insertInto(SERVICE_ACCOUNTS)
                .set(SERVICE_ACCOUNTS.ID, sa.id)
                .set(SERVICE_ACCOUNTS.CODE, sa.code)
                .set(SERVICE_ACCOUNTS.NAME, sa.name)
                .set(SERVICE_ACCOUNTS.DESCRIPTION, sa.description)
                .set(SERVICE_ACCOUNTS.CLIENT_IDS, clientIdsArray)
                .set(SERVICE_ACCOUNTS.APPLICATION_ID, sa.applicationId)
                .set(SERVICE_ACCOUNTS.ACTIVE, sa.active)
                .set(SERVICE_ACCOUNTS.WH_AUTH_TYPE, wc != null && wc.authType != null ? wc.authType.name() : null)
                .set(SERVICE_ACCOUNTS.WH_AUTH_TOKEN_REF, wc != null ? wc.authTokenRef : null)
                .set(SERVICE_ACCOUNTS.WH_SIGNING_SECRET_REF, wc != null ? wc.signingSecretRef : null)
                .set(SERVICE_ACCOUNTS.WH_SIGNING_ALGORITHM, wc != null && wc.signingAlgorithm != null ? wc.signingAlgorithm.name() : null)
                .set(SERVICE_ACCOUNTS.WH_CREDENTIALS_CREATED_AT, wc != null ? toOffsetDateTime(wc.createdAt) : null)
                .set(SERVICE_ACCOUNTS.WH_CREDENTIALS_REGENERATED_AT, wc != null ? toOffsetDateTime(wc.regeneratedAt) : null)
                .set(SERVICE_ACCOUNTS.ROLES, rolesJson)
                .set(SERVICE_ACCOUNTS.LAST_USED_AT, toOffsetDateTime(sa.lastUsedAt))
                .set(SERVICE_ACCOUNTS.CREATED_AT, toOffsetDateTime(sa.createdAt))
                .set(SERVICE_ACCOUNTS.UPDATED_AT, toOffsetDateTime(sa.updatedAt))
                .execute();
        }
    }

    private void persistAuthRole(DSLContext dsl, AuthRole role) {
        String[] permissionsArray = role.permissions != null ? role.permissions.toArray(new String[0]) : new String[0];

        boolean exists = dsl.fetchExists(dsl.selectFrom(AUTH_ROLES).where(AUTH_ROLES.ID.eq(role.id)));
        if (exists) {
            dsl.update(AUTH_ROLES)
                .set(AUTH_ROLES.APPLICATION_ID, role.applicationId)
                .set(AUTH_ROLES.APPLICATION_CODE, role.applicationCode)
                .set(AUTH_ROLES.NAME, role.name)
                .set(AUTH_ROLES.DISPLAY_NAME, role.displayName)
                .set(AUTH_ROLES.DESCRIPTION, role.description)
                .set(AUTH_ROLES.PERMISSIONS, permissionsArray)
                .set(AUTH_ROLES.SOURCE, role.source != null ? role.source.name() : AuthRole.RoleSource.DATABASE.name())
                .set(AUTH_ROLES.CLIENT_MANAGED, role.clientManaged)
                .set(AUTH_ROLES.UPDATED_AT, toOffsetDateTime(role.updatedAt))
                .where(AUTH_ROLES.ID.eq(role.id))
                .execute();
        } else {
            dsl.insertInto(AUTH_ROLES)
                .set(AUTH_ROLES.ID, role.id)
                .set(AUTH_ROLES.APPLICATION_ID, role.applicationId)
                .set(AUTH_ROLES.APPLICATION_CODE, role.applicationCode)
                .set(AUTH_ROLES.NAME, role.name)
                .set(AUTH_ROLES.DISPLAY_NAME, role.displayName)
                .set(AUTH_ROLES.DESCRIPTION, role.description)
                .set(AUTH_ROLES.PERMISSIONS, permissionsArray)
                .set(AUTH_ROLES.SOURCE, role.source != null ? role.source.name() : AuthRole.RoleSource.DATABASE.name())
                .set(AUTH_ROLES.CLIENT_MANAGED, role.clientManaged)
                .set(AUTH_ROLES.CREATED_AT, toOffsetDateTime(role.createdAt))
                .set(AUTH_ROLES.UPDATED_AT, toOffsetDateTime(role.updatedAt))
                .execute();
        }
    }

    private void persistAuthPermission(DSLContext dsl, AuthPermission perm) {
        boolean exists = dsl.fetchExists(dsl.selectFrom(AUTH_PERMISSIONS).where(AUTH_PERMISSIONS.ID.eq(perm.id)));
        if (exists) {
            dsl.update(AUTH_PERMISSIONS)
                .set(AUTH_PERMISSIONS.APPLICATION_ID, perm.applicationId)
                .set(AUTH_PERMISSIONS.NAME, perm.name)
                .set(AUTH_PERMISSIONS.DISPLAY_NAME, perm.displayName)
                .set(AUTH_PERMISSIONS.DESCRIPTION, perm.description)
                .set(AUTH_PERMISSIONS.SOURCE, perm.source != null ? perm.source.name() : AuthPermission.PermissionSource.SDK.name())
                .where(AUTH_PERMISSIONS.ID.eq(perm.id))
                .execute();
        } else {
            dsl.insertInto(AUTH_PERMISSIONS)
                .set(AUTH_PERMISSIONS.ID, perm.id)
                .set(AUTH_PERMISSIONS.APPLICATION_ID, perm.applicationId)
                .set(AUTH_PERMISSIONS.NAME, perm.name)
                .set(AUTH_PERMISSIONS.DISPLAY_NAME, perm.displayName)
                .set(AUTH_PERMISSIONS.DESCRIPTION, perm.description)
                .set(AUTH_PERMISSIONS.SOURCE, perm.source != null ? perm.source.name() : AuthPermission.PermissionSource.SDK.name())
                .set(AUTH_PERMISSIONS.CREATED_AT, toOffsetDateTime(perm.createdAt))
                .execute();
        }
    }

    private void persistDispatchPool(DSLContext dsl, DispatchPool pool) {
        boolean exists = dsl.fetchExists(dsl.selectFrom(DISPATCH_POOLS).where(DISPATCH_POOLS.ID.eq(pool.id())));
        if (exists) {
            dsl.update(DISPATCH_POOLS)
                .set(DISPATCH_POOLS.CODE, pool.code())
                .set(DISPATCH_POOLS.NAME, pool.name())
                .set(DISPATCH_POOLS.DESCRIPTION, pool.description())
                .set(DISPATCH_POOLS.RATE_LIMIT, pool.rateLimit())
                .set(DISPATCH_POOLS.CONCURRENCY, pool.concurrency())
                .set(DISPATCH_POOLS.CLIENT_ID, pool.clientId())
                .set(DISPATCH_POOLS.CLIENT_IDENTIFIER, pool.clientIdentifier())
                .set(DISPATCH_POOLS.STATUS, pool.status() != null ? pool.status().name() : DispatchPoolStatus.ACTIVE.name())
                .set(DISPATCH_POOLS.UPDATED_AT, toOffsetDateTime(pool.updatedAt()))
                .where(DISPATCH_POOLS.ID.eq(pool.id()))
                .execute();
        } else {
            dsl.insertInto(DISPATCH_POOLS)
                .set(DISPATCH_POOLS.ID, pool.id())
                .set(DISPATCH_POOLS.CODE, pool.code())
                .set(DISPATCH_POOLS.NAME, pool.name())
                .set(DISPATCH_POOLS.DESCRIPTION, pool.description())
                .set(DISPATCH_POOLS.RATE_LIMIT, pool.rateLimit())
                .set(DISPATCH_POOLS.CONCURRENCY, pool.concurrency())
                .set(DISPATCH_POOLS.CLIENT_ID, pool.clientId())
                .set(DISPATCH_POOLS.CLIENT_IDENTIFIER, pool.clientIdentifier())
                .set(DISPATCH_POOLS.STATUS, pool.status() != null ? pool.status().name() : DispatchPoolStatus.ACTIVE.name())
                .set(DISPATCH_POOLS.CREATED_AT, toOffsetDateTime(pool.createdAt()))
                .set(DISPATCH_POOLS.UPDATED_AT, toOffsetDateTime(pool.updatedAt()))
                .execute();
        }
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    private void setTimestamps(Object aggregate) {
        if (aggregate.getClass().isRecord()) {
            return;
        }

        try {
            Field updatedAtField = aggregate.getClass().getField("updatedAt");
            if (updatedAtField.getType() == Instant.class) {
                updatedAtField.set(aggregate, Instant.now());
            }
        } catch (NoSuchFieldException | IllegalAccessException ignored) {}

        try {
            Field createdAtField = aggregate.getClass().getField("createdAt");
            if (createdAtField.getType() == Instant.class && createdAtField.get(aggregate) == null) {
                createdAtField.set(aggregate, Instant.now());
            }
        } catch (NoSuchFieldException | IllegalAccessException ignored) {}
    }

    private String extractId(Object aggregate) {
        Class<?> clazz = aggregate.getClass();

        if (clazz.isRecord()) {
            try {
                var method = clazz.getMethod("id");
                return (String) method.invoke(aggregate);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "Record must have an id() accessor returning String: " + clazz.getName(), e);
            }
        }

        try {
            Field field = clazz.getField("id");
            return (String) field.get(aggregate);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalArgumentException(
                "Aggregate must have a public String id field: " + clazz.getName(), e);
        }
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant != null ? instant.atOffset(ZoneOffset.UTC) : null;
    }

    private String toJson(Object obj) {
        if (obj == null) return null;
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception ex) {
            return null;
        }
    }
}
