package tech.flowcatalyst.platform.common;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Handle;
import tech.flowcatalyst.dispatchpool.DispatchPool;
import tech.flowcatalyst.dispatchpool.DispatchPoolDao;
import tech.flowcatalyst.eventtype.EventType;
import tech.flowcatalyst.eventtype.EventTypeDao;
import tech.flowcatalyst.platform.application.Application;
import tech.flowcatalyst.platform.application.ApplicationClientConfig;
import tech.flowcatalyst.platform.application.ApplicationClientConfigDao;
import tech.flowcatalyst.platform.application.ApplicationDao;
import tech.flowcatalyst.platform.authentication.oauth.OAuthClient;
import tech.flowcatalyst.platform.authentication.oauth.OAuthClientDao;
import tech.flowcatalyst.platform.authorization.AuthPermission;
import tech.flowcatalyst.platform.authorization.AuthPermissionDao;
import tech.flowcatalyst.platform.authorization.AuthRole;
import tech.flowcatalyst.platform.authorization.AuthRoleDao;
import tech.flowcatalyst.platform.client.Client;
import tech.flowcatalyst.platform.client.ClientDao;
import tech.flowcatalyst.platform.principal.Principal;
import tech.flowcatalyst.platform.principal.PrincipalDao;
import tech.flowcatalyst.schema.Schema;
import tech.flowcatalyst.schema.SchemaDao;
import tech.flowcatalyst.serviceaccount.entity.ServiceAccount;
import tech.flowcatalyst.serviceaccount.repository.ServiceAccountDao;
import tech.flowcatalyst.subscription.Subscription;
import tech.flowcatalyst.subscription.SubscriptionDao;

import java.lang.reflect.Field;
import java.time.Instant;

/**
 * Registry for persisting aggregates through JDBI within a transaction.
 *
 * <p>This class provides a centralized place for mapping entity types to their
 * respective DAOs and handling insert/update logic within a transactional Handle.
 */
@ApplicationScoped
public class AggregateRegistry {

    /**
     * Persist an aggregate (insert or update) within the given Handle's transaction.
     *
     * @param handle The JDBI handle (within a transaction)
     * @param aggregate The entity to persist
     */
    public void persist(Handle handle, Object aggregate) {
        if (aggregate == null) {
            throw new IllegalArgumentException("Aggregate cannot be null");
        }

        // Set timestamps
        setTimestamps(aggregate);

        Class<?> clazz = aggregate.getClass();

        // Dispatch to the appropriate DAO based on entity type
        if (clazz == Application.class) {
            persistApplication(handle, (Application) aggregate);
        } else if (clazz == ApplicationClientConfig.class) {
            persistApplicationClientConfig(handle, (ApplicationClientConfig) aggregate);
        } else if (clazz == EventType.class) {
            persistEventType(handle, (EventType) aggregate);
        } else if (clazz == Subscription.class) {
            persistSubscription(handle, (Subscription) aggregate);
        } else if (clazz == Schema.class) {
            persistSchema(handle, (Schema) aggregate);
        } else if (clazz == Client.class) {
            persistClient(handle, (Client) aggregate);
        } else if (clazz == Principal.class) {
            persistPrincipal(handle, (Principal) aggregate);
        } else if (clazz == OAuthClient.class) {
            persistOAuthClient(handle, (OAuthClient) aggregate);
        } else if (clazz == ServiceAccount.class) {
            persistServiceAccount(handle, (ServiceAccount) aggregate);
        } else if (clazz == AuthRole.class) {
            persistAuthRole(handle, (AuthRole) aggregate);
        } else if (clazz == AuthPermission.class) {
            persistAuthPermission(handle, (AuthPermission) aggregate);
        } else if (clazz == DispatchPool.class) {
            persistDispatchPool(handle, (DispatchPool) aggregate);
        } else {
            throw new IllegalArgumentException("Unknown aggregate type: " + clazz.getName() +
                ". Register it in AggregateRegistry.");
        }
    }

    /**
     * Delete an aggregate within the given Handle's transaction.
     *
     * @param handle The JDBI handle (within a transaction)
     * @param aggregate The entity to delete
     */
    public void delete(Handle handle, Object aggregate) {
        if (aggregate == null) {
            throw new IllegalArgumentException("Aggregate cannot be null");
        }

        Class<?> clazz = aggregate.getClass();
        String id = extractId(aggregate);

        if (clazz == Application.class) {
            handle.attach(ApplicationDao.class).deleteById(id);
        } else if (clazz == EventType.class) {
            handle.attach(EventTypeDao.class).deleteById(id);
        } else if (clazz == Subscription.class) {
            handle.attach(SubscriptionDao.class).deleteById(id);
        } else if (clazz == Schema.class) {
            handle.attach(SchemaDao.class).deleteById(id);
        } else if (clazz == Client.class) {
            handle.attach(ClientDao.class).deleteById(id);
        } else if (clazz == Principal.class) {
            handle.attach(PrincipalDao.class).deleteById(id);
        } else if (clazz == OAuthClient.class) {
            handle.attach(OAuthClientDao.class).deleteById(id);
        } else if (clazz == ServiceAccount.class) {
            handle.attach(ServiceAccountDao.class).deleteById(id);
        } else if (clazz == AuthRole.class) {
            handle.attach(AuthRoleDao.class).deleteById(id);
        } else if (clazz == AuthPermission.class) {
            handle.attach(AuthPermissionDao.class).deleteById(id);
        } else if (clazz == DispatchPool.class) {
            handle.attach(DispatchPoolDao.class).deleteById(id);
        } else {
            throw new IllegalArgumentException("Unknown aggregate type for delete: " + clazz.getName());
        }
    }

    // ========================================================================
    // Entity-Specific Persist Methods
    // ========================================================================

    private void persistApplication(Handle handle, Application app) {
        ApplicationDao dao = handle.attach(ApplicationDao.class);
        String type = app.type != null ? app.type.name() : "APPLICATION";
        if (dao.findById(app.id).isPresent()) {
            dao.update(app, type);
        } else {
            dao.insert(app, type);
        }
    }

    private void persistApplicationClientConfig(Handle handle, ApplicationClientConfig config) {
        ApplicationClientConfigDao dao = handle.attach(ApplicationClientConfigDao.class);
        // Convert configJson to JSON string
        String configJson = null;
        if (config.configJson != null) {
            try {
                configJson = new com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsString(config.configJson);
            } catch (Exception e) {
                configJson = "{}";
            }
        }
        if (dao.findById(config.id).isPresent()) {
            dao.update(config, configJson);
        } else {
            dao.insert(config, configJson);
        }
    }

    private void persistEventType(Handle handle, EventType eventType) {
        EventTypeDao dao = handle.attach(EventTypeDao.class);
        // Convert specVersions to JSON string (record accessor method)
        String specVersionsJson = null;
        if (eventType.specVersions() != null) {
            try {
                specVersionsJson = new com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsString(eventType.specVersions());
            } catch (Exception e) {
                specVersionsJson = "[]";
            }
        }
        if (dao.findById(eventType.id()).isPresent()) {
            dao.update(eventType, specVersionsJson);
        } else {
            dao.insert(eventType, specVersionsJson);
        }
    }

    private void persistSubscription(Handle handle, Subscription sub) {
        SubscriptionDao dao = handle.attach(SubscriptionDao.class);
        // Convert lists/objects to JSON strings (record accessor methods)
        String eventTypesJson = "[]";
        String customConfigJson = "[]";
        try {
            var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            if (sub.eventTypes() != null) {
                eventTypesJson = mapper.writeValueAsString(sub.eventTypes());
            }
            if (sub.customConfig() != null) {
                customConfigJson = mapper.writeValueAsString(sub.customConfig());
            }
        } catch (Exception ignored) {}

        if (dao.findById(sub.id()).isPresent()) {
            dao.update(sub, eventTypesJson, customConfigJson);
        } else {
            dao.insert(sub, eventTypesJson, customConfigJson);
        }
    }

    private void persistSchema(Handle handle, Schema schema) {
        SchemaDao dao = handle.attach(SchemaDao.class);
        // Record accessor method
        if (dao.findById(schema.id()).isPresent()) {
            dao.update(schema);
        } else {
            dao.insert(schema);
        }
    }

    private void persistClient(Handle handle, Client client) {
        ClientDao dao = handle.attach(ClientDao.class);
        String notesJson = "[]";
        try {
            if (client.notes != null) {
                notesJson = new com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsString(client.notes);
            }
        } catch (Exception ignored) {}

        if (dao.findById(client.id).isPresent()) {
            dao.update(client, notesJson);
        } else {
            dao.insert(client, notesJson);
        }
    }

    private void persistPrincipal(Handle handle, Principal principal) {
        PrincipalDao dao = handle.attach(PrincipalDao.class);
        // Convert embedded objects to JSON
        String userIdentityJson = null;
        String serviceAccountJson = null;
        String rolesJson = "[]";
        try {
            var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            if (principal.userIdentity != null) {
                userIdentityJson = mapper.writeValueAsString(principal.userIdentity);
            }
            if (principal.serviceAccount != null) {
                serviceAccountJson = mapper.writeValueAsString(principal.serviceAccount);
            }
            if (principal.roles != null) {
                rolesJson = mapper.writeValueAsString(principal.roles);
            }
        } catch (Exception ignored) {}

        String type = principal.type != null ? principal.type.name() : "USER";
        String scope = principal.scope != null ? principal.scope.name() : null;

        if (dao.findById(principal.id).isPresent()) {
            dao.update(principal.id, type, scope, principal.clientId, principal.applicationId,
                      principal.name, principal.active, userIdentityJson, serviceAccountJson,
                      rolesJson, principal.updatedAt);
        } else {
            dao.insert(principal.id, type, scope, principal.clientId, principal.applicationId,
                      principal.name, principal.active, userIdentityJson, serviceAccountJson,
                      rolesJson, principal.createdAt, principal.updatedAt);
        }
    }

    private void persistOAuthClient(Handle handle, OAuthClient client) {
        OAuthClientDao dao = handle.attach(OAuthClientDao.class);
        // Convert lists to arrays and enum to string
        String clientType = client.clientType != null ? client.clientType.name() : "PUBLIC";
        String[] redirectUris = client.redirectUris != null
            ? client.redirectUris.toArray(new String[0])
            : new String[0];
        String[] allowedOrigins = client.allowedOrigins != null
            ? client.allowedOrigins.toArray(new String[0])
            : new String[0];
        String[] grantTypes = client.grantTypes != null
            ? client.grantTypes.toArray(new String[0])
            : new String[0];
        String[] applicationIds = client.applicationIds != null
            ? client.applicationIds.toArray(new String[0])
            : new String[0];

        if (dao.findById(client.id).isPresent()) {
            dao.update(client, clientType, redirectUris, allowedOrigins, grantTypes, applicationIds);
        } else {
            dao.insert(client, clientType, redirectUris, allowedOrigins, grantTypes, applicationIds);
        }
    }

    private void persistServiceAccount(Handle handle, ServiceAccount sa) {
        ServiceAccountDao dao = handle.attach(ServiceAccountDao.class);
        // Convert lists to arrays and objects to JSON
        String[] clientIdsArray = sa.clientIds != null
            ? sa.clientIds.toArray(new String[0])
            : new String[0];
        String rolesJson = "[]";
        String credentialsJson = null;
        try {
            var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            if (sa.roles != null) {
                rolesJson = mapper.writeValueAsString(sa.roles);
            }
            if (sa.webhookCredentials != null) {
                credentialsJson = mapper.writeValueAsString(sa.webhookCredentials);
            }
        } catch (Exception ignored) {}

        if (dao.findById(sa.id).isPresent()) {
            dao.update(sa, clientIdsArray, credentialsJson, rolesJson);
        } else {
            dao.insert(sa, clientIdsArray, credentialsJson, rolesJson);
        }
    }

    private void persistAuthRole(Handle handle, AuthRole role) {
        AuthRoleDao dao = handle.attach(AuthRoleDao.class);
        // Convert permissions Set to array and source to string
        String[] permissionsArray = role.permissions != null
            ? role.permissions.toArray(new String[0])
            : new String[0];
        String sourceStr = role.source != null ? role.source.name() : null;

        if (dao.findById(role.id).isPresent()) {
            dao.update(role, permissionsArray, sourceStr);
        } else {
            dao.insert(role, permissionsArray, sourceStr);
        }
    }

    private void persistAuthPermission(Handle handle, AuthPermission perm) {
        AuthPermissionDao dao = handle.attach(AuthPermissionDao.class);
        // Convert source to string
        String sourceStr = perm.source != null ? perm.source.name() : null;

        if (dao.findById(perm.id).isPresent()) {
            dao.update(perm, sourceStr);
        } else {
            dao.insert(perm, sourceStr);
        }
    }

    private void persistDispatchPool(Handle handle, DispatchPool pool) {
        DispatchPoolDao dao = handle.attach(DispatchPoolDao.class);
        // Record accessor method
        if (dao.findById(pool.id()).isPresent()) {
            dao.update(pool);
        } else {
            dao.insert(pool);
        }
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    private void setTimestamps(Object aggregate) {
        // Records are immutable - timestamps should be set by the use case before passing here
        if (aggregate.getClass().isRecord()) {
            return;
        }

        try {
            Field updatedAtField = aggregate.getClass().getField("updatedAt");
            if (updatedAtField.getType() == Instant.class) {
                updatedAtField.set(aggregate, Instant.now());
            }
        } catch (NoSuchFieldException | IllegalAccessException ignored) {
            // Not all aggregates have updatedAt
        }

        try {
            Field createdAtField = aggregate.getClass().getField("createdAt");
            if (createdAtField.getType() == Instant.class && createdAtField.get(aggregate) == null) {
                createdAtField.set(aggregate, Instant.now());
            }
        } catch (NoSuchFieldException | IllegalAccessException ignored) {
            // Not all aggregates have createdAt
        }
    }

    private String extractId(Object aggregate) {
        Class<?> clazz = aggregate.getClass();

        // For records, use accessor method id()
        if (clazz.isRecord()) {
            try {
                var method = clazz.getMethod("id");
                return (String) method.invoke(aggregate);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "Record must have an id() accessor returning String: " + clazz.getName(),
                    e
                );
            }
        }

        // For regular classes, use public field
        try {
            Field field = clazz.getField("id");
            return (String) field.get(aggregate);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalArgumentException(
                "Aggregate must have a public String id field: " + clazz.getName(),
                e
            );
        }
    }
}
