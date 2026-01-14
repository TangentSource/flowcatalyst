package tech.flowcatalyst.subscription;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for Subscription entity.
 */
@RegisterRowMapper(SubscriptionRowMapper.class)
public interface SubscriptionDao {

    @SqlQuery("SELECT * FROM subscriptions WHERE id = :id")
    Optional<Subscription> findById(@Bind("id") String id);

    @SqlQuery("""
        SELECT * FROM subscriptions
        WHERE code = :code AND (client_id = :clientId OR (client_id IS NULL AND :clientId IS NULL))
        """)
    Optional<Subscription> findByCodeAndClient(@Bind("code") String code, @Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM subscriptions WHERE client_id = :clientId ORDER BY name")
    List<Subscription> findByClientId(@Bind("clientId") String clientId);

    @SqlQuery("SELECT * FROM subscriptions WHERE client_id IS NULL ORDER BY name")
    List<Subscription> findAnchorLevel();

    @SqlQuery("SELECT * FROM subscriptions WHERE dispatch_pool_id = :dispatchPoolId ORDER BY name")
    List<Subscription> findByDispatchPoolId(@Bind("dispatchPoolId") String dispatchPoolId);

    @SqlQuery("SELECT * FROM subscriptions WHERE status = :status ORDER BY name")
    List<Subscription> findByStatus(@Bind("status") String status);

    @SqlQuery("SELECT * FROM subscriptions WHERE status = 'ACTIVE' ORDER BY name")
    List<Subscription> findActive();

    @SqlQuery("SELECT * FROM subscriptions ORDER BY name")
    List<Subscription> listAll();

    @SqlQuery("SELECT COUNT(*) FROM subscriptions")
    long count();

    @SqlQuery("""
        SELECT EXISTS(
            SELECT 1 FROM subscriptions
            WHERE code = :code AND (client_id = :clientId OR (client_id IS NULL AND :clientId IS NULL))
        )
        """)
    boolean existsByCodeAndClient(@Bind("code") String code, @Bind("clientId") String clientId);

    @SqlQuery("SELECT EXISTS(SELECT 1 FROM subscriptions WHERE dispatch_pool_id = :dispatchPoolId)")
    boolean existsByDispatchPoolId(@Bind("dispatchPoolId") String dispatchPoolId);

    @SqlUpdate("""
        INSERT INTO subscriptions (id, code, name, description, client_id, client_identifier, event_types,
                                  target, queue, custom_config, source, status, max_age_seconds,
                                  dispatch_pool_id, dispatch_pool_code, delay_seconds, sequence, mode,
                                  timeout_seconds, max_retries, service_account_id, data_only,
                                  created_at, updated_at)
        VALUES (:id, :code, :name, :description, :clientId, :clientIdentifier, :eventTypes::jsonb,
                :target, :queue, :customConfig::jsonb, :source, :status, :maxAgeSeconds,
                :dispatchPoolId, :dispatchPoolCode, :delaySeconds, :sequence, :mode,
                :timeoutSeconds, :maxRetries, :serviceAccountId, :dataOnly,
                :createdAt, :updatedAt)
        """)
    void insert(@BindFields Subscription subscription,
                @Bind("eventTypes") String eventTypesJson,
                @Bind("customConfig") String customConfigJson);

    @SqlUpdate("""
        UPDATE subscriptions SET code = :code, name = :name, description = :description,
               client_id = :clientId, client_identifier = :clientIdentifier, event_types = :eventTypes::jsonb,
               target = :target, queue = :queue, custom_config = :customConfig::jsonb, source = :source,
               status = :status, max_age_seconds = :maxAgeSeconds, dispatch_pool_id = :dispatchPoolId,
               dispatch_pool_code = :dispatchPoolCode, delay_seconds = :delaySeconds, sequence = :sequence,
               mode = :mode, timeout_seconds = :timeoutSeconds, max_retries = :maxRetries,
               service_account_id = :serviceAccountId, data_only = :dataOnly, updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindFields Subscription subscription,
                @Bind("eventTypes") String eventTypesJson,
                @Bind("customConfig") String customConfigJson);

    @SqlUpdate("DELETE FROM subscriptions WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
