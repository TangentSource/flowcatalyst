package tech.flowcatalyst.subscription;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of SubscriptionRepository using JDBI.
 */
@ApplicationScoped
@Typed(SubscriptionRepository.class)
class PostgresSubscriptionRepository implements SubscriptionRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Subscription findById(String id) {
        return jdbi.withExtension(SubscriptionDao.class, dao -> dao.findById(id).orElse(null));
    }

    @Override
    public Optional<Subscription> findByIdOptional(String id) {
        return jdbi.withExtension(SubscriptionDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<Subscription> findByCodeAndClient(String code, String clientId) {
        return jdbi.withExtension(SubscriptionDao.class, dao -> dao.findByCodeAndClient(code, clientId));
    }

    @Override
    public List<Subscription> findByClientId(String clientId) {
        return jdbi.withExtension(SubscriptionDao.class, dao -> dao.findByClientId(clientId));
    }

    @Override
    public List<Subscription> findAnchorLevel() {
        return jdbi.withExtension(SubscriptionDao.class, SubscriptionDao::findAnchorLevel);
    }

    @Override
    public List<Subscription> findByDispatchPoolId(String dispatchPoolId) {
        return jdbi.withExtension(SubscriptionDao.class, dao -> dao.findByDispatchPoolId(dispatchPoolId));
    }

    @Override
    public List<Subscription> findByEventTypeId(String eventTypeId) {
        // Search in the JSONB event_types array for matching eventTypeId
        return jdbi.withHandle(handle ->
            handle.createQuery("""
                SELECT * FROM subscriptions
                WHERE EXISTS (
                    SELECT 1 FROM jsonb_array_elements(event_types) elem
                    WHERE elem->>'eventTypeId' = :eventTypeId
                )
                ORDER BY name
                """)
                .bind("eventTypeId", eventTypeId)
                .registerRowMapper(new SubscriptionRowMapper())
                .mapTo(Subscription.class)
                .list()
        );
    }

    @Override
    public List<Subscription> findByStatus(SubscriptionStatus status) {
        return jdbi.withExtension(SubscriptionDao.class, dao -> dao.findByStatus(status.name()));
    }

    @Override
    public List<Subscription> findActive() {
        return jdbi.withExtension(SubscriptionDao.class, SubscriptionDao::findActive);
    }

    @Override
    public List<Subscription> findWithFilters(String clientId, SubscriptionStatus status, SubscriptionSource source, String dispatchPoolId) {
        StringBuilder sql = new StringBuilder("SELECT * FROM subscriptions WHERE 1=1");

        if (clientId != null) {
            sql.append(" AND client_id = :clientId");
        }
        if (status != null) {
            sql.append(" AND status = :status");
        }
        if (source != null) {
            sql.append(" AND source = :source");
        }
        if (dispatchPoolId != null) {
            sql.append(" AND dispatch_pool_id = :dispatchPoolId");
        }
        sql.append(" ORDER BY name");

        String finalSql = sql.toString();
        return jdbi.withHandle(handle -> {
            var query = handle.createQuery(finalSql)
                .registerRowMapper(new SubscriptionRowMapper());

            if (clientId != null) {
                query.bind("clientId", clientId);
            }
            if (status != null) {
                query.bind("status", status.name());
            }
            if (source != null) {
                query.bind("source", source.name());
            }
            if (dispatchPoolId != null) {
                query.bind("dispatchPoolId", dispatchPoolId);
            }

            return query.mapTo(Subscription.class).list();
        });
    }

    @Override
    public List<Subscription> findActiveByEventTypeAndClient(String eventTypeId, String clientId) {
        return jdbi.withHandle(handle ->
            handle.createQuery("""
                SELECT * FROM subscriptions
                WHERE status = 'ACTIVE'
                AND (client_id = :clientId OR client_id IS NULL)
                AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(event_types) elem
                    WHERE elem->>'eventTypeId' = :eventTypeId
                )
                ORDER BY name
                """)
                .bind("eventTypeId", eventTypeId)
                .bind("clientId", clientId)
                .registerRowMapper(new SubscriptionRowMapper())
                .mapTo(Subscription.class)
                .list()
        );
    }

    @Override
    public List<Subscription> findActiveByEventTypeCodeAndClient(String eventTypeCode, String clientId) {
        return jdbi.withHandle(handle ->
            handle.createQuery("""
                SELECT * FROM subscriptions
                WHERE status = 'ACTIVE'
                AND (client_id = :clientId OR client_id IS NULL)
                AND EXISTS (
                    SELECT 1 FROM jsonb_array_elements(event_types) elem
                    WHERE elem->>'eventTypeCode' = :eventTypeCode
                )
                ORDER BY name
                """)
                .bind("eventTypeCode", eventTypeCode)
                .bind("clientId", clientId)
                .registerRowMapper(new SubscriptionRowMapper())
                .mapTo(Subscription.class)
                .list()
        );
    }

    @Override
    public List<Subscription> listAll() {
        return jdbi.withExtension(SubscriptionDao.class, SubscriptionDao::listAll);
    }

    @Override
    public long count() {
        return jdbi.withExtension(SubscriptionDao.class, SubscriptionDao::count);
    }

    @Override
    public boolean existsByCodeAndClient(String code, String clientId) {
        return jdbi.withExtension(SubscriptionDao.class, dao -> dao.existsByCodeAndClient(code, clientId));
    }

    @Override
    public boolean existsByDispatchPoolId(String dispatchPoolId) {
        return jdbi.withExtension(SubscriptionDao.class, dao -> dao.existsByDispatchPoolId(dispatchPoolId));
    }

    @Override
    public void persist(Subscription subscription) {
        jdbi.useExtension(SubscriptionDao.class, dao ->
            dao.insert(subscription,
                JsonHelper.toJsonArray(subscription.eventTypes()),
                JsonHelper.toJsonArray(subscription.customConfig())));
    }

    @Override
    public void update(Subscription subscription) {
        Subscription updated = subscription.toBuilder()
            .updatedAt(Instant.now())
            .build();
        jdbi.useExtension(SubscriptionDao.class, dao ->
            dao.update(updated,
                JsonHelper.toJsonArray(updated.eventTypes()),
                JsonHelper.toJsonArray(updated.customConfig())));
    }

    @Override
    public void delete(Subscription subscription) {
        jdbi.useExtension(SubscriptionDao.class, dao -> dao.deleteById(subscription.id()));
    }

    @Override
    public boolean deleteById(String id) {
        return jdbi.withExtension(SubscriptionDao.class, dao -> dao.deleteById(id) > 0);
    }
}
