package tech.flowcatalyst.subscription;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import tech.flowcatalyst.dispatch.DispatchMode;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * JDBI row mapper for Subscription entity.
 */
public class SubscriptionRowMapper implements RowMapper<Subscription> {

    @Override
    public Subscription map(ResultSet rs, StatementContext ctx) throws SQLException {
        String sourceStr = rs.getString("source");
        SubscriptionSource source = sourceStr != null ? SubscriptionSource.valueOf(sourceStr) : SubscriptionSource.API;

        String statusStr = rs.getString("status");
        SubscriptionStatus status = statusStr != null ? SubscriptionStatus.valueOf(statusStr) : SubscriptionStatus.ACTIVE;

        String modeStr = rs.getString("mode");
        DispatchMode mode = modeStr != null ? DispatchMode.valueOf(modeStr) : DispatchMode.IMMEDIATE;

        String eventTypesJson = rs.getString("event_types");
        var eventTypes = JsonHelper.fromJsonList(eventTypesJson, EventTypeBinding.class);

        String customConfigJson = rs.getString("custom_config");
        var customConfig = JsonHelper.fromJsonList(customConfigJson, ConfigEntry.class);

        Timestamp createdAt = rs.getTimestamp("created_at");
        Timestamp updatedAt = rs.getTimestamp("updated_at");

        return Subscription.builder()
            .id(rs.getString("id"))
            .code(rs.getString("code"))
            .name(rs.getString("name"))
            .description(rs.getString("description"))
            .clientId(rs.getString("client_id"))
            .clientIdentifier(rs.getString("client_identifier"))
            .eventTypes(eventTypes != null ? eventTypes : new ArrayList<>())
            .target(rs.getString("target"))
            .queue(rs.getString("queue"))
            .customConfig(customConfig != null ? customConfig : new ArrayList<>())
            .source(source)
            .status(status)
            .maxAgeSeconds(rs.getInt("max_age_seconds"))
            .dispatchPoolId(rs.getString("dispatch_pool_id"))
            .dispatchPoolCode(rs.getString("dispatch_pool_code"))
            .delaySeconds(rs.getInt("delay_seconds"))
            .sequence(rs.getInt("sequence"))
            .mode(mode)
            .timeoutSeconds(rs.getInt("timeout_seconds"))
            .maxRetries(rs.getInt("max_retries"))
            .serviceAccountId(rs.getString("service_account_id"))
            .dataOnly(rs.getBoolean("data_only"))
            .createdAt(createdAt != null ? createdAt.toInstant() : null)
            .updatedAt(updatedAt != null ? updatedAt.toInstant() : null)
            .build();
    }
}
