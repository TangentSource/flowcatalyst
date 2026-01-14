package tech.flowcatalyst.platform.shared.jdbi;

import org.jdbi.v3.core.argument.Argument;
import org.jdbi.v3.core.argument.ArgumentFactory;
import org.jdbi.v3.core.config.ConfigRegistry;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Optional;

/**
 * JDBI argument factory for java.time.Instant.
 * Converts Instant to TIMESTAMPTZ for PostgreSQL.
 */
public class InstantArgumentFactory implements ArgumentFactory {

    @Override
    public Optional<Argument> build(Type type, Object value, ConfigRegistry config) {
        if (type != Instant.class) {
            return Optional.empty();
        }

        return Optional.of((position, statement, ctx) -> {
            if (value == null) {
                statement.setNull(position, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                Instant instant = (Instant) value;
                statement.setTimestamp(position, Timestamp.from(instant));
            }
        });
    }
}
