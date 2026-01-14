package tech.flowcatalyst.platform.shared.jdbi;

import org.jdbi.v3.core.mapper.ColumnMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;

/**
 * JDBI column mapper for java.time.Instant.
 * Handles PostgreSQL TIMESTAMPTZ columns correctly.
 */
public class InstantColumnMapper implements ColumnMapper<Instant> {

    @Override
    public Instant map(ResultSet rs, int columnNumber, StatementContext ctx) throws SQLException {
        // Try to get as OffsetDateTime first (best for TIMESTAMPTZ)
        Object obj = rs.getObject(columnNumber);
        if (obj == null) {
            return null;
        }

        if (obj instanceof OffsetDateTime odt) {
            return odt.toInstant();
        }

        if (obj instanceof Timestamp ts) {
            return ts.toInstant();
        }

        if (obj instanceof Instant instant) {
            return instant;
        }

        // Fallback: try to get as Timestamp
        Timestamp timestamp = rs.getTimestamp(columnNumber);
        return timestamp != null ? timestamp.toInstant() : null;
    }
}
