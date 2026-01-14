package tech.flowcatalyst.platform.principal;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * JDBI row mapper for AnchorDomain entity.
 */
public class AnchorDomainRowMapper implements RowMapper<AnchorDomain> {

    @Override
    public AnchorDomain map(ResultSet rs, StatementContext ctx) throws SQLException {
        AnchorDomain ad = new AnchorDomain();
        ad.id = rs.getString("id");
        ad.domain = rs.getString("domain");

        Timestamp createdAt = rs.getTimestamp("created_at");
        ad.createdAt = createdAt != null ? createdAt.toInstant() : null;

        return ad;
    }
}
