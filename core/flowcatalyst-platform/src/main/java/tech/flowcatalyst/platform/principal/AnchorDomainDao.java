package tech.flowcatalyst.platform.principal;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindFields;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for AnchorDomain entity.
 */
@RegisterRowMapper(AnchorDomainRowMapper.class)
public interface AnchorDomainDao {

    @SqlQuery("SELECT * FROM anchor_domains WHERE id = :id")
    Optional<AnchorDomain> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM anchor_domains WHERE domain = :domain")
    Optional<AnchorDomain> findByDomain(@Bind("domain") String domain);

    @SqlQuery("SELECT COUNT(*) > 0 FROM anchor_domains WHERE domain = :domain")
    boolean existsByDomain(@Bind("domain") String domain);

    @SqlQuery("SELECT * FROM anchor_domains ORDER BY domain")
    List<AnchorDomain> listAll();

    @SqlUpdate("""
        INSERT INTO anchor_domains (id, domain, created_at)
        VALUES (:id, :domain, :createdAt)
        """)
    void insert(@BindFields AnchorDomain anchorDomain);

    @SqlUpdate("DELETE FROM anchor_domains WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
