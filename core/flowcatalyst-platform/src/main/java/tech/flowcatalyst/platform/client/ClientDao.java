package tech.flowcatalyst.platform.client;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * JDBI DAO interface for Client entity.
 */
@RegisterRowMapper(ClientRowMapper.class)
public interface ClientDao {

    @SqlQuery("SELECT * FROM clients WHERE id = :id")
    Optional<Client> findById(@Bind("id") String id);

    @SqlQuery("SELECT * FROM clients WHERE identifier = :identifier")
    Optional<Client> findByIdentifier(@Bind("identifier") String identifier);

    @SqlQuery("SELECT * FROM clients WHERE status = :status")
    List<Client> findByStatus(@Bind("status") String status);

    @SqlQuery("SELECT * FROM clients ORDER BY name")
    List<Client> listAll();

    @SqlQuery("SELECT * FROM clients WHERE status = 'ACTIVE' ORDER BY name")
    List<Client> findAllActive();

    @SqlQuery("SELECT COUNT(*) FROM clients")
    long count();

    @SqlUpdate("""
        INSERT INTO clients (id, name, identifier, status, status_reason, status_changed_at,
                            notes, created_at, updated_at)
        VALUES (:id, :name, :identifier, :status, :statusReason, :statusChangedAt,
                :notes::jsonb, :createdAt, :updatedAt)
        """)
    void insert(@BindBean Client client, @Bind("notes") String notesJson);

    @SqlUpdate("""
        UPDATE clients SET name = :name, identifier = :identifier, status = :status,
               status_reason = :statusReason, status_changed_at = :statusChangedAt,
               notes = :notes::jsonb, updated_at = :updatedAt
        WHERE id = :id
        """)
    void update(@BindBean Client client, @Bind("notes") String notesJson);

    @SqlUpdate("DELETE FROM clients WHERE id = :id")
    int deleteById(@Bind("id") String id);
}
