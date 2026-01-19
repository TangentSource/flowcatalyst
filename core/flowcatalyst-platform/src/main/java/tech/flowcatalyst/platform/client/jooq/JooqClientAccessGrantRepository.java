package tech.flowcatalyst.platform.client.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.client.ClientAccessGrant;
import tech.flowcatalyst.platform.client.ClientAccessGrantRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.ClientAccessGrantsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.ClientAccessGrants.CLIENT_ACCESS_GRANTS;

/**
 * JOOQ-based implementation of ClientAccessGrantRepository.
 */
@ApplicationScoped
public class JooqClientAccessGrantRepository implements ClientAccessGrantRepository {

    @Inject
    DSLContext dsl;

    @Override
    public List<ClientAccessGrant> findByPrincipalId(String principalId) {
        return dsl.selectFrom(CLIENT_ACCESS_GRANTS)
            .where(CLIENT_ACCESS_GRANTS.PRINCIPAL_ID.eq(principalId))
            .fetch(this::toDomain);
    }

    @Override
    public List<ClientAccessGrant> findByClientId(String clientId) {
        return dsl.selectFrom(CLIENT_ACCESS_GRANTS)
            .where(CLIENT_ACCESS_GRANTS.CLIENT_ID.eq(clientId))
            .fetch(this::toDomain);
    }

    @Override
    public Optional<ClientAccessGrant> findByPrincipalIdAndClientId(String principalId, String clientId) {
        return Optional.ofNullable(
            dsl.selectFrom(CLIENT_ACCESS_GRANTS)
                .where(CLIENT_ACCESS_GRANTS.PRINCIPAL_ID.eq(principalId))
                .and(CLIENT_ACCESS_GRANTS.CLIENT_ID.eq(clientId))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public boolean existsByPrincipalIdAndClientId(String principalId, String clientId) {
        return dsl.fetchExists(
            dsl.selectFrom(CLIENT_ACCESS_GRANTS)
                .where(CLIENT_ACCESS_GRANTS.PRINCIPAL_ID.eq(principalId))
                .and(CLIENT_ACCESS_GRANTS.CLIENT_ID.eq(clientId))
        );
    }

    @Override
    public void persist(ClientAccessGrant grant) {
        ClientAccessGrantsRecord record = toRecord(grant);
        record.setGrantedAt(toOffsetDateTime(grant.grantedAt));
        dsl.insertInto(CLIENT_ACCESS_GRANTS).set(record).execute();
    }

    @Override
    public void delete(ClientAccessGrant grant) {
        dsl.deleteFrom(CLIENT_ACCESS_GRANTS)
            .where(CLIENT_ACCESS_GRANTS.ID.eq(grant.id))
            .execute();
    }

    @Override
    public void deleteByPrincipalId(String principalId) {
        dsl.deleteFrom(CLIENT_ACCESS_GRANTS)
            .where(CLIENT_ACCESS_GRANTS.PRINCIPAL_ID.eq(principalId))
            .execute();
    }

    @Override
    public long deleteByPrincipalIdAndClientId(String principalId, String clientId) {
        return dsl.deleteFrom(CLIENT_ACCESS_GRANTS)
            .where(CLIENT_ACCESS_GRANTS.PRINCIPAL_ID.eq(principalId))
            .and(CLIENT_ACCESS_GRANTS.CLIENT_ID.eq(clientId))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private ClientAccessGrant toDomain(Record record) {
        if (record == null) return null;

        ClientAccessGrant g = new ClientAccessGrant();
        g.id = record.get(CLIENT_ACCESS_GRANTS.ID);
        g.principalId = record.get(CLIENT_ACCESS_GRANTS.PRINCIPAL_ID);
        g.clientId = record.get(CLIENT_ACCESS_GRANTS.CLIENT_ID);
        g.grantedAt = toInstant(record.get(CLIENT_ACCESS_GRANTS.GRANTED_AT));
        g.expiresAt = toInstant(record.get(CLIENT_ACCESS_GRANTS.EXPIRES_AT));
        return g;
    }

    private ClientAccessGrantsRecord toRecord(ClientAccessGrant g) {
        ClientAccessGrantsRecord rec = new ClientAccessGrantsRecord();
        rec.setId(g.id);
        rec.setPrincipalId(g.principalId);
        rec.setClientId(g.clientId);
        rec.setExpiresAt(toOffsetDateTime(g.expiresAt));
        return rec;
    }

    // ========================================================================
    // Utility Methods
    // ========================================================================

    private Instant toInstant(OffsetDateTime odt) {
        return odt != null ? odt.toInstant() : null;
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant != null ? instant.atOffset(ZoneOffset.UTC) : null;
    }
}
