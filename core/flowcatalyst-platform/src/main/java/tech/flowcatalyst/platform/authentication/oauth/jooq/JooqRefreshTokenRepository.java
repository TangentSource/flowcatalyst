package tech.flowcatalyst.platform.authentication.oauth.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.authentication.oauth.RefreshToken;
import tech.flowcatalyst.platform.authentication.oauth.RefreshTokenRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.RefreshTokensRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.RefreshTokens.REFRESH_TOKENS;

/**
 * JOOQ-based implementation of RefreshTokenRepository.
 */
@ApplicationScoped
public class JooqRefreshTokenRepository implements RefreshTokenRepository {

    @Inject
    DSLContext dsl;

    @Override
    public Optional<RefreshToken> findValidToken(String tokenHash) {
        return Optional.ofNullable(
            dsl.selectFrom(REFRESH_TOKENS)
                .where(REFRESH_TOKENS.TOKEN_HASH.eq(tokenHash))
                .and(REFRESH_TOKENS.REVOKED.eq(false))
                .and(REFRESH_TOKENS.EXPIRES_AT.gt(OffsetDateTime.now(ZoneOffset.UTC)))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public Optional<RefreshToken> findByTokenHash(String tokenHash) {
        return Optional.ofNullable(
            dsl.selectFrom(REFRESH_TOKENS)
                .where(REFRESH_TOKENS.TOKEN_HASH.eq(tokenHash))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public void persist(RefreshToken token) {
        RefreshTokensRecord record = toRecord(token);
        record.setCreatedAt(toOffsetDateTime(token.createdAt));
        dsl.insertInto(REFRESH_TOKENS).set(record).execute();
    }

    @Override
    public void update(RefreshToken token) {
        RefreshTokensRecord record = toRecord(token);
        dsl.update(REFRESH_TOKENS)
            .set(record)
            .where(REFRESH_TOKENS.TOKEN_HASH.eq(token.tokenHash))
            .execute();
    }

    @Override
    public void revokeToken(String tokenHash, String replacedBy) {
        dsl.update(REFRESH_TOKENS)
            .set(REFRESH_TOKENS.REVOKED, true)
            .set(REFRESH_TOKENS.REVOKED_AT, OffsetDateTime.now(ZoneOffset.UTC))
            .set(REFRESH_TOKENS.REPLACED_BY, replacedBy)
            .where(REFRESH_TOKENS.TOKEN_HASH.eq(tokenHash))
            .execute();
    }

    @Override
    public void revokeTokenFamily(String tokenFamily) {
        dsl.update(REFRESH_TOKENS)
            .set(REFRESH_TOKENS.REVOKED, true)
            .set(REFRESH_TOKENS.REVOKED_AT, OffsetDateTime.now(ZoneOffset.UTC))
            .where(REFRESH_TOKENS.TOKEN_FAMILY.eq(tokenFamily))
            .and(REFRESH_TOKENS.REVOKED.eq(false))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private RefreshToken toDomain(Record record) {
        if (record == null) return null;

        RefreshToken t = new RefreshToken();
        t.tokenHash = record.get(REFRESH_TOKENS.TOKEN_HASH);
        t.principalId = record.get(REFRESH_TOKENS.PRINCIPAL_ID);
        t.clientId = record.get(REFRESH_TOKENS.CLIENT_ID);
        t.contextClientId = record.get(REFRESH_TOKENS.CONTEXT_CLIENT_ID);
        t.scope = record.get(REFRESH_TOKENS.SCOPE);
        t.tokenFamily = record.get(REFRESH_TOKENS.TOKEN_FAMILY);
        t.revoked = record.get(REFRESH_TOKENS.REVOKED);
        t.revokedAt = toInstant(record.get(REFRESH_TOKENS.REVOKED_AT));
        t.replacedBy = record.get(REFRESH_TOKENS.REPLACED_BY);
        t.createdAt = toInstant(record.get(REFRESH_TOKENS.CREATED_AT));
        t.expiresAt = toInstant(record.get(REFRESH_TOKENS.EXPIRES_AT));
        return t;
    }

    private RefreshTokensRecord toRecord(RefreshToken t) {
        RefreshTokensRecord rec = new RefreshTokensRecord();
        rec.setTokenHash(t.tokenHash);
        rec.setPrincipalId(t.principalId);
        rec.setClientId(t.clientId);
        rec.setContextClientId(t.contextClientId);
        rec.setScope(t.scope);
        rec.setTokenFamily(t.tokenFamily);
        rec.setRevoked(t.revoked);
        rec.setRevokedAt(toOffsetDateTime(t.revokedAt));
        rec.setReplacedBy(t.replacedBy);
        rec.setExpiresAt(toOffsetDateTime(t.expiresAt));
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
