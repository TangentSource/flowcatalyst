package tech.flowcatalyst.platform.authentication.oauth.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.authentication.oauth.AuthorizationCode;
import tech.flowcatalyst.platform.authentication.oauth.AuthorizationCodeRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.AuthorizationCodesRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.AuthorizationCodes.AUTHORIZATION_CODES;

/**
 * JOOQ-based implementation of AuthorizationCodeRepository.
 */
@ApplicationScoped
public class JooqAuthorizationCodeRepository implements AuthorizationCodeRepository {

    @Inject
    DSLContext dsl;

    @Override
    public Optional<AuthorizationCode> findValidCode(String code) {
        return Optional.ofNullable(
            dsl.selectFrom(AUTHORIZATION_CODES)
                .where(AUTHORIZATION_CODES.CODE.eq(code))
                .and(AUTHORIZATION_CODES.USED.eq(false))
                .and(AUTHORIZATION_CODES.EXPIRES_AT.gt(OffsetDateTime.now(ZoneOffset.UTC)))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public void persist(AuthorizationCode code) {
        AuthorizationCodesRecord record = toRecord(code);
        record.setCreatedAt(toOffsetDateTime(code.createdAt));
        dsl.insertInto(AUTHORIZATION_CODES).set(record).execute();
    }

    @Override
    public void markAsUsed(String code) {
        dsl.update(AUTHORIZATION_CODES)
            .set(AUTHORIZATION_CODES.USED, true)
            .where(AUTHORIZATION_CODES.CODE.eq(code))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private AuthorizationCode toDomain(Record record) {
        if (record == null) return null;

        AuthorizationCode c = new AuthorizationCode();
        c.id = record.get(AUTHORIZATION_CODES.ID);
        c.code = record.get(AUTHORIZATION_CODES.CODE);
        c.clientId = record.get(AUTHORIZATION_CODES.CLIENT_ID);
        c.principalId = record.get(AUTHORIZATION_CODES.PRINCIPAL_ID);
        c.redirectUri = record.get(AUTHORIZATION_CODES.REDIRECT_URI);
        c.scope = record.get(AUTHORIZATION_CODES.SCOPE);
        c.codeChallenge = record.get(AUTHORIZATION_CODES.CODE_CHALLENGE);
        c.codeChallengeMethod = record.get(AUTHORIZATION_CODES.CODE_CHALLENGE_METHOD);
        c.nonce = record.get(AUTHORIZATION_CODES.NONCE);
        c.state = record.get(AUTHORIZATION_CODES.STATE);
        c.contextClientId = record.get(AUTHORIZATION_CODES.CONTEXT_CLIENT_ID);
        c.used = record.get(AUTHORIZATION_CODES.USED);
        c.createdAt = toInstant(record.get(AUTHORIZATION_CODES.CREATED_AT));
        c.expiresAt = toInstant(record.get(AUTHORIZATION_CODES.EXPIRES_AT));
        return c;
    }

    private AuthorizationCodesRecord toRecord(AuthorizationCode c) {
        AuthorizationCodesRecord rec = new AuthorizationCodesRecord();
        rec.setId(c.id);
        rec.setCode(c.code);
        rec.setClientId(c.clientId);
        rec.setPrincipalId(c.principalId);
        rec.setRedirectUri(c.redirectUri);
        rec.setScope(c.scope);
        rec.setCodeChallenge(c.codeChallenge);
        rec.setCodeChallengeMethod(c.codeChallengeMethod);
        rec.setNonce(c.nonce);
        rec.setState(c.state);
        rec.setContextClientId(c.contextClientId);
        rec.setUsed(c.used);
        rec.setExpiresAt(toOffsetDateTime(c.expiresAt));
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
