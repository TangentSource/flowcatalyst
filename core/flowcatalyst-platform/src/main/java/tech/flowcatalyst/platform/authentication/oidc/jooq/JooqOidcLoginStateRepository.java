package tech.flowcatalyst.platform.authentication.oidc.jooq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.authentication.oidc.OidcLoginState;
import tech.flowcatalyst.platform.authentication.oidc.OidcLoginStateRepository;
import tech.flowcatalyst.platform.jooq.generated.tables.records.OidcLoginStatesRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static tech.flowcatalyst.platform.jooq.generated.tables.OidcLoginStates.OIDC_LOGIN_STATES;

/**
 * JOOQ-based implementation of OidcLoginStateRepository.
 */
@ApplicationScoped
public class JooqOidcLoginStateRepository implements OidcLoginStateRepository {

    @Inject
    DSLContext dsl;

    @Override
    public Optional<OidcLoginState> findValidState(String state) {
        return Optional.ofNullable(
            dsl.selectFrom(OIDC_LOGIN_STATES)
                .where(OIDC_LOGIN_STATES.STATE.eq(state))
                .and(OIDC_LOGIN_STATES.EXPIRES_AT.gt(OffsetDateTime.now(ZoneOffset.UTC)))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public void persist(OidcLoginState state) {
        OidcLoginStatesRecord record = toRecord(state);
        record.setCreatedAt(toOffsetDateTime(state.createdAt));
        dsl.insertInto(OIDC_LOGIN_STATES).set(record).execute();
    }

    @Override
    public void deleteByState(String state) {
        dsl.deleteFrom(OIDC_LOGIN_STATES)
            .where(OIDC_LOGIN_STATES.STATE.eq(state))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private OidcLoginState toDomain(Record record) {
        if (record == null) return null;

        OidcLoginState s = new OidcLoginState();
        s.state = record.get(OIDC_LOGIN_STATES.STATE);
        s.emailDomain = record.get(OIDC_LOGIN_STATES.EMAIL_DOMAIN);
        s.authConfigId = record.get(OIDC_LOGIN_STATES.AUTH_CONFIG_ID);
        s.nonce = record.get(OIDC_LOGIN_STATES.NONCE);
        s.codeVerifier = record.get(OIDC_LOGIN_STATES.CODE_VERIFIER);
        s.returnUrl = record.get(OIDC_LOGIN_STATES.RETURN_URL);
        s.oauthClientId = record.get(OIDC_LOGIN_STATES.OAUTH_CLIENT_ID);
        s.oauthRedirectUri = record.get(OIDC_LOGIN_STATES.OAUTH_REDIRECT_URI);
        s.oauthScope = record.get(OIDC_LOGIN_STATES.OAUTH_SCOPE);
        s.oauthState = record.get(OIDC_LOGIN_STATES.OAUTH_STATE);
        s.oauthNonce = record.get(OIDC_LOGIN_STATES.OAUTH_NONCE);
        s.oauthCodeChallenge = record.get(OIDC_LOGIN_STATES.OAUTH_CODE_CHALLENGE);
        s.oauthCodeChallengeMethod = record.get(OIDC_LOGIN_STATES.OAUTH_CODE_CHALLENGE_METHOD);
        s.createdAt = toInstant(record.get(OIDC_LOGIN_STATES.CREATED_AT));
        s.expiresAt = toInstant(record.get(OIDC_LOGIN_STATES.EXPIRES_AT));
        return s;
    }

    private OidcLoginStatesRecord toRecord(OidcLoginState s) {
        OidcLoginStatesRecord rec = new OidcLoginStatesRecord();
        rec.setState(s.state);
        rec.setEmailDomain(s.emailDomain);
        rec.setAuthConfigId(s.authConfigId);
        rec.setNonce(s.nonce);
        rec.setCodeVerifier(s.codeVerifier);
        rec.setReturnUrl(s.returnUrl);
        rec.setOauthClientId(s.oauthClientId);
        rec.setOauthRedirectUri(s.oauthRedirectUri);
        rec.setOauthScope(s.oauthScope);
        rec.setOauthState(s.oauthState);
        rec.setOauthNonce(s.oauthNonce);
        rec.setOauthCodeChallenge(s.oauthCodeChallenge);
        rec.setOauthCodeChallengeMethod(s.oauthCodeChallengeMethod);
        rec.setExpiresAt(toOffsetDateTime(s.expiresAt));
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
