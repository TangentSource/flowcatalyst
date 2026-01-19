package tech.flowcatalyst.platform.client.jooq;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;
import tech.flowcatalyst.platform.client.*;
import tech.flowcatalyst.platform.jooq.generated.tables.records.ClientsRecord;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static tech.flowcatalyst.platform.jooq.generated.tables.Clients.CLIENTS;

/**
 * JOOQ-based implementation of ClientRepository.
 */
@ApplicationScoped
public class JooqClientRepository implements ClientRepository {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    DSLContext dsl;

    @Override
    public Client findById(String id) {
        return dsl.selectFrom(CLIENTS)
            .where(CLIENTS.ID.eq(id))
            .fetchOne(this::toDomain);
    }

    @Override
    public Optional<Client> findByIdOptional(String id) {
        return Optional.ofNullable(findById(id));
    }

    @Override
    public Optional<Client> findByIdentifier(String identifier) {
        return Optional.ofNullable(
            dsl.selectFrom(CLIENTS)
                .where(CLIENTS.IDENTIFIER.eq(identifier))
                .fetchOne(this::toDomain)
        );
    }

    @Override
    public List<Client> findAllActive() {
        return dsl.selectFrom(CLIENTS)
            .where(CLIENTS.STATUS.eq(ClientStatus.ACTIVE.name()))
            .fetch(this::toDomain);
    }

    @Override
    public List<Client> findByIds(Set<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        // Convert to String[] to handle JsonStringImpl objects
        String[] idsArray = ids.stream()
            .map(Object::toString)
            .toArray(String[]::new);
        return dsl.selectFrom(CLIENTS)
            .where(CLIENTS.ID.in(idsArray))
            .fetch(this::toDomain);
    }

    @Override
    public List<Client> listAll() {
        return dsl.selectFrom(CLIENTS)
            .fetch(this::toDomain);
    }

    @Override
    public void persist(Client client) {
        ClientsRecord record = toRecord(client);
        record.setCreatedAt(toOffsetDateTime(client.createdAt));
        record.setUpdatedAt(toOffsetDateTime(client.updatedAt));
        dsl.insertInto(CLIENTS).set(record).execute();
    }

    @Override
    public void update(Client client) {
        client.updatedAt = Instant.now();
        ClientsRecord record = toRecord(client);
        record.setUpdatedAt(toOffsetDateTime(client.updatedAt));
        dsl.update(CLIENTS)
            .set(record)
            .where(CLIENTS.ID.eq(client.id))
            .execute();
    }

    @Override
    public void delete(Client client) {
        dsl.deleteFrom(CLIENTS)
            .where(CLIENTS.ID.eq(client.id))
            .execute();
    }

    // ========================================================================
    // Mapping Methods
    // ========================================================================

    private Client toDomain(Record record) {
        if (record == null) return null;

        Client c = new Client();
        c.id = record.get(CLIENTS.ID);
        c.name = record.get(CLIENTS.NAME);
        c.identifier = record.get(CLIENTS.IDENTIFIER);
        c.status = parseEnum(record.get(CLIENTS.STATUS), ClientStatus.class);
        c.statusReason = record.get(CLIENTS.STATUS_REASON);
        c.statusChangedAt = toInstant(record.get(CLIENTS.STATUS_CHANGED_AT));
        c.createdAt = toInstant(record.get(CLIENTS.CREATED_AT));
        c.updatedAt = toInstant(record.get(CLIENTS.UPDATED_AT));

        // Notes (JSONB array)
        String notesJson = record.get(CLIENTS.NOTES);
        if (notesJson != null && !notesJson.isBlank()) {
            c.notes = parseJson(notesJson, new TypeReference<List<ClientNote>>() {});
            if (c.notes == null) {
                c.notes = new ArrayList<>();
            }
        }

        return c;
    }

    private ClientsRecord toRecord(Client c) {
        ClientsRecord r = new ClientsRecord();
        r.setId(c.id);
        r.setName(c.name);
        r.setIdentifier(c.identifier);
        r.setStatus(c.status != null ? c.status.name() : ClientStatus.ACTIVE.name());
        r.setStatusReason(c.statusReason);
        r.setStatusChangedAt(toOffsetDateTime(c.statusChangedAt));
        r.setNotes(toJson(c.notes != null ? c.notes : new ArrayList<>()));
        return r;
    }

    // ========================================================================
    // Utility Methods
    // ========================================================================

    private <E extends Enum<E>> E parseEnum(String value, Class<E> enumClass) {
        if (value == null) return null;
        try {
            return Enum.valueOf(enumClass, value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private Instant toInstant(OffsetDateTime odt) {
        return odt != null ? odt.toInstant() : null;
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant != null ? instant.atOffset(ZoneOffset.UTC) : null;
    }

    private <T> T parseJson(String json, TypeReference<T> typeRef) {
        if (json == null || json.isBlank()) return null;
        try {
            return objectMapper.readValue(json, typeRef);
        } catch (Exception e) {
            return null;
        }
    }

    private String toJson(Object obj) {
        if (obj == null) return null;
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return null;
        }
    }
}
