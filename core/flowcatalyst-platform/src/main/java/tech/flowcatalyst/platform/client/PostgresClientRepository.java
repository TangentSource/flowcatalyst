package tech.flowcatalyst.platform.client;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * PostgreSQL implementation of ClientRepository using JDBI.
 */
@ApplicationScoped
@Typed(ClientRepository.class)
class PostgresClientRepository implements ClientRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Client findById(String id) {
        return jdbi.withExtension(ClientDao.class, dao -> dao.findById(id).orElse(null));
    }

    @Override
    public Optional<Client> findByIdOptional(String id) {
        return jdbi.withExtension(ClientDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<Client> findByIdentifier(String identifier) {
        return jdbi.withExtension(ClientDao.class, dao -> dao.findByIdentifier(identifier));
    }

    @Override
    public List<Client> findAllActive() {
        return jdbi.withExtension(ClientDao.class, ClientDao::findAllActive);
    }

    @Override
    public List<Client> findByIds(Set<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        return jdbi.withHandle(handle ->
            handle.createQuery("SELECT * FROM clients WHERE id = ANY(:ids)")
                .bindArray("ids", String.class, ids.toArray(new String[0]))
                .registerRowMapper(new ClientRowMapper())
                .mapTo(Client.class)
                .list()
        );
    }

    @Override
    public List<Client> listAll() {
        return jdbi.withExtension(ClientDao.class, ClientDao::listAll);
    }

    @Override
    public void persist(Client client) {
        jdbi.useExtension(ClientDao.class, dao ->
            dao.insert(client, JsonHelper.toJsonArray(client.notes)));
    }

    @Override
    public void update(Client client) {
        client.updatedAt = Instant.now();
        jdbi.useExtension(ClientDao.class, dao ->
            dao.update(client, JsonHelper.toJsonArray(client.notes)));
    }

    @Override
    public void delete(Client client) {
        jdbi.useExtension(ClientDao.class, dao -> dao.deleteById(client.id));
    }
}
