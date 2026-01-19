package tech.flowcatalyst.platform.client.panache;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;
import tech.flowcatalyst.platform.client.Client;
import tech.flowcatalyst.platform.client.ClientRepository;
import tech.flowcatalyst.platform.client.ClientStatus;
import tech.flowcatalyst.platform.client.entity.ClientEntity;
import tech.flowcatalyst.platform.client.mapper.ClientMapper;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Panache-based implementation of ClientRepository.
 */
@ApplicationScoped
public class PanacheClientRepository
    implements ClientRepository, PanacheRepositoryBase<ClientEntity, String> {

    @Override
    public Client findById(String id) {
        return find("id", id)
            .firstResultOptional()
            .map(ClientMapper::toDomain)
            .orElse(null);
    }

    @Override
    public Optional<Client> findByIdOptional(String id) {
        return find("id", id)
            .firstResultOptional()
            .map(ClientMapper::toDomain);
    }

    @Override
    public Optional<Client> findByIdentifier(String identifier) {
        return find("identifier", identifier)
            .firstResultOptional()
            .map(ClientMapper::toDomain);
    }

    @Override
    public List<Client> findAllActive() {
        return find("status", ClientStatus.ACTIVE)
            .stream()
            .map(ClientMapper::toDomain)
            .toList();
    }

    @Override
    public List<Client> findByIds(Set<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return List.of();
        }
        return find("id in ?1", ids)
            .stream()
            .map(ClientMapper::toDomain)
            .toList();
    }

    @Override
    public List<Client> listAll() {
        return findAll().stream()
            .map(ClientMapper::toDomain)
            .toList();
    }

    @Override
    public void persist(Client client) {
        if (client.createdAt == null) {
            client.createdAt = Instant.now();
        }
        client.updatedAt = Instant.now();
        ClientEntity entity = ClientMapper.toEntity(client);
        persist(entity);
    }

    @Override
    public void update(Client client) {
        client.updatedAt = Instant.now();
        ClientEntity entity = findById(client.id);
        if (entity != null) {
            ClientMapper.updateEntity(entity, client);
        }
    }

    @Override
    public void delete(Client client) {
        deleteById(client.id);
    }
}
