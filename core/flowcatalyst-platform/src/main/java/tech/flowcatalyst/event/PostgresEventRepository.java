package tech.flowcatalyst.event;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import tech.flowcatalyst.platform.shared.jdbi.JsonHelper;

import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL implementation of EventRepository using JDBI.
 */
@ApplicationScoped
@Typed(EventRepository.class)
class PostgresEventRepository implements EventRepository {

    @Inject
    Jdbi jdbi;

    @Override
    public Event findById(String id) {
        return jdbi.withExtension(EventDao.class, dao -> dao.findById(id).orElse(null));
    }

    @Override
    public Optional<Event> findByIdOptional(String id) {
        return jdbi.withExtension(EventDao.class, dao -> dao.findById(id));
    }

    @Override
    public Optional<Event> findByDeduplicationId(String deduplicationId) {
        return jdbi.withExtension(EventDao.class, dao -> dao.findByDeduplicationId(deduplicationId));
    }

    @Override
    public List<Event> listAll() {
        return jdbi.withExtension(EventDao.class, EventDao::listAll);
    }

    @Override
    public List<Event> findRecentPaged(int page, int size) {
        int offset = page * size;
        return jdbi.withExtension(EventDao.class, dao -> dao.findRecentPaged(offset, size));
    }

    @Override
    public long count() {
        return jdbi.withExtension(EventDao.class, EventDao::count);
    }

    @Override
    public boolean existsByDeduplicationId(String deduplicationId) {
        return jdbi.withExtension(EventDao.class, dao -> dao.existsByDeduplicationId(deduplicationId));
    }

    @Override
    public void insert(Event event) {
        jdbi.useExtension(EventDao.class, dao ->
            dao.insert(event, JsonHelper.toJsonArray(event.contextData)));
    }

    @Override
    public void persist(Event event) {
        insert(event);
    }

    @Override
    public void persistAll(List<Event> events) {
        jdbi.useTransaction(handle -> {
            var dao = handle.attach(EventDao.class);
            for (Event event : events) {
                dao.insert(event, JsonHelper.toJsonArray(event.contextData));
            }
        });
    }

    @Override
    public void update(Event event) {
        jdbi.useExtension(EventDao.class, dao ->
            dao.update(event, JsonHelper.toJsonArray(event.contextData)));
    }

    @Override
    public void delete(Event event) {
        jdbi.useExtension(EventDao.class, dao -> dao.deleteById(event.id));
    }

    @Override
    public boolean deleteById(String id) {
        return jdbi.withExtension(EventDao.class, dao -> dao.deleteById(id) > 0);
    }
}
