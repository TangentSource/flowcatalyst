package tech.flowcatalyst.platform.cors;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.mongodb.client.model.Filters.eq;

@ApplicationScoped
public class CorsAllowedOriginRepository {

    @Inject
    MongoClient mongoClient;

    @ConfigProperty(name = "quarkus.mongodb.database")
    String database;

    private MongoCollection<CorsAllowedOrigin> collection() {
        return mongoClient.getDatabase(database).getCollection("cors_allowed_origins", CorsAllowedOrigin.class);
    }

    public Optional<CorsAllowedOrigin> findById(String id) {
        return Optional.ofNullable(collection().find(eq("_id", id)).first());
    }

    public Optional<CorsAllowedOrigin> findByOrigin(String origin) {
        return Optional.ofNullable(collection().find(eq("origin", origin)).first());
    }

    public List<CorsAllowedOrigin> listAll() {
        return collection().find().into(new ArrayList<>());
    }

    public boolean existsByOrigin(String origin) {
        return collection().countDocuments(eq("origin", origin)) > 0;
    }

    public void persist(CorsAllowedOrigin entry) {
        collection().insertOne(entry);
    }

    public void delete(CorsAllowedOrigin entry) {
        collection().deleteOne(eq("_id", entry.id));
    }
}
