plugins {
    java
    id("io.quarkus")
    id("org.kordamp.gradle.jandex") version "2.0.0"
    id("com.google.cloud.tools.jib") version "3.4.0"
    id("nu.studer.jooq") version "9.0"
}

repositories {
    mavenCentral()
    mavenLocal()
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project
val resilience4jVersion: String by project

// Exclude netty-nio-client globally - it references AWS CRT classes that cause native image issues
configurations.all {
    exclude(group = "software.amazon.awssdk", module = "netty-nio-client")
}

dependencies {
    // Quarkus BOM
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:quarkus-amazon-services-bom:${quarkusPlatformVersion}"))

    // ==========================================================================
    // Core Quarkus
    // ==========================================================================
    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-rest")
    implementation("io.quarkus:quarkus-rest-jackson")

    // ==========================================================================
    // Database - PostgreSQL + Flyway + Panache + QueryDSL
    // ==========================================================================
    implementation("io.quarkus:quarkus-agroal")           // Connection pool
    implementation("io.quarkus:quarkus-jdbc-postgresql")  // PostgreSQL driver
    implementation("io.quarkus:quarkus-flyway")           // Schema migrations

    // Panache - Repository pattern for Hibernate ORM
    implementation("io.quarkus:quarkus-hibernate-orm-panache")

    // QueryDSL (openfeign fork) - Type-safe dynamic queries
    implementation("io.github.openfeign.querydsl:querydsl-jpa:5.1.0:jakarta")
    annotationProcessor("io.github.openfeign.querydsl:querydsl-apt:5.1.0:jakarta")

    // JOOQ - Type-safe SQL DSL (kept temporarily during migration)
    implementation("org.jooq:jooq:3.19.17")
    implementation("org.jooq:jooq-meta:3.19.17")

    // JOOQ code generation dependencies (DDL-based generation from Flyway migrations)
    jooqGenerator("org.jooq:jooq-meta:3.19.17")
    jooqGenerator("org.jooq:jooq-meta-extensions:3.19.17")

    // Keep MongoDB temporarily for data migration
    implementation("io.quarkus:quarkus-mongodb-client")

    // ==========================================================================
    // Security
    // ==========================================================================
    implementation("io.quarkus:quarkus-security")
    implementation("io.quarkus:quarkus-elytron-security-common")
    implementation("io.quarkus:quarkus-smallrye-jwt")
    implementation("io.quarkus:quarkus-smallrye-jwt-build")

    // OIDC (optional - can be disabled via config)
    implementation("io.quarkus:quarkus-oidc")

    // REST client for external IDP calls
    implementation("io.quarkus:quarkus-rest-client-jackson")

    // ==========================================================================
    // OpenAPI & Validation
    // ==========================================================================
    implementation("io.quarkus:quarkus-smallrye-openapi")
    implementation("io.quarkus:quarkus-hibernate-validator")

    // ==========================================================================
    // Caching for session/token management
    // ==========================================================================
    implementation("io.quarkus:quarkus-cache")


    // ==========================================================================
    // Messaging (SQS for dispatch jobs) - use Quarkus extension
    // ==========================================================================
    implementation("io.quarkiverse.amazonservices:quarkus-amazon-sqs")
    implementation("io.quarkiverse.amazonservices:quarkus-amazon-crt") // Native image support
    implementation("com.github.jnr:jnr-unixsocket:0.38.22") // Required by aws-crt for native builds
    implementation("software.amazon.awssdk:url-connection-client")

    // ==========================================================================
    // Secret Management - use Quarkus extensions
    // ==========================================================================
    // AWS Secrets Manager
    implementation("io.quarkiverse.amazonservices:quarkus-amazon-secretsmanager")
    // AWS Systems Manager Parameter Store
    implementation("io.quarkiverse.amazonservices:quarkus-amazon-ssm")
    // HashiCorp Vault (quarkiverse extension)
    implementation("io.quarkiverse.vault:quarkus-vault:4.4.0")

    // ==========================================================================
    // Resilience & Fault Tolerance
    // ==========================================================================
    implementation("io.quarkus:quarkus-smallrye-fault-tolerance")
    implementation("io.github.resilience4j:resilience4j-ratelimiter:${resilience4jVersion}")

    // ==========================================================================
    // Scheduling
    // ==========================================================================
    implementation("io.quarkus:quarkus-scheduler")

    // ==========================================================================
    // Observability
    // ==========================================================================
    implementation("io.quarkus:quarkus-smallrye-health")
    implementation("io.quarkus:quarkus-micrometer-registry-prometheus")
    implementation("io.micrometer:micrometer-core")
    implementation("io.quarkus:quarkus-logging-json")

    // ==========================================================================
    // Container Image
    // ==========================================================================
    implementation("io.quarkus:quarkus-container-image-jib")

    // ==========================================================================
    // TSID for primary keys
    // ==========================================================================
    implementation("com.github.f4b6a3:tsid-creator:5.2.6")

    // ==========================================================================
    // Password Hashing (Argon2id - OWASP recommended)
    // ==========================================================================
    implementation("de.mkammerer:argon2-jvm:2.11")


    // ==========================================================================
    // Lombok
    // ==========================================================================
    compileOnly("org.projectlombok:lombok:1.18.42")
    annotationProcessor("org.projectlombok:lombok:1.18.42")
    testCompileOnly("org.projectlombok:lombok:1.18.42")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.42")

    // ==========================================================================
    // Testing
    // ==========================================================================
    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.quarkus:quarkus-junit5-mockito")
    testImplementation("io.quarkus:quarkus-narayana-jta") // For QuarkusTransaction in tests
    testImplementation("io.rest-assured:rest-assured")
    testImplementation("org.awaitility:awaitility:4.2.0")
    testImplementation("org.testcontainers:testcontainers:1.19.7")
    testImplementation("org.testcontainers:postgresql:1.19.7")
    testImplementation("org.testcontainers:mongodb:1.19.7")  // Keep temporarily for migration testing
    testImplementation("io.quarkus:quarkus-test-common")
    testImplementation("org.assertj:assertj-core:3.24.2")
}

group = "tech.flowcatalyst"
version = "1.0.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

// ==========================================================================
// Test Configuration
// ==========================================================================

// Unit tests (no @QuarkusTest)
val unitTest = tasks.test.get().apply {
    useJUnitPlatform {
        excludeTags("integration")
    }

    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")

    // Quarkus tests must run sequentially (they share the same port)
    maxParallelForks = 1
    systemProperty("junit.jupiter.execution.parallel.enabled", "false")
}

// Integration tests
val integrationTest by tasks.registering(Test::class) {
    description = "Runs integration tests"
    group = "verification"

    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath

    useJUnitPlatform {
        includeTags("integration")
    }

    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")

    // Run integration tests sequentially
    maxParallelForks = 1
    systemProperty("junit.jupiter.execution.parallel.enabled", "false")

    shouldRunAfter(unitTest)
}

tasks.named("check") {
    dependsOn(integrationTest)
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}

// ==========================================================================
// Java 24+ Compatibility
// ==========================================================================
// Required for JBoss threads thread-local-reset capability on Java 24+
tasks.withType<JavaExec> {
    jvmArgs("--add-opens", "java.base/java.lang=ALL-UNNAMED")
}

tasks.withType<Test> {
    jvmArgs("--add-opens", "java.base/java.lang=ALL-UNNAMED")
}

// ==========================================================================
// Docker Image Configuration
// ==========================================================================

jib {
    from {
        image = "eclipse-temurin:21-jre-alpine"
    }
    to {
        image = "${System.getenv("DOCKER_REGISTRY") ?: "flowcatalyst"}/${project.name}:${project.version}"
        tags = setOf("latest")
    }
    container {
        mainClass = "io.quarkus.runner.GeneratedMain"
        jvmFlags = listOf(
            "-XX:+UseContainerSupport",
            "-XX:MaxRAMPercentage=75.0",
            "-Djava.security.egd=file:/dev/./urandom"
        )
        ports = listOf("8080")
        labels.put("maintainer", "flowcatalyst@example.com")
        labels.put("version", project.version.toString())
        creationTime.set("USE_CURRENT_TIMESTAMP")
        user = "1000:1000"
    }
}

// ==========================================================================
// JOOQ Code Generation Configuration
// ==========================================================================
// Generates type-safe DSL classes from Flyway DDL migration files.
// No database connection required - parses SQL files directly.
//
// Usage:
//   ./gradlew generateJooq
//
// Generated output: src/main/java/tech/flowcatalyst/platform/jooq/generated/

jooq {
    version.set("3.19.17")
    edition.set(nu.studer.gradle.jooq.JooqEdition.OSS)

    configurations {
        create("main") {
            generateSchemaSourceOnCompilation.set(false)  // Manual generation only

            jooqConfiguration.apply {
                logging = org.jooq.meta.jaxb.Logging.WARN

                generator.apply {
                    name = "org.jooq.codegen.JavaGenerator"

                    database.apply {
                        // Use DDLDatabase to parse SQL migration files
                        name = "org.jooq.meta.extensions.ddl.DDLDatabase"

                        // Parse consolidated schema file (final state after all migrations)
                        properties.add(org.jooq.meta.jaxb.Property().apply {
                            key = "scripts"
                            value = "src/main/resources/db/jooq/schema.sql"
                        })

                        // Use PostgreSQL dialect for parsing
                        properties.add(org.jooq.meta.jaxb.Property().apply {
                            key = "defaultNameCase"
                            value = "lower"
                        })

                        // Include all tables
                        includes = ".*"

                        // Exclude Flyway migration tracking table
                        excludes = "flyway_schema_history"

                        // Force types for specific columns
                        forcedTypes.addAll(listOf(
                            // Map TIMESTAMPTZ to java.time.Instant
                            org.jooq.meta.jaxb.ForcedType().apply {
                                name = "INSTANT"
                                includeTypes = "TIMESTAMPTZ"
                            },
                            // Map JSONB to String for manual handling
                            org.jooq.meta.jaxb.ForcedType().apply {
                                userType = "java.lang.String"
                                includeTypes = "JSONB"
                                converter = "tech.flowcatalyst.platform.jooq.converters.JsonbStringConverter"
                            }
                        ))
                    }

                    generate.apply {
                        // Generate POJOs for domain mapping
                        isPojos = true
                        isPojosEqualsAndHashCode = true
                        isPojosToString = true
                        isImmutablePojos = false  // Mutable for easy mapping

                        // Generate fluent setters for builders
                        isFluentSetters = true

                        // Generate DAOs for basic CRUD
                        isDaos = true

                        // Generate records
                        isRecords = true

                        // Use Java 8+ date/time types
                        isJavaTimeTypes = true

                        // Generate deprecated annotations for deprecated columns
                        isDeprecated = false

                        // Generate indexes
                        isIndexes = true

                        // Generate keys
                        isKeys = true

                        // Do not generate global object references
                        isGlobalObjectReferences = false
                        isGlobalCatalogReferences = false
                        isGlobalSchemaReferences = false

                        // Use annotations
                        isGeneratedAnnotation = true
                        generatedAnnotationType = org.jooq.meta.jaxb.GeneratedAnnotationType.DETECT_FROM_JDK

                        // Nullable annotations
                        isNullableAnnotation = true
                        nullableAnnotationType = "jakarta.annotation.Nullable"
                        isNonnullAnnotation = true
                        nonnullAnnotationType = "jakarta.annotation.Nonnull"
                    }

                    target.apply {
                        packageName = "tech.flowcatalyst.platform.jooq.generated"
                        directory = "src/main/java"
                    }
                }
            }
        }
    }
}
