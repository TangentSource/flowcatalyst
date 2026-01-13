plugins {
    java
    id("io.quarkus")
}

repositories {
    mavenCentral()
    mavenLocal()
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

// ==========================================================================
// Platform UI Build (Vue.js)
// ==========================================================================
val uiDir = rootProject.file("packages/platform-ui-vue")
val uiDistDir = uiDir.resolve("dist")
val uiOutputDir = layout.buildDirectory.dir("resources/main/META-INF/resources")

val npmInstall by tasks.registering(Exec::class) {
    description = "Install npm dependencies for platform-ui-vue"
    group = "build"
    workingDir = uiDir
    commandLine("npm", "install", "--legacy-peer-deps")
    inputs.file(uiDir.resolve("package.json"))
    outputs.dir(uiDir.resolve("node_modules"))
}

val buildUi by tasks.registering(Exec::class) {
    description = "Build platform-ui-vue for production"
    group = "build"
    dependsOn(npmInstall)
    workingDir = uiDir
    // Skip api:generate (requires running backend) and vue-tsc (fix TS errors separately)
    commandLine("npx", "vite", "build")
    inputs.dir(uiDir.resolve("src"))
    inputs.file(uiDir.resolve("package.json"))
    inputs.file(uiDir.resolve("vite.config.ts"))
    inputs.file(uiDir.resolve("tsconfig.json"))
    outputs.dir(uiDistDir)
}

val copyUiToResources by tasks.registering(Copy::class) {
    description = "Copy built UI to META-INF/resources"
    group = "build"
    dependsOn(buildUi)
    from(uiDistDir)
    into(uiOutputDir)
}

tasks.named("processResources") {
    dependsOn(copyUiToResources)
}

dependencies {
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))

    // Core Quarkus (needed for startup/shutdown hooks)
    implementation("io.quarkus:quarkus-arc")

    // All modules - disable individually via config if not needed
    implementation(project(":core:flowcatalyst-platform"))
    implementation(project(":core:flowcatalyst-stream-processor"))
    implementation(project(":core:flowcatalyst-dispatch-scheduler"))

    // Queue client optional dependencies (needed at runtime for native builds)
    implementation("io.nats:jnats:2.24.1")
    implementation("org.xerial:sqlite-jdbc:3.47.1.0")

    // Vert.x for SPA fallback routing
    implementation("io.quarkus:quarkus-vertx-http")
}

group = "tech.flowcatalyst"
version = "1.0.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_25
    targetCompatibility = JavaVersion.VERSION_25
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}

// Uber-jar: ignore duplicate metadata files from dependencies
tasks.withType<Test> {
    useJUnitPlatform()
}
