package com.offsetexplorer.repository;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.offsetexplorer.model.EnvironmentProfile;
import com.offsetexplorer.model.SaslMechanismType;
import com.offsetexplorer.model.SecurityProtocolType;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Repositorio de persistencia local para gestionar múltiples ambientes/configuraciones
 * de conexión a Kafka. Almacena los perfiles en ~/.kafka-offset-explorer/profiles.json.
 */
public class ProfileRepository {

    private final Path storagePath;
    private final ObjectMapper objectMapper;

    public ProfileRepository() {
        String userHome = System.getProperty("user.home", ".");
        Path dir = Path.of(userHome, ".kafka-offset-explorer");
        this.storagePath = dir.resolve("profiles.json");
        this.objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * Carga la lista de ambientes configurados. Si no existe archivo previo,
     * genera perfiles iniciales de ejemplo para LOCAL, DEV, QA y PROD.
     */
    public synchronized List<EnvironmentProfile> loadProfiles() {
        File file = storagePath.toFile();
        if (!file.exists()) {
            List<EnvironmentProfile> defaults = createDefaultProfiles();
            saveProfiles(defaults);
            return defaults;
        }
        try {
            List<EnvironmentProfile> loaded = objectMapper.readValue(
                    file, new TypeReference<List<EnvironmentProfile>>() {});
            if (loaded == null || loaded.isEmpty()) {
                loaded = createDefaultProfiles();
                saveProfiles(loaded);
            }
            return loaded;
        } catch (IOException e) {
            return createDefaultProfiles();
        }
    }

    /**
     * Guarda la lista completa de ambientes en el archivo JSON local.
     */
    public synchronized void saveProfiles(List<EnvironmentProfile> profiles) {
        try {
            if (storagePath.getParent() != null) {
                Files.createDirectories(storagePath.getParent());
            }
            objectMapper.writeValue(storagePath.toFile(), profiles);
        } catch (IOException e) {
            throw new RuntimeException("No se pudo guardar la configuración de ambientes en " + storagePath, e);
        }
    }

    public Path getStoragePath() {
        return storagePath;
    }

    private List<EnvironmentProfile> createDefaultProfiles() {
        List<EnvironmentProfile> defaults = new ArrayList<>();

        // 1. Ambiente Local (PLAINTEXT)
        EnvironmentProfile local = new EnvironmentProfile(
                "Kafka Localhost",
                "LOCAL",
                "localhost:9092",
                SecurityProtocolType.PLAINTEXT,
                SaslMechanismType.PLAIN
        );
        local.setTraceabilityTopics(List.of(
                "orders-inbound",
                "orders-validated",
                "payments-processed",
                "shipments-dispatched"
        ));
        defaults.add(local);

        // 2. Ambiente Desarrollo (SASL_PLAINTEXT / SCRAM-SHA-256)
        EnvironmentProfile dev = new EnvironmentProfile(
                "Clúster Desarrollo",
                "DEV",
                "dev-kafka-broker:9092",
                SecurityProtocolType.SASL_PLAINTEXT,
                SaslMechanismType.SCRAM_SHA_256
        );
        dev.setSaslUsername("dev-user");
        dev.setSaslPassword("dev-secret");
        dev.setTraceabilityTopics(List.of(
                "dev.events.entry",
                "dev.events.enriched",
                "dev.events.output"
        ));
        defaults.add(dev);

        // 3. Ambiente QA / Certificación (SASL_SSL / SCRAM-SHA-512)
        EnvironmentProfile qa = new EnvironmentProfile(
                "Clúster QA Certificación",
                "QA",
                "qa-kafka-01.corp.local:9093,qa-kafka-02.corp.local:9093",
                SecurityProtocolType.SASL_SSL,
                SaslMechanismType.SCRAM_SHA_512
        );
        qa.setSaslUsername("qa-tracer-svc");
        qa.setSaslPassword("qa-password");
        qa.setTraceabilityTopics(List.of(
                "qa.transactions.received",
                "qa.transactions.antifraud",
                "qa.transactions.settled"
        ));
        defaults.add(qa);

        // 4. Ambiente Producción (SASL_SSL / PLAIN o API Key)
        EnvironmentProfile prod = new EnvironmentProfile(
                "Clúster Producción (Lectura)",
                "PROD",
                "prod-kafka-bootstrap:9093",
                SecurityProtocolType.SASL_SSL,
                SaslMechanismType.PLAIN
        );
        prod.setSaslUsername("prod-auditor-key");
        prod.setSaslPassword("prod-auditor-secret");
        prod.setTraceabilityTopics(List.of(
                "prod.orders.created",
                "prod.orders.billed",
                "prod.orders.completed"
        ));
        defaults.add(prod);

        return defaults;
    }
}
