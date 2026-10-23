package com.offsetexplorer.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Representa la configuración de un Ambiente o Perfil de conexión a un clúster de Apache Kafka.
 * Soporta configuraciones multi-ambiente (LOCAL, DEV, QA, STAGING, PROD), autenticación SASL/SSL,
 * y una lista predeterminada de tópicos para trazabilidad rápida de eventos.
 */
public class EnvironmentProfile {

    private String id;
    private String name;
    private String environmentTag; // Ej. LOCAL, DEV, QA, PROD
    private String bootstrapServers;
    private SecurityProtocolType securityProtocol;
    private SaslMechanismType saslMechanism;
    private String saslUsername;
    private String saslPassword;
    private String customJaasConfig;

    // Configuración SSL / TLS
    private String sslTruststoreLocation;
    private String sslTruststorePassword;
    private String sslTruststoreType; // JKS o PKCS12
    private String sslKeystoreLocation;
    private String sslKeystorePassword;
    private String sslKeyPassword;
    private boolean disableSslHostnameVerification;

    // Timeouts y lista de tópicos favoritos para trazabilidad
    private int requestTimeoutMs;
    private List<String> traceabilityTopics;

    public EnvironmentProfile() {
        this.id = UUID.randomUUID().toString();
        this.name = "Local Kafka";
        this.environmentTag = "LOCAL";
        this.bootstrapServers = "localhost:9092";
        this.securityProtocol = SecurityProtocolType.PLAINTEXT;
        this.saslMechanism = SaslMechanismType.PLAIN;
        this.saslUsername = "";
        this.saslPassword = "";
        this.customJaasConfig = "";
        this.sslTruststoreLocation = "";
        this.sslTruststorePassword = "";
        this.sslTruststoreType = "JKS";
        this.sslKeystoreLocation = "";
        this.sslKeystorePassword = "";
        this.sslKeyPassword = "";
        this.disableSslHostnameVerification = false;
        this.requestTimeoutMs = 8000;
        this.traceabilityTopics = new ArrayList<>();
    }

    public EnvironmentProfile(String name, String environmentTag, String bootstrapServers,
                              SecurityProtocolType securityProtocol, SaslMechanismType saslMechanism) {
        this();
        this.name = name;
        this.environmentTag = environmentTag;
        this.bootstrapServers = bootstrapServers;
        this.securityProtocol = securityProtocol;
        this.saslMechanism = saslMechanism;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEnvironmentTag() {
        return environmentTag;
    }

    public void setEnvironmentTag(String environmentTag) {
        this.environmentTag = environmentTag;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public SecurityProtocolType getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(SecurityProtocolType securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public SaslMechanismType getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(SaslMechanismType saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getSaslUsername() {
        return saslUsername;
    }

    public void setSaslUsername(String saslUsername) {
        this.saslUsername = saslUsername;
    }

    public String getSaslPassword() {
        return saslPassword;
    }

    public void setSaslPassword(String saslPassword) {
        this.saslPassword = saslPassword;
    }

    public String getCustomJaasConfig() {
        return customJaasConfig;
    }

    public void setCustomJaasConfig(String customJaasConfig) {
        this.customJaasConfig = customJaasConfig;
    }

    public String getSslTruststoreLocation() {
        return sslTruststoreLocation;
    }

    public void setSslTruststoreLocation(String sslTruststoreLocation) {
        this.sslTruststoreLocation = sslTruststoreLocation;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }

    public void setSslTruststorePassword(String sslTruststorePassword) {
        this.sslTruststorePassword = sslTruststorePassword;
    }

    public String getSslTruststoreType() {
        return sslTruststoreType;
    }

    public void setSslTruststoreType(String sslTruststoreType) {
        this.sslTruststoreType = sslTruststoreType;
    }

    public String getSslKeystoreLocation() {
        return sslKeystoreLocation;
    }

    public void setSslKeystoreLocation(String sslKeystoreLocation) {
        this.sslKeystoreLocation = sslKeystoreLocation;
    }

    public String getSslKeystorePassword() {
        return sslKeystorePassword;
    }

    public void setSslKeystorePassword(String sslKeystorePassword) {
        this.sslKeystorePassword = sslKeystorePassword;
    }

    public String getSslKeyPassword() {
        return sslKeyPassword;
    }

    public void setSslKeyPassword(String sslKeyPassword) {
        this.sslKeyPassword = sslKeyPassword;
    }

    public boolean isDisableSslHostnameVerification() {
        return disableSslHostnameVerification;
    }

    public void setDisableSslHostnameVerification(boolean disableSslHostnameVerification) {
        this.disableSslHostnameVerification = disableSslHostnameVerification;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public List<String> getTraceabilityTopics() {
        return traceabilityTopics;
    }

    public void setTraceabilityTopics(List<String> traceabilityTopics) {
        this.traceabilityTopics = traceabilityTopics != null ? traceabilityTopics : new ArrayList<>();
    }

    @Override
    public String toString() {
        return String.format("[%s] %s (%s - %s)",
                environmentTag != null ? environmentTag : "ENV",
                name,
                bootstrapServers,
                securityProtocol != null ? securityProtocol.getKafkaValue() : "PLAINTEXT");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EnvironmentProfile that)) return false;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
