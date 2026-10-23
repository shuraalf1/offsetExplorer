package com.offsetexplorer.model;

/**
 * Protocolos de seguridad soportados por Apache Kafka.
 */
public enum SecurityProtocolType {
    PLAINTEXT("PLAINTEXT", false, false),
    SSL("SSL", false, true),
    SASL_PLAINTEXT("SASL_PLAINTEXT", true, false),
    SASL_SSL("SASL_SSL", true, true);

    private final String kafkaValue;
    private final boolean saslEnabled;
    private final boolean sslEnabled;

    SecurityProtocolType(String kafkaValue, boolean saslEnabled, boolean sslEnabled) {
        this.kafkaValue = kafkaValue;
        this.saslEnabled = saslEnabled;
        this.sslEnabled = sslEnabled;
    }

    public String getKafkaValue() {
        return kafkaValue;
    }

    public boolean isSaslEnabled() {
        return saslEnabled;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    @Override
    public String toString() {
        return kafkaValue;
    }
}
