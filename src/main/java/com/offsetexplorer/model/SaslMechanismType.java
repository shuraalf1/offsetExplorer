package com.offsetexplorer.model;

/**
 * Mecanismos de autenticación SASL soportados.
 */
public enum SaslMechanismType {
    PLAIN("PLAIN", "org.apache.kafka.common.security.plain.PlainLoginModule"),
    SCRAM_SHA_256("SCRAM-SHA-256", "org.apache.kafka.common.security.scram.ScramLoginModule"),
    SCRAM_SHA_512("SCRAM-SHA-512", "org.apache.kafka.common.security.scram.ScramLoginModule"),
    CUSTOM_JAAS("CUSTOM", "");

    private final String mechanismName;
    private final String loginModuleClass;

    SaslMechanismType(String mechanismName, String loginModuleClass) {
        this.mechanismName = mechanismName;
        this.loginModuleClass = loginModuleClass;
    }

    public String getMechanismName() {
        return mechanismName;
    }

    public String getLoginModuleClass() {
        return loginModuleClass;
    }

    @Override
    public String toString() {
        return mechanismName;
    }
}
