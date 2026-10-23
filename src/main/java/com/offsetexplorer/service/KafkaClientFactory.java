package com.offsetexplorer.service;

import com.offsetexplorer.model.EnvironmentProfile;
import com.offsetexplorer.model.SaslMechanismType;
import com.offsetexplorer.model.SecurityProtocolType;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * Fábrica centralizada de clientes de Apache Kafka (AdminClient, KafkaConsumer, KafkaProducer).
 * Traduce un {@link EnvironmentProfile} en las propiedades nativas de conexión incluyendo
 * autenticación SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, Custom JAAS) y cifrado SSL/TLS.
 */
public final class KafkaClientFactory {

    private KafkaClientFactory() {
    }

    /**
     * Construye las propiedades base de conexión y seguridad SASL/SSL para el ambiente indicado.
     */
    public static Properties buildBaseProperties(EnvironmentProfile profile) {
        Properties props = new Properties();
        int timeoutMs = Math.max(2000, profile.getRequestTimeoutMs());

        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, profile.getBootstrapServers().trim());
        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        props.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs + 2000);

        SecurityProtocolType protocol = profile.getSecurityProtocol() != null
                ? profile.getSecurityProtocol()
                : SecurityProtocolType.PLAINTEXT;
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.getKafkaValue());

        // Configuración SASL
        if (protocol.isSaslEnabled()) {
            SaslMechanismType mechanism = profile.getSaslMechanism() != null
                    ? profile.getSaslMechanism()
                    : SaslMechanismType.PLAIN;

            if (mechanism == SaslMechanismType.CUSTOM_JAAS) {
                if (profile.getCustomJaasConfig() != null && !profile.getCustomJaasConfig().isBlank()) {
                    props.put(SaslConfigs.SASL_JAAS_CONFIG, profile.getCustomJaasConfig().trim());
                }
            } else {
                props.put(SaslConfigs.SASL_MECHANISM, mechanism.getMechanismName());
                String username = escapeJaasValue(profile.getSaslUsername());
                String password = escapeJaasValue(profile.getSaslPassword());
                String jaas = String.format(
                        "%s required username=\"%s\" password=\"%s\";",
                        mechanism.getLoginModuleClass(),
                        username,
                        password
                );
                props.put(SaslConfigs.SASL_JAAS_CONFIG, jaas);
            }
        }

        // Configuración SSL / TLS
        if (protocol.isSslEnabled()) {
            if (isNotBlank(profile.getSslTruststoreLocation())) {
                props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, profile.getSslTruststoreLocation().trim());
                if (isNotBlank(profile.getSslTruststorePassword())) {
                    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, profile.getSslTruststorePassword());
                }
                if (isNotBlank(profile.getSslTruststoreType())) {
                    props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, profile.getSslTruststoreType().trim());
                }
            }
            if (isNotBlank(profile.getSslKeystoreLocation())) {
                props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, profile.getSslKeystoreLocation().trim());
                if (isNotBlank(profile.getSslKeystorePassword())) {
                    props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, profile.getSslKeystorePassword());
                }
                if (isNotBlank(profile.getSslKeyPassword())) {
                    props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, profile.getSslKeyPassword());
                }
            }
            if (profile.isDisableSslHostnameVerification()) {
                props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            }
        }

        return props;
    }

    /**
     * Crea una instancia de {@link AdminClient} para inspección y gestión del clúster.
     */
    public static AdminClient createAdminClient(EnvironmentProfile profile) {
        Properties props = buildBaseProperties(profile);
        return AdminClient.create(props);
    }

    /**
     * Crea un {@link KafkaConsumer} en modo de solo lectura (sin auto-commit) para explorar mensajes
     * o rastrear eventos sin alterar los offsets de ningún Consumer Group.
     */
    public static KafkaConsumer<byte[], byte[]> createReadOnlyConsumer(EnvironmentProfile profile) {
        Properties props = buildBaseProperties(profile);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "offset-explorer-reader-" + UUID.randomUUID().toString().substring(0, 8));
        return new KafkaConsumer<>(props);
    }

    /**
     * Crea un {@link KafkaProducer} para publicar eventos de prueba o reenviar mensajes.
     */
    public static KafkaProducer<byte[], byte[]> createProducer(EnvironmentProfile profile) {
        Properties props = buildBaseProperties(profile);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "offset-explorer-producer-" + UUID.randomUUID().toString().substring(0, 8));
        return new KafkaProducer<>(props);
    }

    private static boolean isNotBlank(String s) {
        return s != null && !s.isBlank();
    }

    private static String escapeJaasValue(String val) {
        if (val == null) {
            return "";
        }
        return val.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
