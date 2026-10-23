package com.offsetexplorer.model;

/**
 * Información descriptiva de un nodo Broker dentro del clúster de Kafka.
 */
public record BrokerInfo(
        int id,
        String host,
        int port,
        String rack,
        boolean controller
) {
}
