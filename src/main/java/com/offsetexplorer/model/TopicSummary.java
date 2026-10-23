package com.offsetexplorer.model;

import java.util.List;

/**
 * Resumen de un tópico de Kafka junto con sus particiones y el conteo total de mensajes.
 */
public record TopicSummary(
        String name,
        boolean internal,
        int partitionCount,
        int replicationFactor,
        long totalMessages,
        List<PartitionOffsetInfo> partitions
) {
}
