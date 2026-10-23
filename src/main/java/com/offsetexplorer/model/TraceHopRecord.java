package com.offsetexplorer.model;

/**
 * Representa un salto (Hop) detectado para un evento durante el análisis de Trazabilidad
 * a través de una lista de tópicos de Kafka.
 */
public record TraceHopRecord(
        int hopNumber,
        String topic,
        int partition,
        long offset,
        long timestampMillis,
        String formattedTime,
        long deltaFromStartMs,
        long deltaFromPreviousHopMs,
        String key,
        String payloadPreview,
        KafkaMessageRecord fullRecord
) {
}
