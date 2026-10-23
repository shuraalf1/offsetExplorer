package com.offsetexplorer.model;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Representa un mensaje individual consumido de una partición de Apache Kafka.
 */
public record KafkaMessageRecord(
        String topic,
        int partition,
        long offset,
        long timestampMillis,
        String timestampType,
        String key,
        byte[] rawValue,
        String stringValue,
        Map<String, String> headers
) {
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    public String formattedTimestamp() {
        if (timestampMillis <= 0) {
            return "N/A";
        }
        return FORMATTER.format(Instant.ofEpochMilli(timestampMillis));
    }

    public int sizeBytes() {
        return rawValue != null ? rawValue.length : 0;
    }
}
