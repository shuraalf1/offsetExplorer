package com.offsetexplorer.model;

/**
 * Información de posición de lectura y retraso (Lag) de un Consumer Group sobre una partición.
 */
public record ConsumerGroupLagInfo(
        String groupId,
        String state,
        String topic,
        int partition,
        long committedOffset,
        long beginningOffset,
        long endOffset,
        long lag,
        String memberHost
) {
}
