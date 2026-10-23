package com.offsetexplorer.model;

/**
 * Información de offsets (inicio, fin y cantidad total estimada de mensajes) de una partición.
 */
public record PartitionOffsetInfo(
        String topic,
        int partition,
        String leaderHost,
        int replicasCount,
        int isrCount,
        long beginningOffset,
        long endOffset
) {
    public long messageCount() {
        return Math.max(0L, endOffset - beginningOffset);
    }
}
