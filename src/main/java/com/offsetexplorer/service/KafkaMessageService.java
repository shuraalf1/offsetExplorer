package com.offsetexplorer.service;

import com.offsetexplorer.model.EnvironmentProfile;
import com.offsetexplorer.model.KafkaMessageRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Servicio para consumir mensajes bajo demanda (sin alterar Consumer Groups)
 * y publicar mensajes de prueba o flujos de eventos de demostración.
 */
public class KafkaMessageService {

    /** Modos de posicionamiento de lectura. */
    public enum SeekMode {
        LAST_N_MESSAGES("Últimos N mensajes (Tail)"),
        EARLIEST("Desde el inicio (Earliest)"),
        CUSTOM_OFFSET("Desde un Offset específico"),
        FROM_TIMESTAMP("Desde un Timestamp (Epoch ms)");

        private final String label;

        SeekMode(String label) {
            this.label = label;
        }

        @Override
        public String toString() {
            return label;
        }
    }

    /**
     * Consume mensajes de un tópico (una partición o todas) y aplica un filtro opcional
     * por Key, Payload o Headers. Usa assign() y no hace commit.
     */
    public List<KafkaMessageRecord> fetchMessages(
            EnvironmentProfile profile,
            String topic,
            Integer partitionOrNullForAll,
            SeekMode seekMode,
            long customOffsetOrTimestamp,
            int maxRecords,
            String filterText
    ) throws Exception {
        int limit = Math.max(1, Math.min(maxRecords, 5000));
        String filter = (filterText != null && !filterText.isBlank()) ? filterText.trim().toLowerCase() : null;

        try (KafkaConsumer<byte[], byte[]> consumer = KafkaClientFactory.createReadOnlyConsumer(profile)) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic, Duration.ofSeconds(8));
            if (infos == null || infos.isEmpty()) {
                return List.of();
            }
            List<TopicPartition> partitions = new ArrayList<>();
            for (PartitionInfo pi : infos) {
                if (partitionOrNullForAll == null || partitionOrNullForAll < 0 || pi.partition() == partitionOrNullForAll) {
                    partitions.add(new TopicPartition(topic, pi.partition()));
                }
            }
            if (partitions.isEmpty()) {
                return List.of();
            }

            consumer.assign(partitions);
            Map<TopicPartition, Long> begins = consumer.beginningOffsets(partitions, Duration.ofSeconds(8));
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions, Duration.ofSeconds(8));

            List<TopicPartition> active = new ArrayList<>();
            Map<TopicPartition, Long> tsQuery = new HashMap<>();
            for (TopicPartition tp : partitions) {
                long begin = begins.getOrDefault(tp, 0L);
                long end = ends.getOrDefault(tp, 0L);
                if (end <= begin) {
                    continue;
                }
                switch (seekMode) {
                    case EARLIEST -> { consumer.seek(tp, begin); active.add(tp); }
                    case LAST_N_MESSAGES -> { consumer.seek(tp, Math.max(begin, end - limit)); active.add(tp); }
                    case CUSTOM_OFFSET -> {
                        long off = Math.max(begin, customOffsetOrTimestamp);
                        if (off < end) { consumer.seek(tp, off); active.add(tp); }
                    }
                    case FROM_TIMESTAMP -> tsQuery.put(tp, customOffsetOrTimestamp);
                }
            }
            if (!tsQuery.isEmpty()) {
                Map<TopicPartition, OffsetAndTimestamp> res = consumer.offsetsForTimes(tsQuery, Duration.ofSeconds(8));
                for (Map.Entry<TopicPartition, OffsetAndTimestamp> e : res.entrySet()) {
                    if (e.getValue() != null && e.getValue().offset() < ends.getOrDefault(e.getKey(), 0L)) {
                        consumer.seek(e.getKey(), e.getValue().offset());
                        active.add(e.getKey());
                    }
                }
            }

            List<KafkaMessageRecord> collected = new ArrayList<>();
            int emptyPolls = 0;
            while (collected.size() < limit && emptyPolls < 3 && !active.isEmpty()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(700));
                if (records.isEmpty()) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                    for (ConsumerRecord<byte[], byte[]> rec : records) {
                        KafkaMessageRecord m = toMessageRecord(rec);
                        if (matchesFilter(m, filter)) {
                            collected.add(m);
                            if (collected.size() >= limit) {
                                break;
                            }
                        }
                    }
                }
                active.removeIf(tp -> {
                    if (consumer.position(tp) >= ends.getOrDefault(tp, 0L)) {
                        consumer.pause(List.of(tp));
                        return true;
                    }
                    return false;
                });
            }

            collected.sort(Comparator.comparingLong(KafkaMessageRecord::timestampMillis)
                    .thenComparingInt(KafkaMessageRecord::partition)
                    .thenComparingLong(KafkaMessageRecord::offset));
            return collected;
        }
    }

    /** Publica un mensaje con Key, Payload y Headers. */
    public RecordMetadata publishMessage(
            EnvironmentProfile profile, String topic, Integer partitionOrNull,
            String key, String value, Map<String, String> headers
    ) throws Exception {
        try (KafkaProducer<byte[], byte[]> producer = KafkaClientFactory.createProducer(profile)) {
            byte[] k = (key != null && !key.isEmpty()) ? key.getBytes(StandardCharsets.UTF_8) : null;
            byte[] v = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
            Integer p = (partitionOrNull != null && partitionOrNull >= 0) ? partitionOrNull : null;
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic.trim(), p, k, v);
            if (headers != null) {
                headers.forEach((hk, hv) -> {
                    if (hk != null && !hk.isBlank()) {
                        record.headers().add(hk.trim(), hv != null ? hv.getBytes(StandardCharsets.UTF_8) : new byte[0]);
                    }
                });
            }
            return producer.send(record).get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Publica un evento con la misma Key en cada tópico de la lista, para probar la trazabilidad.
     */
    public int publishDemoTraceFlow(EnvironmentProfile profile, List<String> topics, String eventKey) throws Exception {
        new KafkaAdminService().ensureTopicsExist(profile, topics);
        int published = 0;
        try (KafkaProducer<byte[], byte[]> producer = KafkaClientFactory.createProducer(profile)) {
            byte[] keyBytes = eventKey.getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < topics.size(); i++) {
                String topic = topics.get(i).trim();
                if (topic.isEmpty()) {
                    continue;
                }
                long now = System.currentTimeMillis();
                String json = String.format(
                        "{\"eventId\":\"%s\",\"hopStep\":%d,\"currentTopic\":\"%s\",\"timestampIso\":\"%s\",\"amount\":1450.75,\"currency\":\"MXN\"}",
                        eventKey, i + 1, topic, Instant.ofEpochMilli(now));
                ProducerRecord<byte[], byte[]> r = new ProducerRecord<>(topic, null, now, keyBytes,
                        json.getBytes(StandardCharsets.UTF_8));
                r.headers().add("hop-index", String.valueOf(i + 1).getBytes(StandardCharsets.UTF_8));
                r.headers().add("environment", profile.getEnvironmentTag().getBytes(StandardCharsets.UTF_8));
                producer.send(r).get(10, TimeUnit.SECONDS);
                published++;
                Thread.sleep(65);
            }
        }
        return published;
    }

    /** Convierte un ConsumerRecord de Kafka al modelo de la aplicación. */
    public static KafkaMessageRecord toMessageRecord(ConsumerRecord<byte[], byte[]> rec) {
        String key = rec.key() != null ? new String(rec.key(), StandardCharsets.UTF_8) : "<null>";
        byte[] raw = rec.value();
        String val = raw != null ? new String(raw, StandardCharsets.UTF_8) : "";
        Map<String, String> headers = new LinkedHashMap<>();
        for (Header h : rec.headers()) {
            headers.put(h.key(), h.value() != null ? new String(h.value(), StandardCharsets.UTF_8) : "<null>");
        }
        return new KafkaMessageRecord(rec.topic(), rec.partition(), rec.offset(), rec.timestamp(),
                rec.timestampType().name(), key, raw, val, headers);
    }

    private boolean matchesFilter(KafkaMessageRecord rec, String f) {
        if (f == null) {
            return true;
        }
        if (rec.key().toLowerCase().contains(f) || rec.stringValue().toLowerCase().contains(f)) {
            return true;
        }
        for (Map.Entry<String, String> e : rec.headers().entrySet()) {
            if (e.getKey().toLowerCase().contains(f) || e.getValue().toLowerCase().contains(f)) {
                return true;
            }
        }
        return false;
    }
}
