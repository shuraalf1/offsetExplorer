package com.offsetexplorer.service;

import com.offsetexplorer.model.BrokerInfo;
import com.offsetexplorer.model.ConsumerGroupLagInfo;
import com.offsetexplorer.model.EnvironmentProfile;
import com.offsetexplorer.model.OffsetResetStrategy;
import com.offsetexplorer.model.PartitionOffsetInfo;
import com.offsetexplorer.model.TopicSummary;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Servicio de administración de Apache Kafka encargado de:
 * - Inspeccionar Brokers y metadatos del clúster.
 * - Listar, crear y eliminar tópicos junto con sus offsets de inicio/fin por partición.
 * - Consultar Consumer Groups y calcular el Lag por partición.
 * - Ejecutar el reseteo de offsets (Earliest, Latest, Offset específico o Timestamp).
 */
public class KafkaAdminService {

    /**
     * Obtiene la lista de nodos Broker del clúster e identifica cuál es el nodo controlador.
     */
    public List<BrokerInfo> listBrokers(EnvironmentProfile profile) throws Exception {
        try (AdminClient admin = KafkaClientFactory.createAdminClient(profile)) {
            DescribeClusterResult cluster = admin.describeCluster();
            Collection<Node> nodes = cluster.nodes().get(10, TimeUnit.SECONDS);
            Node controller = cluster.controller().get(10, TimeUnit.SECONDS);
            int controllerId = controller != null ? controller.id() : -1;

            List<BrokerInfo> brokers = new ArrayList<>();
            for (Node n : nodes) {
                brokers.add(new BrokerInfo(
                        n.id(),
                        n.host(),
                        n.port(),
                        n.rack() != null ? n.rack() : "-",
                        n.id() == controllerId
                ));
            }
            brokers.sort(Comparator.comparingInt(BrokerInfo::id));
            return brokers;
        }
    }

    /**
     * Lista todos los tópicos del clúster calculando los offsets de inicio y fin de cada partición.
     */
    public List<TopicSummary> listTopicsWithOffsets(EnvironmentProfile profile, boolean includeInternal) throws Exception {
        try (AdminClient admin = KafkaClientFactory.createAdminClient(profile)) {
            org.apache.kafka.clients.admin.ListTopicsOptions options =
                    new org.apache.kafka.clients.admin.ListTopicsOptions().listInternal(includeInternal);
            Collection<TopicListing> listings = admin.listTopics(options).listings().get(10, TimeUnit.SECONDS);
            if (listings.isEmpty()) {
                return Collections.emptyList();
            }

            List<String> topicNames = listings.stream().map(TopicListing::name).toList();
            Map<String, TopicDescription> descriptions =
                    admin.describeTopics(topicNames).allTopicNames().get(15, TimeUnit.SECONDS);

            // Preparar consultas de earliest y latest offset para todas las particiones
            Map<TopicPartition, OffsetSpec> earliestQuery = new HashMap<>();
            Map<TopicPartition, OffsetSpec> latestQuery = new HashMap<>();

            for (TopicDescription desc : descriptions.values()) {
                for (TopicPartitionInfo p : desc.partitions()) {
                    TopicPartition tp = new TopicPartition(desc.name(), p.partition());
                    earliestQuery.put(tp, OffsetSpec.earliest());
                    latestQuery.put(tp, OffsetSpec.latest());
                }
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
                    earliestQuery.isEmpty() ? Collections.emptyMap()
                            : admin.listOffsets(earliestQuery).all().get(15, TimeUnit.SECONDS);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                    latestQuery.isEmpty() ? Collections.emptyMap()
                            : admin.listOffsets(latestQuery).all().get(15, TimeUnit.SECONDS);

            List<TopicSummary> summaries = new ArrayList<>();
            for (TopicDescription desc : descriptions.values()) {
                List<PartitionOffsetInfo> partitionInfos = new ArrayList<>();
                long totalMessages = 0L;
                int replicationFactor = 0;

                for (TopicPartitionInfo p : desc.partitions()) {
                    TopicPartition tp = new TopicPartition(desc.name(), p.partition());
                    long begin = earliestOffsets.containsKey(tp) ? earliestOffsets.get(tp).offset() : 0L;
                    long end = latestOffsets.containsKey(tp) ? latestOffsets.get(tp).offset() : 0L;
                    String leader = p.leader() != null ? p.leader().host() + ":" + p.leader().port() + " (id=" + p.leader().id() + ")" : "N/A";
                    int replicas = p.replicas() != null ? p.replicas().size() : 0;
                    int isr = p.isr() != null ? p.isr().size() : 0;
                    replicationFactor = Math.max(replicationFactor, replicas);

                    PartitionOffsetInfo poi = new PartitionOffsetInfo(
                            desc.name(), p.partition(), leader, replicas, isr, begin, end
                    );
                    partitionInfos.add(poi);
                    totalMessages += poi.messageCount();
                }

                partitionInfos.sort(Comparator.comparingInt(PartitionOffsetInfo::partition));
                summaries.add(new TopicSummary(
                        desc.name(),
                        desc.isInternal(),
                        partitionInfos.size(),
                        replicationFactor,
                        totalMessages,
                        partitionInfos
                ));
            }

            summaries.sort(Comparator.comparing(TopicSummary::name));
            return summaries;
        }
    }

    /**
     * Crea un nuevo tópico en el clúster activo.
     */
    public void createTopic(EnvironmentProfile profile, String topicName, int partitions, short replicationFactor) throws Exception {
        try (AdminClient admin = KafkaClientFactory.createAdminClient(profile)) {
            NewTopic newTopic = new NewTopic(topicName.trim(), partitions, replicationFactor);
            admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Elimina un tópico del clúster activo.
     */
    public void deleteTopic(EnvironmentProfile profile, String topicName) throws Exception {
        try (AdminClient admin = KafkaClientFactory.createAdminClient(profile)) {
            admin.deleteTopics(List.of(topicName)).all().get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * Lista los identificadores de todos los Consumer Groups registrados en el clúster.
     */
    public List<String> listConsumerGroupIds(EnvironmentProfile profile) throws Exception {
        try (AdminClient admin = KafkaClientFactory.createAdminClient(profile)) {
            Collection<ConsumerGroupListing> groups = admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS);
            return groups.stream()
                    .map(ConsumerGroupListing::groupId)
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    /**
     * Obtiene el detalle de offsets confirmados (committed), fin de log y Lag por partición
     * para un Consumer Group específico.
     */
    public List<ConsumerGroupLagInfo> getConsumerGroupLag(EnvironmentProfile profile, String groupId) throws Exception {
        try (AdminClient admin = KafkaClientFactory.createAdminClient(profile)) {
            Map<String, ConsumerGroupDescription> descMap =
                    admin.describeConsumerGroups(List.of(groupId)).all().get(10, TimeUnit.SECONDS);
            ConsumerGroupDescription groupDesc = descMap.get(groupId);
            String state = groupDesc != null && groupDesc.state() != null ? groupDesc.state().toString() : "UNKNOWN";

            Map<TopicPartition, String> partitionMemberHost = new HashMap<>();
            if (groupDesc != null && groupDesc.members() != null) {
                for (MemberDescription member : groupDesc.members()) {
                    if (member.assignment() != null && member.assignment().topicPartitions() != null) {
                        for (TopicPartition tp : member.assignment().topicPartitions()) {
                            partitionMemberHost.put(tp, member.clientId() + " (" + member.host() + ")");
                        }
                    }
                }
            }

            Map<TopicPartition, OffsetAndMetadata> committedMap =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);

            if (committedMap == null || committedMap.isEmpty()) {
                return Collections.emptyList();
            }

            Map<TopicPartition, OffsetSpec> earliestQuery = new HashMap<>();
            Map<TopicPartition, OffsetSpec> latestQuery = new HashMap<>();
            for (TopicPartition tp : committedMap.keySet()) {
                earliestQuery.put(tp, OffsetSpec.earliest());
                latestQuery.put(tp, OffsetSpec.latest());
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestMap =
                    admin.listOffsets(earliestQuery).all().get(10, TimeUnit.SECONDS);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestMap =
                    admin.listOffsets(latestQuery).all().get(10, TimeUnit.SECONDS);

            List<ConsumerGroupLagInfo> result = new ArrayList<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committedMap.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata om = entry.getValue();
                if (om == null) {
                    continue;
                }
                long committed = om.offset();
                long begin = earliestMap.containsKey(tp) ? earliestMap.get(tp).offset() : 0L;
                long end = latestMap.containsKey(tp) ? latestMap.get(tp).offset() : 0L;
                long lag = Math.max(0L, end - committed);
                String member = partitionMemberHost.getOrDefault(tp, "<Sin consumidor activo>");

                result.add(new ConsumerGroupLagInfo(
                        groupId,
                        state,
                        tp.topic(),
                        tp.partition(),
                        committed,
                        begin,
                        end,
                        lag,
                        member
                ));
            }

            result.sort(Comparator.comparing(ConsumerGroupLagInfo::topic)
                    .thenComparingInt(ConsumerGroupLagInfo::partition));
            return result;
        }
    }

    /**
     * Resetea los offsets de un Consumer Group para un tópico (una partición específica o todas)
     * utilizando la estrategia seleccionada (EARLIEST, LATEST, SPECIFIC_OFFSET o TIMESTAMP).
     *
     * @return Mapa con las particiones actualizadas y el nuevo offset asignado.
     */
    public Map<TopicPartition, Long> resetConsumerGroupOffsets(
            EnvironmentProfile profile,
            String groupId,
            String topic,
            Integer partitionOrNullForAll,
            OffsetResetStrategy strategy,
            long targetValue
    ) throws Exception {
        try (AdminClient admin = KafkaClientFactory.createAdminClient(profile)) {
            Map<String, TopicDescription> descMap =
                    admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS);
            TopicDescription topicDesc = descMap.get(topic);
            if (topicDesc == null) {
                throw new IllegalArgumentException("El tópico '" + topic + "' no existe en el clúster.");
            }

            List<TopicPartition> targetPartitions = new ArrayList<>();
            for (TopicPartitionInfo p : topicDesc.partitions()) {
                if (partitionOrNullForAll == null || partitionOrNullForAll < 0 || p.partition() == partitionOrNullForAll) {
                    targetPartitions.add(new TopicPartition(topic, p.partition()));
                }
            }

            if (targetPartitions.isEmpty()) {
                throw new IllegalArgumentException("No se encontraron particiones válidas para el tópico " + topic);
            }

            Map<TopicPartition, OffsetAndMetadata> newOffsets = new HashMap<>();
            Map<TopicPartition, Long> appliedOffsets = new HashMap<>();

            if (strategy == OffsetResetStrategy.SPECIFIC_OFFSET) {
                long safeOffset = Math.max(0L, targetValue);
                for (TopicPartition tp : targetPartitions) {
                    newOffsets.put(tp, new OffsetAndMetadata(safeOffset));
                    appliedOffsets.put(tp, safeOffset);
                }
            } else {
                Map<TopicPartition, OffsetSpec> specQuery = new HashMap<>();
                for (TopicPartition tp : targetPartitions) {
                    OffsetSpec spec = switch (strategy) {
                        case EARLIEST -> OffsetSpec.earliest();
                        case LATEST -> OffsetSpec.latest();
                        case TIMESTAMP -> OffsetSpec.forTimestamp(targetValue);
                        default -> OffsetSpec.latest();
                    };
                    specQuery.put(tp, spec);
                }

                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsetResults =
                        admin.listOffsets(specQuery).all().get(10, TimeUnit.SECONDS);

                // Para TIMESTAMP, si no hay ningún mensaje posterior al timestamp dado, Kafka devuelve offset = -1.
                // En ese caso consultamos el offset latest como respaldo seguro.
                Map<TopicPartition, OffsetSpec> fallbackLatestQuery = new HashMap<>();
                for (TopicPartition tp : targetPartitions) {
                    ListOffsetsResult.ListOffsetsResultInfo info = offsetResults.get(tp);
                    if (info == null || info.offset() < 0) {
                        fallbackLatestQuery.put(tp, OffsetSpec.latest());
                    }
                }
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> fallbackLatest =
                        fallbackLatestQuery.isEmpty() ? Collections.emptyMap()
                                : admin.listOffsets(fallbackLatestQuery).all().get(10, TimeUnit.SECONDS);

                for (TopicPartition tp : targetPartitions) {
                    ListOffsetsResult.ListOffsetsResultInfo info = offsetResults.get(tp);
                    long resolvedOffset;
                    if (info != null && info.offset() >= 0) {
                        resolvedOffset = info.offset();
                    } else {
                        resolvedOffset = fallbackLatest.containsKey(tp) ? fallbackLatest.get(tp).offset() : 0L;
                    }
                    newOffsets.put(tp, new OffsetAndMetadata(resolvedOffset));
                    appliedOffsets.put(tp, resolvedOffset);
                }
            }

            admin.alterConsumerGroupOffsets(groupId, newOffsets).all().get(10, TimeUnit.SECONDS);
            return appliedOffsets;
        }
    }

    /**
     * Asegura que una lista de tópicos exista en el clúster (útil para pruebas locales de trazabilidad).
     */
    public void ensureTopicsExist(EnvironmentProfile profile, List<String> topics) throws Exception {
        try (AdminClient admin = KafkaClientFactory.createAdminClient(profile)) {
            Set<String> existing = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            List<NewTopic> toCreate = new ArrayList<>();
            for (String t : topics) {
                String clean = t.trim();
                if (!clean.isEmpty() && !existing.contains(clean)) {
                    toCreate.add(new NewTopic(clean, 3, (short) 1));
                }
            }
            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get(10, TimeUnit.SECONDS);
            }
        }
    }
}
