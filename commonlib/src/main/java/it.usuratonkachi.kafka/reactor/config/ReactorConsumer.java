package it.usuratonkachi.kafka.reactor.config;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ReactorConsumer {

    private final KafkaConsumerProperties kafkaConsumerProperties;
    private final BindingProperties bindingProperties;
    private final ConsumerProperties consumerProperties;
    private final String hosts;
    private final String group;
    @Getter
    private List<Integer> assignedPartitions;
    @Getter
    private KafkaReceiver<byte[], byte[]> receiver;

    private Map<Integer, List<ReceiverRecord<byte[], byte[]>>> toAck = new ConcurrentHashMap<>();

    public void toBeAcked(ReceiverRecord<byte[], byte[]> receiverRecord){
        List<ReceiverRecord<byte[], byte[]>> pendingAck = toAck.getOrDefault(receiverRecord.partition(), new ArrayList<>());
        pendingAck.add(receiverRecord);
    }

    public void ackRecord(ReceiverRecord<byte[], byte[]> receiverRecord){
        receiverRecord.receiverOffset().acknowledge();
        receiverRecord.receiverOffset().commit().subscribe();
        Optional.ofNullable(toAck.get(receiverRecord.partition())).ifPresent(pendingAck -> pendingAck.remove(receiverRecord));
    }

    public ReactorConsumer(
            KafkaConsumerProperties kafkaConsumerProperties, BindingProperties bindingProperties, ConsumerProperties consumerProperties, String hosts, String group){
        this.kafkaConsumerProperties = kafkaConsumerProperties;
        this.bindingProperties = bindingProperties;
        this.consumerProperties = consumerProperties;
        this.hosts = hosts;
        this.group = group;
        this.receiver = reactiveKafkaReceiver();
    }

    public Flux<ReceiverRecord<byte[], byte[]>> receive(){
        return receiver.receive()
                .doOnError(Throwable::printStackTrace);
    }

    public Flux<ConsumerRecord<byte[], byte[]>> receiveAtmostOnce(){
        return receiver.receiveAtmostOnce()
                .doOnError(Throwable::printStackTrace);
    }

    public Boolean hasPartitionAssigned(int partition){
        return assignedPartitions.contains(partition);
    }

    private Map<String, Object> kafkaConsumerConfiguration() {
        return Map.of(
                ConsumerConfig.CLIENT_ID_CONFIG, bindingProperties.getDestination() + "-" + UUID.randomUUID().toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hosts,
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1",
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor",
                //ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true",
                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100"
        );
    }

    private ReceiverOptions<byte[], byte[]> kafkaReceiverOptions() {
        ReceiverOptions<byte[], byte[]> options = ReceiverOptions.create(kafkaConsumerConfiguration());
        return options.subscription(Arrays.asList(this.bindingProperties.getDestination()))
                .withKeyDeserializer(new ByteArrayDeserializer())
                .withValueDeserializer(new ByteArrayDeserializer())
                .addAssignListener(receiverPartitions -> {
                    assignedPartitions = receiverPartitions.stream().map(receiverPartition -> receiverPartition.topicPartition().partition()).collect(Collectors.toList());
                })
                .addRevokeListener(receiverPartitions -> {
                    toAck.values().stream().flatMap(Collection::stream).forEach(this::ackRecord);
                    List<Integer> revokedPartition = receiverPartitions.stream().map(receiverPartition -> receiverPartition.topicPartition().partition()).collect(Collectors.toList());
                    assignedPartitions = assignedPartitions.stream().filter(assignedPartition -> !revokedPartition.contains(assignedPartition)).collect(Collectors.toList());
                })
                .maxCommitAttempts(0);
    }

    private KafkaReceiver<byte[], byte[]> reactiveKafkaReceiver() {
        return KafkaReceiver.create(kafkaReceiverOptions());
    }

}

