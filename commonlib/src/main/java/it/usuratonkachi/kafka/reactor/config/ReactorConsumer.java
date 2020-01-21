package it.usuratonkachi.kafka.reactor.config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.kafka.listener.ContainerProperties;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
public class ReactorConsumer {

    private final KafkaConsumerProperties kafkaConsumerProperties;
    private final BindingProperties bindingProperties;
    private final ConsumerProperties consumerProperties;
    private final String destination;
    private final String hosts;
    private final String group;
    @Getter
    private List<Integer> assignedPartitions;
    @Getter
    private KafkaReceiver<byte[], byte[]> receiver;
    private ContainerProperties.AckMode ackMode = ContainerProperties.AckMode.MANUAL;

    private Map<TopicPartition, ReceiverRecord<byte[], byte[]>> offsetsToCommit = new ConcurrentHashMap<>();

    public synchronized void toBeAcked(ReceiverRecord<byte[], byte[]> receiverRecord){
        Optional.ofNullable(offsetsToCommit.get(receiverRecord.receiverOffset().topicPartition())).ifPresentOrElse(oldReceiverRecord-> {
            if (oldReceiverRecord.receiverOffset().offset() < receiverRecord.receiverOffset().offset()) offsetsToCommit.put(receiverRecord.receiverOffset().topicPartition(), receiverRecord);
        }, () -> offsetsToCommit.put(receiverRecord.receiverOffset().topicPartition(), receiverRecord));
    }

    public synchronized void ackRecord(ReceiverRecord<byte[], byte[]> receiverRecord){
        Optional.ofNullable(offsetsToCommit.get(receiverRecord.receiverOffset().topicPartition()))
                .ifPresent(offsetAndMetadata -> {
                    receiverRecord.receiverOffset().acknowledge();
                    receiverRecord.receiverOffset().commit().subscribe();
                    Optional.ofNullable(offsetsToCommit.get(receiverRecord.receiverOffset().topicPartition()))
                            .ifPresent(oof -> {
                                if (oof.receiverOffset().offset() == receiverRecord.receiverOffset().offset())
                                    offsetsToCommit.remove(receiverRecord.receiverOffset().topicPartition());
                            });
                });
    }

    public Boolean hasManualAck(){
        return ContainerProperties.AckMode.MANUAL.equals(ackMode);
    }

    public ReactorConsumer(String hosts, String group, String labelNameDestinationIsMissing){
        kafkaConsumerProperties = new KafkaConsumerProperties();
        bindingProperties = new BindingProperties();
        consumerProperties = new ConsumerProperties();
        this.destination = labelNameDestinationIsMissing;
        this.hosts = hosts;
        this.group = group;
        this.receiver = reactiveKafkaReceiver();
    }

    public ReactorConsumer(
            KafkaConsumerProperties kafkaConsumerProperties, BindingProperties bindingProperties, ConsumerProperties consumerProperties, String hosts, String group){
        this.kafkaConsumerProperties = kafkaConsumerProperties;
        this.destination = bindingProperties.getDestination();
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
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, destination + "-" + UUID.randomUUID().toString());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hosts);

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConsumerProperties.getConfiguration().getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, kafkaConsumerProperties.getConfiguration().getOrDefault(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor"));

        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaConsumerProperties.getConfiguration().getOrDefault(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "120000"));
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConsumerProperties.getConfiguration().getOrDefault(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaConsumerProperties.getConfiguration().getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"));

        if ("true".equals(properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))) {
            properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, kafkaConsumerProperties.getConfiguration().getOrDefault(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "false"));
            ackMode = ContainerProperties.AckMode.RECORD;
        }

        return properties;
    }

    private ReceiverOptions<byte[], byte[]> kafkaReceiverOptions() {
        ReceiverOptions<byte[], byte[]> options = ReceiverOptions.create(kafkaConsumerConfiguration());
        return options.subscription(Arrays.asList(this.bindingProperties.getDestination().split(",")))
                .pollTimeout(Duration.ofMillis(120000))
                .withKeyDeserializer(new ByteArrayDeserializer())
                .withValueDeserializer(new ByteArrayDeserializer())
                .addAssignListener(receiverPartitions -> {
                    // org.springframework.kafka.listener.KafkaMessageListenerContainer:1721:onPartitionsAssigned
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                    assignedPartitions = receiverPartitions.stream()
                            .peek(receiverPartition -> offsetsToCommit.put(receiverPartition.topicPartition(), new OffsetAndMetadata(receiverPartition.position())))
                            .map(receiverPartition -> receiverPartition.topicPartition().partition())
                            .collect(Collectors.toList());
                    ((DefaultKafkaReceiver<byte[], byte[]>)receiver).commitSync(offsetsToCommit);
                })
                .addRevokeListener(receiverPartitions -> {
                    if (!offsetsToCommit.isEmpty()){
                        offsetsToCommit.entrySet().stream().filter(Objects::nonNull)
                                .peek(topicPartitionReceiverRecordEntry ->
                                                log.warn(String.format("Rebalancing, acking element with topic %s partition %s and offset %s",
                                                        topicPartitionReceiverRecordEntry.getKey().topic(),
                                                        topicPartitionReceiverRecordEntry.getKey().partition(),
                                                        topicPartitionReceiverRecordEntry.getValue().offset()
                                                        ))
                                        )
                                .forEach(topicPartitionReceiverRecordEntry -> this.ackRecord(topicPartitionReceiverRecordEntry.getValue()));
                    }

                    List<Integer> revokedPartition = receiverPartitions.stream()
                            .map(receiverPartition -> receiverPartition.topicPartition().partition())
                            .collect(Collectors.toList());

                    assignedPartitions = assignedPartitions.stream()
                            .filter(assignedPartition -> !revokedPartition.contains(assignedPartition))
                            .collect(Collectors.toList());
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                })
                .maxCommitAttempts(0);
    }

    private KafkaReceiver<byte[], byte[]> reactiveKafkaReceiver() {
        return KafkaReceiver.create(kafkaReceiverOptions());
    }

}

