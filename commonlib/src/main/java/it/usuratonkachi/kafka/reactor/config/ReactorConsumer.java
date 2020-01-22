package it.usuratonkachi.kafka.reactor.config;

import io.netty.handler.logging.LogLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.kafka.listener.ContainerProperties;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.internals.ReactorKafkaReceiver;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static it.usuratonkachi.kafka.reactor.config.LoggingUtils.log;

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

    private final BlockingQueue<ReceiverRecord<byte[], byte[]>> acks = new LinkedBlockingQueue<>();

    public synchronized void toBeAcked(ReceiverRecord<byte[], byte[]> record){
        log(LogLevel.TRACE, "Adding element with topic %s partition %s and offset %s", record);
        acks.add(record);
    }

    public synchronized void ackRecord(ReceiverRecord<byte[], byte[]> receiverRecord){
        log(LogLevel.TRACE, "Acking element with topic %s partition %s and offset %s", receiverRecord);
        receiverRecord.receiverOffset().acknowledge();
        receiverRecord.receiverOffset().commit().subscribe();
        acks.remove(receiverRecord);
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
                //.pollTimeout(Duration.ofMillis(120000))
                .withKeyDeserializer(new ByteArrayDeserializer())
                .withValueDeserializer(new ByteArrayDeserializer())
                .addAssignListener(receiverPartitions -> {
                    assignedPartitions = receiverPartitions.stream()
                            .map(receiverPartition -> receiverPartition.topicPartition().partition())
                            .collect(Collectors.toList());
                })
                .addRevokeListener(receiverPartitions -> {

                    List<Integer> revokedPartition = receiverPartitions.stream()
                            .map(receiverPartition -> receiverPartition.topicPartition().partition())
                            .collect(Collectors.toList());

                    assignedPartitions = assignedPartitions.stream()
                            .filter(assignedPartition -> !revokedPartition.contains(assignedPartition))
                            .collect(Collectors.toList());

                    ReceiverRecord<byte[], byte[]> record = this.acks.poll();
                    while (record != null) {
                        log(LogLevel.TRACE, "Rebalancing, acking element with topic %s partition %s and offset %s", record);
                        ackRecord(record);
                        record = this.acks.poll();
                    }
                })
                .maxCommitAttempts(0);
    }

    private KafkaReceiver<byte[], byte[]> reactiveKafkaReceiver() {
        return ReactorKafkaReceiver.create(kafkaReceiverOptions());
    }

}

