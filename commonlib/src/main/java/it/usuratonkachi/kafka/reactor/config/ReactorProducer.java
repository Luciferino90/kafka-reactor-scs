package it.usuratonkachi.kafka.reactor.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.util.HashMap;
import java.util.Map;

public class ReactorProducer {

    private final KafkaProducerProperties kafkaProducerProperties;
    private final ProducerProperties producerProperties;
    private final String hosts;

    private final KafkaSender<byte[], byte[]> producer;

    public ReactorProducer(
            KafkaProducerProperties kafkaProducerProperties, ProducerProperties producerProperties, String hosts){
        this.kafkaProducerProperties = kafkaProducerProperties;
        this.producerProperties = producerProperties;
        this.hosts = hosts;
        this.producer = reactiveKafkaSender();
    }

    public ReactorProducer(String hosts, String group, String labelNameDestinationIsMissing){
        this.hosts = hosts;
        producerProperties = new ProducerProperties();
        kafkaProducerProperties = new KafkaProducerProperties();
        this.producer = reactiveKafkaSender();
    }

    public Flux<SenderResult<Object>> send(Flux<SenderRecord<byte[], byte[], Object>> messageSource){
        return producer.send(messageSource);
    }

    private Map<String, Object> kafkaProducerConfiguration() {
        Map<String, String> scsProps = kafkaProducerProperties != null ? kafkaProducerProperties.getConfiguration() : new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hosts);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, scsProps.getOrDefault(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, scsProps.getOrDefault(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, ByteArraySerializer.class.getName()));
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, scsProps.getOrDefault(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, ByteArraySerializer.class.getName()));
        return properties;
    }

    private SenderOptions<byte[], byte[]> kafkaSenderOptions() {
        return SenderOptions.create(kafkaProducerConfiguration());
    }

    private KafkaSender<byte[], byte[]> reactiveKafkaSender() {
        return KafkaSender.create(kafkaSenderOptions());
    }

}

