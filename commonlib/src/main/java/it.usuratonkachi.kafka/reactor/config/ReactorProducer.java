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

    public Flux<SenderResult<Object>> send(Flux<SenderRecord<byte[], byte[], Object>> messageSource){
        return producer.send(messageSource);
    }

    private Map<String, Object> kafkaProducerConfiguration() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hosts,
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
        );
    }

    private SenderOptions<byte[], byte[]> kafkaSenderOptions() {
        return SenderOptions.create(kafkaProducerConfiguration());
    }

    private KafkaSender<byte[], byte[]> reactiveKafkaSender() {
        return KafkaSender.create(kafkaSenderOptions());
    }

}

