package it.usuratonkachi.kafka.reactor.config;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class ReactiveKafkaConfiguration {

	@Getter
	private String topicName;
	@Getter
	private String labelName;
	@Getter
	private KafkaReceiver<byte[], byte[]> consumer;
	@Getter
	private KafkaSender<byte[], byte[]> producer;

	public ReactiveKafkaConfiguration(ReactiveKafkaProperties reactiveKafkaProperties, String labelTopicName){
		labelName = labelTopicName;
		topicName = reactiveKafkaProperties.getBindingServiceProperties().getBindingDestination(labelName);
		consumer = new ConsumerConfiguration(reactiveKafkaProperties.getKafkaProperties(), reactiveKafkaProperties.getBindingServiceProperties().getBindingProperties(labelName)).builder();
		producer = new ProducerConfiguration(reactiveKafkaProperties.getKafkaProperties(), reactiveKafkaProperties.getBindingServiceProperties().getBindingProperties(labelName)).builder();
	}

	static class ConsumerConfiguration {

		private final KafkaProperties kafkaProperties;
		private final BindingProperties bindingProperties;

		public KafkaReceiver<byte[], byte[]> builder(){
			return reactiveKafkaReceiver();
		}

		public ConsumerConfiguration(KafkaProperties kafkaProperties, BindingProperties bindingProperties){
			this.kafkaProperties = kafkaProperties;
			this.bindingProperties = bindingProperties;

		}

		private Map<String, Object> kafkaConsumerConfiguration() {
			return Map.of(
					ConsumerConfig.CLIENT_ID_CONFIG, bindingProperties.getDestination() + "-" + UUID.randomUUID().toString(),
					ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
					ConsumerConfig.GROUP_ID_CONFIG, bindingProperties.getDestination(),
					ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers(),
					ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "900000",
					ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1",
					ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor",
					ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"
					//ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true",
					//ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100"
			);
		}

		private ReceiverOptions<byte[], byte[]> kafkaReceiverOptions() {
			ReceiverOptions<byte[], byte[]> options = ReceiverOptions.create(kafkaConsumerConfiguration());

			return options.subscription(Arrays.asList(this.bindingProperties.getDestination()))
					.withKeyDeserializer(new ByteArrayDeserializer())
					.withValueDeserializer(new ByteArrayDeserializer());
		}

		private KafkaReceiver<byte[], byte[]> reactiveKafkaReceiver() {
			return KafkaReceiver.create(kafkaReceiverOptions());
		}

	}

	static class ProducerConfiguration {

		private final KafkaProperties kafkaProperties;
		private final BindingProperties bindingProperties;

		public KafkaSender<byte[], byte[]> builder(){
			return reactiveKafkaSender();
		}

		public ProducerConfiguration(KafkaProperties kafkaProperties, BindingProperties bindingProperties){
			this.kafkaProperties = kafkaProperties;
			this.bindingProperties = bindingProperties;
		}

		private Map<String, Object> kafkaProducerConfiguration() {
			return Map.of(
					ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers(),
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

	public static <T> Collector<T, ?, T> toSingleton() {
		return Collectors.collectingAndThen(
				Collectors.toList(),
				list -> {
					if (list.size() != 1) {
						throw new RuntimeException("More than one result");
					}
					return list.get(0);
				}
		);
	}

}
