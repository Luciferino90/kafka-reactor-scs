package it.usuratonkachi.kafka.reactor.config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Slf4j
@Deprecated // Use ByteArray Version
public class _ReactiveKafkaConfiguration<T> {

	@Getter
	private String topicName;
	@Getter
	private String labelName;
	@Getter
	private KafkaReceiver<String, T> consumer;
	@Getter
	private KafkaSender<String, T> producer;

	public _ReactiveKafkaConfiguration(ReactiveKafkaProperties reactiveKafkaProperties, String labelTopicName){
		labelName = labelTopicName;
		topicName = reactiveKafkaProperties.getBindingServiceProperties().getBindingDestination(labelName);
		consumer = new ConsumerConfiguration<T>(reactiveKafkaProperties.getKafkaProperties(), reactiveKafkaProperties.getBindingServiceProperties().getBindingProperties(labelName)).builder();
		producer = new ProducerConfiguration<T>(reactiveKafkaProperties.getKafkaProperties(), reactiveKafkaProperties.getBindingServiceProperties().getBindingProperties(labelName)).builder();
	}

	static class ConsumerConfiguration<T> {

		private final KafkaProperties kafkaProperties;
		private final BindingProperties bindingProperties;

		public KafkaReceiver<String, T> builder(){
			return reactiveKafkaReceiver();
		}

		public ConsumerConfiguration(KafkaProperties kafkaProperties, BindingProperties bindingProperties){
			this.kafkaProperties = kafkaProperties;
			this.bindingProperties = bindingProperties;

		}

		private Map<String, Object> kafkaConsumerConfiguration() {
			Map<String, Object> configuration = new HashMap<>();
			configuration.put(ConsumerConfig.CLIENT_ID_CONFIG, bindingProperties.getDestination() + "-" + UUID.randomUUID().toString());
			configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			configuration.put(ConsumerConfig.GROUP_ID_CONFIG, bindingProperties.getDestination());
			configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
			configuration.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "900000");
			configuration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
			configuration.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor");
			configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
			configuration.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
			return configuration;
		}

		private ReceiverOptions<String, T> kafkaReceiverOptions() {
			ReceiverOptions<String, T> options = ReceiverOptions.create(kafkaConsumerConfiguration());

			options
					.addAssignListener(partitions -> {
						log.info("{} Partitions Assigned {}", options.consumerProperty(ConsumerConfig.CLIENT_ID_CONFIG), partitions);
					})
					.addRevokeListener(partitions -> {
						log.info("{} Partitions Revoked {}", options.consumerProperty(ConsumerConfig.CLIENT_ID_CONFIG), partitions);
					})
					.commitInterval(Duration.ZERO)
					.commitBatchSize(1)
					.maxCommitAttempts(0);

			JsonDeserializer<T> t = new JsonDeserializer<>();
			t.addTrustedPackages("*");

			return options.subscription(Arrays.asList(this.bindingProperties.getDestination()))
					.withKeyDeserializer(new StringDeserializer())
					.withValueDeserializer(t);
		}

		private KafkaReceiver<String, T> reactiveKafkaReceiver() {
			return KafkaReceiver.create(kafkaReceiverOptions());
		}

	}

	static class ProducerConfiguration<T> {

		private final KafkaProperties kafkaProperties;
		private final BindingProperties bindingProperties;

		public KafkaSender<String, T> builder(){
			return reactiveKafkaSender();
		}

		public ProducerConfiguration(KafkaProperties kafkaProperties, BindingProperties bindingProperties){
			this.kafkaProperties = kafkaProperties;
			this.bindingProperties = bindingProperties;
		}

		private Map<String, Object> kafkaProducerConfiguration() {
			Map<String, Object> configuration = new HashMap<>();
			configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
			configuration.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
			configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
			configuration.put(ProducerConfig.ACKS_CONFIG, "all");
			return configuration;
		}

		private SenderOptions<String, T> kafkaSenderOptions() {
			SenderOptions<String, T> options = SenderOptions.create(kafkaProducerConfiguration());
			return options;
		}

		private KafkaSender<String, T> reactiveKafkaSender() {
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
