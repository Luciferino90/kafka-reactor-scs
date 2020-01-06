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
import java.util.HashMap;
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
			Map<String, Object> configuration = new HashMap<>();

			configuration.put(ConsumerConfig.CLIENT_ID_CONFIG, bindingProperties.getDestination() + "-" + UUID.randomUUID().toString());
			configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			configuration.put(ConsumerConfig.GROUP_ID_CONFIG, bindingProperties.getDestination());
			configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
			configuration.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "900000");
			configuration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
			configuration.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor");
			//configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
			//configuration.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");

			configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			//configuration.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");

			/*configuration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, );
			configuration.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, );
			configuration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, );
			configuration.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, );
			configuration.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, );
			configuration.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, );
			configuration.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, );
			configuration.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, );
			configuration.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, );
			configuration.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, );
			configuration.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, );
			configuration.put(ConsumerConfig.SEND_BUFFER_CONFIG, );
			configuration.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, );
			configuration.put(ConsumerConfig.CLIENT_ID_CONFIG, );
			configuration.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, );
			configuration.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, );
			configuration.put(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, );
			configuration.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, );
			configuration.put(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, );
			configuration.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, );
			configuration.put(ConsumerConfig.CHECK_CRCS_CONFIG, );
			configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, );
			configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, );
			configuration.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, );
			configuration.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, );
			configuration.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, );
			configuration.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, );
			configuration.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, );
			configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, );
			configuration.put(ConsumerConfig.DEFAULT_ISOLATION_LEVEL, );*/

			return configuration;
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
			Map<String, Object> configuration = new HashMap<>();
			configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
			configuration.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
			configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
			configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
			return configuration;
		}

		private SenderOptions<byte[], byte[]> kafkaSenderOptions() {
			SenderOptions<byte[], byte[]> options = SenderOptions.create(kafkaProducerConfiguration());
			return options;
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
