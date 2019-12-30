package it.usuratonkachi.kafka.reactor.config;

import lombok.Getter;
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
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class ReactiveKafkaConfiguration<T> {

	@Getter
	private String topicName;
	@Getter
	private String labelName;
	@Getter
	private KafkaReceiver<String, T> consumer;
	@Getter
	private KafkaSender<String, T> producer;

	public ReactiveKafkaConfiguration(ReactiveKafkaProperties reactiveKafkaProperties, String labelTopicName){
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

			configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
			configuration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			configuration.put(ConsumerConfig.GROUP_ID_CONFIG, bindingProperties.getGroup());
			configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
			configuration.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "900000");
			configuration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
			configuration.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
			configuration.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
			//configuration.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");
			configuration.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RangeAssignor");

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

		private ReceiverOptions<String, T> kafkaReceiverOptions() {
			ReceiverOptions<String, T> options = ReceiverOptions.create(kafkaConsumerConfiguration());

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

			/*configuration.put((ProducerConfig.METADATA_MAX_AGE_CONFIG, );
			configuration.put((ProducerConfig.BATCH_SIZE_CONFIG, );
			configuration.put((ProducerConfig.ACKS_CONFIG, );
			configuration.put((ProducerConfig.LINGER_MS_CONFIG, );
			configuration.put((ProducerConfig.CLIENT_ID_CONFIG, );
			configuration.put((ProducerConfig.SEND_BUFFER_CONFIG, );
			configuration.put((ProducerConfig.RECEIVE_BUFFER_CONFIG, );
			configuration.put((ProducerConfig.MAX_REQUEST_SIZE_CONFIG, );
			configuration.put((ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, );
			configuration.put((ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, );
			configuration.put((ProducerConfig.MAX_BLOCK_MS_CONFIG, );
			configuration.put((ProducerConfig.BUFFER_MEMORY_CONFIG, );
			configuration.put((ProducerConfig.RETRY_BACKOFF_MS_CONFIG, );
			configuration.put((ProducerConfig.COMPRESSION_TYPE_CONFIG, );
			configuration.put((ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, );
			configuration.put((ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, );
			configuration.put((ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, );
			configuration.put((ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, );
			configuration.put((ProducerConfig.RETRIES_CONFIG, );
			configuration.put((ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, );
			configuration.put((ProducerConfig.PARTITIONER_CLASS_CONFIG, );
			configuration.put((ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, );
			configuration.put((ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, );
			configuration.put((ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, );
			configuration.put((ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, );
			configuration.put((ProducerConfig.TRANSACTIONAL_ID_CONFIG, );*/

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
