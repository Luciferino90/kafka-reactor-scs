package it.usuratonkachi.kafka.reactor.config;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Classe di configurazione per i dispatcher di reactor-kafka.
 * Converte l'autoconfigurazione di Spring Cloud Stream in un'autoconfigurazione per reactor-kafka.
 */
public class ReactorKafkaConfiguration {

	@Getter
	private String topicName;
	@Getter
	private String labelName;
	@Getter
	private ConsumerConfiguration consumer;
	@Getter
	private List<String> assignedPartition;
	@Getter
	private KafkaSender<byte[], byte[]> producer;
	@Getter
	private Integer concurrency = 1;

	public ReactorKafkaConfiguration(ReactorKafkaProperties reactorKafkaProperties, String labelTopicName){
		labelName = labelTopicName;
		topicName = reactorKafkaProperties.getBindingServiceProperties().getBindingDestination(labelName);

		String port = reactorKafkaProperties.getKafkaBinderConfigurationProperties().getBrokers()[reactorKafkaProperties.getKafkaBinderConfigurationProperties().getBrokers().length - 1].split(":")[1];
		String hosts = Arrays.stream(reactorKafkaProperties.getKafkaBinderConfigurationProperties().getBrokers()).map(host -> host.split(":")[0] + ":" + port).collect(Collectors.joining(","));

		Optional<BindingProperties> bindingPropertiesConsumer = reactorKafkaProperties.getBindingServiceProperties()
				.getBindings()
				.entrySet()
				.stream()
				.filter(b -> labelName.equalsIgnoreCase(b.getKey()) && b.getValue().getConsumer() != null)
				.map(Map.Entry::getValue)
				.findFirst();

		bindingPropertiesConsumer
				.ifPresent(bindingProperties -> concurrency = bindingProperties.getConsumer().getConcurrency());

		Optional<BindingProperties> bindingPropertiesProducer = reactorKafkaProperties.getBindingServiceProperties()
				.getBindings()
				.entrySet()
				.stream()
				.filter(b -> labelName.equalsIgnoreCase(b.getKey()) && b.getValue().getProducer() != null)
				.map(Map.Entry::getValue)
				.findFirst();

		bindingPropertiesConsumer.ifPresent(bindingProperties -> {
			consumer = new ConsumerConfiguration(
					reactorKafkaProperties.getKafkaExtendedBindingProperties().getBindings().get(labelName)
							.getConsumer(), bindingPropertiesConsumer.get(),
					reactorKafkaProperties.getBindingServiceProperties().getBindings().get(labelName).getConsumer(),
					hosts, reactorKafkaProperties.getApplicationName());
			consumer.builder();
		});


		bindingPropertiesProducer.ifPresent(bindingProperties -> {
			ProducerConfiguration p = new ProducerConfiguration(
					null, //reactiveKafkaProperties.getKafkaExtendedBindingProperties().getBindings().get(labelName).getProducer(),
					reactorKafkaProperties.getBindingServiceProperties().getBindings().get(labelName).getProducer(),
					hosts
			);
			producer = p.builder();
		});
	}

	public static class ConsumerConfiguration {

		private final KafkaConsumerProperties kafkaConsumerProperties;
		private final BindingProperties bindingProperties;
		private final ConsumerProperties consumerProperties;
		private final String hosts;
		private final String group;
		@Getter
		private List<String> notRevokedPartition;
		@Getter
		private KafkaReceiver<byte[], byte[]> receiver;

		public void builder(){
			receiver = reactiveKafkaReceiver();
		}

		public ConsumerConfiguration(
				KafkaConsumerProperties kafkaConsumerProperties, BindingProperties bindingProperties, ConsumerProperties consumerProperties, String hosts, String group){
			this.kafkaConsumerProperties = kafkaConsumerProperties;
			this.bindingProperties = bindingProperties;
			this.consumerProperties = consumerProperties;
			this.hosts = hosts;
			this.group = group;
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
					ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"
			);
		}

		private ReceiverOptions<byte[], byte[]> kafkaReceiverOptions() {
			ReceiverOptions<byte[], byte[]> options = ReceiverOptions.create(kafkaConsumerConfiguration());
			 return options.subscription(Arrays.asList(this.bindingProperties.getDestination()))
					.withKeyDeserializer(new ByteArrayDeserializer())
					.withValueDeserializer(new ByteArrayDeserializer())
					 .addAssignListener(receiverPartitions -> {
					 	notRevokedPartition = receiverPartitions.stream().map(receiverPartition -> receiverPartition.topicPartition().toString()).collect(Collectors.toList());
					 	System.out.println("");
					 })
					 .addRevokeListener(receiverPartitions -> {
						 System.out.println("");
					 });
		}

		private KafkaReceiver<byte[], byte[]> reactiveKafkaReceiver() {
			return KafkaReceiver.create(kafkaReceiverOptions());
		}

	}

	private static class ProducerConfiguration {

		private final KafkaProducerProperties kafkaProducerProperties;
		private final ProducerProperties producerProperties;
		private final String hosts;

		public KafkaSender<byte[], byte[]> builder(){
			return reactiveKafkaSender();
		}

		public ProducerConfiguration(
				KafkaProducerProperties kafkaProducerProperties, ProducerProperties producerProperties, String hosts){
			this.kafkaProducerProperties = kafkaProducerProperties;
			this.producerProperties = producerProperties;
			this.hosts = hosts;
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

}
