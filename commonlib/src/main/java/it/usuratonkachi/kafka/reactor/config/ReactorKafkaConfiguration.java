package it.usuratonkachi.kafka.reactor.config;

import lombok.Getter;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBindingProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
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
	private ReactorConsumer consumer;
	@Getter
	private ReactorProducer producer;
	@Getter
	private Integer concurrency = 1;

	private ReactorKafkaProperties r;

	public ReactorKafkaConfiguration(ReactorKafkaProperties reactorKafkaProperties, String labelTopicName){
		this.r = reactorKafkaProperties;
		labelName = labelTopicName;
		topicName = reactorKafkaProperties.getBindingServiceProperties().getBindingDestination(labelName);

		String port = reactorKafkaProperties.getKafkaBinderConfigurationProperties().getBrokers()[reactorKafkaProperties.getKafkaBinderConfigurationProperties().getBrokers().length - 1].split(":")[1];
		String hosts = Arrays.stream(reactorKafkaProperties.getKafkaBinderConfigurationProperties().getBrokers()).map(host -> host.split(":")[0] + ":" + port).collect(
				Collectors.joining(","));

		Optional<BindingProperties> bindingPropertiesConsumer = reactorKafkaProperties.getBindingServiceProperties()
				.getBindings()
				.entrySet()
				.stream()
				.filter(b -> labelName.equalsIgnoreCase(b.getKey()) && b.getValue().getConsumer() != null)
				.map(Map.Entry::getValue)
				.findFirst();

		bindingPropertiesConsumer
				.ifPresent(bindingProperties -> concurrency = bindingProperties.getConsumer().getConcurrency());

		bindingPropertiesConsumer.ifPresent(bindingProperties -> {
			consumer = new ReactorConsumer(
					reactorKafkaProperties.getKafkaExtendedBindingProperties().getBindings().get(labelName).getConsumer(),
					bindingPropertiesConsumer.get(),
					reactorKafkaProperties.getBindingServiceProperties().getBindings().get(labelName).getConsumer(),
					hosts,
					reactorKafkaProperties.getApplicationName());
		});

		Optional.ofNullable(reactorKafkaProperties.getKafkaExtendedBindingProperties())
				.flatMap(kafkaExtendedBindingProperties -> Optional.ofNullable(kafkaExtendedBindingProperties.getBindings()))
				.ifPresent(bindings -> {
					if (bindings.containsKey(labelName))
						Optional.ofNullable(bindings.get(labelName).getConsumer())
								.ifPresent(kafkaConsumerProperties -> {
									if (consumer == null) {
										consumer = new ReactorConsumer(hosts, reactorKafkaProperties.getApplicationName(), labelName);
									}
									concurrency = Integer.valueOf(Optional.ofNullable(
											kafkaConsumerProperties.getConfiguration().get("concurrency"))
											.orElse(concurrency.toString()));
								});
				});

		Optional<BindingProperties> bindingPropertiesProducer = reactorKafkaProperties.getBindingServiceProperties()
				.getBindings()
				.entrySet()
				.stream()
				.filter(b -> labelName.equalsIgnoreCase(b.getKey()) && b.getValue().getProducer() != null)
				.map(Map.Entry::getValue)
				.findFirst();

		bindingPropertiesProducer.ifPresent(bindingProperties -> {
			KafkaProducerProperties kafkaProducerProperties = null;
			if (reactorKafkaProperties.getKafkaExtendedBindingProperties() != null
					&& reactorKafkaProperties.getKafkaExtendedBindingProperties().getBindings() != null
					&& !reactorKafkaProperties.getKafkaExtendedBindingProperties().getBindings().isEmpty()
					&& reactorKafkaProperties.getKafkaExtendedBindingProperties().getBindings().containsKey(labelTopicName)
			)
				kafkaProducerProperties = reactorKafkaProperties.getKafkaExtendedBindingProperties().getBindings().get(labelName).getProducer();
			producer = new ReactorProducer(
					kafkaProducerProperties,
					reactorKafkaProperties.getBindingServiceProperties().getBindings().get(labelName).getProducer(),
					hosts
			);
		});

		Optional.ofNullable(reactorKafkaProperties.getKafkaExtendedBindingProperties())
				.flatMap(kafkaExtendedBindingProperties -> Optional.ofNullable(kafkaExtendedBindingProperties.getBindings()))
				.ifPresent(bindings -> {
					if (bindings.containsKey(labelName))
						Optional.ofNullable(bindings.get(labelName).getProducer())
								.ifPresent(kafkaProducerProperties -> {
									if (producer == null) {
										producer = new ReactorProducer(hosts, reactorKafkaProperties.getApplicationName(), labelName);
									}
								});
				});

	}



}
