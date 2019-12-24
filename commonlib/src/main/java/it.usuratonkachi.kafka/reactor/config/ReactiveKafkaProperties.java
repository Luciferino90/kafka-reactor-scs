package it.usuratonkachi.kafka.reactor.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(value = { BindingServiceProperties.class })
public class ReactiveKafkaProperties {

	@Getter
	private final KafkaProperties kafkaProperties;
	@Getter
	private final BindingServiceProperties bindingServiceProperties;

}
