package it.usuratonkachi.kafka.reactor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(value = { KafkaBinderConfigurationProperties.class, KafkaExtendedBindingProperties.class, KafkaProperties.class, BindingServiceProperties.class })
public class ReactorKafkaProperties {

	@Getter
	@Value("${spring.application.name}")
	protected String applicationName;
	@Getter
	private final ObjectMapper objectMapper;
	@Getter
	private final KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties; // kafka scs
	@Getter
	private final KafkaExtendedBindingProperties kafkaExtendedBindingProperties; // kafka scs
	@Getter
	private final KafkaProperties kafkaProperties; // kafka - not used
	@Getter
	private final BindingServiceProperties bindingServiceProperties; // commons scs

}
