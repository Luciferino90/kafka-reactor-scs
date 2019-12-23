package it.usuratonkachi.kafka.reactor.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ReactiveKafkaConfigurator {

	@Autowired
	protected ReactiveKafkaProperties reactiveKafkaProperties;

}
