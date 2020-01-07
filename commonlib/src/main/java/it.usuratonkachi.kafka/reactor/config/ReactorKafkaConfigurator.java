package it.usuratonkachi.kafka.reactor.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ReactorKafkaConfigurator {

	@Autowired
	protected ReactorKafkaProperties reactorKafkaProperties;

}
