package it.usuratonkachi.kafka.reactor.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Classe di partenza con cui configurare tutti i dispatcher che si vogliono creare, facendo così ereditare tutte
 * le properties di Spring Cloud Stream
 */
@Service
public class ReactorKafkaConfigurator {

	@Autowired
	protected ReactorKafkaProperties reactorKafkaProperties;

}
