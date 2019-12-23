package it.usuratonkachi.kafka.spring.streamconfig;

import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Configuration;

/**
 *	Configuration for KafkaStreams.
 */
@EnableBinding(Streams.class)
@Configuration
public class StreamsConfiguration {
}
