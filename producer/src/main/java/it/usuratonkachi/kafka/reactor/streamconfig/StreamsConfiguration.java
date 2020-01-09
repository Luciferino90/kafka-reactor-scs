package it.usuratonkachi.kafka.reactor.streamconfig;

import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Configuration;

/**
 *	Configuration for KafkaStreams.
 */
//@EnableBinding(Streams.class)
//@Configuration
public class StreamsConfiguration {

	final static String MAIL_CHANNEL_OUTPUT = "mail-kafka-out";
	final static String MESSAGE_CHANNEL_OUTPUT = "message-kafka-out";
	final static String MMS_CHANNEL_OUTPUT = "mms-kafka-out";
	final static String SMS_CHANNEL_OUTPUT = "sms-kafka-out";

}
