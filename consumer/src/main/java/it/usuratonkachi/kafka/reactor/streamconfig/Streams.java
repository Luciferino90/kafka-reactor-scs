package it.usuratonkachi.kafka.reactor.streamconfig;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.messaging.SubscribableChannel;

/**
 */
public interface Streams {

	String MAIL_CHANNEL_INPUT = "mail-kafka-in";
	String MESSAGE_CHANNEL_INPUT = "message-kafka-in";
	String MMS_CHANNEL_INPUT = "mms-kafka-in";
	String SMS_CHANNEL_INPUT = "sms-kafka-in";

	//@Input(MAIL_CHANNEL_INPUT)
	//SubscribableChannel inboundMailKafka();

}
