package it.usuratonkachi.kafka.spring.streamconfig;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.messaging.SubscribableChannel;

/**
 */
public interface Streams {

	String MAIL_CHANNEL_INPUT = "mail-kafka-in";
	String MESSAGE_CHANNEL_INPUT = "message-kafka-in";
	String MMS_CHANNEL_INPUT = "mms-kafka-in";
	String SMS_CHANNEL_INPUT = "sms-kafka-in";


	@Input(MAIL_CHANNEL_INPUT)
	SubscribableChannel inboundMailKafka();
	//@Input(MESSAGE_CHANNEL_INPUT)
	SubscribableChannel inboundMeesageKafka();
	//@Input(MMS_CHANNEL_INPUT)
	SubscribableChannel inboundMmsKafka();
	//@Input(SMS_CHANNEL_INPUT)
	SubscribableChannel inboundSmsKafka();

}
