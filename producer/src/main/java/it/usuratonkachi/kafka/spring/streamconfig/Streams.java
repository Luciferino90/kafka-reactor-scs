package it.usuratonkachi.kafka.spring.streamconfig;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

/**
 */
public interface Streams {

	String MAIL_CHANNEL_OUTPUT = "mail-kafka-out";
	String MESSAGE_CHANNEL_OUTPUT = "message-kafka-out";
	String MMS_CHANNEL_OUTPUT = "mms-kafka-out";
	String SMS_CHANNEL_OUTPUT = "sms-kafka-out";

	@Output(MAIL_CHANNEL_OUTPUT)
	MessageChannel outboundMailKafka();

	@Output(MESSAGE_CHANNEL_OUTPUT)
	MessageChannel outboundMessageKafka();

	@Output(MMS_CHANNEL_OUTPUT)
	MessageChannel outboundMmsKafka();

	@Output(SMS_CHANNEL_OUTPUT)
	MessageChannel outboundSmsKafka();

}
