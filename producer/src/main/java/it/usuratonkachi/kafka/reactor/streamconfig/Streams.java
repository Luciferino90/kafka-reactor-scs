package it.usuratonkachi.kafka.reactor.streamconfig;

import it.usuratonkachi.kafka.dto.*;
import it.usuratonkachi.kafka.reactor.config.binder.ReactorChannel;
import it.usuratonkachi.kafka.reactor.config.binder.ReactorChannelBinder;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

/**
 */
public interface Streams {

	String MAIL_CHANNEL_OUTPUT = "mail-kafka-out";
	String MAIL_CHANNEL_OUTPUT_NEW = "mail-kafka-out-new";
	String MESSAGE_CHANNEL_OUTPUT = "message-kafka-out";
	String MMS_CHANNEL_OUTPUT = "mms-kafka-out";
	String SMS_CHANNEL_OUTPUT = "sms-kafka-out";
	String NOTIFICATION_CHANNEL_OUTPUT = "notification-out";

	@Output(MAIL_CHANNEL_OUTPUT)
	MessageChannel mailOutbunt();
	@Output(MAIL_CHANNEL_OUTPUT_NEW)
	MessageChannel mailOutbuntNew();
	@Output(MESSAGE_CHANNEL_OUTPUT)
	MessageChannel messageOutbunt();
	@Output(MMS_CHANNEL_OUTPUT)
	MessageChannel mmsOutbunt();
	@Output(SMS_CHANNEL_OUTPUT)
	MessageChannel smsOutbunt();
	@Output(NOTIFICATION_CHANNEL_OUTPUT)
	MessageChannel notificationOutbunt();

}
