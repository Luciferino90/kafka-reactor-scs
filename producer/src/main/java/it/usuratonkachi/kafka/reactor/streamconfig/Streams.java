package it.usuratonkachi.kafka.reactor.streamconfig;

import it.usuratonkachi.kafka.dto.*;
import it.usuratonkachi.kafka.reactor.config.binder.ReactorChannel;
import it.usuratonkachi.kafka.reactor.config.binder.ReactorChannelBinder;
import org.springframework.messaging.MessageChannel;

/**
 */
@ReactorChannelBinder
public class Streams {

	public final static String MAIL_CHANNEL_OUTPUT = "mail-kafka-out";
	public final static String MESSAGE_CHANNEL_OUTPUT = "message-kafka-out";
	public final static String MMS_CHANNEL_OUTPUT = "mms-kafka-out";
	public final static String SMS_CHANNEL_OUTPUT = "sms-kafka-out";
	public final static String NOTIFICATION_CHANNEL_OUTPUT = "notification-out";

	@ReactorChannel(value = MAIL_CHANNEL_OUTPUT, messageType = Mail.class)
	MessageChannel outboundMailKafka;
	@ReactorChannel(value = MESSAGE_CHANNEL_OUTPUT, messageType = Message.class)
	MessageChannel outboundMessageKafka;
	@ReactorChannel(value = MMS_CHANNEL_OUTPUT, messageType = Mms.class)
	MessageChannel outboundMmsKafka;
	@ReactorChannel(value = SMS_CHANNEL_OUTPUT, messageType = Sms.class)
	MessageChannel outboundSmsKafka;
	@ReactorChannel(value = NOTIFICATION_CHANNEL_OUTPUT, messageType = Notification.class)
	MessageChannel outboundNotificationKafka;

}
