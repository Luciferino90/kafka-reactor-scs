package it.usuratonkachi.kafka.reactor.streamconfig;

import it.usuratonkachi.kafka.dto.*;
import it.usuratonkachi.kafka.reactor.config.binder.ReactorChannel;
import it.usuratonkachi.kafka.reactor.config.binder.ReactorChannelBinder;
import org.springframework.messaging.MessageChannel;

/**
 */
@ReactorChannelBinder
public class Streams {

	public final static String MAIL_CHANNEL_INPUT = "mail-kafka-in";
	public final static String MESSAGE_CHANNEL_INPUT = "message-kafka-in";
	public final static String MMS_CHANNEL_INPUT = "mms-kafka-in";
	public final static String SMS_CHANNEL_INPUT = "sms-kafka-in";
	public final static String NOTIFICATION_CHANNEL_INPUT = "notification-in";

	@ReactorChannel(value = MAIL_CHANNEL_INPUT, messageType = Mail.class)
	MessageChannel inboundMailKafka;
	@ReactorChannel(value = MESSAGE_CHANNEL_INPUT, messageType = Message.class)
	MessageChannel inboundMessageKafka;
	@ReactorChannel(value = MMS_CHANNEL_INPUT, messageType = Mms.class)
	MessageChannel inboundMmsKafka;
	@ReactorChannel(value = SMS_CHANNEL_INPUT, messageType = Sms.class)
	MessageChannel inboundSmsKafka;
	@ReactorChannel(value = NOTIFICATION_CHANNEL_INPUT, messageType = Notification.class)
	MessageChannel inboundNotificationKafka;

}
