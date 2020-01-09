package it.usuratonkachi.kafka.reactor.streamconfig;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;

import static it.usuratonkachi.kafka.reactor.streamconfig.StreamsConfiguration.*;

/**
 */
@Component(value = "puppa")
public class Streams {

	@Output(MAIL_CHANNEL_OUTPUT)
	MessageChannel outboundMailKafka;

	@Output(MESSAGE_CHANNEL_OUTPUT)
	MessageChannel outboundMessageKafka;

	@Output(MMS_CHANNEL_OUTPUT)
	MessageChannel outboundMmsKafka;

	@Output(SMS_CHANNEL_OUTPUT)
	MessageChannel outboundSmsKafka;

}
