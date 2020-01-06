package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.reactor.config.ReactiveKafkaConfigurator;
import it.usuratonkachi.kafka.reactor.config._ReactiveStreamDispatcher;
import it.usuratonkachi.kafka.reactor.streamconfig.Streams;
import org.springframework.context.annotation.Bean;

//@Service
@Deprecated // Use ByteArray version
public class _ReactiveKafkaConfiguratorProject extends ReactiveKafkaConfigurator {

	@Bean _ReactiveStreamDispatcher<Mail> mailDispatcher(){
		return new _ReactiveStreamDispatcher<>(reactiveKafkaProperties, Streams.MAIL_CHANNEL_INPUT);
	}

	@Bean _ReactiveStreamDispatcher<Message> messageDispatcher(){
		return new _ReactiveStreamDispatcher<>(reactiveKafkaProperties, Streams.MESSAGE_CHANNEL_INPUT);
	}

	@Bean _ReactiveStreamDispatcher<Mms> mmsDispatcher(){
		return new _ReactiveStreamDispatcher<>(reactiveKafkaProperties, Streams.MMS_CHANNEL_INPUT);
	}

	@Bean _ReactiveStreamDispatcher<Sms> smsDispatcher(){
		return new _ReactiveStreamDispatcher<>(reactiveKafkaProperties, Streams.SMS_CHANNEL_INPUT);
	}

}
