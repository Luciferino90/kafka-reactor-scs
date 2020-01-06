package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.reactor.config.ReactiveKafkaConfigurator;
import it.usuratonkachi.kafka.reactor.config.ReactiveStreamDispatcher;
import it.usuratonkachi.kafka.reactor.streamconfig.Streams;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

@Service
public class ReactiveKafkaConfiguratorProject extends ReactiveKafkaConfigurator {

	@Bean ReactiveStreamDispatcher<Mail> mailDispatcher(){
		return new ReactiveStreamDispatcher<>(Mail.class, reactiveKafkaProperties, Streams.MAIL_CHANNEL_INPUT);
	}

	@Bean ReactiveStreamDispatcher<Message> messageDispatcher(){
		return new ReactiveStreamDispatcher<>(Message.class, reactiveKafkaProperties, Streams.MESSAGE_CHANNEL_INPUT);
	}

	@Bean ReactiveStreamDispatcher<Mms> mmsDispatcher(){
		return new ReactiveStreamDispatcher<>(Mms.class, reactiveKafkaProperties, Streams.MMS_CHANNEL_INPUT);
	}

	@Bean ReactiveStreamDispatcher<Sms> smsDispatcher(){
		return new ReactiveStreamDispatcher<>(Sms.class, reactiveKafkaProperties, Streams.SMS_CHANNEL_INPUT);
	}

}
