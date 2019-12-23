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

	@Bean
	ReactiveStreamDispatcher<Mail> mailDispatcher(){
		return new ReactiveStreamDispatcher<>(reactiveKafkaProperties, Streams.MAIL_CHANNEL_OUTPUT);
	}

	@Bean
	ReactiveStreamDispatcher<Message> messageDispatcher(){
		return new ReactiveStreamDispatcher<>(reactiveKafkaProperties, Streams.MESSAGE_CHANNEL_OUTPUT);
	}

	@Bean
	ReactiveStreamDispatcher<Mms> mmsDispatcher(){
		return new ReactiveStreamDispatcher<>(reactiveKafkaProperties, Streams.MMS_CHANNEL_OUTPUT);
	}

	@Bean
	ReactiveStreamDispatcher<Sms> smsDispatcher(){
		return new ReactiveStreamDispatcher<>(reactiveKafkaProperties, Streams.SMS_CHANNEL_OUTPUT);
	}

}
