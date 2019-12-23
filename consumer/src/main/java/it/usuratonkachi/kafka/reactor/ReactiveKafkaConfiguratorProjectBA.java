package it.usuratonkachi.kafka.reactor;

import it.usuratonkachi.kafka.dto.Mail;
import it.usuratonkachi.kafka.dto.Message;
import it.usuratonkachi.kafka.dto.Mms;
import it.usuratonkachi.kafka.dto.Sms;
import it.usuratonkachi.kafka.reactor.config.ReactiveKafkaConfigurator;
import it.usuratonkachi.kafka.reactor.config.ReactiveStreamDispatcherBA;
import it.usuratonkachi.kafka.reactor.streamconfig.Streams;
import org.springframework.context.annotation.Bean;

//@Service
public class ReactiveKafkaConfiguratorProjectBA extends ReactiveKafkaConfigurator {

	@Bean
	ReactiveStreamDispatcherBA<Mail> mailDispatcher(){
		return new ReactiveStreamDispatcherBA<>(reactiveKafkaProperties, Streams.MAIL_CHANNEL_INPUT);
	}

	@Bean
	ReactiveStreamDispatcherBA<Message> messageDispatcher(){
		return new ReactiveStreamDispatcherBA<>(reactiveKafkaProperties, Streams.MESSAGE_CHANNEL_INPUT);
	}

	@Bean
	ReactiveStreamDispatcherBA<Mms> mmsDispatcher(){
		return new ReactiveStreamDispatcherBA<>(reactiveKafkaProperties, Streams.MMS_CHANNEL_INPUT);
	}

	@Bean
	ReactiveStreamDispatcherBA<Sms> smsDispatcher(){
		return new ReactiveStreamDispatcherBA<>(reactiveKafkaProperties, Streams.SMS_CHANNEL_INPUT);
	}

}
